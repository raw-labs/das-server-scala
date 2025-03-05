/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.server.manager

import java.util.ServiceLoader

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.rawlabs.das.sdk._
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.services.RegisterResponse
import com.typesafe.scalalogging.StrictLogging

object DASSdkManager {
  private val BUILTIN_DAS = "raw.das.server.builtin"
}

// In memory spec of a DAS configuration. Used to index running DASes.
private case class DASConfig(dasType: String, options: Map[String, String])

/**
 * Manages the lifecycle of Data Access Services (DAS) in the server.
 *
 * @param settings Configuration settings used by the manager.
 */
class DASSdkManager(implicit settings: DASSettings) extends StrictLogging {

  import DASSdkManager._

  /** Dynamically load all available DAS SDK builders. */
  private val dasSdkLoader = ServiceLoader.load(classOf[DASSdkBuilder]).asScala
  dasSdkLoader.foreach(builder => logger.debug(s"Found DAS SDK builder: ${builder.getDasType}"))

  /**
   * Contains the config for each registered DAS ID. We store the config so that we can either retrieve an existing
   * instance from `dasSdkCache` or create a new one if needed.
   */
  private val dasSdkConfigCache = mutable.HashMap[DASId, DASConfig]()
  private val dasSdkConfigCacheLock = new Object

  /**
   * Cache for DAS instances. The key is the DASConfig, the value is the fully-initialized `DASSdk` instance. On
   * removal, we'll call `close()` on the DAS.
   */
  private val dasSdkCache = CacheBuilder
    .newBuilder()
    .removalListener { (notification: RemovalNotification[DASConfig, DASSdk]) =>
      val config = notification.getKey
      val sdk = notification.getValue
      logger.debug(s"Removing DAS SDK for type: ${config.dasType}")

      // Attempt to close the SDK if it's not null
      if (sdk != null) {
        try {
          sdk.close()
          logger.debug(s"Successfully closed DAS SDK for type: ${config.dasType}")
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error closing DAS SDK for type: ${config.dasType}", e)
        }
      }
    }
    .build(new CacheLoader[DASConfig, DASSdk] {
      override def load(dasConfig: DASConfig): DASSdk = {
        logger.debug(s"Loading DAS SDK for type: ${dasConfig.dasType}")
        logger.trace(s"DAS Options: ${dasConfig.options}")

        val dasType = dasConfig.dasType
        val builder = dasSdkLoader
          .find(_.getDasType == dasType)
          .getOrElse {
            throw new IllegalArgumentException(s"DAS type '$dasType' not supported")
          }

        // Build the SDK instance
        val das = builder.build(dasConfig.options.asJava, settings)
        // Call init to set up resources
        das.init()
        logger.debug(s"DAS SDK for type: $dasType initialized.")
        das
      }
    })

  // At startup, read any available DAS configurations from the local config file and register them.
  registerDASFromConfig()

  /**
   * Registers a new DAS instance with the specified type and options.
   *
   * @param dasType The type of the DAS to register.
   * @param options A map of options for configuring the DAS.
   * @param maybeDasId An optional DAS ID; if not provided a new one will be generated.
   * @return The registered DAS ID (wrapped in a RegisterResponse).
   */
  def registerDAS(dasType: String, options: Map[String, String], maybeDasId: Option[DASId] = None): RegisterResponse = {
    // Use the provided DAS ID or create a new one.
    val dasId = maybeDasId.getOrElse(DASId.newBuilder().setId(java.util.UUID.randomUUID().toString).build())
    // Construct the config for this DAS
    val config = DASConfig(dasType, stripOptions(options))

    dasSdkConfigCacheLock.synchronized {
      dasSdkConfigCache.get(dasId) match {
        case Some(existingConfig) =>
          if (existingConfig != config) {
            logger.error(s"DAS with ID $dasId is already registered with a different configuration")
            throw new IllegalArgumentException(s"DAS with id $dasId already registered")
          }
        // If the existing config is the same, we do nothing.
        case None =>
          dasSdkConfigCache.put(dasId, config)
      }
    }

    // Attempt to create or fetch from cache
    try {
      dasSdkCache.get(config) // This will block until the new DAS is loaded if not already
      RegisterResponse.newBuilder().setId(dasId).build()
    } catch {
      case e: UncheckedExecutionException =>
        // Guava wraps exceptions in UncheckedExecutionException. Strip and rethrow the cause.
        logger.error(s"Failed to create DAS for type: $dasType with id: $dasId", e.getCause)
        // Remove the broken config since we failed to build the DAS
        dasSdkConfigCacheLock.synchronized {
          dasSdkConfigCache.remove(dasId)
        }
        throw e.getCause
    }
  }

  /**
   * Unregisters (and closes) a DAS instance based on the provided DAS ID.
   *
   * @param dasId The DAS ID to unregister.
   */
  def unregisterDAS(dasId: DASId): Unit = {
    dasSdkConfigCacheLock.synchronized {
      dasSdkConfigCache.get(dasId) match {
        case Some(config) =>
          logger.debug(s"Unregistering DAS with ID: $dasId")
          dasSdkConfigCache.remove(dasId)
          logger.debug(s"DAS unregistered successfully with ID: $dasId")

          // If no other DAS references this config, invalidate it to trigger a close
          if (!dasSdkConfigCache.values.exists(_ == config)) {
            dasSdkCache.invalidate(config)
            logger.debug(s"DAS cache invalidated for config of type ${config.dasType}")
          }

        case None =>
          logger.warn(s"Tried to unregister DAS with ID: $dasId, but it was not found.")
      }
    }
  }

  /**
   * Retrieves a DAS instance by its ID. If the DAS is not already registered, it will attempt to fetch it from a remote
   * source.
   *
   * @param dasId The DAS ID to retrieve.
   * @return An optional DAS instance.
   */
  def getDAS(dasId: DASId): Option[DASSdk] = {
    val maybeConfig = dasSdkConfigCacheLock.synchronized {
      dasSdkConfigCache.get(dasId).orElse {
        // Try to fetch from a remote registry, if applicable.
        val remote = getDASFromRemote(dasId)
        remote.foreach(cfg => dasSdkConfigCache.put(dasId, cfg))
        remote
      }
    }
    maybeConfig.map(cfg => dasSdkCache.get(cfg))
  }

  /**
   * Closes all DAS instances currently managed by this manager. This will invalidate all entries in the Guava cache,
   * triggering the removalListener and hence calling `close()` on each DAS.
   */
  def closeAll(): Unit = {
    logger.info("Shutting down all DAS instances in manager.")
    // Invalidate all entries, causing the removal listener to fire for each.
    dasSdkCache.invalidateAll()
    // Force synchronous clean-up of cache entries (and thus calls close()).
    dasSdkCache.cleanUp()
    logger.info("All DAS instances have been shut down.")
  }

  /**
   * Reads DAS configurations from the local config file.
   *
   * The config settings are expected to be structured as:
   * {{{
   * raw.das.server.builtin {
   *   someId {
   *     type = <dasType>
   *     options {
   *       <key1> = <value1>
   *       <key2> = <value2>
   *       ...
   *     }
   *   }
   * }
   * }}}
   *
   * @return A map of DAS configurations with ID as the key and a tuple of (dasType, options).
   */
  private def readDASFromConfig(): Map[String, (String, Map[String, String])] = {
    val ids = mutable.Map[String, (String, Map[String, String])]()

    dasSdkConfigCacheLock.synchronized {
      settings.getConfigSubTree(BUILTIN_DAS).toScala match {
        case None =>
          logger.warn("No DAS configuration found in the local config file")
        case Some(configSubTree) =>
          configSubTree.asScala.foreach { entry =>
            val id = entry.getKey
            val dasType = settings.getString(s"$BUILTIN_DAS.$id.type")
            val options = settings
              .getConfigSubTree(s"$BUILTIN_DAS.$id.options")
              .get
              .asScala
              .map { kv => kv.getKey -> kv.getValue.unwrapped().toString }
              .toMap

            ids.put(id, (dasType, options))
            logger.debug(s"Read DAS configuration for ID: $id, Type: $dasType")
          }
      }
    }

    ids.toMap
  }

  /**
   * Registers DAS instances based on the configurations found in the local config file.
   */
  private def registerDASFromConfig(): Unit = {
    logger.debug("Registering DAS from local config file.")
    readDASFromConfig().foreach { case (id, (dasType, options)) =>
      val dasId = DASId.newBuilder().setId(id).build()
      registerDAS(dasType, options, Some(dasId))
      logger.debug(s"Registered DAS from config with ID: $id")
    }
  }

  /**
   * Retrieves a DAS instance configuration from a remote source. (Not implemented.)
   *
   * @param dasId The DAS ID to retrieve.
   * @return An optional DASConfig for the given DASId.
   */
  private def getDASFromRemote(dasId: DASId): Option[DASConfig] = None

  /**
   * Removes any internal "das_*" options from the map so that they don't affect caching.
   *
   * @param options The options sent by the client.
   * @return Filtered options that exclude "das_*" keys.
   */
  private def stripOptions(options: Map[String, String]): Map[String, String] =
    options.view.filterKeys(!_.startsWith("das_")).toMap
}
