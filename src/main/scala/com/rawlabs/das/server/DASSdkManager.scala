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

package com.rawlabs.das.server

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalNotification}
import com.rawlabs.das.sdk.{DASSdk, DASSdkBuilder}
import com.rawlabs.protocol.das.DASId
import com.rawlabs.utils.core.RawSettings
import com.typesafe.scalalogging.StrictLogging

import java.util.ServiceLoader
import scala.collection.JavaConverters._
import scala.collection.mutable

object DASSdkManager {
  private val BUILTIN_DAS = "raw.das.server.builtin"
}

// TODO (msb): Remove if NOT USED since M hours AND/OR does not exist in creds if it came from creds

// In memory spec of a DAS configuration. Used to index running DASes.
private case class DASConfig(dasType: String, options: Map[String, String])

/**
 * Manages the lifecycle of Data Access Services (DAS) in the server.
 *
 * @param settings Configuration settings used by the manager.
 */
class DASSdkManager(implicit settings: RawSettings) extends StrictLogging {

  import DASSdkManager._

  private val dasSdkLoader = ServiceLoader.load(classOf[DASSdkBuilder]).asScala

  private val dasSdkConfigCache = mutable.HashMap[DASId, DASConfig]()
  private val dasSdkConfigCacheLock = new Object
  private val dasSdkCache = CacheBuilder
    .newBuilder()
    .removalListener((notification: RemovalNotification[DASConfig, DASSdk]) => {
      logger.debug(s"Removing DAS SDK for type: ${notification.getKey.dasType}")
      // That's where a DAS instance should be closed (e.g. close connections, etc.)
    })
    .build(new CacheLoader[DASConfig, DASSdk] {
      override def load(dasConfig: DASConfig): DASSdk = {
        logger.debug(s"Loading DAS SDK for type: ${dasConfig.dasType}")
        logger.trace(s"DAS Options: ${dasConfig.options}")
        val dasType = dasConfig.dasType
        dasSdkLoader
          .find(_.dasType == dasType)
          .getOrElse {
            logger.error(s"DAS type '$dasType' not supported.")
            throw new IllegalArgumentException(s"DAS type '$dasType' not supported")
          }
          .build(dasConfig.options)
      }
    })

  // At startup, read any available DAS configurations from the local config file and register them.
  registerDASFromConfig()

  /**
   * Registers a new DAS instance with the specified type and options.
   *
   * @param dasType    The type of the DAS to register.
   * @param options    A map of options for configuring the DAS.
   * @param maybeDasId An optional DAS ID, if not provided a new one will be generated.
   * @return The registered DAS ID.
   */
  def registerDAS(dasType: String, options: Map[String, String], maybeDasId: Option[DASId] = None): DASId = {
    // Start from the provided DAS ID, or create a new one.
    val dasId = maybeDasId.getOrElse(DASId.newBuilder().setId(java.util.UUID.randomUUID().toString).build())
    // Then make sure that the DAS is not already registered with a different config.
    val config = DASConfig(dasType, stripOptions(options))
    dasSdkConfigCacheLock.synchronized {
      dasSdkConfigCache.get(dasId) match {
        case Some(registeredConfig) => if (registeredConfig != config) {
            logger.error(
              s"DAS with ID $dasId is already registered with a different configuration"
            )
            throw new IllegalArgumentException(s"DAS with id $dasId already registered")
          }
        case None => dasSdkConfigCache.put(dasId, config)
      }
    }
    // Everything is fine at dasId/config level. Create (or use previously cached instance) an SDK with the config.
    dasSdkCache.get(config) // If the config didn't exist, that blocks until the new DAS is loaded
    dasId
  }

  /**
   * Unregisters an existing DAS instance based on the provided DAS ID.
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
          if (!dasSdkConfigCache.values.exists(_ == config)) {
            // It was the last DAS with this config, so invalidate the cache
            dasSdkCache.invalidate(config)
          }
        case None => {
          logger.warn(s"Tried to unregister DAS with ID: $dasId, but it was not found.")
        }
      }
    }
  }

  /**
   * Retrieves a DAS instance by its ID. If the DAS is not already registered, it will attempt to fetch it remotely.
   *
   * @param dasId The DAS ID to retrieve.
   * @return The DAS instance.
   */
  def getDAS(dasId: DASId): DASSdk = {
    // Pick the known config
    val config = dasSdkConfigCacheLock.synchronized {
      dasSdkConfigCache.getOrElseUpdate(
        dasId,
        getDASFromRemote(dasId).getOrElse(throw new IllegalArgumentException(s"DAS not found: $dasId"))
      )
    }
    // Get the matching DAS from the cache
    dasSdkCache.get(DASConfig(config.dasType, config.options))
  }

  /**
   * Reads DAS configurations from the local config file.
   *
   * The config settings should be structured as:
   * {{{
   * raw.server.das.builtin {
   *   id1 {
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
   * @return A map of DAS configurations with ID as the key and a tuple of DAS type and options as the value.
   */
  private def readDASFromConfig(): Map[String, (String, Map[String, String])] = {
    val ids = mutable.Map[String, (String, Map[String, String])]()
    dasSdkConfigCacheLock.synchronized {
      try {
        settings.config.getConfig(BUILTIN_DAS).root().entrySet().asScala.foreach { entry =>
          val id = entry.getKey
          val dasType = settings.config.getString(s"$BUILTIN_DAS.$id.type")
          val options = settings.config
            .getConfig(s"$BUILTIN_DAS.$id.options")
            .entrySet()
            .asScala
            .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
            .toMap
          ids.put(id, (dasType, options))
          logger.debug(s"Read DAS configuration for ID: $id, Type: $dasType")
        }
      } catch {
        case _: com.typesafe.config.ConfigException.Missing =>
          // Ignore missing config
          logger.warn("No DAS configuration found in the local config file")
      }
    }
    ids.toMap
  }

  /**
   * Registers DAS instances based on the configurations found in the local config file.
   */
  private def registerDASFromConfig(): Unit = {
    logger.debug("Registering DAS from local config file.")
    readDASFromConfig().foreach {
      case (id, (dasType, options)) =>
        val dasId = DASId.newBuilder().setId(id).build()
        registerDAS(dasType, options, Some(dasId))
        logger.debug(s"Registered DAS from config with ID: $id")
    }
  }

  /**
   * Retrieves a DAS instance from a remote source. (Not implemented yet)
   *
   * @param dasId The DAS ID to retrieve.
   * @return The DAS instance.
   */
  private def getDASFromRemote(dasId: DASId): Option[DASConfig] = {
    None
  }

  /**
   * Strips the DAS-specific options (das_*) from the provided options map. They aren't needed
   * for the SDK instance creation and shouldn't be used as part of the cache key.
   * @param options The options sent by the client.
   * @return The same options with the DAS-specific entries removed.
   */
  private def stripOptions(options: Map[String, String]): Map[String, String] = {
    options.filterKeys(!_.startsWith("das_"))
  }
}
