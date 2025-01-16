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

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.stream.Materializer
import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.catalog.{CacheCatalog, CacheDefinition, CacheEntry, SqliteCacheCatalog}
import com.rawlabs.das.server.cache.iterator.{CacheSelector, QualEvaluator}
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.grpc.{HealthCheckServiceGrpcImpl, RegistrationServiceGrpcImpl, TableServiceGrpcImpl, ThrowableHandlingInterceptor}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.das.server.webui.{DASWebUIServer, DebugAppService}
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.services.{HealthCheckServiceGrpc, RegistrationServiceGrpc, TablesServiceGrpc}
import com.rawlabs.protocol.das.v1.tables.Row
import io.grpc.{Server, ServerBuilder}

import java.io.File
import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters.JavaDurationOps

class DASServer(cacheManager: ActorRef[CacheManager.Command[Row]])(
    implicit settings: DASSettings,
    implicit val ec: ExecutionContext,
    implicit val materializer: Materializer,
    implicit val scheduler: Scheduler) {

  private[this] var server: Server = _

  private val dasSdkManager = new DASSdkManager

  private val healthCheckService = HealthCheckServiceGrpc
    .bindService(new HealthCheckServiceGrpcImpl)
  private val registrationService = RegistrationServiceGrpc
    .bindService(new RegistrationServiceGrpcImpl(dasSdkManager))

  private val tablesService = {
    val defaultCacheAge = settings.getDuration("das.server.max-cache-age").toScala
    val maxChunkSize = settings.getInt("das.server.max-chunk-size")
    TablesServiceGrpc
      .bindService(new TableServiceGrpcImpl(dasSdkManager, cacheManager, maxChunkSize, defaultCacheAge))
  }
//  private val functionsService - FunctionsServiceGrpc.bindService(new FunctionsServiceGrpcImpl(dasSdkManager))

  def start(port: Int): Unit =
    server = ServerBuilder
      .forPort(port)
      .addService(healthCheckService)
      .addService(registrationService)
      .addService(tablesService)
//      .addService(functionsService)
      .intercept(new ThrowableHandlingInterceptor)
      .build()
      .start()

  def stop(): Unit = if (server != null) server.shutdown()

  def blockUntilShutdown(): Unit = if (server != null) server.awaitTermination()

}

object DASServer {

  def main(args: Array[String]): Unit = {
    implicit val settings = new DASSettings()

    // 1) Create a typed ActorSystem
    implicit val system: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, "das-server")

    // 2) Create manager
    val catalog: CacheCatalog = {
      val sqliteFile = settings.getString("das.cache.sqlite-catalog-file")
      new SqliteCacheCatalog(f"jdbc:sqlite:$sqliteFile")
    }
    val baseDir: File = {
      val cacheData = settings.getString("das.cache.data-dir")
      new File(cacheData)
    }
    val maxEntries = settings.getInt("das.cache.max-entries")
    val batchSize = settings.getInt("das.cache.batch-size")
    val gracePeriod = settings.getDuration("das.cache.grace-period").toScala
    val producerInterval = settings.getDuration("das.cache.producer-interval").toScala
    val port = settings.getInt("das.server.port")
    val monitoringPort = settings.getInt("das.server.monitoring-port")

    val chooseBestEntry: (CacheDefinition, Seq[CacheEntry]) => Option[(CacheEntry, Seq[Qual])] = {
      case (definition, possible) => CacheSelector.pickBestCache(possible, definition.quals, definition.columns)
    }
    val satisfiesAllQuals: (Row, Seq[Qual]) => Boolean = { case (row, quals) =>
      QualEvaluator.satisfiesAllQuals(row, quals)
    }

    val cacheManager =
      system.systemActorOf(
        CacheManager[Row](
          catalog,
          baseDir,
          maxEntries,
          batchSize,
          gracePeriod,
          producerInterval,
          chooseBestEntry,
          satisfiesAllQuals),
        "cacheManager")

    // 3) Derive or import the required implicits
    //    - Typically from the typed ActorSystem
    implicit val ec: ExecutionContext = system.executionContext
    implicit val mat: Materializer = Materializer(system)
    implicit val scheduler: Scheduler = system.scheduler

    // 4) Start the grpc server
    val dasServer = new DASServer(cacheManager)
    dasServer.start(port)

    // 5) Start the new server-side HTML UI
    val debugService = new DebugAppService(cacheManager)
    DASWebUIServer.startHttpInterface("0.0.0.0", monitoringPort, debugService)

    dasServer.blockUntilShutdown()
  }

}
