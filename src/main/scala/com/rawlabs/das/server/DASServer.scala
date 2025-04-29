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

import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters.JavaDurationOps

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Scheduler}
import org.apache.pekko.stream.Materializer

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.QueryResultCache
import com.rawlabs.das.server.grpc.{
  FunctionServiceGrpcImpl,
  HealthCheckServiceGrpcImpl,
  RegistrationServiceGrpcImpl,
  TableServiceGrpcImpl,
  ThrowableHandlingInterceptor
}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.das.server.webui.{DASWebUIServer, DebugAppService}
import com.rawlabs.protocol.das.v1.services.{
  FunctionsServiceGrpc,
  HealthCheckServiceGrpc,
  RegistrationServiceGrpc,
  TablesServiceGrpc
}

import io.grpc.{Server, ServerBuilder}
import kamon.Kamon

class DASServer(resultCache: QueryResultCache)(
    implicit settings: DASSettings,
    implicit val ec: ExecutionContext,
    implicit val materializer: Materializer,
    implicit val scheduler: Scheduler,
    implicit val system: ActorSystem[Nothing]) {

  private[this] var server: Server = _

  private val dasSdkManager = new DASSdkManager

  private val healthCheckService = HealthCheckServiceGrpc
    .bindService(new HealthCheckServiceGrpcImpl)
  private val registrationService = RegistrationServiceGrpc
    .bindService(new RegistrationServiceGrpcImpl(dasSdkManager))

  private val tablesService = {
    val batchLatency = settings.getDuration("das.server.batch-latency").toScala
    TablesServiceGrpc
      .bindService(new TableServiceGrpcImpl(dasSdkManager, resultCache, batchLatency))
  }
  private val functionsService = FunctionsServiceGrpc.bindService(new FunctionServiceGrpcImpl(dasSdkManager))

  def start(port: Int): Unit =
    server = ServerBuilder
      .forPort(port)
      .addService(healthCheckService)
      .addService(registrationService)
      .addService(tablesService)
      .addService(functionsService)
      .intercept(new ThrowableHandlingInterceptor)
      .build()
      .start()

  def stop(): Unit = if (server != null) server.shutdown()

  def blockUntilShutdown(): Unit = if (server != null) server.awaitTermination()

}

object DASServer {

  def main(args: Array[String]): Unit = {
    // 1) Load settings
    implicit val settings: DASSettings = new DASSettings()
    Kamon.init()

    // 2) Start the actor system
    implicit val system: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, "das-server")
    implicit val ec: ExecutionContext = system.executionContext
    implicit val mat: Materializer = Materializer(system)
    implicit val scheduler: Scheduler = system.scheduler

    // 3) Start the results cache
    val resultCache =
      new QueryResultCache(
        maxEntries = settings.getInt("das.cache.max-entries"),
        maxChunksPerEntry = settings.getInt("das.cache.max-chunks-per-entry"))

    // 4) Start the web monitoring UI
    settings.getIntOpt("das.server.monitoring-port").ifPresent { monitoringPort =>
      val debugService = new DebugAppService(resultCache)
      DASWebUIServer.startHttpInterface("0.0.0.0", monitoringPort, debugService)
    }

    // 5) Start the grpc server
    val port = settings.getInt("das.server.port")
    val dasServer = new DASServer(resultCache)
    try {
      dasServer.start(port)
      // Block until shutdown
      dasServer.blockUntilShutdown()
    } finally {
      Kamon.stop()
    }
  }

}
