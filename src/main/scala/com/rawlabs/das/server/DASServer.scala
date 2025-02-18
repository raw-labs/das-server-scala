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

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.QueryResultCache
import com.rawlabs.das.server.grpc.{
  HealthCheckServiceGrpcImpl,
  RegistrationServiceGrpcImpl,
  TableServiceGrpcImpl,
  ThrowableHandlingInterceptor
}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.das.server.webui.{DASWebUIServer, DebugAppService}
import com.rawlabs.protocol.das.v1.services.{HealthCheckServiceGrpc, RegistrationServiceGrpc, TablesServiceGrpc}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.stream.Materializer
import io.grpc.{Server, ServerBuilder}

class DASServer()(
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

  private val resultCache =
    new QueryResultCache(
      maxEntries = settings.getInt("das.cache.max-entries"),
      maxChunksPerEntry = settings.getInt("das.cache.max-chunks-per-entry"))

  private val tablesService = {
    val batchLatency = settings.getDuration("das.server.batch-latency").toScala
    TablesServiceGrpc
      .bindService(new TableServiceGrpcImpl(dasSdkManager, resultCache, batchLatency))
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
    implicit val settings: DASSettings = new DASSettings()

    // 1) Create a typed ActorSystem
    implicit val system: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, "das-server")

    val port = settings.getInt("das.server.port")
    val monitoringPort = settings.getInt("das.server.monitoring-port")

    implicit val ec: ExecutionContext = system.executionContext
    implicit val mat: Materializer = Materializer(system)
    implicit val scheduler: Scheduler = system.scheduler

    // 4) Start the grpc server
    val dasServer = new DASServer()
    dasServer.start(port)

    // 5) Start the new server-side HTML UI
    val debugService = new DebugAppService()
    DASWebUIServer.startHttpInterface("0.0.0.0", monitoringPort, debugService)

    dasServer.blockUntilShutdown()
  }

}
