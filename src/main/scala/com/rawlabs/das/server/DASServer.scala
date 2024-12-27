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

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.grpc.{
  HealthCheckServiceGrpcImpl,
  RegistrationServiceGrpcImpl,
  TableServiceGrpcImpl,
  ThrowableHandlingInterceptor
}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.services.HealthCheckServiceGrpc
import com.rawlabs.protocol.das.v1.services.RegistrationServiceGrpc
import com.rawlabs.protocol.das.v1.services.TablesServiceGrpc

import io.grpc.Server
import io.grpc.ServerBuilder

class DASServer(implicit settings: DASSettings) {

  private[this] var server: Server = _

  private val dasSdkManager = new DASSdkManager
//  private val cache = new DASResultCache()

  private val healthCheckService = HealthCheckServiceGrpc
    .bindService(new HealthCheckServiceGrpcImpl)
  private val registrationService = RegistrationServiceGrpc
    .bindService(new RegistrationServiceGrpcImpl(dasSdkManager))
  private val tablesService = TablesServiceGrpc
    .bindService(new TableServiceGrpcImpl(dasSdkManager))
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
    val dasServer = new DASServer
    dasServer.start(50051)
    dasServer.blockUntilShutdown()
  }

}
