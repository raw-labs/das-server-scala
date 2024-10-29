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

import com.rawlabs.protocol.das.services.{
  HealthCheckServiceGrpc,
  QueryServiceGrpc,
  RegistrationServiceGrpc,
  TablesServiceGrpc
}
import com.rawlabs.utils.core.RawSettings
import io.grpc.{Server, ServerBuilder}

class DASServer(implicit settings: RawSettings) {

  private[this] var server: Server = _

  private val dasSdkManager = new DASSdkManager
  private val cache = new DASResultCache()

  private val healthCheckService = HealthCheckServiceGrpc.bindService(new HealthCheckServiceGrpcImpl)
  private val registrationService = RegistrationServiceGrpc.bindService(new RegistrationServiceGrpcImpl(dasSdkManager))
  private val tablesService = TablesServiceGrpc.bindService(new TablesServiceGrpcImpl(dasSdkManager, cache))
  private val queryService = QueryServiceGrpc.bindService(new QueryServiceGrpcImpl(dasSdkManager, cache))

  def start(port: Int): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(healthCheckService)
      .addService(registrationService)
      .addService(tablesService)
      .addService(queryService)
      .intercept(new ThrowableHandlingInterceptor)
      .build()
      .start()
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

}
