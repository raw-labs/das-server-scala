/**
 * Copyright 2024 RAW Labs S.A.
 * All rights reserved.
 *
 * This source code is the property of RAW Labs S.A. It contains
 * proprietary and confidential information that is protected by applicable
 * intellectual property and other laws. Unauthorized use, reproduction,
 * or distribution of this code, or any portion of it, may result in severe
 * civil and criminal penalties and will be prosecuted to the maximum
 * extent possible under the law.
 */

package com.rawlabs.das.server

import com.rawlabs.protocol.das.services.{RegistrationServiceGrpc, TablesServiceGrpc}
import com.rawlabs.utils.core.RawSettings
import io.grpc.{Server, ServerBuilder}

class DASServer(implicit settings: RawSettings) {

  private[this] var server: Server = _

  private val dasSdkManager = new DASSdkManager
  private val cache = new Cache()

  private val registrationService = RegistrationServiceGrpc.bindService(new RegistrationServiceGrpcImpl(dasSdkManager))
  private val tablesService = TablesServiceGrpc.bindService(new TableServiceGrpcImpl(dasSdkManager, cache))

  def start(port: Int): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(registrationService)
      .addService(tablesService)
      .intercept(new ExceptionHandlingInterceptor)
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
