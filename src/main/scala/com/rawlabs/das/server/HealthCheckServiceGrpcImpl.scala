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

import com.rawlabs.protocol.das.services.{HealthCheckRequest, HealthCheckResponse, HealthCheckServiceGrpc}
import com.typesafe.scalalogging.StrictLogging
import io.grpc.stub.StreamObserver

/**
 * gRPC service implementation for health checks.
 */
class HealthCheckServiceGrpcImpl extends HealthCheckServiceGrpc.HealthCheckServiceImplBase with StrictLogging {

  /**
   * Check the health of the service.
   *
   * @param request the health check request
   * @return the health check response
   */
  override def check(request: HealthCheckRequest, responseObserver: StreamObserver[HealthCheckResponse]): Unit = {
    responseObserver.onNext(
      HealthCheckResponse
        .newBuilder()
        .setStatus(HealthCheckResponse.ServingStatus.SERVING)
        .setDescription(s"Version: ${BuildInfo.version}")
        .build()
    )
    responseObserver.onCompleted()
  }

}
