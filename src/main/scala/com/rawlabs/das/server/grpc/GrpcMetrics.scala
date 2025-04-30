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

package com.rawlabs.das.server.grpc

import scala.util.control.NonFatal

import kamon.Kamon
import kamon.metric.MeasurementUnit

trait GrpcMetrics {
  protected def serviceName: String

  private val grpcRequestCounter = Kamon
    .counter("grpc_requests_total")
    .withTag("grpc_service", serviceName)
  private val requestTimeHistogram = Kamon
    .histogram("grpc_requests_latency_millis", MeasurementUnit.time.milliseconds)
    .withTag("grpc_service", serviceName)

  protected def withMetrics[T](operationName: String)(f: => T): T = {
    val start = System.currentTimeMillis()

    try {
      val v = f
      grpcRequestCounter
        .withTag("grpc_method", operationName)
        .withTag("grpc_status", "OK")
        .increment()
      requestTimeHistogram
        .withTag("grpc_method", operationName)
        .withTag("grpc_status", "OK")
        .record(System.currentTimeMillis() - start)
      v
    } catch {
      case NonFatal(t) =>
        grpcRequestCounter
          .withTag("grpc_method", operationName)
          .withTag("grpc_status", "FAIL")
          .increment()
        requestTimeHistogram
          .withTag("grpc_method", operationName)
          .withTag("grpc_status", "FAIL")
          .record(System.currentTimeMillis() - start)
        throw t
    }
  }
}
