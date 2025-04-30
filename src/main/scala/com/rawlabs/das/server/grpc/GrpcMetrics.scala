/*
 * Copyright 2025 RAW Labs S.A.
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

/**
 * Simple Kamon instrumentation for gRPC services.
 *
 * This adds two metrics:
 *   - grpc_requests_total: Counter of requests, tagged with service, method and status (OK/FAIL).
 *   - grpc_requests_latency_millis: Histogram of request latencies, tagged with service, method and status (OK/FAIL).
 *
 * Mix this trait in and wrap each RPC entry-point with:
 * {{{
 *   withMetrics("methodName") { ... }
 * }}}
 */
trait GrpcMetrics {

  /** Name of the concrete gRPC service (e.g. "TableService"). */
  protected def serviceName: String

  private lazy val grpcRequestCounter = Kamon
    .counter("grpc_requests_total")
    .withTag("grpc_service", serviceName)
  private lazy val requestTimeHistogram = Kamon
    .histogram("grpc_requests_latency_millis", MeasurementUnit.time.milliseconds)
    .withTag("grpc_service", serviceName)

  /**
   * Wrap a block of code (<i>typically the body of a gRPC method</i>) with automatic metric collection.
   *
   * @param operationName Logical name of the RPC method, used for tagging.
   * @param f Code block to execute.
   * @tparam T Return type of the code block.
   * @return The value produced by {@code f} .
   */
  def withMetrics[T](operationName: String)(f: => T): T = {
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
