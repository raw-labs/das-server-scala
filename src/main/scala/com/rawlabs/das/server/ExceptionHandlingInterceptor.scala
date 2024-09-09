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

import com.typesafe.scalalogging.StrictLogging
import io.grpc._

class ExceptionHandlingInterceptor extends ServerInterceptor with StrictLogging {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]
  ): ServerCall.Listener[ReqT] = {

    val serverCall = new ForwardingServerCall.SimpleForwardingServerCall[ReqT, RespT](call) {
      override def close(status: Status, trailers: Metadata): Unit = {
        // Convert any exception to a gRPC status
        if (status.getCause != null) {
          val newStatus = status.withDescription(status.getCause.getMessage).withCause(status.getCause)
          super.close(newStatus, trailers)
        } else {
          super.close(status, trailers)
        }
      }
    }

    val listener = next.startCall(serverCall, headers)

    new ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](listener) {
      override def onHalfClose(): Unit = {
        try {
          super.onHalfClose()
        } catch {
          case ex: Exception =>
            logger.debug(s"Exception caught in interceptor", ex)
            // Close the call with an error status
            serverCall.close(Status.INTERNAL.withDescription(ex.getMessage).withCause(ex), new Metadata())
        }
      }
    }
  }
}
