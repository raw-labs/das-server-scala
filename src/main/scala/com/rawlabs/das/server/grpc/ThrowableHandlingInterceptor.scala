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

import com.typesafe.scalalogging.StrictLogging

import io.grpc._

class ThrowableHandlingInterceptor extends ServerInterceptor with StrictLogging {

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {

    val serverCall =
      new ForwardingServerCall.SimpleForwardingServerCall[ReqT, RespT](call) {
        override def close(status: Status, trailers: Metadata): Unit =
          // Convert any throwable to a gRPC status
          if (status.getCause != null) {
            val newStatus = status.withDescription(status.getCause.getMessage).withCause(status.getCause)
            super.close(newStatus, trailers)
          } else super.close(status, trailers)
      }

    val listener = next.startCall(serverCall, headers)

    new ForwardingServerCallListener.SimpleForwardingServerCallListener[ReqT](listener) {
      override def onHalfClose(): Unit =
        try super.onHalfClose()
        catch {
          case t: Throwable =>
            logger.debug(s"Throwable caught in interceptor", t)
            // Close the call with an error status
            serverCall.close(Status.INTERNAL.withDescription(t.getMessage).withCause(t), new Metadata())
        }
    }
  }
}
