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
