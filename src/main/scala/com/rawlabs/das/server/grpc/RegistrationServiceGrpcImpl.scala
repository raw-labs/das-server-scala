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

import scala.jdk.CollectionConverters._

import com.rawlabs.das.sdk.{
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException,
  DASSdkUnsupportedException
}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.services._
import com.typesafe.scalalogging.StrictLogging

import io.grpc.Status
import io.grpc.stub.StreamObserver

/**
 * gRPC service implementation for registering and unregistering DAS (Data Access Service) instances.
 *
 * @param dasSdkManager Manages the lifecycle of DAS instances.
 */
class RegistrationServiceGrpcImpl(dasSdkManager: DASSdkManager)
    extends RegistrationServiceGrpc.RegistrationServiceImplBase
    with StrictLogging {

  /**
   * Registers a new DAS instance based on the provided definition.
   *
   * @param request The request containing the DAS definition.
   * @param responseObserver The observer to send responses.
   */
  override def register(request: RegisterRequest, responseObserver: StreamObserver[RegisterResponse]): Unit = {
    logger.debug(s"Registering DAS with type: ${request.getDefinition.getType}")
    try {
      val dasId = dasSdkManager.registerDAS(
        request.getDefinition.getType,
        request.getDefinition.getOptionsMap.asScala.toMap,
        maybeDasId = if (request.hasId) Some(request.getId) else None)
      responseObserver.onNext(dasId)
      responseObserver.onCompleted()
      logger.debug(s"DAS registered successfully with ID: $dasId")
    } catch {
      case ex: DASSdkInvalidArgumentException =>
        logger.error("DASSdk invalid argument error", ex)
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ex.getMessage).asRuntimeException())
      case ex: DASSdkPermissionDeniedException =>
        logger.error("DASSdk permission denied error", ex)
        responseObserver.onError(Status.PERMISSION_DENIED.withDescription(ex.getMessage).asRuntimeException())
      case ex: DASSdkUnauthenticatedException =>
        logger.error("DASSdk unauthenticated error", ex)
        responseObserver.onError(Status.UNAUTHENTICATED.withDescription(ex.getMessage).asRuntimeException())
      case ex: DASSdkUnsupportedException =>
        logger.error("DASSdk unsupported feature", ex)
        responseObserver.onError(Status.UNIMPLEMENTED.withDescription(ex.getMessage).asRuntimeException())
      case t: Throwable =>
        logger.error("DASSdk unexpected error", t)
        responseObserver.onError(Status.INTERNAL.withCause(t).asRuntimeException())

    }
  }

  /**
   * Unregisters an existing DAS instance based on the provided DAS ID.
   *
   * @param request The DAS ID.
   * @param responseObserver The observer to send responses.
   */
  override def unregister(request: DASId, responseObserver: StreamObserver[UnregisterResponse]): Unit = {
    logger.debug(s"Unregistering DAS with ID: ${request.getId}")
    dasSdkManager.unregisterDAS(request)
    responseObserver.onNext(UnregisterResponse.newBuilder().build())
    responseObserver.onCompleted()
    logger.debug(s"DAS unregistered successfully with ID: ${request.getId}")
  }

}
