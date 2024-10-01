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

import com.rawlabs.protocol.das.DASId
import com.rawlabs.protocol.das.services.{
  OperationsSupportedResponse,
  RegisterRequest,
  RegistrationServiceGrpc,
  UnregisterResponse
}
import com.typesafe.scalalogging.StrictLogging
import io.grpc.stub.StreamObserver

import scala.collection.JavaConverters._

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
  override def register(request: RegisterRequest, responseObserver: StreamObserver[DASId]): Unit = {
    logger.debug(s"Registering DAS with type: ${request.getDefinition.getType}")
    val dasId = dasSdkManager.registerDAS(
      request.getDefinition.getType,
      request.getDefinition.getOptionsMap.asScala.toMap,
      maybeDasId = if (request.hasId) Some(request.getId) else None
    )
    responseObserver.onNext(dasId)
    responseObserver.onCompleted()
    logger.debug(s"DAS registered successfully with ID: $dasId")
  }

  /**
   * Checks which operations are supported by the DAS instance with the provided DAS ID.
   *
   * @param dasId The DAS ID to check.
   * @param streamObserver The observer to send responses.
   */
  override def operationsSupported(dasId: DASId, streamObserver: StreamObserver[OperationsSupportedResponse]): Unit = {
    logger.debug(s"Checking operations supported for DAS with ID: ${dasId.getId}")
    val dasSdk = dasSdkManager.getDAS(dasId)
    streamObserver.onNext(dasSdk.operationsSupported)
    streamObserver.onCompleted()
    logger.debug(s"Operations supported for DAS with ID: ${dasId.getId} sent successfully.")
  }

  /**
   * Unregisters an existing DAS instance based on the provided DAS ID.
   *
   * @param request The request containing the DAS ID.
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
