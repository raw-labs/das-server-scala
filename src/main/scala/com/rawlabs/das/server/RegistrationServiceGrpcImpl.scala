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

import com.rawlabs.protocol.das.DASId
import com.rawlabs.protocol.das.services.{RegisterRequest, RegistrationServiceGrpc, UnregisterResponse}
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
      request.getDefinition.getOptionsMap.asScala.toMap
    )
    responseObserver.onNext(dasId)
    responseObserver.onCompleted()
    logger.debug(s"DAS registered successfully with ID: $dasId")
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
