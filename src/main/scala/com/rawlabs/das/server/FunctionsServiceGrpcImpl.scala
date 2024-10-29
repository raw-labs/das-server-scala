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

import com.rawlabs.protocol.das.services._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.stub.StreamObserver

import scala.collection.JavaConverters._

/**
 * Implementation of the gRPC service for handling function operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class FunctionsServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
    extends FunctionsServiceGrpc.FunctionsServiceImplBase
    with StrictLogging {

  /**
   * Retrieves function definitions based on the DAS ID provided in the request.
   *
   * @param request The request containing the DAS ID.
   * @param responseObserver The observer to send responses.
   */
  override def getFunctionDefinitions(
      request: GetFunctionDefinitionsRequest,
      responseObserver: StreamObserver[GetFunctionDefinitionsResponse]
  ): Unit = {
    logger.debug(s"Fetching function definitions for DAS ID: ${request.getDasId}")
    val functionDefinitions = provider.getDAS(request.getDasId).functionDefinitions
    val response =
      GetFunctionDefinitionsResponse.newBuilder().addAllFunctionDefinitions(functionDefinitions.asJava).build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
    logger.debug("Function definitions sent successfully.")
  }

  /**
   * Executes a function on the specified DAS instance.
   *
   * @param request The request containing the function name and arguments.
   * @param responseObserver The observer to send responses.
   */
  override def executeFunction(
      request: ExecuteFunctionRequest,
      responseObserver: StreamObserver[ExecuteFunctionResponse]
  ): Unit = {
    logger.debug(s"Executing function: ${request.getFunctionName}")
    val dasSdk = provider.getDAS(request.getDasId)
    dasSdk.getFunction(request.getFunctionName) match {
      case Some(function) =>
        val result = function.execute(request.getArgumentsList.asScala)
        val response = ExecuteFunctionResponse.newBuilder().setResult(result).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Function executed successfully.")
      case None =>
        logger.error(s"Function ${request.getFunctionName} not found.")
        responseObserver.onError(new RuntimeException(s"Function ${request.getFunctionName} not found"))
    }
  }

}
