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
import scala.jdk.OptionConverters._

import com.rawlabs.das.sdk._
import com.rawlabs.das.sdk.{
  DASSdkInvalidArgumentException,
  DASSdkPermissionDeniedException,
  DASSdkUnauthenticatedException,
  DASSdkUnsupportedException
}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.functions.FunctionId
import com.rawlabs.protocol.das.v1.services._
import com.typesafe.scalalogging.StrictLogging

import io.grpc.Status
import io.grpc.stub.StreamObserver

/**
 * Implementation of the gRPC service for handling function-related operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 */
class FunctionServiceGrpcImpl(provider: DASSdkManager)
    extends FunctionsServiceGrpc.FunctionsServiceImplBase
    with StrictLogging
    with GrpcMetrics {

  protected val serviceName = "FunctionService"

  /**
   * Retrieves function definitions based on the DAS ID (and optionally environment) provided in the request.
   *
   * @param request The request containing the DAS ID and optional environment.
   * @param responseObserver The observer to send responses.
   */
  override def getFunctionDefinitions(
      request: GetFunctionDefinitionsRequest,
      responseObserver: StreamObserver[GetFunctionDefinitionsResponse]): Unit = withMetrics("getFunctionDefinitions") {
    logger.debug(s"Fetching function definitions for DAS ID: ${request.getDasId}")

    withDAS(request.getDasId, responseObserver) { das =>
      val functionDefs = das.getFunctionDefinitions

      val response = GetFunctionDefinitionsResponse
        .newBuilder()
        .addAllDefinitions(functionDefs)
        .build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Function definitions sent successfully.")
    }
  }

  /**
   * Executes a function based on the Function ID, arguments, and optional environment in the request.
   *
   * @param request The request containing the function ID, arguments, and optional environment.
   * @param responseObserver The observer to send responses.
   */
  override def executeFunction(
      request: ExecuteFunctionRequest,
      responseObserver: StreamObserver[ExecuteFunctionResponse]): Unit = withMetrics("executeFunction") {
    logger.debug(s"Executing function for DAS ID: ${request.getDasId}, function ID: ${request.getFunctionId.getName}")

    withFunction(request.getDasId, request.getFunctionId, responseObserver) { function =>
      val sdkArgs = request.getArgsList.asScala
        .map { arg =>
          assert(arg.hasNamedArg)
          arg.getNamedArg.getName -> arg.getNamedArg.getValue
        }
        .toMap
        .asJava
      val resultValue = function.execute(sdkArgs, if (request.hasEnv) request.getEnv else null)

      val response = ExecuteFunctionResponse.newBuilder().setOutput(resultValue).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Function executed successfully.")
    }
  }

  /**
   * Helper method to look up a DAS instance from the provider, returning an error if not found.
   */
  private def withDAS(dasId: DASId, responseObserver: StreamObserver[_])(f: DASSdk => Unit): Unit = {
    provider.getDAS(dasId) match {
      case None =>
        responseObserver.onError(
          Status.NOT_FOUND.withDescription(s"DAS with ID [${dasId.getId}] not found.").asRuntimeException())
      case Some(das) =>
        try {
          f(das)
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
  }

  /**
   * Helper method to retrieve a function from the DAS. If the function is not found, respond with an error.
   */
  private def withFunction(DASId: DASId, functionId: FunctionId, responseObserver: StreamObserver[_])(
      f: DASFunction => Unit): Unit = {
    withDAS(DASId, responseObserver) { das =>
      val maybeFunction = das.getFunction(functionId.getName).toScala
      maybeFunction match {
        case None =>
          logger.error(s"Function ${functionId.getName} not found.")
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription(s"Function ${functionId.getName} not found").asRuntimeException())
        case Some(fn) => f(fn)
      }
    }
  }

}
