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
 * Implementation of the gRPC service for handling table definition operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TableDefinitionServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
    extends TableDefinitionsServiceGrpc.TableDefinitionsServiceImplBase
    with StrictLogging {

  /**
   * Retrieves table definitions based on the DAS ID provided in the request.
   *
   * @param request The request containing the DAS ID.
   * @param responseObserver The observer to send responses.
   */
  override def getDefinitions(
      request: GetDefinitionsRequest,
      responseObserver: StreamObserver[GetDefinitionsResponse]
  ): Unit = {
    logger.debug(s"Fetching table definitions for DAS ID: ${request.getDasId}")
    val tableDefinitions = provider.getDAS(request.getDasId).tableDefinitions
    val response = GetDefinitionsResponse.newBuilder().addAllDefinitions(tableDefinitions.asJava).build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
    logger.debug("Table definitions sent successfully.")
  }

}
