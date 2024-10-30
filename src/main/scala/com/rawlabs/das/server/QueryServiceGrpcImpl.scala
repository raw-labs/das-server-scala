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

import com.rawlabs.das.sdk.DASSQLParser
import com.rawlabs.protocol.das.Rows
import com.rawlabs.protocol.das.services._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.Context
import io.grpc.stub.StreamObserver

/**
 * Implementation of the gRPC service for query operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class QueryServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
    extends QueryServiceGrpc.QueryServiceImplBase
    with StrictLogging {

  /**
   * Retrieves an estimate of the number of rows and bytes that a query will return.
   *
   * @param queryRequest The request containing the query.
   * @param streamObserver The observer to send responses.
   */
  override def getQueryEstimate(
      queryRequest: QueryRequest,
      streamObserver: StreamObserver[GetQueryEstimateResponse]
  ): Unit = {
    logger.debug(s"Getting query estimate for SQL: ${queryRequest.getSql}")
    val dasSdk = provider.getDAS(queryRequest.getDasId)
    DASSQLParser.parseSQL(queryRequest.getSql) match {
      case Left(error) => throw new IllegalArgumentException(s"Error parsing SQL: $error")
      case Right(query) =>
        val (rows, bytes) = dasSdk.estimateQuery(query)
        val response = GetQueryEstimateResponse.newBuilder().setRows(rows).setBytes(bytes).build()
        streamObserver.onNext(response)
        streamObserver.onCompleted()
        logger.debug("Query estimate sent successfully.")
    }
  }

  /**
   * Executes a query on the specified table and streams the results.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def executeQuery(request: QueryRequest, responseObserver: StreamObserver[Rows]): Unit = {
    logger.debug(s"Executing SQL query: ${request.getSql}")
    val dasSdk = provider.getDAS(request.getDasId)
    DASSQLParser.parseSQL(request.getSql) match {
      case Left(error) => throw new IllegalArgumentException(s"Error parsing SQL: $error")
      case Right(query) =>
        val result = dasSdk.executeQuery(query)

        val MAX_CHUNK_SIZE = 100
        logger.debug(
          s"Creating iterator (chunk size $MAX_CHUNK_SIZE rows) for query"
        )
        // Wrap the result processing logic in the iterator
        val it = new ChunksIterator(request.toString, result, MAX_CHUNK_SIZE)

        val context = Context.current()
        try {
          it.foreach { rows =>
            if (context.isCancelled) {
              logger.warn("Context cancelled during query execution. Closing reader.")
              return
            }
            responseObserver.onNext(rows)
          }
          logger.debug("Query execution completed successfully.")
          responseObserver.onCompleted()
        } catch {
          case ex: Exception =>
            logger.error("Error occurred during query execution.", ex)
            responseObserver.onError(ex)
        } finally {
          it.close()
        }
    }
  }

}
