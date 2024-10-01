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

import com.rawlabs.protocol.das.{PathKey, Rows, SortKeys}
import com.rawlabs.protocol.das.services._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.Context
import io.grpc.stub.StreamObserver

import java.io.Closeable
import scala.collection.JavaConverters._

/**
 * Implementation of the gRPC service for handling table-related operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TableQueryServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
    extends TableQueryServiceGrpc.TableQueryServiceImplBase
    with StrictLogging {

  /**
   * Retrieves the size (rows and bytes) of a table based on the table ID provided in the request.
   *
   * @param request The request containing the table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getRelSize(request: GetRelSizeRequest, responseObserver: StreamObserver[GetRelSizeResponse]): Unit = {
    logger.debug(s"Fetching table size for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val (rows, bytes) = table.getRelSize(request.getQualsList.asScala, request.getColumnsList.asScala)
        val response = GetRelSizeResponse.newBuilder().setRows(rows).setBytes(bytes).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug(s"Table size (rows: $rows, bytes: $bytes) sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Checks if the table can be sorted using the provided sort keys.
   *
   * @param request The request containing the sort keys.
   * @param responseObserver The observer to send responses.
   */
  override def canSort(request: CanSortRequest, responseObserver: StreamObserver[CanSortResponse]): Unit = {
    logger.debug(s"Checking if table can be sorted for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val sortKeys = table.canSort(request.getSortKeys.getSortKeysList.asScala)
        val response =
          CanSortResponse.newBuilder().setSortKeys(SortKeys.newBuilder().addAllSortKeys(sortKeys.asJava)).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Sort capability information sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Retrieves the path keys of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getPathKeys(request: GetPathKeysRequest, responseObserver: StreamObserver[GetPathKeysResponse]): Unit = {
    logger.debug(s"Fetching path keys for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val pathKeys = table.getPathKeys
        val response = GetPathKeysResponse
          .newBuilder()
          .addAllPathKeys(pathKeys.map {
            case (keys, rows) => PathKey.newBuilder().addAllKeyColumns(keys.asJava).setExpectedRows(rows).build()
          }.asJava)
          .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Path keys information sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Provides an explanation of the query execution plan.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def explain(request: ExplainRequest, responseObserver: StreamObserver[ExplainResponse]): Unit = {
    logger.debug(s"Explaining query for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val explanation = table.explain(
          request.getQualsList.asScala,
          request.getColumnsList.asScala,
          if (request.hasSortKeys) Some(request.getSortKeys.getSortKeysList.asScala) else None,
          if (request.hasLimit) Some(request.getLimit) else None,
          request.getVerbose
        )
        val response = ExplainResponse.newBuilder().addAllStmts(explanation.asJava).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Query explanation sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Executes a query on the specified table and streams the results.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def execute(request: ExecuteRequest, responseObserver: StreamObserver[Rows]): Unit = {
    logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        def task(): Iterator[Rows] with Closeable = {
          logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}")
          val result = table.execute(
            request.getQualsList.asScala,
            request.getColumnsList.asScala,
            if (request.hasSortKeys) Some(request.getSortKeys.getSortKeysList.asScala) else None,
            if (request.hasLimit) Some(request.getLimit) else None
          )

          val MAX_CHUNK_SIZE = 100
          logger.debug(
            s"Creating iterator (chunk size $MAX_CHUNK_SIZE rows) for query execution for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}"
          )
          // Wrap the result processing logic in the iterator
          new ChunksIterator(request.toString, result, MAX_CHUNK_SIZE)
        }

        // Wrap the result processing logic in the iterator
        val it = {
          DASChunksCache.get(request.toString) match {
            case Some(cachedChunks) =>
              logger.debug(s"Using cached chunks for Table ID: ${request.getTableId.getName}")
              val cachedChunksIterator = cachedChunks.iterator
              new Iterator[Rows] with Closeable {
                override def hasNext: Boolean = cachedChunksIterator.hasNext

                override def next(): Rows = cachedChunksIterator.next()

                override def close(): Unit = {}
              }
            case None =>
              logger.debug(s"Cache miss for Table ID: ${request.getTableId.getName}")
              task()
          }
        }

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
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

}
