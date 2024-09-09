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

import com.rawlabs.protocol.das.{Row, Rows, SortKeys}
import com.rawlabs.protocol.das.services._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.Context
import io.grpc.stub.StreamObserver

import java.io.Closeable
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Implementation of the gRPC service for handling table-related operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TableServiceGrpcImpl(provider: DASSdkManager, cache: Cache)
    extends TablesServiceGrpc.TablesServiceImplBase
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
        logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}")
        val result = table.execute(
          request.getQualsList.asScala,
          request.getColumnsList.asScala,
          if (request.hasSortKeys) Some(request.getSortKeys.getSortKeysList.asScala) else None,
          if (request.hasLimit) Some(request.getLimit) else None
        )
        val context = Context.current()
        val MAX_CHUNK_SIZE = 100
        logger.debug(
          s"Creating iterator (chunk size $MAX_CHUNK_SIZE rows) for query execution for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}"
        )
        // Wrap the result processing logic in the iterator
        val it = new ChunksIterator(result, MAX_CHUNK_SIZE)
        try {
          it.foreach { rows =>
            if (context.isCancelled) {
              logger.warn("Context cancelled during query execution. Closing reader.")
              it.close()
              return
            }
            responseObserver.onNext(rows)
          }
          responseObserver.onCompleted()
          logger.debug("Query execution completed successfully.")
        } catch {
          case ex: Exception =>
            logger.error("Error occurred during query execution.", ex)
            it.close()
            responseObserver.onError(ex)
        }
//        assert(request.hasPlanId, "Plan ID is required for caching query results.")
//
//        def task(): Iterator[Rows] with Closeable = {
//          logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}")
//          val result = table.execute(
//            request.getQualsList.asScala,
//            request.getColumnsList.asScala,
//            if (request.hasSortKeys) Some(request.getSortKeys.getSortKeysList.asScala) else None,
//            if (request.hasLimit) Some(request.getLimit) else None
//          )
//
//          val MAX_CHUNK_SIZE = 100
//          logger.debug(
//            s"Creating iterator (chunk size $MAX_CHUNK_SIZE rows) for query execution for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}"
//          )
//          // Wrap the result processing logic in the iterator
//          new ChunksIterator(result, MAX_CHUNK_SIZE)
//        }
//
//        val cacheKey = s"${request.getTableId.getName}:${request.getPlanId}"
//        logger.debug(s"Storing result in cache with key: $cacheKey")
//        cache.writeIfNotExists(cacheKey, task())
//
//        val it = cache.read(cacheKey)
//        try {
//          it.foreach { rows =>
//            if (context.isCancelled) {
//              logger.warn("Context cancelled during query execution. Closing reader.")
//              it.close()
//              return
//            }
//            responseObserver.onNext(rows)
//          }
//          responseObserver.onCompleted()
//          logger.debug("Query execution completed successfully.")
//        } catch {
//          case ex: Exception =>
//            logger.error("Error occurred during query execution.", ex)
//            it.close()
//            responseObserver.onError(ex)
//        }
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Retrieves the unique columns of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def uniqueColumn(
      request: UniqueColumnRequest,
      responseObserver: StreamObserver[UniqueColumnResponse]
  ): Unit = {
    logger.debug(s"Fetching unique columns for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val response = UniqueColumnResponse.newBuilder().setColumn(table.uniqueColumn).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Unique column information sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Modifies the batch size of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def modifyBatchSize(
      request: ModifyBatchSizeRequest,
      responseObserver: StreamObserver[ModifyBatchSizeResponse]
  ): Unit = {
    logger.debug(s"Modifying batch size for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val batchSize = table.modifyBatchSize
        val response = ModifyBatchSizeResponse.newBuilder().setSize(batchSize).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Batch size modification completed successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Inserts a row into the specified table.
   *
   * @param request The request containing the row to be inserted.
   * @param responseObserver The observer to send responses.
   */
  override def insert(request: InsertRequest, responseObserver: StreamObserver[InsertResponse]): Unit = {
    logger.debug(s"Inserting row into Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val row = table.insert(request.getValues)
        responseObserver.onNext(InsertResponse.newBuilder().setRow(row).build())
        responseObserver.onCompleted()
        logger.debug("Row inserted successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Performs a bulk insert of multiple rows into the specified table.
   *
   * @param request The request containing the rows to be inserted.
   * @param responseObserver The observer to send responses.
   */
  override def bulkInsert(request: BulkInsertRequest, responseObserver: StreamObserver[BulkInsertResponse]): Unit = {
    logger.debug(s"Performing bulk insert into Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val rows = table.bulkInsert(request.getValuesList.asScala)
        responseObserver.onNext(BulkInsertResponse.newBuilder().addAllRows(rows.asJava).build())
        responseObserver.onCompleted()
        logger.debug("Bulk insert completed successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Updates rows in the specified table based on the unique columns and new values provided.
   *
   * @param request The request containing the unique columns and new values.
   * @param responseObserver The observer to send responses.
   */
  override def update(request: UpdateRequest, responseObserver: StreamObserver[UpdateResponse]): Unit = {
    logger.debug(s"Updating rows in Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        val newRow = table.update(request.getRowId, request.getNewValues)
        responseObserver.onNext(UpdateResponse.newBuilder().setRow(newRow).build())
        responseObserver.onCompleted()
        logger.debug("Rows updated successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

  /**
   * Deletes rows from the specified table based on the unique columns provided.
   *
   * @param request The request containing the unique columns.
   * @param responseObserver The observer to send responses.
   */
  override def delete(request: DeleteRequest, responseObserver: StreamObserver[DeleteResponse]): Unit = {
    logger.debug(s"Deleting rows from Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName) match {
      case Some(table) =>
        table.delete(request.getRowId)
        responseObserver.onNext(DeleteResponse.getDefaultInstance)
        responseObserver.onCompleted()
        logger.debug("Rows deleted successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new RuntimeException(s"Table ${request.getTableId.getName} not found"))
    }
  }

}

/**
 * Iterator implementation for processing query results in chunks.
 *
 * @param resultIterator The iterator of query results.
 * @param maxChunkSize The maximum size of each chunk.
 */
class ChunksIterator(
    resultIterator: Iterator[Row] with Closeable,
    maxChunkSize: Int
) extends Iterator[Rows]
    with Closeable
    with StrictLogging {

  private val currentChunk: mutable.Buffer[Row] = mutable.Buffer.empty

  /**
   * Checks if there are more rows to process.
   *
   * @return true if there are more rows, false otherwise.
   */
  override def hasNext: Boolean = resultIterator.hasNext || currentChunk.nonEmpty

  /**
   * Retrieves the next chunk of rows.
   *
   * @return The next chunk of rows.
   */
  override def next(): Rows = {
    if (!hasNext) throw new NoSuchElementException("No more elements")

    currentChunk.clear()

    while (resultIterator.hasNext && currentChunk.size < maxChunkSize) {
      currentChunk += resultIterator.next()
    }

    // Return the current chunk
    Rows.newBuilder().addAllRows(currentChunk.asJava).build()
  }

  /**
   * Closes the underlying result iterator.
   */
  override def close(): Unit = {
    resultIterator.close()
  }

}
