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

import com.rawlabs.das.sdk.{DASSdk, DASSdkUnsupportedException, DASTable}
import com.rawlabs.protocol.das._
import com.rawlabs.protocol.das.services._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Status}

import java.io.Closeable
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Implementation of the gRPC service for handling table-related operations.
 * Errors are returned to the client as gRPC errors. Those tagged as `INVALID_ARGUMENT` are errors that go
 * to the client, while others are internal errors.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TableServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
    extends TablesServiceGrpc.TablesServiceImplBase
    with StrictLogging {

  /**
   * Retrieves table definitions based on the DAS ID provided in the request.
   *
   * @param request          The request containing the DAS ID.
   * @param responseObserver The observer to send responses.
   */
  override def getDefinitions(
      request: GetDefinitionsRequest,
      responseObserver: StreamObserver[GetDefinitionsResponse]
  ): Unit = {
    logger.debug(s"Fetching table definitions for DAS ID: ${request.getDasId}")
    withDAS(request.getDasId, responseObserver) { das =>
      val tableDefinitions = das.tableDefinitions
      val response = GetDefinitionsResponse.newBuilder().addAllDefinitions(tableDefinitions.asJava).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Table definitions sent successfully.")
    }
  }

  /**
   * Retrieves the size (rows and bytes) of a table based on the table ID provided in the request.
   *
   * @param request          The request containing the table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getRelSize(request: GetRelSizeRequest, responseObserver: StreamObserver[GetRelSizeResponse]): Unit = {
    logger.debug(s"Fetching table size for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val (rows, bytes) = table.getRelSize(request.getQualsList.asScala, request.getColumnsList.asScala)
      val response = GetRelSizeResponse.newBuilder().setRows(rows).setBytes(bytes).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug(s"Table size (rows: $rows, bytes: $bytes) sent successfully.")
    }
  }

  /**
   * Checks if the table can be sorted using the provided sort keys.
   *
   * @param request          The request containing the sort keys.
   * @param responseObserver The observer to send responses.
   */
  override def canSort(request: CanSortRequest, responseObserver: StreamObserver[CanSortResponse]): Unit = {
    logger.debug(s"Checking if table can be sorted for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val sortKeys = table.canSort(request.getSortKeys.getSortKeysList.asScala)
      val response =
        CanSortResponse.newBuilder().setSortKeys(SortKeys.newBuilder().addAllSortKeys(sortKeys.asJava)).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Sort capability information sent successfully.")
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
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
      } catch {
        case t: Throwable =>
          logger.error("Error explaining query", t)
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error explaining query").withCause(t).asRuntimeException()
          )
      }
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      def runQuery(): Iterator[Rows] with Closeable = {
        logger.debug(
          s"Executing query for Table ID: ${request.getTableId.getName}, Plan ID: ${request.getPlanId}"
        )
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
        new ChunksIterator(request, result, MAX_CHUNK_SIZE)
      }

      // Wrap the result processing logic in the iterator
      val it = {
        DASChunksCache.get(request) match {
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
            try {
              runQuery()
            } catch {
              case t: Throwable =>
                logger.error("Error executing query", t)
                responseObserver.onError(
                  Status.INVALID_ARGUMENT.withDescription("Error executing query").withCause(t).asRuntimeException()
                )
                return
            }
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
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error occurred during query execution").asRuntimeException()
          )
      } finally {
        it.close()
      }
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
        val response = UniqueColumnResponse.newBuilder().setColumn(table.uniqueColumn).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Unique column information sent successfully.")
      } catch {
        case t: DASSdkUnsupportedException => responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Unsupported operation").withCause(t).asRuntimeException()
          )
        case t: Throwable =>
          logger.error("Error fetching unique column", t)
          responseObserver.onError(
            Status.INTERNAL.withDescription("Error fetching unique column").withCause(t).asRuntimeException()
          )
      }
    }
  }

  /**
   * Returns the modify batch size of the specified table (how many rows can be sent in an update).
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def modifyBatchSize(
      request: ModifyBatchSizeRequest,
      responseObserver: StreamObserver[ModifyBatchSizeResponse]
  ): Unit = {
    logger.debug(s"Returning modify batch size for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val batchSize = table.modifyBatchSize
      val response = ModifyBatchSizeResponse.newBuilder().setSize(batchSize).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Batch size returned successfully.")
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
        val row = table.insert(request.getValues)
        responseObserver.onNext(InsertResponse.newBuilder().setRow(row).build())
        responseObserver.onCompleted()
        logger.debug("Row inserted successfully.")
      } catch {
        case t: Throwable =>
          logger.error("Error inserting row", t)
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error inserting row").withCause(t).asRuntimeException()
          )
      }
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
        val rows = table.bulkInsert(request.getValuesList.asScala)
        responseObserver.onNext(BulkInsertResponse.newBuilder().addAllRows(rows.asJava).build())
        responseObserver.onCompleted()
        logger.debug("Bulk insert completed successfully.")
      } catch {
        case t: Throwable =>
          logger.error("Error inserting rows", t)
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error inserting rows").withCause(t).asRuntimeException()
          )
      }
    }
  }

  /**
   * Updates a row in the specified table based on the unique column and new values provided.
   *
   * @param request The request containing the unique column and new values.
   * @param responseObserver The observer to send responses.
   */
  override def update(request: UpdateRequest, responseObserver: StreamObserver[UpdateResponse]): Unit = {
    logger.debug(s"Updating row in Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
        val newRow = table.update(request.getRowId, request.getNewValues)
        responseObserver.onNext(UpdateResponse.newBuilder().setRow(newRow).build())
        responseObserver.onCompleted()
        logger.debug("Row updated successfully.")
      } catch {
        case t: Throwable =>
          logger.error("Error updating row", t)
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error updating row").withCause(t).asRuntimeException()
          )
      }
    }
  }

  /**
   * Deletes a row from the specified table based on the unique column provided.
   *
   * @param request The request containing the unique column.
   * @param responseObserver The observer to send responses.
   */
  override def delete(request: DeleteRequest, responseObserver: StreamObserver[DeleteResponse]): Unit = {
    logger.debug(s"Deleting row from Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      try {
        table.delete(request.getRowId)
        responseObserver.onNext(DeleteResponse.getDefaultInstance)
        responseObserver.onCompleted()
        logger.debug("Row deleted successfully.")
      } catch {
        case t: Throwable =>
          logger.error("Error deleting row", t)
          responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Error deleting row").withCause(t).asRuntimeException()
          )
      }
    }
  }

  private def withDAS(DASId: DASId, responseObserver: StreamObserver[_])(f: DASSdk => Unit): Unit = {
    provider.getDAS(DASId) match {
      case None =>
        // We use 'NOT_FOUND' so that the client doesn't confuse that error with a user-visible error.
        responseObserver.onError(Status.NOT_FOUND.withDescription("DAS not found").asRuntimeException())
      case Some(das) => f(das)
    }
  }

  private def withTable(DASId: DASId, table: TableId, responseObserver: StreamObserver[_])(
      f: DASTable => Unit
  ): Unit = {
    withDAS(DASId, responseObserver) { das =>
      val tableName = table.getName
      das.getTable(tableName) match {
        case None =>
          logger.error(s"Table $tableName not found.")
          // If we're here, the table wasn't found although Postgres thought it was. The DAS was restarted and has now
          // fewer tables than before. We should return an error to the client.
          responseObserver.onError(
            Status.INVALID_ARGUMENT
              .withDescription(s"Table $tableName not found")
              .asRuntimeException()
          )
        case Some(table) => f(table)
      }
    }
  }
}

/**
 * Iterator implementation for processing query results in chunks.
 *
 * @param request The request containing query details (used to build a cache key).
 * @param resultIterator The iterator of query results.
 * @param maxChunkSize The maximum size of each chunk.
 */
class ChunksIterator(
    request: ExecuteRequest,
    resultIterator: Iterator[Row] with Closeable,
    maxChunkSize: Int
) extends Iterator[Rows]
    with Closeable
    with StrictLogging {

  logger.debug(s"Initializing ChunksIterator with maxChunkSize: $maxChunkSize, request: $request")

  private val currentChunk = mutable.Buffer[Row]()
  private val maxChunksToCache = 5
  private var chunkCounter = 0
  private var eofReached = false

  private val completeChunkCache = mutable.Buffer[Rows]()

  /**
   * Checks if there are more rows to process.
   *
   * @return true if there are more rows, false otherwise.
   */
  override def hasNext: Boolean = {
    val hasNext = resultIterator.hasNext || currentChunk.nonEmpty
    logger.debug(s"hasNext() called. hasNext: $hasNext")
    if (!hasNext) {
      eofReached = true
      logger.debug("EOF reached in hasNext()")
    }
    hasNext
  }

  /**
   * Retrieves the next chunk of rows.
   *
   * @return The next chunk of rows.
   */
  override def next(): Rows = {
    if (!hasNext) {
      logger.debug("No more elements in next()")
      throw new NoSuchElementException("No more elements")
    }

    logger.debug(s"Fetching next chunk. Chunk counter: $chunkCounter")

    val nextChunk = getNextChunk()

    logger.debug(s"Next chunk fetched with ${nextChunk.getRowsCount} rows")

    // Cache the chunks up to a certain limit
    if (chunkCounter < maxChunksToCache) {
      // Append the chunk to the cache
      completeChunkCache.append(nextChunk)
      logger.debug(s"Appended chunk to cache. Cache size: ${completeChunkCache.size}")

      // If we reached the end of the result set (or this is the last chunk, since it's not complete),
      // cache the complete chunks read thus far for future use
      if (eofReached || nextChunk.getRowsCount < maxChunkSize) {
        logger.debug("Reached end of result set or last incomplete chunk. Caching complete chunks.")
        DASChunksCache.put(request, completeChunkCache)
        logger.debug("Chunks cached successfully.")
      }
    } else if (chunkCounter == maxChunksToCache) {
      // We bail out of trying to cache chunks because it's getting too big
      completeChunkCache.clear()
      logger.debug("Reached maxChunksToCache limit. Cleared completeChunkCache.")
    }

    chunkCounter += 1
    logger.debug(s"Incremented chunk counter to $chunkCounter")
    nextChunk
  }

  // Builds a chunk of rows by reading from the result iterator.
  private def getNextChunk(): Rows = {
    currentChunk.clear()
    logger.debug("Cleared currentChunk")
    while (resultIterator.hasNext && currentChunk.size < maxChunkSize) {
      currentChunk += resultIterator.next()
    }
    val rows = Rows.newBuilder().addAllRows(currentChunk.asJava).build()
    logger.debug(s"Built next chunk with ${currentChunk.size} rows")
    rows
  }

  /**
   * Closes the underlying result iterator.
   */
  override def close(): Unit = {
    resultIterator.close()
    logger.debug(s"Closed resultIterator for $request")
  }

}
