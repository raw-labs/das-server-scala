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

import java.io.Closeable
import java.util.Optional

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.server.cache.catalog.CacheDefinition
import com.rawlabs.das.server.cache.iterator.QueryProcessorFlow
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.cache.manager.CacheManager.{GetIterator, WrappedGetIterator}
import com.rawlabs.das.server.cache.queue.{CloseableIterator, DataProducingTask}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.typesafe.scalalogging.StrictLogging

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, Scheduler}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import io.grpc.stub.StreamObserver
import io.grpc.{Context, Status, StatusRuntimeException}

/**
 * Implementation of the gRPC service for handling table-related operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TableServiceGrpcImpl(
    provider: DASSdkManager,
    cacheManager: ActorRef[CacheManager.Command[Row]],
    maxChunkSize: Int = 1000)(
    implicit val ec: ExecutionContext,
    implicit val materializer: Materializer,
    implicit val scheduler: Scheduler)
    extends TablesServiceGrpc.TablesServiceImplBase
    with StrictLogging {

  /**
   * Retrieves table definitions based on the DAS ID provided in the request.
   *
   * @param request The request containing the DAS ID.
   * @param responseObserver The observer to send responses.
   */
  override def getTableDefinitions(
      request: GetTableDefinitionsRequest,
      responseObserver: StreamObserver[GetTableDefinitionsResponse]): Unit = {
    logger.debug(s"Fetching table definitions for DAS ID: ${request.getDasId}")
    val tableDefinitions = provider.getDAS(request.getDasId).getTableDefinitions
    val response = GetTableDefinitionsResponse.newBuilder().addAllDefinitions(tableDefinitions).build()
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
  override def getTableEstimate(
      request: GetTableEstimateRequest,
      responseObserver: StreamObserver[GetTableEstimateResponse]): Unit = {
    logger.debug(s"Fetching table size for Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val relSizeResult = table.getTableEstimate(request.getQualsList, request.getColumnsList)
        val rows = relSizeResult.getExpectedNumberOfRows
        val bytes = relSizeResult.getAvgRowWidthBytes
        val response = GetTableEstimateResponse.newBuilder().setRows(rows).setBytes(bytes).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug(s"Table size (rows: $rows, bytes: $bytes) sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Checks if the table can be sorted using the provided sort keys.
   *
   * @param request The request containing the sort keys.
   * @param responseObserver The observer to send responses.
   */
  override def getTableSortOrders(
      request: GetTableSortOrdersRequest,
      responseObserver: StreamObserver[GetTableSortOrdersResponse]): Unit = {
    logger.debug(s"Fetching table sort orders for Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val sortKeys = table.getTableSortOrders(request.getSortKeysList)
        val response = GetTableSortOrdersResponse.newBuilder().addAllSortKeys(sortKeys).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Table sort orders sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Retrieves the path keys of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getTablePathKeys(
      request: GetTablePathKeysRequest,
      responseObserver: StreamObserver[GetTablePathKeysResponse]): Unit = {
    logger.debug(s"Fetching table path keys for Table ID: ${request.getTableId.getName}")
    provider
      .getDAS(request.getDasId)
      .getTable(request.getTableId.getName)
      .toScala match {
      case Some(table) =>
        val pathKeys = table.getTablePathKeys
        val response = GetTablePathKeysResponse.newBuilder().addAllPathKeys(pathKeys).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Table path keys sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Provides an explanation of the query execution plan.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def explainTable(
      request: ExplainTableRequest,
      responseObserver: StreamObserver[ExplainTableResponse]): Unit = {
    logger.debug(s"Explaining query for Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val explanation =
          table.explain(
            request.getQuery.getQualsList,
            request.getQuery.getColumnsList,
            request.getQuery.getSortKeysList)
        val response = ExplainTableResponse.newBuilder().addAllStmts(explanation).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Query explanation sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Executes a query on the specified table and streams the results.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def executeTable(request: ExecuteTableRequest, responseObserver: StreamObserver[Rows]): Unit = {
    logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}")

    // 1) Attempt to get the table from provider
    val tableOpt = provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala
    tableOpt match {
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          Status.NOT_FOUND
            .withDescription(s"Table ${request.getTableId.getName} not found")
            .asRuntimeException())

      case Some(table) =>
        val quals = request.getQuery.getQualsList.asScala.toSeq
        val columns = request.getQuery.getColumnsList.asScala.toSeq
        val sortKeys = request.getQuery.getSortKeysList.asScala.toSeq

        // Build a data-producing task for the table, if the cache manager needs to create a new cache
        val makeTask = () =>
          new DataProducingTask[Row] {
            override def run(): CloseableIterator[Row] = {
              // table.execute(...) returns a CloseableIterator[Row], presumably
              val dasExecuteResult: DASExecuteResult = table.execute(quals.asJava, columns.asJava, sortKeys.asJava)

              // Adapt DASExecuteResult to CloseableIterator
              new CloseableIterator[Row] {
                override def hasNext: Boolean = dasExecuteResult.hasNext

                override def next(): Row = dasExecuteResult.next()

                override def close(): Unit = dasExecuteResult.close()
              }
            }
          }
        // ^ returns a CloseableIterator[Row] presumably, or some adapter

        // 2) Ask the CacheManager for a Source[Row, _]
        import akka.util.Timeout
        implicit val timeout: Timeout = Timeout.create(java.time.Duration.ofSeconds(3))

        val futureAck = cacheManager.ask[CacheManager.GetIteratorAck[Row]] { replyTo =>
          WrappedGetIterator(
            GetIterator(
              dasId = request.getDasId.getId,
              definition = CacheDefinition(
                tableId = request.getTableId.getName,
                quals = quals,
                columns = columns,
                sortKeys = sortKeys),
              minCreationDate = None,
              makeTask = makeTask,
              codec = new RowCodec,
              replyTo = replyTo))
        }

        futureAck.onComplete {
          case Failure(ex) =>
            logger.error("CacheManager lookup failed", ex)
            responseObserver.onError(
              new StatusRuntimeException(Status.INTERNAL.withDescription("CacheManager lookup failed")))

          case Success(ack) =>
            // The ack gives us a future Option[Source[Row, _]]
            ack.sourceFuture.onComplete {
              case Failure(err) =>
                logger.error("Failed to subscribe to cached data source", err)
                responseObserver.onError(
                  new StatusRuntimeException(
                    Status.INTERNAL.withDescription(s"Failed to subscribe to cached data source: $err")))

              case Success(None) =>
                // Means the data source is not available or stopped
                logger.warn("CacheManager returned None => no data source.")
                responseObserver.onCompleted()

              case Success(Some(cachedSource)) =>
                // 3) Build a flow that applies filtering + projection
                //    (purely streamed row-by-row, no large in-memory collections).
                //
                // If you have a QueryProcessor that can produce a Flow:
                //   val queryFlow = new QueryProcessor().asFlow(quals, columns)
                //
                // Or if using QueryProcessorFlow directly:
                //   val queryFlow = QueryProcessorFlow(quals, columns)
                //
                // For illustration:
                val queryFlow: Flow[Row, Row, NotUsed] =
                  QueryProcessorFlow(quals, columns)

                // 4) Merge the cached source with the flow
                val finalSource: Source[Row, NotUsed] =
                  cachedSource
                    .via(queryFlow)
                    .mapMaterializedValue(_ => NotUsed)

                // 5) Chunk the final stream and push to gRPC observer
                runStreamedResult(finalSource, request, responseObserver)
            }
        }
    }
  }

  /**
   * Runs the final Source[Row, _] in chunks of size `maxChunkSize`, sending them to gRPC. Cancels if gRPC context is
   * cancelled.
   */
  private def runStreamedResult(
      source: Source[Row, NotUsed],
      request: ExecuteTableRequest,
      responseObserver: StreamObserver[Rows]): Unit = {

    val clientMaxBytes = /* request.getMaxBytes */ 4194304 * 3 / 4
    // Build a stream that splits the rows by the client's max byte size
    val rowBatches = source
      .via(new SizeBasedBatcher(maxBatchCount = maxChunkSize, maxBatchSizeBytes = clientMaxBytes)) // custom stage
      .map { batchOfRows =>
        Rows
          .newBuilder()
          .addAllRows(batchOfRows.asJava)
          .build()
      }

    val context = io.grpc.Context.current()

    val doneF = rowBatches.runForeach { rowsProto =>
      if (context.isCancelled) {
        logger.warn(s"Context cancelled for planID=${request.getPlanId}, stopping stream.")
        // We can throw to terminate the stream
        throw new RuntimeException("gRPC context canceled")
      }
      responseObserver.onNext(rowsProto)
    }(materializer)

    doneF.onComplete {
      case Success(_) =>
        logger.debug(s"Streaming completed successfully for planID=${request.getPlanId}.")
        responseObserver.onCompleted()
      case Failure(ex) =>
        logger.error(s"Error during streaming for planID=${request.getPlanId}.", ex)
        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Error during streaming")))
    }(ec)
  }

  /**
   * Retrieves the unique columns of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getTableUniqueColumn(
      request: GetTableUniqueColumnRequest,
      responseObserver: StreamObserver[GetTableUniqueColumnResponse]): Unit = {
    logger.debug(s"Fetching unique columns for Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val response = GetTableUniqueColumnResponse.newBuilder().setColumn(table.uniqueColumn).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Unique column information sent successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Modifies the batch size of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getBulkInsertTableSize(
      request: GetBulkInsertTableSizeRequest,
      responseObserver: StreamObserver[GetBulkInsertTableSizeResponse]): Unit = {
    logger.debug(s"Modifying batch size for Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val batchSize = table.bulkInsertBatchSize()
        val response = GetBulkInsertTableSizeResponse.newBuilder().setSize(batchSize).build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
        logger.debug("Batch size modification completed successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Inserts a row into the specified table.
   *
   * @param request The request containing the row to be inserted.
   * @param responseObserver The observer to send responses.
   */
  override def insertTable(request: InsertTableRequest, responseObserver: StreamObserver[InsertTableResponse]): Unit = {
    logger.debug(s"Inserting row into Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val row = table.insert(request.getRow)
        responseObserver.onNext(InsertTableResponse.newBuilder().setRow(row).build())
        responseObserver.onCompleted()
        logger.debug("Row inserted successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Performs a bulk insert of multiple rows into the specified table.
   *
   * @param request The request containing the rows to be inserted.
   * @param responseObserver The observer to send responses.
   */
  override def bulkInsertTable(
      request: BulkInsertTableRequest,
      responseObserver: StreamObserver[BulkInsertTableResponse]): Unit = {
    logger.debug(s"Performing bulk insert into Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val rows = table.bulkInsert(request.getRowsList)
        responseObserver.onNext(BulkInsertTableResponse.newBuilder().addAllRows(rows).build())
        responseObserver.onCompleted()
        logger.debug("Bulk insert completed successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Updates rows in the specified table based on the unique columns and new values provided.
   *
   * @param request The request containing the unique columns and new values.
   * @param responseObserver The observer to send responses.
   */
  override def updateTable(request: UpdateTableRequest, responseObserver: StreamObserver[UpdateTableResponse]): Unit = {
    logger.debug(s"Updating rows in Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        val newRow = table.update(request.getRowId, request.getNewRow)
        responseObserver.onNext(UpdateTableResponse.newBuilder().setRow(newRow).build())
        responseObserver.onCompleted()
        logger.debug("Rows updated successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(
          new StatusRuntimeException(
            Status.NOT_FOUND.withDescription(s"Table ${request.getTableId.getName} not found")))
    }
  }

  /**
   * Deletes rows from the specified table based on the unique columns provided.
   *
   * @param request The request containing the unique columns.
   * @param responseObserver The observer to send responses.
   */
  override def deleteTable(request: DeleteTableRequest, responseObserver: StreamObserver[DeleteTableResponse]): Unit = {
    logger.debug(s"Deleting rows from Table ID: ${request.getTableId.getName}")
    provider.getDAS(request.getDasId).getTable(request.getTableId.getName).toScala match {
      case Some(table) =>
        table.delete(request.getRowId)
        responseObserver.onNext(DeleteTableResponse.getDefaultInstance)
        responseObserver.onCompleted()
        logger.debug("Rows deleted successfully.")
      case None =>
        logger.error(s"Table ${request.getTableId.getName} not found.")
        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription("Table not found")))
    }
  }

}
