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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}

import com.rawlabs.das.sdk._
import com.rawlabs.das.server.cache.{QueryCacheKey, QueryResultCache}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.typesafe.scalalogging.StrictLogging

import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Status, StatusRuntimeException}
import kamon.Kamon

/**
 * Implementation of the gRPC service for handling table-related operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param batchLatency Time delay for batching rows (used in groupingWeightedWithin).
 */
class TableServiceGrpcImpl(
    provider: DASSdkManager,
    resultCache: QueryResultCache,
    batchLatency: FiniteDuration = 500.millis)(implicit val ec: ExecutionContext, materializer: Materializer)
    extends TablesServiceGrpc.TablesServiceImplBase
    with StrictLogging
    with GrpcMetrics {

  protected val serviceName = "TableService"

  private val tableInFlightGauge = Kamon
    .rangeSampler("in_flight_queries")
    .withTag("service", "TableService")
  private val cacheHitTotal = Kamon
    .counter("das_query_cache_hit_total")
    .withTag("service", "TableService")
  private val cacheMissTotal = Kamon
    .counter("das_query_cache_miss_total")
    .withTag("service", "TableService")

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
    withDAS(request.getDasId, responseObserver) { das =>
      val tableDefinitions = das.getTableDefinitions
      val response = GetTableDefinitionsResponse.newBuilder().addAllDefinitions(tableDefinitions).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Table definitions sent successfully.")
    }
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val relSizeResult = table.getTableEstimate(request.getQualsList, request.getColumnsList)
      val rows = relSizeResult.getExpectedNumberOfRows
      val bytes = relSizeResult.getAvgRowWidthBytes
      val response = GetTableEstimateResponse.newBuilder().setRows(rows).setBytes(bytes).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug(s"Table size (rows: $rows, bytes: $bytes) sent successfully.")
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val sortKeys = table.getTableSortOrders(request.getSortKeysList)
      val response = GetTableSortOrdersResponse.newBuilder().addAllSortKeys(sortKeys).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Table sort orders sent successfully.")
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
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val pathKeys = table.getTablePathKeys
      val response = GetTablePathKeysResponse.newBuilder().addAllPathKeys(pathKeys).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Table path keys sent successfully.")

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
      responseObserver: StreamObserver[ExplainTableResponse]): Unit = withMetrics("explainTable") {
    logger.debug(s"Explaining query for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val explanation =
        table.explain(
          request.getQuery.getQualsList,
          request.getQuery.getColumnsList,
          request.getQuery.getSortKeysList,
          if (request.getQuery.hasLimit) java.lang.Long.valueOf(request.getQuery.getLimit) else null,
          if (request.hasEnv) request.getEnv else null)
      val response = ExplainTableResponse.newBuilder().addAllStmts(explanation).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Query explanation sent successfully.")
    }
  }

  /**
   * Executes a query on the specified table and streams the results.
   *
   * @param request The request containing query details.
   * @param responseObserver The observer to send responses.
   */
  override def executeTable(request: ExecuteTableRequest, responseObserver: StreamObserver[Rows]): Unit =
    withMetrics("executeTable") {
      logger.debug(s"Executing query for Table ID: ${request.getTableId.getName}")

      // Check if the responseObserver is a ServerCallStreamObserver. If so, we can set an onCancel handler.
      val maybeServerCallObs: Option[ServerCallStreamObserver[Rows]] = responseObserver match {
        case sco: ServerCallStreamObserver[Rows] => Some(sco)
        case _ =>
          logger.warn("ResponseObserver is not a ServerCallStreamObserver. onCancelHandler not available.")
          None
      }

      // We'll keep a reference to the kill switch or a "cancel" function
      val killSwitchRef = new java.util.concurrent.atomic.AtomicReference[Option[UniqueKillSwitch]](None)

      // If we have a ServerCallStreamObserver, set the handler *immediately*. This isn't possible later.
      maybeServerCallObs.foreach { sco =>
        sco.setOnCancelHandler(() => {
          logger.warn(s"Client canceled for planID=${request.getPlanId}, shutting down stream if possible.")
          // Use whatever is in killSwitchRef at the time of cancellation:
          val maybeKs = killSwitchRef.get()
          maybeKs.foreach(_.shutdown())
        })
      }
      val quals = request.getQuery.getQualsList.asScala.toSeq
      val columns = request.getQuery.getColumnsList.asScala.toSeq
      val sortKeys = request.getQuery.getSortKeysList.asScala.toSeq
      val maybeLimit = if (request.getQuery.hasLimit) Some(request.getQuery.getLimit) else None
      val maybeEnv = if (request.hasEnv) Some(request.getEnv) else None

      withTable(request.getDasId, request.getTableId, responseObserver) { table =>
        /* This function runs the query and returns a Source of Rows. Rows are batches of Row aggregated
         * by the client's max batch size (clientMaxBytes below), or because the source is slow (server's batchLatency).
         */
        def runQuery(): Source[Rows, NotUsed] = {
          val clientMaxBytes = {
            if (request.hasMaxBatchSizeBytes) {
              // We multiply by 3/4 to leave some room for gRPC overhead
              request.getMaxBatchSizeBytes * 3 / 4
            } else {
              // Default to 2M
              2_000_000
            }
          }
          val dasExecuteResult: DASExecuteResult =
            table.execute(
              quals.asJava,
              columns.asJava,
              sortKeys.asJava,
              maybeLimit.map(java.lang.Long.valueOf).orNull,
              maybeEnv.orNull)

          val source: Source[Row, NotUsed] = Source.unfoldResource[Row, DASExecuteResult](
            create = () => dasExecuteResult,
            read = r => if (r.hasNext) Some(r.next()) else None,
            close = r => r.close())

          // Build a stream that splits the rows by the client's max byte size
          source
            // Group rows by size (but also by time if source is slow). Assume a minimum size of 8 bytes per row.
            .groupedWeightedWithin(clientMaxBytes, batchLatency)(row => Math.max(row.getSerializedSize.toLong, 8))
            .map { batchOfRows =>
              Rows
                .newBuilder()
                .addAllRows(batchOfRows.asJava)
                .build()
            }
        }

        val key = QueryCacheKey(request)
        // Check if we have a cached result for this query
        val source: Source[Rows, NotUsed] = resultCache.get(key) match {
          case Some(iterator) =>
            cacheHitTotal.increment()
            // We do. Use the iterator to build the Source.
            logger.debug(s"Using cached result for $request.")
            Source.fromIterator(() => iterator)
          case None =>
            cacheMissTotal.increment()
            // We don't. Run the query and build a Source that populates a new cache entry.
            // We tap the source to cache the results as they are streamed to the client.
            // A callback is added to the source to mark the cache entry as done when the stream completes.
            logger.debug(s"Cache miss for $request.")
            val source = runQuery()
            val cachedResult = resultCache.newBuffer(key)
            val tappingSource: Source[Rows, NotUsed] = source.map { chunk =>
              cachedResult.addChunk(chunk) // This is NOP if the internal buffer is full.
              chunk
            }
            val withCallBack = tappingSource.watchTermination() { (_, doneF) =>
              doneF.onComplete {
                case Success(_) =>
                  // Registers the entry, making it available for future queries. Unless the buffer was full. Then it's a NOP.
                  cachedResult.register()
                case Failure(ex) =>
                  // If the stream fails, we don't cache the result.
                  logger.warn(s"Failed streaming for $request", ex)
              }(ec)
            }
            withCallBack.mapMaterializedValue(_ => NotUsed)
        }
        // Run the final streaming result: pipe the source through a kill switch and to the gRPC response observer.
        val ks = runStreamedResult(source, request, responseObserver, maybeServerCallObs)
        // Store the kill switch so that we can cancel the stream if needed.
        killSwitchRef.set(Some(ks))
      }

    }

  /**
   * Runs the given Source[Rows, NotUsed] and streams its data to the gRPC client. The stream is connected to a
   * KillSwitch so that it can be cancelled if needed. When the stream terminates (successfully or with failure), it
   * completes the gRPC call.
   *
   * @param rowBatches the source of row batches to stream to the client
   * @param request the original executeTable request (used for logging)
   * @param responseObserver the gRPC observer used to send responses
   * @param maybeServerCallObs optionally, a ServerCallStreamObserver that supports cancellation notifications
   * @return a UniqueKillSwitch that can be used to cancel the stream if needed
   */
  private def runStreamedResult(
      rowBatches: Source[Rows, NotUsed],
      request: ExecuteTableRequest,
      responseObserver: StreamObserver[Rows],
      maybeServerCallObs: Option[ServerCallStreamObserver[Rows]]) = {

    tableInFlightGauge.increment()

    // Create a KillSwitch and integrate it within the stream
    val (killSwitch, doneF) =
      rowBatches
        .viaMat(KillSwitches.single[Rows])(Keep.right) // Keep the KillSwitch
        .toMat(Sink.foreach { rowsProto =>
          logger.debug(s"Sending ${rowsProto.getRowsCount} rows to client for planID=${request.getPlanId}.")
          // Push data to the client
          responseObserver.onNext(rowsProto)
        })(Keep.both) // Keep both KillSwitch and Future[Done]
        .run()

    // When the stream completes, either successfully or with a failure:
    doneF.onComplete {
      case Success(_) =>
        tableInFlightGauge.decrement()
        // If it's a normal completion, check if the stream was cancelled
        logger.debug(s"Streaming completed successfully for planID=${request.getPlanId}.")
        maybeServerCallObs match {
          case Some(sco) if !sco.isCancelled =>
            sco.onCompleted()
          case _ =>
            // Not a ServerCallStreamObserver or already cancelled
            responseObserver.onCompleted()
        }
      case Failure(ex) =>
        tableInFlightGauge.decrement()
        // If the stream fails or is forcibly shutdown, log it and call onError if not cancelled
        logger.error(s"Error during streaming for planID=${request.getPlanId}.", ex)
        maybeServerCallObs match {
          case Some(sco) if !sco.isCancelled =>
            sco.onError(new StatusRuntimeException(Status.INTERNAL.withCause(ex)))
          case _ =>
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(ex)))
          // If cancelled, no need to call onError (client is gone).
        }
    }(ec)

    // Return the KillSwitch
    killSwitch
  }

  /**
   * Retrieves the unique columns of the specified table.
   *
   * @param request The request containing table ID.
   * @param responseObserver The observer to send responses.
   */
  override def getTableUniqueColumn(
      request: GetTableUniqueColumnRequest,
      responseObserver: StreamObserver[GetTableUniqueColumnResponse]): Unit = withMetrics("getTableUniqueColumn") {
    logger.debug(s"Fetching unique columns for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val response = GetTableUniqueColumnResponse.newBuilder().setColumn(table.uniqueColumn).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Unique column information sent successfully.")
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
      responseObserver: StreamObserver[GetBulkInsertTableSizeResponse]): Unit = withMetrics("getBulkInsertTableSize") {
    logger.debug(s"Fetching bulk insert size for Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val batchSize = table.bulkInsertBatchSize()
      val response = GetBulkInsertTableSizeResponse.newBuilder().setSize(batchSize).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
      logger.debug("Bulk insert size retrieved successfully.")
    }
  }

  /**
   * Inserts a row into the specified table.
   *
   * @param request The request containing the row to be inserted.
   * @param responseObserver The observer to send responses.
   */
  override def insertTable(request: InsertTableRequest, responseObserver: StreamObserver[InsertTableResponse]): Unit =
    withMetrics("insertTable") {
      logger.debug(s"Inserting row into Table ID: ${request.getTableId.getName}")
      withTable(request.getDasId, request.getTableId, responseObserver) { table =>
        val row = table.insert(request.getRow)
        responseObserver.onNext(InsertTableResponse.newBuilder().setRow(row).build())
        responseObserver.onCompleted()
        logger.debug("Row inserted successfully.")
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
      responseObserver: StreamObserver[BulkInsertTableResponse]): Unit = withMetrics("bulkInsertTable") {
    logger.debug(s"Performing bulk insert into Table ID: ${request.getTableId.getName}")
    withTable(request.getDasId, request.getTableId, responseObserver) { table =>
      val rows = table.bulkInsert(request.getRowsList)
      responseObserver.onNext(BulkInsertTableResponse.newBuilder().addAllRows(rows).build())
      responseObserver.onCompleted()
      logger.debug("Bulk insert completed successfully.")
    }
  }

  /**
   * Updates rows in the specified table based on the unique columns and new values provided.
   *
   * @param request The request containing the unique columns and new values.
   * @param responseObserver The observer to send responses.
   */
  override def updateTable(request: UpdateTableRequest, responseObserver: StreamObserver[UpdateTableResponse]): Unit =
    withMetrics("updateTable") {
      logger.debug(s"Updating rows in Table ID: ${request.getTableId.getName}")
      withTable(request.getDasId, request.getTableId, responseObserver) { table =>
        val newRow = table.update(request.getRowId, request.getNewRow)
        responseObserver.onNext(UpdateTableResponse.newBuilder().setRow(newRow).build())
        responseObserver.onCompleted()
        logger.debug("Rows updated successfully.")
      }
    }

  /**
   * Deletes rows from the specified table based on the unique columns provided.
   *
   * @param request The request containing the unique columns.
   * @param responseObserver The observer to send responses.
   */
  override def deleteTable(request: DeleteTableRequest, responseObserver: StreamObserver[DeleteTableResponse]): Unit =
    withMetrics("deleteTable") {
      logger.debug(s"Deleting rows from Table ID: ${request.getTableId.getName}")
      withTable(request.getDasId, request.getTableId, responseObserver) { table =>
        table.delete(request.getRowId)
        responseObserver.onNext(DeleteTableResponse.getDefaultInstance)
        responseObserver.onCompleted()
        logger.debug("Rows deleted successfully.")
      }
    }

  private def withDAS(DASId: DASId, responseObserver: StreamObserver[_])(f: DASSdk => Unit): Unit = {
    provider.getDAS(DASId) match {
      case None =>
        // We use 'NOT_FOUND' so that the client doesn't confuse that error with a user-visible error.
        responseObserver.onError(Status.NOT_FOUND.withDescription("DAS not found").asRuntimeException())
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

  private def withTable(DASId: DASId, table: TableId, responseObserver: StreamObserver[_])(
      f: DASTable => Unit): Unit = {
    withDAS(DASId, responseObserver) { das =>
      val tableName = table.getName
      das.getTable(tableName).toScala match {
        case None =>
          logger.error(s"Table $tableName not found.")
          // If we're here, the table wasn't found although Postgres thought it was. The DAS was restarted and has now
          // fewer tables than before. We return an error to the client.
          responseObserver.onError(
            Status.INVALID_ARGUMENT
              .withDescription(s"Table $tableName not found")
              .asRuntimeException())
        case Some(table) => f(table)
      }
    }
  }

}
