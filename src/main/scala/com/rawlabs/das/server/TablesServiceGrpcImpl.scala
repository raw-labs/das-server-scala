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
 * Implementation of the gRPC service for handling table operations.
 *
 * @param provider Provides access to DAS (Data Access Service) instances.
 * @param cache Cache for storing query results.
 */
class TablesServiceGrpcImpl(provider: DASSdkManager, cache: DASResultCache)
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
      responseObserver: StreamObserver[GetTableDefinitionsResponse]
  ): Unit = {
    logger.debug(s"Fetching table definitions for DAS ID: ${request.getDasId}")
    val tableDefinitions = provider.getDAS(request.getDasId).tableDefinitions
    val response = GetTableDefinitionsResponse.newBuilder().addAllDefinitions(tableDefinitions.asJava).build()
    responseObserver.onNext(response)
    responseObserver.onCompleted()
    logger.debug("Table definitions sent successfully.")
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
