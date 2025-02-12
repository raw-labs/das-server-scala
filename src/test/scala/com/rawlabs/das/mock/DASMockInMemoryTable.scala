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

package com.rawlabs.das.mock

import scala.jdk.CollectionConverters._

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Operator
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.query.SortKey
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

class DASMockInMemoryTable(private val dasMockStorage: DASMockStorage) extends DASTable with StrictLogging {

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate =
    DASTable.TableEstimate(dasMockStorage.size, 200)

  override def uniqueColumn: String = dasMockStorage.key

  /**
   * A SELECT statement is executed by calling the execute method. Quals, colums, sortKeys, limit help to filter,
   * project, sort and limit the results. Implementing Quals, colums and limit is optional, but they can be useful to
   * optimize the query execution. Example: lets assume that this execute method calls an API that supports filtering,
   * projection and limiting. If the params are ignored, the API will return all the rows in the table and postgres will
   * do the filtering, projection and limiting. If the params are used and propagated to the backend of the API, the
   * results will be filtered, projected and limited, and the data volume transferred will be minimized. If sortKeys are
   * not empty, the execute method **has to** return the results sorted by the keys in the same order as the keys in the
   * sortKeys param, postgres won't do it. sortKeys are correlated to canSort method. Depending on what canSort returns,
   * this param will either contain the keys or be empty.
   * @param quals \- Qualifiers for filtering the rows
   * @param columns \- Columns to project
   * @param maybeSortKeys \- Sort keys. The sort keys are correlated to canSort method. Depending on what canSort
   *   returns, this param will either contain the keys or be empty. If this param has values, then the execute method
   *   **has to** return the results sorted by the keys in the same order as the keys in the sortKeys param, postgres
   *   will assume that the results are sorted.
   * @param maybeLimit \- Limit the number of rows returned
   * @return
   */
  override def execute(quals: Seq[Qual], columns: Seq[String], sortKeys: Seq[SortKey], maybeLimit: Option[Long]): DASExecuteResult = {
    logger.info(s"Executing query with quals: $quals, columns: $columns, sortKeys: $sortKeys")

    new DASExecuteResult {
      private val iterator = dasMockStorage.iterator
      private var currentIndex: Int = 1
      private var currentRow: Option[Row] = None
      private var hasNextValue: Boolean = false
      override def close(): Unit = {}

      // hasNext is called multiple times, before next is called
      override def hasNext: Boolean = {
        if (currentRow.isEmpty) {
          currentRow = getNextRow(quals, iterator)
          hasNextValue = currentRow.isDefined && currentIndex <= dasMockStorage.size
        }
        hasNextValue
      }
      override def next(): Row = {
        currentIndex += 1
        val result = currentRow.get
        currentRow = None
        result
      }
    }
  }

  private def getNextRow(quals: Seq[Qual], iterator: Iterator[Row]): Option[Row] = {
    while (iterator.hasNext) {
      val row = iterator.next()
      if (allPredicatesHold(quals, row)) return Some(row)
    }
    None
  }

  private def allPredicatesHold(quals: Seq[Qual], row: Row): Boolean =
    quals.filter(_.hasSimpleQual).forall(predicateHolds(_, row))

  private def predicateHolds(qual: Qual, row: Row): Boolean = {
    val column = qual.getName
    val value = row.getColumnsList.asScala
      .collectFirst { case c if c.getName == column => c.getData }
      .get
      .getInt
      .getV
    val operator = qual.getSimpleQual.getOperator
    val targetValue = qual.getSimpleQual.getValue.getInt.getV

    if (operator == Operator.GREATER_THAN) value > targetValue
    else if (operator == Operator.LESS_THAN) value < targetValue
    else if (operator == Operator.EQUALS) value == targetValue
    else if (operator == Operator.NOT_EQUALS) value != targetValue
    else if (operator == Operator.GREATER_THAN_OR_EQUAL) value >= targetValue
    else if (operator == Operator.LESS_THAN_OR_EQUAL) value <= targetValue
    else throw new IllegalArgumentException(s"Unsupported operator: $operator")
  }

  override def insert(row: Row): Row = dasMockStorage.add(row)

  override def bulkInsert(rows: Seq[Row]): Seq[Row] = rows.map(insert)

  override def update(rowId: Value, newValues: Row): Row = dasMockStorage
    .update(rowId.getInt.getV.toString, newValues)

  override def delete(rowId: Value): Unit = dasMockStorage
    .remove(rowId.getInt.getV.toString)

}
