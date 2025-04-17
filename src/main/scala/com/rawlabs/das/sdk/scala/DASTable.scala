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

package com.rawlabs.das.sdk.scala

import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkUnsupportedException}
import com.rawlabs.protocol.das.v1.common.Environment
import com.rawlabs.protocol.das.v1.query.{PathKey, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.Row
import com.rawlabs.protocol.das.v1.types.Value

object DASTable {

  /** Represents the expected size of a table: (expectedNumberOfRows, avgRowWidthInBytes). */
  final case class TableEstimate(expectedNumberOfRows: Int, avgRowWidthBytes: Int)
}

trait DASTable {

  /**
   * Estimate the size of the table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @return (expected_number_of_rows (long), avg_row_width (in bytes))
   */
  def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate

  /**
   * Get the sort orders supported by the table.
   *
   * @param sortKeys list of sort keys to consider
   * @return list of supported sort keys
   */
  def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = Seq.empty

  /**
   * Get the path keys supported by the table.
   *
   * @return list of path keys
   */
  def getTablePathKeys: Seq[PathKey] = Seq.empty

  /**
   * Explain the execution plan.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a list of explanation lines
   */
  def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long],
      maybeEnv: Option[Environment]): Seq[String] =
    Seq.empty

  /**
   * Execute a query operation on a table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a closeable iterator of rows
   */
  def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long],
      maybeEnv: Option[Environment]): DASExecuteResult

  /**
   * Unique column of the table, if any. This is used to identify rows in the table in case of update or delete
   * operations.
   *
   * @return unique column name
   */
  def uniqueColumn: String = throw new DASSdkUnsupportedException

  /**
   * Batch size for bulk table modification operations.
   *
   * @return batch size
   */
  def bulkInsertBatchSize: Int = 1

  /**
   * Insert a row into the table.
   *
   * @param row the row to insert
   * @return the inserted row
   */
  def insert(row: Row): Row = throw new DASSdkUnsupportedException

  /**
   * Bulk insert rows into the table.
   *
   * @param rows the rows to insert
   * @return the inserted rows
   */
  def bulkInsert(rows: Seq[Row]): Seq[Row] =
    throw new DASSdkUnsupportedException

  /**
   * Update a row in the table.
   *
   * @param rowId the row id to update (the value of the column defined in `uniqueColumn()`)
   * @param newValues the new values to update
   * @return the updated row
   */
  def update(rowId: Value, newValues: Row): Row =
    throw new DASSdkUnsupportedException

  /**
   * Delete a row from the table.
   *
   * @param rowId the row id to delete (the value of the column defined in `uniqueColumn()`)
   */
  def delete(rowId: Value): Unit = throw new DASSdkUnsupportedException

}
