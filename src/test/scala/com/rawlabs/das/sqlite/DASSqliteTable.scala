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

package com.rawlabs.das.sqlite

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.TableDefinition

class DASSqliteTable(backend: SqliteBackend, defn: TableDefinition) extends DASTable {

  /**
   * Estimate the size of the table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @return (expected_number_of_rows (long), avg_row_width (in bytes))
   */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): TableEstimate = {
    TableEstimate(100, 100)
  }

  /**
   * Execute a query operation on a table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a closeable iterator of rows
   */
  override def execute(quals: Seq[Qual], columns: Seq[String], sortKeys: Seq[SortKey]): DASExecuteResult = {
    backend.execute("SELECT * FROM " + defn.getTableId.getName)
  }

  /**
   * @return the table definition
   */
  def definition: TableDefinition = defn
}
