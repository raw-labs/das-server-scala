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

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.query.SortKey
import com.rawlabs.protocol.das.v1.tables.Column
import com.rawlabs.protocol.das.v1.tables.Row
import com.rawlabs.protocol.das.v1.types.Value
import com.rawlabs.protocol.das.v1.types.ValueInt
import com.rawlabs.protocol.das.v1.types.ValueString
import com.typesafe.scalalogging.StrictLogging

class DASMockTable(maxRows: Int, sleepPerRowMills: Int = 0, breakOnRow: Int = -1) extends DASTable with StrictLogging {

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    DASTable.TableEstimate(maxRows, 100)
  }

  override def explain(quals: Seq[Qual], columns: Seq[String], sortKeys: Seq[SortKey]): Seq[String] = Seq.empty

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
      private var currentIndex: Int = 1

      override def close(): Unit = {}

      override def hasNext: Boolean = currentIndex <= maxRows

      override def next(): Row = {
        if (breakOnRow > 0 && currentIndex == breakOnRow) {
          throw new RuntimeException(s"Breaking on row $breakOnRow")
        }

        if (sleepPerRowMills > 0)
          Thread.sleep(sleepPerRowMills)

        if (!hasNext) throw new NoSuchElementException("No more elements")

        val row = Row
          .newBuilder()
          .addColumns(
            Column
              .newBuilder()
              .setName("column1")
              .setData(
                Value
                  .newBuilder()
                  .setInt(ValueInt.newBuilder().setV(currentIndex).build())
                  .build()))
          .addColumns(
            Column
              .newBuilder()
              .setName("column2")
              .setData(
                Value.newBuilder().setString(ValueString.newBuilder().setV(s"row_tmp_$currentIndex").build()).build()))
          .build()

//        logger.debug(s"Returning row: $row")

        currentIndex += 1
        row
      }
    }
  }

}
