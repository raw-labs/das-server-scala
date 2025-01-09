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

package com.rawlabs.das.sdk;

import com.rawlabs.protocol.das.v1.query.PathKey;
import com.rawlabs.protocol.das.v1.query.Qual;
import com.rawlabs.protocol.das.v1.query.SortKey;
import com.rawlabs.protocol.das.v1.tables.Row;
import com.rawlabs.protocol.das.v1.types.Value;
import java.util.Collections;
import java.util.List;

/** Represents a table in the DAS. */
public interface DASTable {

  /** Represents the expected size of a table: (expectedNumberOfRows, avgRowWidthInBytes). */
  class TableEstimate {
    private final long expectedNumberOfRows;
    private final int avgRowWidthBytes;

    public TableEstimate(long expectedNumberOfRows, int avgRowWidthBytes) {
      this.expectedNumberOfRows = expectedNumberOfRows;
      this.avgRowWidthBytes = avgRowWidthBytes;
    }

    public long getExpectedNumberOfRows() {
      return expectedNumberOfRows;
    }

    public int getAvgRowWidthBytes() {
      return avgRowWidthBytes;
    }
  }

  /**
   * Estimate the size of the result table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @return (expected_number_of_rows (long), avg_row_width (in bytes))
   */
  TableEstimate getTableEstimate(List<Qual> quals, List<String> columns);

  /**
   * Get the sort orders supported by the table.
   *
   * @param sortKeys list of sort keys to consider
   * @return list of supported sort keys
   */
  default List<SortKey> getTableSortOrders(List<SortKey> sortKeys) {
    return Collections.emptyList();
  }

  /**
   * Get the path keys supported by the table.
   *
   * @return list of path keys
   */
  default List<PathKey> getTablePathKeys() {
    return Collections.emptyList();
  }

  /**
   * Explain the execution plan.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a list of explanation lines
   */
  default List<String> explain(
      List<Qual> quals,
      List<String> columns,
      List<SortKey> sortKeys) {
    return Collections.emptyList();
  }

  /**
   * Execute a query operation on a table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a closeable iterator of rows
   */
  DASExecuteResult execute(
      List<Qual> quals,
      List<String> columns,
      List<SortKey> sortKeys);

  /**
   * Unique column of the table, if any. This is used to identify rows in the table in case of
   * update or delete operations.
   *
   * @return unique column name
   */
  default String uniqueColumn() {
    throw new DASSdkUnsupportedException();
  }

  /**
   * Batch size for bulk table insert modification operations.
   *
   * @return batch size
   */
  default int bulkInsertBatchSize() {
    return 1;
  }

  /**
   * Insert a row into the table.
   *
   * @param row the row to insert
   * @return the inserted row
   */
  default Row insert(Row row) {
    throw new DASSdkUnsupportedException();
  }

  /**
   * Bulk insert rows into the table.
   *
   * @param rows the rows to insert
   * @return the inserted rows
   */
  default List<Row> bulkInsert(List<Row> rows) {
    throw new DASSdkUnsupportedException();
  }

  /**
   * Update a row in the table.
   *
   * @param rowId the row id to update (the value of the column defined in `uniqueColumn()`)
   * @param newValues the new values to update
   * @return the updated row
   */
  default Row update(Value rowId, Row newValues) {
    throw new DASSdkUnsupportedException();
  }

  /**
   * Delete a row from the table.
   *
   * @param rowId the row id to delete (the value of the column defined in `uniqueColumn()`)
   */
  default void delete(Value rowId) {
    throw new DASSdkUnsupportedException();
  }
}
