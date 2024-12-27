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

package com.rawlabs.das.server.cache.iterator

import scala.jdk.CollectionConverters._

import com.rawlabs.das.server.cache.queue.CloseableIterator
import com.rawlabs.protocol.das.v1.query.{Qual, Query}
import com.rawlabs.protocol.das.v1.tables.Row

class QueryProcessor {

  /**
   * Produces a new CloseableIterator that applies: 1) Filter (quals) 2) Project columns 3) (Ignoring sort for now)
   */
  def execute(inputRows: CloseableIterator[Row], quals: Seq[Qual], neededCols: Seq[String]): CloseableIterator[Row] = {

    // 1) Filter rows based on quals
    val filtered: CloseableIterator[Row] =
      filterRows(inputRows, quals)

    // 2) Project columns
    val projected: CloseableIterator[Row] =
      projectRows(filtered, neededCols)

    // 3) Return the final iterator
    projected
  }

  /**
   * Wraps the given `rows` in a new iterator that yields only rows passing `QualEvaluator.satisfiesAllQuals`.
   */
  private def filterRows(rows: CloseableIterator[Row], quals: Seq[Qual]): CloseableIterator[Row] = {

    new CloseableIterator[Row] {

      // We need a buffer for the next row that passes
      private var nextRowOpt: Option[Row] = None

      override def hasNext: Boolean = {
        if (nextRowOpt.isDefined) {
          // We already have a row waiting
          true
        } else {
          // Try to find the next row that satisfies the quals
          nextRowOpt = fetchNext()
          nextRowOpt.isDefined
        }
      }

      override def next(): Row = {
        if (hasNext) {
          val row = nextRowOpt.get
          nextRowOpt = None
          row
        } else {
          throw new NoSuchElementException("No more rows")
        }
      }

      /**
       * Find the next row from the underlying iterator that satisfies the quals. If none found, return None.
       */
      private def fetchNext(): Option[Row] = {
        while (rows.hasNext) {
          val candidate = rows.next()
          if (QualEvaluator.satisfiesAllQuals(candidate, quals)) {
            return Some(candidate)
          }
        }
        None
      }

      override def close(): Unit = rows.close()
    }
  }

  /**
   * Wraps the given `rows` in a new iterator that applies column projection to each row on-the-fly.
   */
  private def projectRows(rows: CloseableIterator[Row], neededCols: Seq[String]): CloseableIterator[Row] = {

    new CloseableIterator[Row] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): Row = {
        val originalRow = rows.next()
        // apply projection
        Projection.project(originalRow, neededCols)
      }

      override def close(): Unit = rows.close()
    }
  }
}
