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

import com.rawlabs.das.server.cache.queue.CloseableIterator
import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.Row

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}

/**
 * A "purely streamed" query processor that can produce either:
 *   - An Akka Streams Flow[Row, Row, NotUsed]
 *   - A legacy CloseableIterator[Row] for backward compatibility.
 */
class QueryProcessor {

  /**
   * Returns a Flow that filters by `quals` and projects columns. This is purely streaming in the reactive sense: no
   * in-memory accumulation.
   */
  def asFlow(quals: Seq[Qual], neededCols: Seq[String]): Flow[Row, Row, NotUsed] = {
    QueryProcessorFlow(quals, neededCols)
  }

  /**
   * Legacy method: produces a new CloseableIterator that applies: 1) Filter (quals) 2) Project columns 3) (Ignoring
   * sort for now)
   *
   * This preserves your existing interface for tests. It's already lazy, so it doesn't load all rows at once in memory.
   */
  def execute(inputRows: CloseableIterator[Row], quals: Seq[Qual], neededCols: Seq[String]): CloseableIterator[Row] = {

    // 1) Filter
    val filtered = filterRows(inputRows, quals)

    // 2) Project
    val projected = projectRows(filtered, neededCols)

    projected
  }

  // --------------------------------------------------------------------------
  // Private "lazy" filter, same as original
  // --------------------------------------------------------------------------
  private def filterRows(rows: CloseableIterator[Row], quals: Seq[Qual]): CloseableIterator[Row] = {

    new CloseableIterator[Row] {
      private var nextRowOpt: Option[Row] = None

      override def hasNext: Boolean = {
        if (nextRowOpt.isDefined) true
        else {
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

  // --------------------------------------------------------------------------
  // Private "lazy" projection, same as original
  // --------------------------------------------------------------------------
  private def projectRows(rows: CloseableIterator[Row], neededCols: Seq[String]): CloseableIterator[Row] = {

    new CloseableIterator[Row] {
      override def hasNext: Boolean = rows.hasNext

      override def next(): Row = {
        val originalRow = rows.next()
        Projection.project(originalRow, neededCols)
      }

      override def close(): Unit = rows.close()
    }
  }
}
