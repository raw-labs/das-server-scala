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

import com.rawlabs.protocol.das.v1.query.Qual
import com.rawlabs.protocol.das.v1.tables.Row

import akka.NotUsed
import akka.stream.scaladsl.Flow

object QueryProcessorFlow {

  /**
   * A flow that filters by `quals` (dropping rows not satisfying them), then projects columns.
   */
  def apply(quals: Seq[Qual], neededCols: Seq[String]): Flow[Row, Row, NotUsed] =
    Flow[Row]
      .filter(row => QualEvaluator.satisfiesAllQuals(row, quals))
      .map(row => Projection.project(row, neededCols))

  /**
   * If you prefer two separate flows: .via(QueryProcessorFlow.filterFlow(quals))
   * .via(QueryProcessorFlow.projectionFlow(neededCols))
   */
  def filterFlow(quals: Seq[Qual]): Flow[Row, Row, NotUsed] =
    Flow[Row].filter(row => QualEvaluator.satisfiesAllQuals(row, quals))

  def projectionFlow(neededCols: Seq[String]): Flow[Row, Row, NotUsed] =
    Flow[Row].map(row => Projection.project(row, neededCols))
}
