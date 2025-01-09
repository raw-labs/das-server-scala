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

import com.rawlabs.protocol.das.v1.tables.Row

object Projection {

  def project(row: Row, neededCols: Seq[String]): Row = {
    val filteredColumns = row.getColumnsList.asScala.filter { col =>
      neededCols.contains(col.getName)
    }
    // Build a new Row with repeatedColumn = filteredColumns
    Row.newBuilder().addAllColumns(filteredColumns.asJava).build()
  }
}
