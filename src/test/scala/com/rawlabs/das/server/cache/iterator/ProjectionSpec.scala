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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

class ProjectionSpec extends AnyFlatSpec with Matchers {

  behavior of "Projection"

  it should "return an empty Row if neededCols is empty" in {
    val row = createRow(Seq("col1", "col2"))
    val neededCols = Seq.empty[String]

    val projected = Projection.project(row, neededCols)

    projected.getColumnsCount shouldBe 0
  }

  it should "filter out columns not in neededCols" in {
    // Row has col1, col2, col3
    val row = createRow(Seq("col1", "col2", "col3"))
    val neededCols = Seq("col2")

    val projected = Projection.project(row, neededCols)
    projected.getColumnsCount shouldBe 1
    projected.getColumns(0).getName shouldBe "col2"
  }

  it should "return all columns if neededCols are all present" in {
    val row = createRow(Seq("id", "name", "value"))
    val neededCols = Seq("id", "name", "value")

    val projected = Projection.project(row, neededCols)
    projected.getColumnsCount shouldBe 3
    projected.getColumns(0).getName shouldBe "id"
    projected.getColumns(1).getName shouldBe "name"
    projected.getColumns(2).getName shouldBe "value"
  }

  it should "ignore nonexistent columns in neededCols" in {
    val row = createRow(Seq("c1", "c2"))
    val neededCols = Seq("c2", "nonexistent")

    val projected = Projection.project(row, neededCols)
    projected.getColumnsCount shouldBe 1
    projected.getColumns(0).getName shouldBe "c2"
  }

  it should "handle a row with no columns" in {
    val emptyRow = Row.newBuilder().build() // no columns
    val neededCols = Seq("whatever")

    val projected = Projection.project(emptyRow, neededCols)
    projected.getColumnsCount shouldBe 0
  }

  /**
   * Helper method to create a Row with named columns only (no data).
   */
  private def createRow(colNames: Seq[String]): Row = {
    // For illustration, we set dummy data as ValueNull or something minimal
    val builder = Row.newBuilder()
    colNames.foreach { name =>
      val col = Column
        .newBuilder()
        .setName(name)
        .setData(Value.newBuilder().setNull(ValueNull.getDefaultInstance)) // or some real data
      builder.addColumns(col)
    }
    builder.build()
  }
}
