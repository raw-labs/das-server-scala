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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.server.cache.queue.CloseableIterator
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

class QueryProcessorSpec extends AnyFunSpec with Matchers {

  // ------------------------------------------------------------------
  // A simple fake CloseableIterator for testing
  // ------------------------------------------------------------------
  class TestCloseableIterator(rows: Seq[Row]) extends CloseableIterator[Row] {
    private val iter = rows.iterator
    var closed = false

    override def hasNext: Boolean =
      !closed && iter.hasNext

    override def next(): Row = {
      if (closed) throw new IllegalStateException("Iterator already closed")
      iter.next()
    }

    override def close(): Unit = { closed = true }
  }

  // ------------------------------------------------------------------
  // Example Row builder utility
  // ------------------------------------------------------------------
  private def buildRow(columns: (String, Int)*): Row = {
    // Just an example that sets IntValue columns:
    val colBuilders = columns.map { case (colName, intVal) =>
      Column
        .newBuilder()
        .setName(colName)
        .setData(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal).build()))
        .build()
    }
    Row.newBuilder().addAllColumns(colBuilders.asJava).build()
  }

  // ------------------------------------------------------------------
  // Example Qual builder (SIMPLE_QUAL) for convenience
  // colName op intVal
  // ------------------------------------------------------------------
  private def simpleQual(colName: String, operator: Operator, intVal: Int): Qual = {
    val sq = SimpleQual
      .newBuilder()
      .setOperator(operator)
      .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal)).build())
      .build()

    Qual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(sq)
      .build()
  }

  // ------------------------------------------------------------------
  // Mocks for Projection and QualEvaluator if needed
  // (If you have the real classes, remove these stubs)
  // ------------------------------------------------------------------
  object Projection {
    def project(originalRow: Row, neededCols: Seq[String]): Row = {
      // For simplicity, filter out columns not in neededCols
      val newCols = originalRow.getColumnsList.asScala.filter(col => neededCols.contains(col.getName))
      Row.newBuilder().addAllColumns(newCols.asJava).build()
    }
  }

  object QualEvaluator {
    def satisfiesAllQuals(row: Row, quals: Seq[Qual]): Boolean = {
      // For this example, handle only SIMPLE_QUAL with EQUALS or GREATER_THAN
      // You can replace with your real QualEvaluator
      for (q <- quals) {
        if (q.getQualCase == Qual.QualCase.SIMPLE_QUAL) {
          val colName = q.getName
          val sq = q.getSimpleQual
          val op = sq.getOperator
          val targetVal = sq.getValue.getInt.getV // ignoring other types
          val colOpt = row.getColumnsList.asScala.find(_.getName == colName)
          val colVal = colOpt.map(_.getData.getInt.getV).getOrElse(0)
          op match {
            case Operator.EQUALS                => if (colVal != targetVal) return false
            case Operator.GREATER_THAN          => if (colVal <= targetVal) return false
            case Operator.GREATER_THAN_OR_EQUAL => if (colVal < targetVal) return false
            case _                              => return false
          }
        } else {
          // unknown qual => false
          return false
        }
      }
      true
    }
  }

  // ------------------------------------------------------------------
  // Instantiate the QueryProcessor we want to test
  // ------------------------------------------------------------------
  val processor = new QueryProcessor()

  // ------------------------------------------------------------------
  // Tests
  // ------------------------------------------------------------------
  describe("QueryProcessor") {

    it("should return an iterator that filters rows and projects columns") {
      // Input rows
      val rows = Seq(
        buildRow("colA" -> 10, "colB" -> 100),
        buildRow("colA" -> 20, "colB" -> 200),
        buildRow("colA" -> 30, "colB" -> 300))

      // Create a test iterator
      val inputIter = new TestCloseableIterator(rows)

      // Qual: colA > 10
      val quals = Seq(simpleQual("colA", Operator.GREATER_THAN, 10))
      // neededCols: Only "colB"
      val neededCols = Seq("colB")

      // Execute
      val resultIter = processor.execute(inputIter, quals, neededCols)

      // Collect results
      val results = mutable.Buffer[Row]()
      while (resultIter.hasNext) {
        results += resultIter.next()
      }

      // We expect rows with colA=20 and colA=30 to pass => 2 rows
      results.size shouldBe 2

      // Each row should contain only colB in the final projection
      results.foreach { row =>
        row.getColumnsCount shouldBe 1
        row.getColumns(0).getName shouldBe "colB"
      }
    }

    it("should handle empty quals (no filtering) and project all columns") {
      val rows = Seq(buildRow("colA" -> 10, "colB" -> 100), buildRow("colA" -> 20, "colB" -> 200))
      val inputIter = new TestCloseableIterator(rows)

      // No quals => no filtering
      val quals = Seq.empty[Qual]
      val neededCols = Seq("colA", "colB")

      val resultIter = processor.execute(inputIter, quals, neededCols)
      val results = resultIterToSeq(resultIter)

      results.size shouldBe 2
      // Should keep all columns
      results(0).getColumnsCount shouldBe 2
      results(0).getColumns(0).getName shouldBe "colA"
      results(0).getColumns(1).getName shouldBe "colB"
    }

    it("should close the underlying iterator when the result iterator is closed") {
      val rows = Seq(buildRow("colA" -> 1), buildRow("colA" -> 2))
      val inputIter = new TestCloseableIterator(rows)
      val quals = Seq(simpleQual("colA", Operator.EQUALS, 1))
      val neededCols = Seq("colA")

      val resultIter = processor.execute(inputIter, quals, neededCols)

      // Check the inputIter is not closed yet
      inputIter.closed shouldBe false

      // Close the resultIter
      resultIter.close()

      // Now the original inputIter should also be closed
      inputIter.closed shouldBe true
    }

    it("should return no rows if filtering excludes everything") {
      val rows = Seq(buildRow("colA" -> 5), buildRow("colA" -> 10), buildRow("colA" -> 15))
      val inputIter = new TestCloseableIterator(rows)

      // Qual that excludes everything: colA > 100
      val quals = Seq(simpleQual("colA", Operator.GREATER_THAN, 100))
      val neededCols = Seq("colA")

      val resultIter = processor.execute(inputIter, quals, neededCols)
      val results = resultIterToSeq(resultIter)

      results shouldBe empty
    }

    it("should lazily filter rows (only fetch as needed)") {
      // This test ensures we don't read all rows ahead of time
      val rows = (1 to 10).map(i => buildRow("colX" -> i))
      val inputIter = new TestCloseableIterator(rows)

      // Qual colX >= 5
      val quals = Seq(simpleQual("colX", Operator.GREATER_THAN_OR_EQUAL, 5))
      val neededCols = Seq("colX")

      val resultIter = processor.execute(inputIter, quals, neededCols)

      // We haven't consumed anything yet, so inputIter shouldn't be closed, and the pointer hasn't advanced.
      inputIter.closed shouldBe false

      // Start reading from resultIter
      // We expect rows with colX=5..10 => 6 rows
      val firstRow = resultIter.next()
      firstRow.getColumns(0).getData.getInt.getV shouldBe 5

      // Let's read the rest
      val remain = resultIterToSeq(resultIter)
      remain.map(_.getColumns(0).getData.getInt.getV) shouldBe Seq(6, 7, 8, 9, 10)

      // All done
      resultIter.hasNext shouldBe false
    }

    it("should project zero columns if neededCols is empty, discarding others") {
      val rows = Seq(buildRow("colA" -> 1, "colB" -> 2))
      val inputIter = new TestCloseableIterator(rows)

      // Filter pass everything
      val quals = Seq.empty[Qual]
      // neededCols: empty => produce row with zero columns
      val neededCols = Seq.empty[String]

      val resultIter = processor.execute(inputIter, quals, neededCols)
      val results = resultIterToSeq(resultIter)

      results.size shouldBe 1
      results.head.getColumnsCount shouldBe 0
    }
  }

  // ------------------------------------------------------------------
  // Helper to consume a CloseableIterator into a Seq
  // ------------------------------------------------------------------
  private def resultIterToSeq(it: CloseableIterator[Row]): Seq[Row] = {
    val buf = mutable.Buffer[Row]()
    while (it.hasNext) {
      buf += it.next()
    }
    buf.toSeq
  }
}
