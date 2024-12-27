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

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

class ExpressionEvaluatorSpec extends AnyFunSpec with Matchers {

  // -----------------------------------
  // Helper functions to build Rows/Columns/Values
  // -----------------------------------

  private def buildColumn(name: String, value: Value): Column =
    Column.newBuilder().setName(name).setData(value).build()

  private def buildRow(columns: Column*): Row =
    Row.newBuilder().addAllColumns(columns.asJava).build()

  private def buildIntValue(i: Int): Value =
    Value.newBuilder().setInt(ValueInt.newBuilder().setV(i)).build()

  private def buildStringValue(s: String): Value =
    Value.newBuilder().setString(ValueString.newBuilder().setV(s)).build()

  private def buildBoolValue(b: Boolean): Value =
    Value.newBuilder().setBool(ValueBool.newBuilder().setV(b)).build()

  private def buildDoubleValue(d: Double): Value =
    Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(d)).build()

  private def buildNullValue(): Value =
    Value.newBuilder().setNull(ValueNull.getDefaultInstance).build()

  private def buildDecimalValue(str: String): Value =
    Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV(str)).build()

  private def buildBinaryValue(bytes: Array[Byte]): Value =
    Value.newBuilder().setBinary(ValueBinary.newBuilder().setV(com.google.protobuf.ByteString.copyFrom(bytes))).build()

  private def buildDateValue(year: Int, month: Int, day: Int): Value =
    Value.newBuilder().setDate(ValueDate.newBuilder().setYear(year).setMonth(month).setDay(day)).build()

  private def buildTimeValue(h: Int, m: Int, s: Int, n: Int): Value =
    Value.newBuilder().setTime(ValueTime.newBuilder().setHour(h).setMinute(m).setSecond(s).setNano(n)).build()

  private def buildTimestampValue(year: Int, month: Int, day: Int, hour: Int, min: Int, sec: Int, nano: Int): Value =
    Value
      .newBuilder()
      .setTimestamp(
        ValueTimestamp
          .newBuilder()
          .setYear(year)
          .setMonth(month)
          .setDay(day)
          .setHour(hour)
          .setMinute(min)
          .setSecond(sec)
          .setNano(nano))
      .build()

  private def buildIntervalValue(yrs: Int, months: Int, days: Int, hrs: Int, mins: Int, secs: Int, micros: Int): Value =
    Value
      .newBuilder()
      .setInterval(
        ValueInterval
          .newBuilder()
          .setYears(yrs)
          .setMonths(months)
          .setDays(days)
          .setHours(hrs)
          .setMinutes(mins)
          .setSeconds(secs)
          .setMicros(micros))
      .build()

  private def buildRecordValue(fields: (String, Value)*): Value = {
    val recordBuilder = ValueRecord.newBuilder()
    fields.foreach { case (name, v) =>
      recordBuilder.addAtts(ValueRecordAttr.newBuilder().setName(name).setValue(v))
    }
    Value.newBuilder().setRecord(recordBuilder).build()
  }

  private def buildListValue(values: Value*): Value = {
    val listBuilder = ValueList.newBuilder()
    values.foreach(listBuilder.addValues)
    Value.newBuilder().setList(listBuilder).build()
  }

  // Utility to build a Row with one column named `name` containing a string `value`.
  private def buildStringColumn(name: String, value: String): Row = {
    buildRow(buildColumn(name, buildStringValue(value)))
  }

  // For convenience, define a helper to build a BinaryOp(Operator.LIKE, colRef, Literal(...))
  private def likeExpr(op: Operator, colName: String, pattern: String): Expression = {
    BinaryOp(op, ColumnRef(colName), Literal(buildStringValue(pattern)))
  }

  // -----------------------------------
  // Tests
  // -----------------------------------
  describe("ExpressionEvaluator") {

    it("should evaluate a ColumnRef correctly") {
      val row = buildRow(buildColumn("colA", buildIntValue(42)), buildColumn("colB", buildStringValue("Hello")))

      val exprA = ColumnRef("colA")
      val exprB = ColumnRef("colB")

      ExpressionEvaluator.evaluateExpression(row, exprA) shouldBe IntVal(42)
      ExpressionEvaluator.evaluateExpression(row, exprB) shouldBe StringVal("Hello")
    }

    it("should evaluate a Literal correctly (simple types)") {
      val intLiteral = Literal(buildIntValue(100))
      val strLiteral = Literal(buildStringValue("abc"))
      val nullLiteral = Literal(buildNullValue())

      ExpressionEvaluator.evaluateExpression(null, intLiteral) shouldBe IntVal(100)
      ExpressionEvaluator.evaluateExpression(null, strLiteral) shouldBe StringVal("abc")
      ExpressionEvaluator.evaluateExpression(null, nullLiteral) shouldBe NullVal
    }

    it("should evaluate a Literal correctly (date/time/interval)") {
      val dateLiteral = Literal(buildDateValue(2024, 12, 31))
      val timeLiteral = Literal(buildTimeValue(23, 59, 59, 999999999))
      val timestampLiteral = Literal(buildTimestampValue(2024, 12, 31, 23, 59, 59, 999999999))
      val intervalLiteral = Literal(buildIntervalValue(1, 2, 3, 4, 5, 6, 100))

      ExpressionEvaluator.evaluateExpression(null, dateLiteral) shouldBe DateVal(2024, 12, 31)
      ExpressionEvaluator.evaluateExpression(null, timeLiteral) shouldBe TimeVal(23, 59, 59, 999999999)
      ExpressionEvaluator.evaluateExpression(null, timestampLiteral) shouldBe
      TimestampVal(2024, 12, 31, 23, 59, 59, 999999999)
      ExpressionEvaluator.evaluateExpression(null, intervalLiteral) shouldBe
      IntervalVal(1, 2, 3, 4, 5, 6, 100)
    }

    it("should evaluate a Literal correctly (decimal/binary)") {
      val decimalLit = Literal(buildDecimalValue("123.456"))
      val binaryLit = Literal(buildBinaryValue(Array[Byte](1, 2, 3)))

      // Decimal
      ExpressionEvaluator.evaluateExpression(null, decimalLit) shouldBe DecimalVal(BigDecimal("123.456"))

      // Binary
      val result = ExpressionEvaluator.evaluateExpression(null, binaryLit)
      result shouldBe a[BinaryVal]
      // Compare the array contents explicitly:
      java.util.Arrays.equals(result.asInstanceOf[BinaryVal].v, Array[Byte](1, 2, 3)) shouldBe true
    }

    it("should evaluate a Literal correctly (record/list)") {
      val record = buildRecordValue("id" -> buildIntValue(1), "name" -> buildStringValue("John"))
      val list = buildListValue(buildIntValue(10), buildIntValue(20), buildIntValue(30))

      val recordExpr = Literal(record)
      val listExpr = Literal(list)

      val recResult = ExpressionEvaluator.evaluateExpression(null, recordExpr)
      recResult shouldBe a[RecordVal]
      val recordVal = recResult.asInstanceOf[RecordVal]
      recordVal.atts.map(_._1) shouldBe Seq("id", "name")
      recordVal.atts.map(_._2) shouldBe Seq(IntVal(1), StringVal("John"))

      val listResult = ExpressionEvaluator.evaluateExpression(null, listExpr)
      listResult shouldBe a[ListVal]
      listResult.asInstanceOf[ListVal].vals shouldBe Seq(IntVal(10), IntVal(20), IntVal(30))
    }

    it("should handle arithmetic operators (PLUS, MINUS, TIMES, DIV, MOD)") {
      val row = buildRow(buildColumn("x", buildIntValue(10)), buildColumn("y", buildIntValue(3)))

      def binOp(op: Operator, left: Expression, right: Expression): Expression = BinaryOp(op, left, right)

      // plus
      ExpressionEvaluator.evaluateExpression(
        row,
        binOp(Operator.PLUS, ColumnRef("x"), ColumnRef("y"))) shouldBe DecimalVal(BigDecimal(13))

      // minus
      ExpressionEvaluator.evaluateExpression(
        row,
        binOp(Operator.MINUS, ColumnRef("x"), ColumnRef("y"))) shouldBe DecimalVal(BigDecimal(7))

      // times
      ExpressionEvaluator.evaluateExpression(
        row,
        binOp(Operator.TIMES, ColumnRef("x"), ColumnRef("y"))) shouldBe DecimalVal(BigDecimal(30))

      // div
      val actual = ExpressionEvaluator.evaluateExpression(row, BinaryOp(Operator.DIV, ColumnRef("x"), ColumnRef("y")))
      actual shouldBe a[DecimalVal]
      val actualDecimal = actual.asInstanceOf[DecimalVal].v
      actualDecimal.setScale(10, BigDecimal.RoundingMode.HALF_UP) shouldBe BigDecimal("3.3333333333")

      // mod
      ExpressionEvaluator.evaluateExpression(
        row,
        binOp(Operator.MOD, ColumnRef("x"), ColumnRef("y"))) shouldBe DecimalVal(
        BigDecimal(10).remainder(BigDecimal(3)))
    }

    it("should handle division by zero for DIV and MOD") {
      val row = buildRow(buildColumn("x", buildIntValue(10)), buildColumn("z", buildIntValue(0)))

      def binOp(op: Operator): Expression = BinaryOp(op, ColumnRef("x"), ColumnRef("z"))

      // div by zero => NullVal
      ExpressionEvaluator.evaluateExpression(row, binOp(Operator.DIV)) shouldBe NullVal

      // mod by zero => NullVal
      ExpressionEvaluator.evaluateExpression(row, binOp(Operator.MOD)) shouldBe NullVal
    }

    it("should handle comparison operators") {
      val row = buildRow(
        buildColumn("a", buildIntValue(5)),
        buildColumn("b", buildIntValue(5)),
        buildColumn("c", buildIntValue(10)))

      def cmp(op: Operator, l: Expression, r: Expression) = BinaryOp(op, l, r)

      // EQUALS
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.EQUALS, ColumnRef("a"), ColumnRef("b"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.EQUALS, ColumnRef("a"), ColumnRef("c"))) shouldBe BoolVal(false)

      // NOT_EQUALS
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.NOT_EQUALS, ColumnRef("a"), ColumnRef("b"))) shouldBe BoolVal(false)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.NOT_EQUALS, ColumnRef("a"), ColumnRef("c"))) shouldBe BoolVal(true)

      // GREATER_THAN
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.GREATER_THAN, ColumnRef("c"), ColumnRef("b"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.GREATER_THAN, ColumnRef("b"), ColumnRef("c"))) shouldBe BoolVal(false)

      // GREATER_THAN_OR_EQUAL
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.GREATER_THAN_OR_EQUAL, ColumnRef("b"), ColumnRef("a"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.GREATER_THAN_OR_EQUAL, ColumnRef("a"), ColumnRef("c"))) shouldBe BoolVal(false)

      // LESS_THAN
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.LESS_THAN, ColumnRef("a"), ColumnRef("c"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.LESS_THAN, ColumnRef("c"), ColumnRef("b"))) shouldBe BoolVal(false)

      // LESS_THAN_OR_EQUAL
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.LESS_THAN_OR_EQUAL, ColumnRef("a"), ColumnRef("b"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        cmp(Operator.LESS_THAN_OR_EQUAL, ColumnRef("c"), ColumnRef("b"))) shouldBe BoolVal(false)
    }

    it("should handle plain substring or exact match if pattern uses % explicitly") {
      val row = buildStringColumn("txt", "HelloWorld")

      // 'HelloWorld' LIKE 'Hello%' => true
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "Hello%")) shouldBe BoolVal(true)

      // 'HelloWorld' LIKE '%World' => true
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "%World")) shouldBe BoolVal(true)

      // 'HelloWorld' LIKE '%Hello%' => true (substring in the middle)
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "%Hello%")) shouldBe BoolVal(true)

      // 'HelloWorld' LIKE 'world%' => false (case sensitive by default)
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "world%")) shouldBe BoolVal(false)
    }

    it("should support underscore _ as a single-character wildcard") {
      val row = buildStringColumn("txt", "HelloWorld")

      // 'HelloWorld' LIKE 'HelloWorl_' => true => the underscore matches exactly one character 'd'
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "HelloWorl_")) shouldBe BoolVal(true)

      // 'HelloWorld' LIKE 'Hell_' => false => pattern is only 5 chars total, cannot match 'Hello'
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "Hell_")) shouldBe BoolVal(false)

      // 'HelloWorld' LIKE '%orl_' => true => underscores one char 'd' at the end
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.LIKE, "txt", "%orl_")) shouldBe BoolVal(true)
    }

    it("should handle NOT_LIKE inversely") {
      val row = buildStringColumn("txt", "HelloWorld")

      // 'HelloWorld' NOT LIKE 'Hello%' => false
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.NOT_LIKE, "txt", "Hello%")) shouldBe BoolVal(false)

      // 'HelloWorld' NOT LIKE '%hello%' => true (case sensitive mismatch)
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.NOT_LIKE, "txt", "%hello%")) shouldBe BoolVal(true)
    }

    it("should handle ILIKE case-insensitively") {
      val row = buildStringColumn("txt", "HelloWorld")

      // 'HelloWorld' ILIKE '%world' => true => ignoring case
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.ILIKE, "txt", "%world")) shouldBe BoolVal(true)

      // 'HelloWorld' ILIKE 'hell_%' => true => ignoring case
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.ILIKE, "txt", "hell_%")) shouldBe BoolVal(true)

      // 'HelloWorld' ILIKE 'HELLO%' => true
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.ILIKE, "txt", "HELLO%")) shouldBe BoolVal(true)
    }

    it("should handle NOT_ILIKE the same but negated") {
      val row = buildStringColumn("txt", "HelloWorld")

      // 'HelloWorld' NOT ILIKE '%world' => false
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.NOT_ILIKE, "txt", "%world")) shouldBe BoolVal(false)

      // 'HelloWorld' NOT ILIKE 'xxx%' => true => does not match
      ExpressionEvaluator.evaluateExpression(row, likeExpr(Operator.NOT_ILIKE, "txt", "xxx%")) shouldBe BoolVal(true)
    }

    it("should handle AND and OR operators") {
      val row = buildRow(
        buildColumn("b1", buildBoolValue(true)),
        buildColumn("b2", buildBoolValue(false)),
        buildColumn("b3", buildBoolValue(true)))

      def boolOp(op: Operator, l: Expression, r: Expression) = BinaryOp(op, l, r)

      // AND
      ExpressionEvaluator.evaluateExpression(
        row,
        boolOp(Operator.AND, ColumnRef("b1"), ColumnRef("b3"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        boolOp(Operator.AND, ColumnRef("b1"), ColumnRef("b2"))) shouldBe BoolVal(false)
      ExpressionEvaluator.evaluateExpression(
        row,
        boolOp(Operator.AND, ColumnRef("b2"), ColumnRef("b3"))) shouldBe BoolVal(false)

      // OR
      ExpressionEvaluator.evaluateExpression(
        row,
        boolOp(Operator.OR, ColumnRef("b1"), ColumnRef("b2"))) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(
        row,
        boolOp(Operator.OR, ColumnRef("b2"), ColumnRef("b2"))) shouldBe BoolVal(false)
    }

    it("should apply Postgres 3-valued logic for AND/OR with null booleans") {
      // bTrue = true
      // bFalse = false
      // bNull = null
      val row = buildRow(
        buildColumn("bTrue", buildBoolValue(true)),
        buildColumn("bFalse", buildBoolValue(false)),
        buildColumn("bNull", buildNullValue()))

      // Helper to build AND/OR expressions easily
      def and(l: String, r: String): Expression =
        BinaryOp(Operator.AND, ColumnRef(l), ColumnRef(r))
      def or(l: String, r: String): Expression =
        BinaryOp(Operator.OR, ColumnRef(l), ColumnRef(r))

      // 1) AND
      // true AND null => null
      ExpressionEvaluator.evaluateExpression(row, and("bTrue", "bNull")) shouldBe NullVal
      // false AND null => false
      ExpressionEvaluator.evaluateExpression(row, and("bFalse", "bNull")) shouldBe BoolVal(false)
      // null AND null => null
      ExpressionEvaluator.evaluateExpression(row, and("bNull", "bNull")) shouldBe NullVal
      // true AND false => false
      ExpressionEvaluator.evaluateExpression(row, and("bTrue", "bFalse")) shouldBe BoolVal(false)
      // false AND true => false
      ExpressionEvaluator.evaluateExpression(row, and("bFalse", "bTrue")) shouldBe BoolVal(false)
      // true AND true => true
      ExpressionEvaluator.evaluateExpression(row, and("bTrue", "bTrue")) shouldBe BoolVal(true)

      // 2) OR
      // true OR null => true
      ExpressionEvaluator.evaluateExpression(row, or("bTrue", "bNull")) shouldBe BoolVal(true)
      // false OR null => null
      ExpressionEvaluator.evaluateExpression(row, or("bFalse", "bNull")) shouldBe NullVal
      // null OR null => null
      ExpressionEvaluator.evaluateExpression(row, or("bNull", "bNull")) shouldBe NullVal
      // true OR false => true
      ExpressionEvaluator.evaluateExpression(row, or("bTrue", "bFalse")) shouldBe BoolVal(true)
      // false OR true => true
      ExpressionEvaluator.evaluateExpression(row, or("bFalse", "bTrue")) shouldBe BoolVal(true)
      // false OR false => false
      ExpressionEvaluator.evaluateExpression(row, or("bFalse", "bFalse")) shouldBe BoolVal(false)
    }

    it("should raise an error (or produce null) if AND/OR operands are not boolean") {
      // e.g., iVal = 99 is not BoolVal
      val row = buildRow(buildColumn("bTrue", buildBoolValue(true)), buildColumn("iVal", buildIntValue(99)))

      // If your evaluator *throws* for non-bool, test that:
      intercept[UnsupportedExpressionError] {
        ExpressionEvaluator.evaluateExpression(row, BinaryOp(Operator.AND, ColumnRef("bTrue"), ColumnRef("iVal")))
      }.getMessage should include("Unsupported types for AND")

      // OR, if your evaluator returns NullVal for non-boolean, you can test that instead:
      // ExpressionEvaluator.evaluateExpression(
      //   row, BinaryOp(Operator.AND, ColumnRef("bTrue"), ColumnRef("iVal"))
      // ) shouldBe NullVal
    }

    it("should compare RecordVal and ListVal properly") {
      // We'll compare: record1 == record2 is true if all fields match, etc.
      val recordVal1 = buildRecordValue("k1" -> buildIntValue(1), "k2" -> buildIntValue(2))
      val recordVal2 = buildRecordValue("k1" -> buildIntValue(1), "k2" -> buildIntValue(2))
      val recordVal3 = buildRecordValue("k1" -> buildIntValue(1), "k2" -> buildIntValue(99))

      val listVal1 = buildListValue(buildIntValue(1), buildIntValue(2), buildIntValue(3))
      val listVal2 = buildListValue(buildIntValue(1), buildIntValue(2), buildIntValue(3))
      val listVal3 = buildListValue(buildIntValue(1), buildIntValue(2), buildIntValue(999))

      def eqExpr(l: Value, r: Value) =
        BinaryOp(Operator.EQUALS, Literal(l), Literal(r))

      ExpressionEvaluator.evaluateExpression(null, eqExpr(recordVal1, recordVal2)) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(null, eqExpr(recordVal1, recordVal3)) shouldBe BoolVal(false)
      ExpressionEvaluator.evaluateExpression(null, eqExpr(listVal1, listVal2)) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(null, eqExpr(listVal1, listVal3)) shouldBe BoolVal(false)
    }

    it("should compare BinaryVal properly") {
      val bin1 = buildBinaryValue(Array[Byte](1, 2, 3))
      val bin2 = buildBinaryValue(Array[Byte](1, 2, 3))
      val bin3 = buildBinaryValue(Array[Byte](1, 2, 4))

      def eqExpr(l: Value, r: Value) = BinaryOp(Operator.EQUALS, Literal(l), Literal(r))

      ExpressionEvaluator.evaluateExpression(null, eqExpr(bin1, bin2)) shouldBe BoolVal(true)
      ExpressionEvaluator.evaluateExpression(null, eqExpr(bin1, bin3)) shouldBe BoolVal(false)
    }

    it("should throw an error for UNRECOGNIZED operators") {
      intercept[IllegalArgumentException] {
        ExpressionEvaluator.evaluateExpression(
          null,
          BinaryOp(Operator.UNRECOGNIZED, Literal(buildIntValue(1)), Literal(buildIntValue(2))))
      }.getMessage should include("Unrecognized operator")
    }

    it("should handle NullVal in comparisons properly") {
      val row = buildRow(buildColumn("valA", buildNullValue()))
      val exprEquals = BinaryOp(Operator.EQUALS, ColumnRef("valA"), Literal(buildIntValue(0)))
      val exprLess = BinaryOp(Operator.LESS_THAN, ColumnRef("valA"), Literal(buildIntValue(0)))

      ExpressionEvaluator.evaluateExpression(row, exprEquals) shouldBe BoolVal(false)
      ExpressionEvaluator.evaluateExpression(row, exprLess) shouldBe BoolVal(false)
    }

  } // end describe
}
