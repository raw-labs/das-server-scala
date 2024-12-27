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

class QualEvaluatorSpec extends AnyFunSpec with Matchers {

  // -----------------------------------
  // Helper builders (same as in your ExpressionEvaluatorSpec, or adapted)
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

  private def buildNullValue(): Value =
    Value.newBuilder().setNull(ValueNull.getDefaultInstance).build()

  // Build a SimpleQual
  private def buildSimpleQual(op: Operator, value: Value): SimpleQual =
    SimpleQual.newBuilder().setOperator(op).setValue(value).build()

  // Build an IsAnyQual
  private def buildIsAnyQual(op: Operator, values: Value*): IsAnyQual = {
    val builder = IsAnyQual.newBuilder().setOperator(op)
    values.foreach(builder.addValues)
    builder.build()
  }

  // Build an IsAllQual
  private def buildIsAllQual(op: Operator, values: Value*): IsAllQual = {
    val builder = IsAllQual.newBuilder().setOperator(op)
    values.foreach(builder.addValues)
    builder.build()
  }

  // Build a Qual container with either SimpleQual, IsAnyQual, IsAllQual, etc.
  private def buildSimpleQualContainer(colName: String, op: Operator, value: Value): Qual =
    Qual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(buildSimpleQual(op, value))
      .build()

  private def buildIsAnyQualContainer(colName: String, op: Operator, values: Value*): Qual =
    Qual
      .newBuilder()
      .setName(colName)
      .setIsAnyQual(buildIsAnyQual(op, values: _*))
      .build()

  private def buildIsAllQualContainer(colName: String, op: Operator, values: Value*): Qual =
    Qual
      .newBuilder()
      .setName(colName)
      .setIsAllQual(buildIsAllQual(op, values: _*))
      .build()

  describe("QualEvaluator") {

    it("should return true if there are no quals") {
      val row = buildRow(buildColumn("colA", buildIntValue(10)))
      val emptyQuals: Seq[Qual] = Seq.empty
      QualEvaluator.satisfiesAllQuals(row, emptyQuals) shouldBe true
    }

    it("should handle a SIMPLE_QUAL with EQUALS operator") {
      val row = buildRow(buildColumn("colA", buildIntValue(42)), buildColumn("colB", buildStringValue("Hello")))

      // colA == 42
      val qual: Qual = buildSimpleQualContainer("colA", Operator.EQUALS, buildIntValue(42))
      QualEvaluator.satisfiesAllQuals(row, Seq(qual)) shouldBe true

      // colA == 999  => false
      val qualFalse: Qual = buildSimpleQualContainer("colA", Operator.EQUALS, buildIntValue(999))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualFalse)) shouldBe false
    }

    it("should handle a SIMPLE_QUAL with GREATER_THAN operator") {
      val row = buildRow(buildColumn("age", buildIntValue(30)))

      val qual: Qual = buildSimpleQualContainer("age", Operator.GREATER_THAN, buildIntValue(20))
      QualEvaluator.satisfiesAllQuals(row, Seq(qual)) shouldBe true

      val qualFalse: Qual = buildSimpleQualContainer("age", Operator.GREATER_THAN, buildIntValue(50))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualFalse)) shouldBe false
    }

    it("should handle multiple SIMPLE_QUALS combined (logical AND)") {
      val row = buildRow(buildColumn("colX", buildIntValue(10)), buildColumn("colY", buildStringValue("Hello")))

      // colX == 10 AND colY == "Hello"
      val qualX = buildSimpleQualContainer("colX", Operator.EQUALS, buildIntValue(10))
      val qualY = buildSimpleQualContainer("colY", Operator.EQUALS, buildStringValue("Hello"))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualX, qualY)) shouldBe true

      // colX == 10 AND colY == "nope"
      val qualYFalse = buildSimpleQualContainer("colY", Operator.EQUALS, buildStringValue("nope"))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualX, qualYFalse)) shouldBe false
    }

    it("should handle IS_ANY_QUAL properly (OR chain)") {
      val row = buildRow(buildColumn("color", buildStringValue("green")))

      // color == ANY("red", "green", "blue") => green is in that set => true
      val qualAny: Qual = buildIsAnyQualContainer(
        "color",
        Operator.EQUALS,
        buildStringValue("red"),
        buildStringValue("green"),
        buildStringValue("blue"))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualAny)) shouldBe true

      // color == ANY("red", "purple") => green not in that set => false
      val qualAnyFalse: Qual =
        buildIsAnyQualContainer("color", Operator.EQUALS, buildStringValue("red"), buildStringValue("purple"))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualAnyFalse)) shouldBe false
    }

    it("should handle empty IS_ANY_QUAL as false") {
      val row = buildRow(buildColumn("age", buildIntValue(10)))

      // age == ANY([]) => false by definition
      val anyEmpty: Qual = Qual
        .newBuilder()
        .setName("age")
        .setIsAnyQual(buildIsAnyQual(Operator.EQUALS)) // no values => empty
        .build()

      QualEvaluator.satisfiesAllQuals(row, Seq(anyEmpty)) shouldBe false
    }

    it("should handle IS_ALL_QUAL properly (AND chain)") {
      val row = buildRow(buildColumn("score", buildIntValue(100)))

      // score >= ALL(50, 70, 99) => 100 >= 50, 70, 99 => true
      val qualAll: Qual = buildIsAllQualContainer(
        "score",
        Operator.GREATER_THAN_OR_EQUAL,
        buildIntValue(50),
        buildIntValue(70),
        buildIntValue(99))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualAll)) shouldBe true

      // score >= ALL(50, 150) => fails on 150 => false
      val qualAllFalse: Qual =
        buildIsAllQualContainer("score", Operator.GREATER_THAN_OR_EQUAL, buildIntValue(50), buildIntValue(150))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualAllFalse)) shouldBe false
    }

    it("should handle empty IS_ALL_QUAL as true") {
      val row = buildRow(buildColumn("age", buildIntValue(10)))

      // age == ALL([]) => true by definition
      val allEmpty: Qual = Qual
        .newBuilder()
        .setName("age")
        .setIsAllQual(buildIsAllQual(Operator.EQUALS)) // no values => empty
        .build()

      QualEvaluator.satisfiesAllQuals(row, Seq(allEmpty)) shouldBe true
    }

    it("should handle unknown or unsupported QualCase by returning false") {
      val row = buildRow(buildColumn("whatever", buildIntValue(0)))

      // create a Qual with an unsupported QualCase
      val builder = Qual.newBuilder()
      // set something that isn't SIMPLE_QUAL, IS_ANY_QUAL, IS_ALL_QUAL:
      // e.g., set a random field or let it remain empty => default case
      // or set an unrecognized field. For safety:
      builder.setName("whatever")
      // Not setting qualCase => we get Qual.QualCase.QUAL_NOT_SET in most protos

      val unknownQual = builder.build()

      QualEvaluator.satisfiesAllQuals(row, Seq(unknownQual)) shouldBe false
    }

    it("should combine multiple Quals of different types with AND logic") {
      val row = buildRow(buildColumn("city", buildStringValue("New York")), buildColumn("temp", buildIntValue(80)))

      // city == "New York" AND (temp == ANY(78, 79, 80))
      val qualCity = buildSimpleQualContainer("city", Operator.EQUALS, buildStringValue("New York"))
      val qualTempAny =
        buildIsAnyQualContainer("temp", Operator.EQUALS, buildIntValue(78), buildIntValue(79), buildIntValue(80))

      // Both are satisfied => overall true
      QualEvaluator.satisfiesAllQuals(row, Seq(qualCity, qualTempAny)) shouldBe true

      // If we change city => false
      val qualCityFalse = buildSimpleQualContainer("city", Operator.EQUALS, buildStringValue("Los Angeles"))
      QualEvaluator.satisfiesAllQuals(row, Seq(qualCityFalse, qualTempAny)) shouldBe false
    }

    it("should treat an expression returning false bool as overall false in satisfiesAllQuals") {
      // Just a quick check that if we get a false boolean expression, we get false
      val row = buildRow(buildColumn("dummy", buildIntValue(999)))

      // dummy > 1000 => false
      val qual = buildSimpleQualContainer("dummy", Operator.GREATER_THAN, buildIntValue(1000))
      QualEvaluator.satisfiesAllQuals(row, Seq(qual)) shouldBe false
    }
  }

}
