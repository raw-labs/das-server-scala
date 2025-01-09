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
import com.rawlabs.protocol.das.v1.types._

class QualSelectivityAnalyzerSpec extends AnyFunSpec with Matchers {

  describe("QualSelectivityAnalyzer") {

    // Helper to build SimpleQual
    def simpleQual(colName: String, op: Operator, intValue: Int): Qual = {
      val sq = SimpleQual
        .newBuilder()
        .setOperator(op)
        .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intValue)))
        .build()

      Qual
        .newBuilder()
        .setName(colName)
        .setSimpleQual(sq)
        .build()
    }

    // Helper to build IsAnyQual
    def isAnyQual(colName: String, op: Operator, values: Int*): Qual = {
      val builder = IsAnyQual.newBuilder().setOperator(op)
      values.foreach { v =>
        builder.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(v)))
      }
      Qual
        .newBuilder()
        .setName(colName)
        .setIsAnyQual(builder.build())
        .build()
    }

    // Helper to build IsAllQual
    def isAllQual(colName: String, op: Operator, values: Int*): Qual = {
      val builder = IsAllQual.newBuilder().setOperator(op)
      values.foreach { v =>
        builder.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(v)))
      }
      Qual
        .newBuilder()
        .setName(colName)
        .setIsAllQual(builder.build())
        .build()
    }

    it("should match the provided example: old=[x>10, y=2], new=[x>20,y=2,z<3] => difference=[x>20,z<3]") {
      val oldQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 10), simpleQual("y", Operator.EQUALS, 2))
      val newQuals = Seq(
        simpleQual("x", Operator.GREATER_THAN, 20),
        simpleQual("y", Operator.EQUALS, 2),
        simpleQual("z", Operator.LESS_THAN, 3))

      val result = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      result.isDefined shouldBe true

      val diff = result.get
      // We expect x>20 and z<3
      diff.size shouldBe 2
      diff.exists(q =>
        q.getName == "x" && q.getSimpleQual.getOperator == Operator.GREATER_THAN && q.getSimpleQual.getValue.getInt.getV == 20) shouldBe true
      diff.exists(q =>
        q.getName == "z" && q.getSimpleQual.getOperator == Operator.LESS_THAN && q.getSimpleQual.getValue.getInt.getV == 3) shouldBe true
    }

    it("should match the provided example: old=[x>10, z=3], new=[x>10, k<2] => None") {
      val oldQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 10), simpleQual("z", Operator.EQUALS, 3))
      val newQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 10), simpleQual("k", Operator.LESS_THAN, 2))

      val result = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      result shouldBe None
    }

    it("should return None when newQuals is actually broader for a column (e.g. old=x>20, new=x>10)") {
      val oldQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 20))
      val newQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 10))

      // "x>10" doesn't guarantee "x>20" => so new is less restrictive
      val result = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      result shouldBe None
    }

    it(
      "should return Some(...) if the sets are the same but with an additional narrower constraint on another column") {
      val oldQuals = Seq(simpleQual("x", Operator.EQUALS, 5))
      val newQuals = Seq(simpleQual("x", Operator.EQUALS, 5), simpleQual("y", Operator.LESS_THAN, 100))

      val result = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      result.isDefined shouldBe true
      // difference should be just y<100
      val diff = result.get
      diff.size shouldBe 1
      diff.head.getName shouldBe "y"
    }

    it("should return None if newQuals does not cover an old column at all") {
      val oldQuals = Seq(simpleQual("x", Operator.GREATER_THAN, 10), simpleQual("y", Operator.EQUALS, 7))
      val newQuals = Seq(
        simpleQual("x", Operator.GREATER_THAN, 15)
        // y is not mentioned => doesn't guarantee y=7
      )

      val result = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      result shouldBe None
    }

    it("should handle IsAnyQual (ANY => ANY) set logic") {
      // old: color in {1,2,3}
      // new: color in {1,2} => that is narrower => old => new?
      // Actually, if color is in {1,2,3}, is that guaranteed to be in {1,2}? No, it might be 3 => new not satisfied.
      // So old => new is false. That means new is more restrictive, so that can't be guaranteed => None if we try the reverse.
      //
      // Let's test the direction "a => b" with the sets. We do a direct check in a smaller test.
      val oldQuals = Seq(isAnyQual("color", Operator.EQUALS, 1, 2, 3))
      val newQuals = Seq(isAnyQual("color", Operator.EQUALS, 1, 2))

      // We want to see if new is more selective. That means old => new must be true.
      // But if color=3, old is satisfied, new is not => so old => new is false => return None
      val res = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      res shouldBe None
    }

    it("should handle IsAllQual (ALL => ALL) set logic") {
      // old: x >= ALL(10,15)
      // new: x >= ALL(10,15,20)
      //   old => new is false, because if x=17, it satisfies old but not new (fails x>=20).
      //   So new is more restrictive => we can't guarantee old => new => None
      // But we want new to be at least as selective => for new to cover old, we need old => new.
      // Actually let's test the scenario the other way:
      val oldQuals = Seq(isAllQual("x", Operator.GREATER_THAN_OR_EQUAL, 10, 15))
      val newQuals = Seq(isAllQual("x", Operator.GREATER_THAN_OR_EQUAL, 10, 15, 20))
      // If x satisfies old (x >=10 and x>=15 => x>=15), that doesn't guarantee x>=20.
      // So no coverage => None
      val res = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      res shouldBe None
    }

    it("should handle IsAnyQual subset properly for a => b case where bSet is subset of aSet") {
      // old => color in {1,2,3,4}, new => color in {2,3}
      // For old => new, we are checking if new is "covered"? Actually in differenceIfMoreSelective,
      // we want each old to be covered by new => we do isCoveredBy(oldQ, newQuals), i.e. newQuals must have a qual that implies oldQ.
      // We also do the difference by isAlreadyCovered(newQ, oldQuals).
      //
      // Actually let's invert the usual logic: If a => b to hold, we want b's set to be subset of a's set for ANY.
      // But we have old=ANY(1,2,3,4) => new=ANY(2,3). This means if a value is in {1,2,3,4}, it's not necessarily in {2,3}. So a => b is false.
      // But for the differenceIfMoreSelective, we want new => old or old => new? The code does old => new. So we expect None again.
      val oldQuals = Seq(isAnyQual("color", Operator.EQUALS, 1, 2, 3, 4))
      val newQuals = Seq(isAnyQual("color", Operator.EQUALS, 2, 3))

      val res = QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)
      res shouldBe None
    }
  }
}
