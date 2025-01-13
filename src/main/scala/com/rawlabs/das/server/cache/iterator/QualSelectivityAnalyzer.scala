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

import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import scala.jdk.CollectionConverters._

import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.types._

/**
 * Analyzes two sets of Quals to see if `newQuals` is "more selective" than `oldQuals`.
 *
 * "More selective" means: 1) If a row satisfies `newQuals`, then it must satisfy `oldQuals` as well (new ⇒ old). 2)
 * `newQuals` introduces at least one strictly narrower constraint not already covered by `oldQuals`.
 */
object QualSelectivityAnalyzer {

  /**
   * @param oldQuals Existing qualifiers
   * @param newQuals New qualifiers
   * @return None if `newQuals` is NOT as selective as `oldQuals`, Some(difference) if it is, where 'difference' is the
   *   subset of `newQuals` that imposes new constraints missing in `oldQuals`.
   *
   * Examples: old=[x>10, y=2], new=[x>20, y=2, z<3] => Some([x>20, z<3]) Because "x>20,y=2,z<3" ⇒ "x>10,y=2", and we
   * have new constraints (x>20,z<3) old=[x>10, z=3], new=[x>10, k<2] => None Because "x>10,k<2" does NOT imply "z=3" =>
   * fails new⇒old check.
   *
   * The returned difference can be empty if qualifiers are the same.
   */
  def differenceIfMoreSelective(oldQuals: Seq[Qual], newQuals: Seq[Qual]): Option[Seq[Qual]] = {

    // 1) Check that new⇒old, i.e. for every oldQ in oldQuals,
    //    there's some newQ in newQuals s.t. newQ => oldQ.
    //    If not, we fail => return None.
    val newImpliesOld: Boolean =
      oldQuals.forall(oldQ => isCoveredBy(oldQ, newQuals))

    if (!newImpliesOld) {
      // If new does not imply old, we cannot call new "more selective."
      return None
    }

    // 2) Among newQuals, find which ones are not already implied by oldQuals
    //    i.e. does old⇒ newQ?
    val additionalConstraints: Seq[Qual] =
      newQuals.filterNot(nq => isCoveredBy(nq, oldQuals))

    // It is possible additionalConstraints is empty. It means
    // the cache exactly covers the predicates.
    Some(additionalConstraints)
  }

  /**
   * "isCoveredBy(qual, fromSet)" checks if `fromSet` collectively implies `qual`. For simplicity, we say it suffices if
   * there's at least one fromSet-qual that individually implies `qual`.
   *
   * So we want: ∃ q_in_fromSet such that q_in_fromSet => qual.
   */
  private def isCoveredBy(qual: Qual, fromSet: Seq[Qual]): Boolean = {
    fromSet.exists(q => implies(q, qual))
  }

  /**
   * implies(a, b) = "if 'a' is satisfied, then 'b' is also satisfied." a => b. We compare their qualCase (SimpleQual,
   * IsAnyQual, IsAllQual) and see if a's operator + values can imply b's operator + values.
   */
  private def implies(a: Qual, b: Qual): Boolean = {
    // Must have same column name to reason about it
    if (a.getName != b.getName) return false

    (a.getQualCase, b.getQualCase) match {
      // Simple -> Simple
      case (Qual.QualCase.SIMPLE_QUAL, Qual.QualCase.SIMPLE_QUAL) =>
        impliesSimple(a.getSimpleQual, b.getSimpleQual)

      // ANY -> ANY
      case (Qual.QualCase.IS_ANY_QUAL, Qual.QualCase.IS_ANY_QUAL) =>
        impliesIsAny(a.getIsAnyQual, b.getIsAnyQual)

      // ALL -> ALL
      case (Qual.QualCase.IS_ALL_QUAL, Qual.QualCase.IS_ALL_QUAL) =>
        impliesIsAll(a.getIsAllQual, b.getIsAllQual)

      // Different oneofs => not handled => false
      case _ => false
    }
  }

  // ----------------------------------------------------------------
  //  SimpleQual => SimpleQual
  // ----------------------------------------------------------------
  private def impliesSimple(a: SimpleQual, b: SimpleQual): Boolean = {
    if (a.getOperator == b.getOperator) {
      // same operator => compare thresholds
      canOperatorAImplyB(a.getOperator, a.getValue, b.getOperator, b.getValue)
    } else {
      // different operators => maybe x>20 => x>10, etc.
      canOperatorAImplyB(a.getOperator, a.getValue, b.getOperator, b.getValue)
    }
  }

  // A minimal approach for numeric comparisons:
  private def canOperatorAImplyB(opA: Operator, valA: Value, opB: Operator, valB: Value): Boolean = {
    // If both are EQUALS, require same value
    (opA, opB) match {
      case (Operator.EQUALS, Operator.EQUALS) =>
        sameValue(valA, valB)

      // EQUALS => anything else: we do not assume numeric knowledge like 2 => >1
      case (Operator.EQUALS, _) => false

      // GREATER_THAN => GREATER_THAN: x>20 => x>10 if 20 >= 10
      case (Operator.GREATER_THAN, Operator.GREATER_THAN) =>
        numericImplies(valA, valB)(_ >= _)

      // GREATER_THAN => GREATER_THAN_OR_EQUAL: x>20 => x>=10 is also valid if 20 >=10
      case (Operator.GREATER_THAN, Operator.GREATER_THAN_OR_EQUAL) =>
        numericImplies(valA, valB)(_ >= _)

      // GREATER_THAN_OR_EQUAL => GREATER_THAN_OR_EQUAL
      case (Operator.GREATER_THAN_OR_EQUAL, Operator.GREATER_THAN_OR_EQUAL) =>
        numericImplies(valA, valB)(_ >= _)

      // GREATER_THAN_OR_EQUAL => GREATER_THAN
      // x>=20 => x>10 if 20>10
      case (Operator.GREATER_THAN_OR_EQUAL, Operator.GREATER_THAN) =>
        numericImplies(valA, valB)(_ > _)

      // LESS_THAN => LESS_THAN
      case (Operator.LESS_THAN, Operator.LESS_THAN) =>
        numericImplies(valA, valB)(_ <= _)

      // LESS_THAN => LESS_THAN_OR_EQUAL
      case (Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUAL) =>
        numericImplies(valA, valB)(_ <= _)

      // LESS_THAN_OR_EQUAL => LESS_THAN_OR_EQUAL
      case (Operator.LESS_THAN_OR_EQUAL, Operator.LESS_THAN_OR_EQUAL) =>
        numericImplies(valA, valB)(_ <= _)

      // LESS_THAN_OR_EQUAL => LESS_THAN
      case (Operator.LESS_THAN_OR_EQUAL, Operator.LESS_THAN) =>
        numericImplies(valA, valB)(_ < _)

      // Otherwise
      case _ => false
    }
  }

  // Utility to interpret Value as BigDecimal and apply a comparator:
  private def numericImplies(aVal: Value, bVal: Value)(cmp: (BigDecimal, BigDecimal) => Boolean): Boolean = {
    (valueAsBigDecimal(aVal), valueAsBigDecimal(bVal)) match {
      case (Some(aNum), Some(bNum)) => cmp(aNum, bNum)
      case _                        => false
    }
  }

  // Attempt to interpret a Value as BigDecimal. If not numeric, return None.
  private def valueAsBigDecimal(v: Value): Option[BigDecimal] = {
    if (v == null) return None
    v.getValueCase match {
      case Value.ValueCase.BYTE    => Some(BigDecimal(v.getByte.getV))
      case Value.ValueCase.SHORT   => Some(BigDecimal(v.getShort.getV))
      case Value.ValueCase.INT     => Some(BigDecimal(v.getInt.getV))
      case Value.ValueCase.LONG    => Some(BigDecimal(v.getLong.getV))
      case Value.ValueCase.FLOAT   => Some(BigDecimal.decimal(v.getFloat.getV.toDouble))
      case Value.ValueCase.DOUBLE  => Some(BigDecimal.decimal(v.getDouble.getV))
      case Value.ValueCase.DECIMAL => Some(BigDecimal(v.getDecimal.getV))
      case Value.ValueCase.DATE    =>
        // Convert date -> days since epoch => BigDecimal
        val d = v.getDate
        try {
          val localDate = LocalDate.of(d.getYear, d.getMonth, d.getDay)
          // toEpochDay => number of days since 1970-01-01
          val days = localDate.toEpochDay
          Some(BigDecimal(days))
        } catch {
          case _: Throwable => None
        }

      case Value.ValueCase.TIME =>
        // Convert time -> microseconds since midnight => BigDecimal
        val t = v.getTime
        try {
          // second + nano
          val totalSeconds = t.getHour * 3600L + t.getMinute * 60L + t.getSecond
          // We'll represent time of day as microseconds from midnight
          val micros = totalSeconds * 1_000_000L + (t.getNano.toLong / 1000L)
          Some(BigDecimal(micros))
        } catch {
          case _: Throwable => None
        }

      case Value.ValueCase.TIMESTAMP =>
        // Convert timestamp -> microseconds since epoch => BigDecimal
        val ts = v.getTimestamp
        try {
          val ldt =
            LocalDateTime.of(ts.getYear, ts.getMonth, ts.getDay, ts.getHour, ts.getMinute, ts.getSecond, ts.getNano)
          // Convert to epoch second in UTC.
          // Note: This ignores time zones if your data is naive. Adjust if needed.
          val epochSec = ldt.toEpochSecond(ZoneOffset.UTC)
          val micros = epochSec * 1_000_000L + (ts.getNano.toLong / 1000L)
          Some(BigDecimal(micros))
        } catch {
          case _: Throwable => None
        }
      case _ => None
    }
  }

  // Very naive equality check for EQUALS case:
  private def sameValue(a: Value, b: Value): Boolean = a == b

  // ----------------------------------------------------------------
  //  IsAnyQual => IsAnyQual
  // ----------------------------------------------------------------
  /**
   * ANY => ANY means: a => b if: a: col OP ANY(aSet), b: col OP ANY(bSet) If a is satisfied, we want b guaranteed
   * satisfied => that suggests bSet ⊆ aSet (because if col is in 'aSet', it must be in 'bSet' to also satisfy b). But
   * we must also ensure same operator.
   */
  private def impliesIsAny(a: IsAnyQual, b: IsAnyQual): Boolean = {
    if (a.getOperator != b.getOperator) return false
    val aSet = a.getValuesList.asScala.map(_.toByteString).toSet
    val bSet = b.getValuesList.asScala.map(_.toByteString).toSet
    // For a => b under ANY semantics: bSet ⊆ aSet
    bSet.subsetOf(aSet)
  }

  // ----------------------------------------------------------------
  //  IsAllQual => IsAllQual
  // ----------------------------------------------------------------
  /**
   * ALL => ALL means: a => b if a: col OP ALL(aSet), b: col OP ALL(bSet). If a is satisfied => col OP aSet => we want
   * to guarantee col OP bSet => typically that means aSet ⊆ bSet (since a has more conditions to meet => definitely
   * meets bSet's subset).
   *
   * Example: old => x >= ALL(10,15), new => x >= ALL(10,15,20) If old is satisfied (x>=10,x>=15 => x>=15) that does
   * *not* imply x>=20. So aSet= {10,15}, bSet= {10,15,20}. aSet⊆ bSet? => true => means a is stricter. But we want a =>
   * b => that fails => so we return false.
   */
  private def impliesIsAll(a: IsAllQual, b: IsAllQual): Boolean = {
    if (a.getOperator != b.getOperator) return false
    val aSet = a.getValuesList.asScala.map(_.toByteString).toSet
    val bSet = b.getValuesList.asScala.map(_.toByteString).toSet
    // For a => b under ALL semantics: aSet ⊆ bSet
    // (a must have all items that b has + possibly more).
    aSet.subsetOf(bSet)
  }

}
