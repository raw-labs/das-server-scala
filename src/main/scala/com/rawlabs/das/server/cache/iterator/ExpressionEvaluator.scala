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

import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

/**
 * Example expression AST. In a real system, you might generate this AST from your Protobuf (e.g., if Query references
 * an expression tree).
 */
sealed trait Expression
case class ColumnRef(name: String) extends Expression
case class Literal(value: Value) extends Expression
case class BinaryOp(operator: Operator, left: Expression, right: Expression) extends Expression

/** Internal typed values. */
sealed trait ValueWrapper
sealed trait NumericVal extends ValueWrapper

case object NullVal extends ValueWrapper

case class ByteVal(v: Byte) extends NumericVal
case class ShortVal(v: Short) extends NumericVal
case class IntVal(v: Int) extends NumericVal
case class LongVal(v: Long) extends NumericVal
case class FloatVal(v: Float) extends NumericVal
case class DoubleVal(v: Double) extends NumericVal
case class DecimalVal(v: BigDecimal) extends NumericVal

case class BoolVal(v: Boolean) extends ValueWrapper
case class StringVal(v: String) extends ValueWrapper
case class BinaryVal(v: Array[Byte]) extends ValueWrapper

case class DateVal(year: Int, month: Int, day: Int) extends ValueWrapper
case class TimeVal(hour: Int, minute: Int, second: Int, nano: Int) extends ValueWrapper
case class TimestampVal(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nano: Int)
    extends ValueWrapper
case class IntervalVal(years: Int, months: Int, days: Int, hours: Int, minutes: Int, seconds: Int, micros: Int)
    extends ValueWrapper

case class RecordVal(atts: Seq[(String, ValueWrapper)]) extends ValueWrapper
case class ListVal(vals: Seq[ValueWrapper]) extends ValueWrapper

case class UnsupportedExpressionError(msg: String) extends Exception(msg)

object ExpressionEvaluator {

  // ============================
  // 1) Evaluate an Expression
  // ============================
  def evaluateExpression(row: Row, expr: Expression): ValueWrapper = expr match {
    case ColumnRef(name) =>
      getColumnValueWrapper(row, name)

    case Literal(v) =>
      toValueWrapper(v)

    case BinaryOp(op, leftExpr, rightExpr) =>
      val lhs = evaluateExpression(row, leftExpr)
      val rhs = evaluateExpression(row, rightExpr)
      applyOperator(op, lhs, rhs)
  }

  // ============================
  // 2) Row -> Column Value
  // ============================
  private def getColumnValueWrapper(row: Row, colName: String): ValueWrapper = {
    val colOpt = row.getColumnsList.asScala.find(_.getName == colName)
    colOpt match {
      case Some(col) => toValueWrapper(col.getData)
      case None      => throw UnsupportedExpressionError(s"Column not found: $colName")
    }
  }

  // ============================
  // 3) Convert Protobuf Value -> ValueWrapper
  // ============================
  private def toValueWrapper(value: Value): ValueWrapper = {
    if (value == null || value.getValueCase == Value.ValueCase.NULL) {
      NullVal
    } else
      value.getValueCase match {
        case Value.ValueCase.BYTE    => ByteVal(value.getByte.getV.toByte)
        case Value.ValueCase.SHORT   => ShortVal(value.getShort.getV.toShort)
        case Value.ValueCase.INT     => IntVal(value.getInt.getV)
        case Value.ValueCase.LONG    => LongVal(value.getLong.getV)
        case Value.ValueCase.FLOAT   => FloatVal(value.getFloat.getV)
        case Value.ValueCase.DOUBLE  => DoubleVal(value.getDouble.getV)
        case Value.ValueCase.DECIMAL => DecimalVal(BigDecimal(value.getDecimal.getV))
        case Value.ValueCase.BOOL    => BoolVal(value.getBool.getV)
        case Value.ValueCase.STRING  => StringVal(value.getString.getV)
        case Value.ValueCase.BINARY  => BinaryVal(value.getBinary.getV.toByteArray)
        case Value.ValueCase.DATE =>
          DateVal(value.getDate.getYear, value.getDate.getMonth, value.getDate.getDay)
        case Value.ValueCase.TIME =>
          TimeVal(value.getTime.getHour, value.getTime.getMinute, value.getTime.getSecond, value.getTime.getNano)
        case Value.ValueCase.TIMESTAMP =>
          TimestampVal(
            value.getTimestamp.getYear,
            value.getTimestamp.getMonth,
            value.getTimestamp.getDay,
            value.getTimestamp.getHour,
            value.getTimestamp.getMinute,
            value.getTimestamp.getSecond,
            value.getTimestamp.getNano)
        case Value.ValueCase.INTERVAL =>
          IntervalVal(
            value.getInterval.getYears,
            value.getInterval.getMonths,
            value.getInterval.getDays,
            value.getInterval.getHours,
            value.getInterval.getMinutes,
            value.getInterval.getSeconds,
            value.getInterval.getMicros)
        case Value.ValueCase.RECORD =>
          val pairs = value.getRecord.getAttsList.asScala.map { att =>
            att.getName -> toValueWrapper(att.getValue)
          }
          RecordVal(pairs.toSeq)
        case Value.ValueCase.LIST =>
          val lst = value.getList.getValuesList.asScala.map(toValueWrapper)
          ListVal(lst.toSeq)
        case _ =>
          throw UnsupportedExpressionError(s"Unsupported value type: ${value.getValueCase}")
      }
  }

  // ============================
  // 4) Apply an Operator to Two ValueWrappers
  // ============================
  import Operator._

  private def applyOperator(op: Operator, lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = op match {
    // -------- Arithmetic (return numeric) --------
    case PLUS  => evalPlus(lhs, rhs)
    case MINUS => evalMinus(lhs, rhs)
    case TIMES => evalTimes(lhs, rhs)
    case DIV   => evalDiv(lhs, rhs)
    case MOD   => evalMod(lhs, rhs)

    // -------- Comparison (return bool) --------
    case EQUALS =>
      BoolVal(evalEquals(lhs, rhs))
    case NOT_EQUALS =>
      BoolVal(!evalEquals(lhs, rhs))
    case GREATER_THAN =>
      BoolVal(evalGreaterThan(lhs, rhs))
    case GREATER_THAN_OR_EQUAL =>
      BoolVal(evalGreaterThan(lhs, rhs) || evalEquals(lhs, rhs))
    case LESS_THAN =>
      BoolVal(evalLessThan(lhs, rhs))
    case LESS_THAN_OR_EQUAL =>
      BoolVal(evalLessThan(lhs, rhs) || evalEquals(lhs, rhs))

    // -------- LIKE / ILIKE (return bool) --------
    case LIKE =>
      BoolVal(evalLike(lhs, rhs, caseInsensitive = false))
    case NOT_LIKE =>
      BoolVal(!evalLike(lhs, rhs, caseInsensitive = false))
    case ILIKE =>
      BoolVal(evalLike(lhs, rhs, caseInsensitive = true))
    case NOT_ILIKE =>
      BoolVal(!evalLike(lhs, rhs, caseInsensitive = true))

    // -------- Logical (bool -> bool) --------
    case AND =>
      evalAnd(lhs, rhs)
    case OR =>
      evalOr(lhs, rhs)

    case UNRECOGNIZED =>
      throw new IllegalArgumentException(s"Unrecognized operator: $op")
  }

  // 4.1 Arithmetic
  private def evalPlus(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (n1: NumericVal, n2: NumericVal) =>
        DecimalVal(numericToBigDecimal(n1) + numericToBigDecimal(n2))
      case _ => throw UnsupportedExpressionError(s"Unsupported types for PLUS: $lhs, $rhs")
    }
  }

  private def evalMinus(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (n1: NumericVal, n2: NumericVal) =>
        DecimalVal(numericToBigDecimal(n1) - numericToBigDecimal(n2))
      case _ => throw UnsupportedExpressionError(s"Unsupported types for MINUS: $lhs, $rhs")
    }
  }

  private def evalTimes(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (n1: NumericVal, n2: NumericVal) =>
        DecimalVal(numericToBigDecimal(n1) * numericToBigDecimal(n2))
      case _ => throw UnsupportedExpressionError(s"Unsupported types for TIMES: $lhs, $rhs")
    }
  }

  private def evalDiv(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (n1: NumericVal, n2: NumericVal) =>
        val d2 = numericToBigDecimal(n2)
        if (d2 == 0) NullVal
        else DecimalVal(numericToBigDecimal(n1) / d2)
      case _ => throw UnsupportedExpressionError(s"Unsupported types for DIV: $lhs, $rhs")
    }
  }

  private def evalMod(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (n1: NumericVal, n2: NumericVal) =>
        val d2 = numericToBigDecimal(n2)
        if (d2 == 0) NullVal
        else DecimalVal(numericToBigDecimal(n1).remainder(d2.bigDecimal))
      case _ => throw UnsupportedExpressionError(s"Unsupported types for MOD: $lhs, $rhs")
    }
  }

  private def numericToBigDecimal(n: NumericVal): BigDecimal = n match {
    case ByteVal(v)    => BigDecimal(v)
    case ShortVal(v)   => BigDecimal(v)
    case IntVal(v)     => BigDecimal(v)
    case LongVal(v)    => BigDecimal(v)
    case FloatVal(v)   => BigDecimal.decimal(v.toDouble)
    case DoubleVal(v)  => BigDecimal.decimal(v)
    case DecimalVal(v) => v
  }

  // 4.2 Comparison
  private def evalEquals(lhs: ValueWrapper, rhs: ValueWrapper): Boolean = {
    (lhs, rhs) match {
      case (NullVal, NullVal) => true
      case (NullVal, _)       => false
      case (_, NullVal)       => false

      case (ByteVal(a), ByteVal(b))       => a == b
      case (ShortVal(a), ShortVal(b))     => a == b
      case (IntVal(a), IntVal(b))         => a == b
      case (LongVal(a), LongVal(b))       => a == b
      case (FloatVal(a), FloatVal(b))     => a == b
      case (DoubleVal(a), DoubleVal(b))   => a == b
      case (DecimalVal(a), DecimalVal(b)) => a == b
      case (BoolVal(a), BoolVal(b))       => a == b
      case (StringVal(a), StringVal(b))   => a == b
      case (BinaryVal(a), BinaryVal(b))   => java.util.Arrays.equals(a, b)

      case (DateVal(y1, m1, d1), DateVal(y2, m2, d2)) =>
        (y1 == y2) && (m1 == m2) && (d1 == d2)

      case (TimeVal(h1, m1, s1, n1), TimeVal(h2, m2, s2, n2)) =>
        h1 == h2 && m1 == m2 && s1 == s2 && n1 == n2

      case (TimestampVal(y1, mo1, d1, h1, mi1, s1, n1), TimestampVal(y2, mo2, d2, h2, mi2, s2, n2)) =>
        y1 == y2 && mo1 == mo2 && d1 == d2 &&
        h1 == h2 && mi1 == mi2 && s1 == s2 && n1 == n2

      case (IntervalVal(yrs1, mon1, da1, hr1, min1, sec1, mic1), IntervalVal(yrs2, mon2, da2, hr2, min2, sec2, mic2)) =>
        yrs1 == yrs2 && mon1 == mon2 && da1 == da2 &&
        hr1 == hr2 && min1 == min2 && sec1 == sec2 && mic1 == mic2

      case (RecordVal(a), RecordVal(b)) =>
        a.size == b.size && a.zip(b).forall { case ((an, av), (bn, bv)) =>
          an == bn && evalEquals(av, bv)
        }

      case (ListVal(xs), ListVal(ys)) =>
        xs.size == ys.size && xs.zip(ys).forall { case (x, y) =>
          evalEquals(x, y)
        }

      case _ => throw UnsupportedExpressionError(s"Unsupported types for EQUALS: $lhs, $rhs")
    }
  }

  private def evalLessThan(lhs: ValueWrapper, rhs: ValueWrapper): Boolean = {
    (lhs, rhs) match {
      case (NullVal, _) => false
      case (_, NullVal) => false

      // Numeric
      case (n1: NumericVal, n2: NumericVal) =>
        numericToBigDecimal(n1) < numericToBigDecimal(n2)

      // Strings
      case (StringVal(s1), StringVal(s2)) =>
        s1 < s2
      case (DateVal(y1, m1, d1), DateVal(y2, m2, d2)) =>
        (y1 < y2) || (y1 == y2 && m1 < m2) || (y1 == y2 && m1 == m2 && d1 < d2)
      case (TimestampVal(y1, mo1, d1, h1, mi1, s1, n1), TimestampVal(y2, mo2, d2, h2, mi2, s2, n2)) =>
        (y1 < y2) || (y1 == y2 && mo1 < mo2) || (y1 == y2 && mo1 == mo2 && d1 < d2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 < h2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 < mi2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 < s2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 == s2 && n1 < n2)
      case _ => throw UnsupportedExpressionError(s"Unsupported types for LESS_THAN: $lhs, $rhs")
    }
  }

  private def evalGreaterThan(lhs: ValueWrapper, rhs: ValueWrapper): Boolean = {
    (lhs, rhs) match {
      case (NullVal, _) => false
      case (_, NullVal) => false
      case (n1: NumericVal, n2: NumericVal) =>
        numericToBigDecimal(n1) > numericToBigDecimal(n2)
      case (StringVal(s1), StringVal(s2)) =>
        s1 > s2
      case (DateVal(y1, m1, d1), DateVal(y2, m2, d2)) =>
        (y1 > y2) || (y1 == y2 && m1 > m2) || (y1 == y2 && m1 == m2 && d1 > d2)
      case (TimestampVal(y1, mo1, d1, h1, mi1, s1, n1), TimestampVal(y2, mo2, d2, h2, mi2, s2, n2)) =>
        (y1 > y2) || (y1 == y2 && mo1 > mo2) || (y1 == y2 && mo1 == mo2 && d1 > d2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 > h2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 > mi2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 > s2) ||
        (y1 == y2 && mo1 == mo2 && d1 == d2 && h1 == h2 && mi1 == mi2 && s1 == s2 && n1 > n2)
      case _ => throw UnsupportedExpressionError(s"Unsupported types for GREATER_THAN: $lhs, $rhs")
    }
  }

  // 4.3 LIKE / ILIKE
  private def evalLike(lhs: ValueWrapper, rhs: ValueWrapper, caseInsensitive: Boolean): Boolean = {
    (lhs, rhs) match {
      case (StringVal(a), StringVal(b)) =>
        if (caseInsensitive) a.toLowerCase.contains(b.toLowerCase) else a.contains(b)
      case _ => throw UnsupportedExpressionError(s"Unsupported types for LIKE/ILIKE: $lhs, $rhs")
    }
  }

  // 4.4 Boolean AND / OR
  private def evalAnd(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (BoolVal(a), BoolVal(b)) => BoolVal(a && b)
      case _                        => throw UnsupportedExpressionError(s"Unsupported types for AND: $lhs, $rhs")
    }
  }

  private def evalOr(lhs: ValueWrapper, rhs: ValueWrapper): ValueWrapper = {
    (lhs, rhs) match {
      case (BoolVal(a), BoolVal(b)) => BoolVal(a || b)
      case _                        => throw UnsupportedExpressionError(s"Unsupported types for OR: $lhs, $rhs")
    }
  }
}
