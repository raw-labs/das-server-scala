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
import com.typesafe.scalalogging.StrictLogging

/**
 * QualEvaluator uses ExpressionEvaluator to interpret each Qual as an Expression and check whether a row satisfies all
 * Quals in a Query.
 */
object QualEvaluator extends StrictLogging {

  /**
   * Returns true if the row satisfies ALL the qualifiers in `query.quals`.
   *
   * Implementation strategy:
   *   1. Convert each Qual into an Expression that yields a boolean. 2. Combine all these Expressions with an AND
   *      operation. 3. Evaluate the resulting Expression on the row using ExpressionEvaluator.
   */
  def satisfiesAllQuals(row: Row, quals: Seq[Qual]): Boolean = {
    val qualExprs: Seq[Expression] =
      quals.map(qual => qualToExpression(qual))

    val qualValues = qualExprs.map { expr =>
      try {
        evaluateBooleanExpression(row, expr)
      } catch {
        case UnsupportedExpressionError(msg) =>
          // Expressions that aren't supported are considered true.
          // We log it as a warning in case it's missing support.
          logger.warn("Unsupported expression: {}", msg)
          true
      }
    }

    qualValues.forall(identity)
  }

  /**
   * Convert a single Qual (could be SimpleQual, IsAnyQual, etc.) into an Expression that yields a boolean result when
   * evaluated.
   */
  private def qualToExpression(qual: Qual): Expression = {
    qual.getQualCase match {
      case Qual.QualCase.SIMPLE_QUAL =>
        simpleQualToExpression(qual.getName, qual.getSimpleQual)

      case Qual.QualCase.IS_ANY_QUAL =>
        isAnyQualToExpression(qual.getName, qual.getIsAnyQual)

      case Qual.QualCase.IS_ALL_QUAL =>
        isAllQualToExpression(qual.getName, qual.getIsAllQual)

      case _ =>
        // Unknown or unsupported => return something that always false
        // or we can do "Literal(falseValue)"
        val falseValue = Value.newBuilder().setBool(ValueBool.newBuilder().setV(false)).build()
        Literal(falseValue)
    }
  }

  /**
   * For SimpleQual: "columnName op value"
   */
  private def simpleQualToExpression(columnName: String, sq: SimpleQual): Expression = {
    BinaryOp(sq.getOperator, ColumnRef(columnName), Literal(sq.getValue))
  }

  /**
   * For IsAnyQual: "columnName op ANY(values)" => OR chain. If there's an empty list, we might return a "false" literal
   * by convention.
   */
  private def isAnyQualToExpression(columnName: String, anyQ: IsAnyQual): Expression = {
    val operator = anyQ.getOperator
    val values = anyQ.getValuesList.asScala

    if (values.isEmpty) {
      // ANY over empty => false
      val falseVal = Value.newBuilder().setBool(ValueBool.newBuilder().setV(false)).build()
      Literal(falseVal)
    } else {
      val subExprs = values.map { v =>
        BinaryOp(operator, ColumnRef(columnName), Literal(v))
      }
      // Fold them with OR
      subExprs.reduceLeft { (acc, next) =>
        BinaryOp(Operator.OR, acc, next)
      }
    }
  }

  /**
   * For IsAllQual: "columnName op ALL(values)" => AND chain. If there's an empty list, we might interpret ALL empty =>
   * true, but that's your choice.
   */
  private def isAllQualToExpression(columnName: String, allQ: IsAllQual): Expression = {
    val operator = allQ.getOperator
    val values = allQ.getValuesList.asScala

    if (values.isEmpty) {
      // ALL over empty => true
      val trueVal = Value.newBuilder().setBool(ValueBool.newBuilder().setV(true)).build()
      Literal(trueVal)
    } else {

      val subExprs = values.map { v =>
        BinaryOp(operator, ColumnRef(columnName), Literal(v))
      }
      // Fold them with AND
      subExprs.reduceLeft { (acc, next) =>
        BinaryOp(Operator.AND, acc, next)
      }
    }
  }

  /**
   * Evaluate an Expression that should yield a bool. If it yields null, we consider it false, if it's a non-bool, we
   * log a warning and return false.
   */
  private def evaluateBooleanExpression(row: Row, expr: Expression): Boolean = {
    val result: ValueWrapper = ExpressionEvaluator.evaluateExpression(row, expr)
    result match {
      case BoolVal(v) => v
      case NullVal    => false // Null is considered false
      case _ =>
        logger.warn("Expression did not evaluate to a boolean: {}", result)
        false
    }
  }
}
