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

package com.rawlabs.das.postgresql

import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.rawlabs.protocol.das.v1.types.Value
import com.rawlabs.protocol.das.v1.types.Value.ValueCase

class DASPostgresqlTable(backend: PostgresqlBackend, defn: TableDefinition) extends DASTable {

  /**
   * Estimate the size of the table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @return (expected_number_of_rows (long), avg_row_width (in bytes))
   */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): TableEstimate = {
    // 1) Build the same WHERE clause used in `execute(...)`.
    val whereClause =
      if (quals.isEmpty) ""
      else " WHERE " + quals.map(qualToSql).mkString(" AND ")

    // 2) Possibly use columns if you want to estimate only the subset of columns,
    // or just use `*` to get the overall row width estimate from the DB.
    val selectClause =
      if (columns.isEmpty) "1"
      else columns.map(quoteIdentifier).mkString(", ")

    val tableName = quoteIdentifier(defn.getTableId.getName)
    val sql = s"SELECT $selectClause FROM $tableName$whereClause"
    backend.estimate(sql)
  }

  /**
   * Execute a query operation on a table.
   *
   * @param quals filters applied
   * @param columns columns to return
   * @param sortKeys sort keys to apply
   * @return a closeable iterator of rows
   */
  override def execute(quals: Seq[Qual], columns: Seq[String], sortKeys: Seq[SortKey]): DASExecuteResult = {

    // Build SELECT list
    val selectClause =
      if (columns.isEmpty) "1"
      else columns.map(c => quoteIdentifier(c)).mkString(", ")

    // Build WHERE from `quals`
    val whereClause =
      if (quals.isEmpty) ""
      else " WHERE " + quals.map(qualToSql).mkString(" AND ")

    // Build ORDER BY
    val orderByClause =
      if (sortKeys.isEmpty) ""
      else " ORDER BY " + sortKeys.map(sortKeyToSql).mkString(", ")

    val tableName = quoteIdentifier(defn.getTableId.getName)
    val sql = s"SELECT $selectClause FROM $tableName$whereClause$orderByClause"

    println(s"Executing SQL: $sql")

    backend.execute(sql)
  }

  private def quoteIdentifier(ident: String): String = {
    // naive approach
    s""""$ident""""
  }

  private def sortKeyToSql(sk: SortKey): String = {
    // e.g. "colName DESC NULLS FIRST"
    val col = quoteIdentifier(sk.getName)
    val direction = if (sk.getIsReversed) "DESC" else "ASC"

    // Some DBs only allow "NULLS FIRST/LAST" with some directions, or might not support it at all.
    // If your DB does support it:
    val nullsPart = if (sk.getNullsFirst) " NULLS FIRST" else " NULLS LAST"

    // We can also handle collate if needed:
    val collation = if (sk.getCollate.nonEmpty) s" COLLATE ${sk.getCollate}" else ""

    s"$col$collation $direction$nullsPart"
  }

  private def valueToSql(v: Value): String = {

    v.getValueCase match {
      case ValueCase.NULL    => "NULL"
      case ValueCase.BYTE    => v.getByte.getV.toString
      case ValueCase.SHORT   => v.getShort.getV.toString
      case ValueCase.INT     => v.getInt.getV.toString
      case ValueCase.LONG    => v.getLong.getV.toString
      case ValueCase.FLOAT   => v.getFloat.getV.toString
      case ValueCase.DOUBLE  => v.getDouble.getV.toString
      case ValueCase.DECIMAL =>
        // typically a string representing numeric, might need quoting or not
        v.getDecimal.getV
      case ValueCase.BOOL =>
        // some DBs want 'TRUE'/'FALSE', or 1/0
        if (v.getBool.getV) "TRUE" else "FALSE"
      case ValueCase.STRING =>
        // single-quote or escape
        s"'${escape(v.getString.getV)}'"
      case ValueCase.BINARY =>
        // In some DBs, you'd do e.g. E'\\xDEADBEAF' in Postgres
        // or 0xDEADBEAF in MySQL
        // We'll do a simple fallback string
        val bytes = v.getBinary.getV.toByteArray
        s"'${byteArrayToPostgresHex(bytes)}'"

      case ValueCase.DATE =>
        // e.g. '2024-01-15'
        val d = v.getDate
        f"'${d.getYear}%04d-${d.getMonth}%02d-${d.getDay}%02d'"

      case ValueCase.TIME =>
        // e.g. '10:30:25'
        val t = v.getTime
        f"'${t.getHour}%02d:${t.getMinute}%02d:${t.getSecond}%02d'"

      case ValueCase.TIMESTAMP =>
        // '2024-01-15 10:30:25'
        val ts = v.getTimestamp
        f"'${ts.getYear}%04d-${ts.getMonth}%02d-${ts.getDay}%02d ${ts.getHour}%02d:${ts.getMinute}%02d:${ts.getSecond}%02d'"

      case ValueCase.INTERVAL | ValueCase.RECORD | ValueCase.LIST =>
        // for a simple example, store them as string or skip:
        s"'${escape(v.toString)}'"
      case ValueCase.VALUE_NOT_SET =>
        "NULL"
    }
  }

  def byteArrayToPostgresHex(byteArray: Array[Byte]): String = {
    // Convert each byte to a 2-character hexadecimal string
    val hexString = byteArray.map("%02x".format(_)).mkString
    // Prefix with PostgreSQL BYTEA syntax
    s"\\x$hexString"
  }

  private def escape(str: String): String =
    str.replace("'", "''") // naive approach for single quotes

  private def operatorToSql(op: Operator): String = {
    op match {
      case Operator.EQUALS                => "="
      case Operator.NOT_EQUALS            => "<>"
      case Operator.LESS_THAN             => "<"
      case Operator.LESS_THAN_OR_EQUAL    => "<="
      case Operator.GREATER_THAN          => ">"
      case Operator.GREATER_THAN_OR_EQUAL => ">="
      case Operator.LIKE                  => "LIKE"
      case Operator.NOT_LIKE              => "NOT LIKE"
      case Operator.ILIKE                 => "ILIKE" // Some DBs (Postgres) support case-insensitive
      case Operator.NOT_ILIKE             => "NOT ILIKE"
      // For arithmetic ops, it's unclear how they'd appear in a WHERE clause, might skip or handle differently
      case Operator.PLUS  => "+" // typically not used in a "where col + 10"??
      case Operator.MINUS => "-"
      case Operator.TIMES => "*"
      case Operator.DIV   => "/"
      case Operator.MOD   => "%"
      case Operator.AND   => "AND" // typically we used these as conjunctions, so not sure in a col op?
      case Operator.OR    => "OR"
      case _              => "=" // fallback
    }
  }

  private def isAllQualToSql(colName: String, iq: IsAllQual): String = {
    val opStr = operatorToSql(iq.getOperator)
    val clauses = iq.getValuesList.asScala.map(v => s"$colName $opStr ${valueToSql(v)}")
    // Combine with AND
    clauses.mkString("(", " AND ", ")")
  }

  private def isAnyQualToSql(colName: String, iq: IsAnyQual): String = {
    val opStr = operatorToSql(iq.getOperator)
    val clauses = iq.getValuesList.asScala.map(v => s"$colName $opStr ${valueToSql(v)}")
    // Combine with OR
    clauses.mkString("(", " OR ", ")")
  }

  private def simpleQualToSql(colName: String, sq: SimpleQual): String = {
    if (sq.getValue.hasNull && sq.getOperator == Operator.EQUALS) {
      s"$colName IS NULL"
    } else if (sq.getValue.hasNull && sq.getOperator == Operator.NOT_EQUALS) {
      s"$colName IS NOT NULL"
    } else {
      val opStr = operatorToSql(sq.getOperator)
      val valStr = valueToSql(sq.getValue)
      s"$colName $opStr $valStr"
    }
  }

  private def qualToSql(q: Qual): String = {
    val colName = quoteIdentifier(q.getName)
    if (q.hasSimpleQual) simpleQualToSql(colName, q.getSimpleQual)
    else
      q.getQualCase match {
        case Qual.QualCase.IS_ANY_QUAL => isAnyQualToSql(colName, q.getIsAnyQual)
        case Qual.QualCase.IS_ALL_QUAL => isAllQualToSql(colName, q.getIsAllQual)
        case _                         => ???
      }
  }

  /**
   * @return the table definition
   */
  def definition: TableDefinition = defn
}
