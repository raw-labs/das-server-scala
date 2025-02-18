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

import java.sql.{Connection, DriverManager, ResultSet, SQLException}

import scala.jdk.CollectionConverters.IteratorHasAsScala

import com.fasterxml.jackson.databind.ObjectMapper
import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * A simple example DAS backend for PostgreSQL.
 *
 * @param url The JDBC connection string (e.g., "jdbc:postgresql://localhost:5432/mydb")
 * @param user The database user
 * @param password The password for that user
 */
class PostgresqlBackend(url: String, user: String, password: String, schema: String) extends StrictLogging {

  Class.forName("org.postgresql.Driver")

  /**
   * Returns a map of tableName -> DASPostgresTable, each containing a TableDefinition.
   */

  def tables(): Map[String, DASPostgresqlTable] = {
    val tables = scala.collection.mutable.Map.empty[String, DASPostgresqlTable]
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, user, password)

      // Confirm the schema actually exists
      val checkSchemaSql =
        s"""
           |SELECT 1
           |FROM information_schema.schemata
           |WHERE schema_name = '$schema'
       """.stripMargin

      val schemaCheckStmt = conn.createStatement()
      try {
        val rs = schemaCheckStmt.executeQuery(checkSchemaSql)
        if (!rs.next()) {
          throw new IllegalArgumentException(s"Schema not found: $schema")
        }
      } finally {
        schemaCheckStmt.close()
      }

      // List all user tables in the "public" schema (or adapt if you want all schemas).
      // We’ll skip system tables or other schemas by default.
      val listTablesSQL =
        s"""
          |SELECT tablename
          |FROM pg_tables
          |WHERE schemaname = '$schema'
        """.stripMargin

      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(listTablesSQL)
        try {
          while (rs.next()) {
            val tableName = rs.getString("tablename").toLowerCase
            tables += tableName -> new DASPostgresqlTable(this, schema, tableDefinition(conn, tableName))
          }
        } finally rs.close()
      } finally stmt.close()

      tables.toMap
    } finally {
      if (conn != null) {
        try {
          conn.close()
        } catch {
          case ex: SQLException => ex.printStackTrace()
        }
      }
    }
  }

  private def tableDefinition(conn: Connection, tableName: String): TableDefinition = {
    val descriptionBuilder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableName))
      .setDescription(s"Table $tableName in $schema schema")

    // 2) For each table, list columns using the information schema.
    //    Retrieve name, type, and nullability.
    val columnsSQL =
      s"""
         |SELECT column_name, data_type, is_nullable
         |FROM information_schema.columns
         |WHERE table_schema='$schema' AND table_name='$tableName'
               """.stripMargin

    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(columnsSQL)
      try {
        while (rs.next()) {
          val columnName = rs.getString("column_name")
          val columnType = rs.getString("data_type") // e.g. "character varying", "integer", "numeric", ...
          val isNullable = rs.getString("is_nullable") // "YES" or "NO"
          val nullable = isNullable == "YES"

          val columnDefinition = ColumnDefinition
            .newBuilder()
            .setName(columnName)
            .setType(toRaw(columnType, nullable))
            .build()

          descriptionBuilder.addColumns(columnDefinition)
        }
      } finally rs.close()
    } finally stmt.close()

    descriptionBuilder.build()
  }

  /**
   * Converts a PostgreSQL 'data_type' (from information_schema) into a RAW type. This is a simplistic mapping that you
   * can expand.
   */
  private def toRaw(pgType: String, nullable: Boolean): Type = {
    val builder = Type.newBuilder()
    pgType.toLowerCase match {
      case "smallint" =>
        builder.setShort(ShortType.newBuilder().setNullable(nullable))
      case "integer" =>
        builder.setInt(IntType.newBuilder().setNullable(nullable))
      case "bigint" =>
        builder.setLong(LongType.newBuilder().setNullable(nullable))
      case "real" =>
        builder.setFloat(FloatType.newBuilder().setNullable(nullable))
      case "double precision" =>
        builder.setDouble(DoubleType.newBuilder().setNullable(nullable))
      case "numeric" =>
        builder.setDecimal(DecimalType.newBuilder().setNullable(nullable))
      case "character varying" | "text" | "varchar" | "character" =>
        builder.setString(StringType.newBuilder().setNullable(nullable))
      case "boolean" =>
        builder.setBool(BoolType.newBuilder().setNullable(nullable))
      case "date" =>
        builder.setDate(DateType.newBuilder().setNullable(nullable))
      case "time" =>
        builder.setTime(TimeType.newBuilder().setNullable(nullable))
      case "timestamp without time zone" =>
        builder.setTimestamp(TimestampType.newBuilder().setNullable(nullable))
    }
    builder.build()
  }

  private def toValue(rs: ResultSet, i: Int, colType: Int) = {
    val valueBuilder = Value.newBuilder()
    val obj = rs.getObject(i)
    if (obj == null) {
      // The column is NULL => we simply don't set a sub-value, or we can mark it in some way.
      // Typically, you'd represent it with an empty sub-value or a special case.
      // For example, skip or store a "null" marker. Here we do nothing => it remains "null"
      valueBuilder.setNull(ValueNull.newBuilder())
    } else {
      colType match {
        // --- Numeric types ---
        case java.sql.Types.SMALLINT =>
          val shortVal = rs.getShort(i)
          valueBuilder.setShort(ValueShort.newBuilder().setV(shortVal))

        case java.sql.Types.INTEGER =>
          val intVal = rs.getInt(i)
          valueBuilder.setInt(ValueInt.newBuilder().setV(intVal))

        case java.sql.Types.BIGINT =>
          val longVal = rs.getLong(i)
          valueBuilder.setLong(ValueLong.newBuilder().setV(longVal))

        case java.sql.Types.REAL =>
          val floatVal = rs.getFloat(i)
          valueBuilder.setFloat(ValueFloat.newBuilder().setV(floatVal))

        case java.sql.Types.DOUBLE =>
          val doubleVal = rs.getDouble(i)
          valueBuilder.setDouble(ValueDouble.newBuilder().setV(doubleVal))

        case java.sql.Types.NUMERIC | java.sql.Types.DECIMAL =>
          // If your 'DecimalType' proto can handle precision/scale, you may want to
          // retrieve them via metadata. For now, just store as string or do getBigDecimal:
          val bigDec = rs.getBigDecimal(i)
          if (bigDec != null) {
            valueBuilder.setDecimal(ValueDecimal.newBuilder().setV(bigDec.toPlainString))
          } else {
            // handle null BigDecimal
            valueBuilder.setNull(ValueNull.newBuilder())
          }

        // --- Boolean ---
        case java.sql.Types.BOOLEAN | java.sql.Types.BIT =>
          val boolVal = rs.getBoolean(i)
          valueBuilder.setBool(ValueBool.newBuilder().setV(boolVal))

        // --- Text/Char ---
        case java.sql.Types.CHAR | java.sql.Types.VARCHAR | java.sql.Types.LONGVARCHAR | java.sql.Types.CLOB =>
          val strVal = rs.getString(i)
          valueBuilder.setString(ValueString.newBuilder().setV(strVal))

        // --- Date & Time ---
        case java.sql.Types.DATE =>
          val dateVal = rs.getDate(i) // java.sql.Date
          if (dateVal != null) {
            val localDate = dateVal.toLocalDate
            valueBuilder.setDate(
              ValueDate
                .newBuilder()
                .setYear(localDate.getYear)
                .setMonth(localDate.getMonthValue)
                .setDay(localDate.getDayOfMonth))
          }

        case java.sql.Types.TIME =>
          val timeVal = rs.getTime(i) // java.sql.Time
          if (timeVal != null) {
            val localTime = timeVal.toLocalTime
            // e.g., store hours/min/sec in your proto:
            valueBuilder.setTime(
              ValueTime
                .newBuilder()
                .setHour(localTime.getHour)
                .setMinute(localTime.getMinute)
                .setSecond(localTime.getSecond))
          }

        case java.sql.Types.TIMESTAMP =>
          val tsVal = rs.getTimestamp(i) // java.sql.Timestamp
          if (tsVal != null) {
            val localDateTime = tsVal.toLocalDateTime
            // your proto might store date+time, or a string, or epoch millis
            valueBuilder.setTimestamp(
              ValueTimestamp
                .newBuilder()
                .setYear(localDateTime.getYear)
                .setMonth(localDateTime.getMonthValue)
                .setDay(localDateTime.getDayOfMonth)
                .setHour(localDateTime.getHour)
                .setMinute(localDateTime.getMinute)
                .setSecond(localDateTime.getSecond)
                .setNano(localDateTime.getNano))
          }
      }
    }
    valueBuilder.build()
  }

  def estimate(query: String): TableEstimate = {
    // 3) Build an EXPLAIN statement in JSON format
    val explainSql = s"EXPLAIN (FORMAT JSON) $query"
    val mapper = new ObjectMapper

    // 4) Execute EXPLAIN
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      try {
        logger.info(s"Executing: $explainSql")
        val rs = stmt.executeQuery(explainSql)
        try {
          if (!rs.next()) {
            // No rows from EXPLAIN? Strange. Return a default
            return TableEstimate(100, 100)
          }
          val explainJson = rs.getString(1)
          // parse the JSON, which is typically an array with plan info
          val parsed = mapper.readTree(explainJson).elements().asScala.toList
          // The structure often: [ { "Plan": { "Node Type": "Seq Scan", "Plan Rows": ..., "Plan Width": ...} } ]
          val planObj = parsed.head.get("Plan")
          // "Plan Rows" is an integer estimate of # of rows
          val planRows = planObj.get("Plan Rows").asInt(100)
          // "Plan Width" is the average width in bytes
          val planWidth = planObj.get("Plan Width").asInt(100)

          // Return that as your estimate
          TableEstimate(planRows, planWidth)
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
   * Executes a SQL query and returns a DASExecuteResult that streams rows from the ResultSet. You can adapt for
   * prepared statements, parameters, etc. as needed.
   */
  def execute(query: String): DASExecuteResult = {
    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.createStatement()
    logger.info(s"Executing: $query")
    val rs = stmt.executeQuery(query)
    val metadata = rs.getMetaData

    new DASExecuteResult {
      private var nextFetched = false
      private var hasMore = false

      override def close(): Unit = {
        try rs.close()
        finally {
          try stmt.close()
          finally conn.close()
        }
      }

      override def hasNext: Boolean = {
        if (!nextFetched) {
          hasMore = rs.next()
          nextFetched = true
        }
        hasMore
      }

      override def next(): Row = {
        if (!hasNext) throw new NoSuchElementException("No more rows")
        // We already advanced once in hasNext(), so reset for next iteration
        nextFetched = false

        val rowBuilder = Row.newBuilder()
        val colCount = metadata.getColumnCount

        for (i <- 1 to colCount) {
          val columnName = metadata.getColumnName(i).toLowerCase
          val colType = metadata.getColumnType(i)
          val value = toValue(rs, i, colType)
          val column = Column
            .newBuilder()
            .setName(columnName)
            .setData(value)
            .build()

          rowBuilder.addColumns(column)
        }

        rowBuilder.build()
      }
    }
  }
}
