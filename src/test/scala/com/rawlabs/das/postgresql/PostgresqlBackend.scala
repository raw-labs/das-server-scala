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

import java.sql.{Connection, DriverManager, SQLException}

import scala.jdk.CollectionConverters.IteratorHasAsScala

import com.fasterxml.jackson.databind.ObjectMapper
import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.scala.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

/**
 * A simple example backend for PostgreSQL, analogous to the SqliteBackend example.
 *
 * @param url The JDBC connection string (e.g., "jdbc:postgresql://localhost:5432/mydb")
 * @param user The database user
 * @param password The password for that user
 */
class PostgresqlBackend(url: String, user: String, password: String, schema: String) {

  /**
   * Returns a map of tableName -> DASPostgresTable, each containing a TableDefinition. Similar to the original
   * SqliteBackend#tables().
   */
  Class.forName("org.postgresql.Driver")
  def tables(): Map[String, DASPostgresqlTable] = {
    val tables = scala.collection.mutable.Map.empty[String, DASPostgresqlTable]
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, user, password)

      // 1) List all user tables in the "public" schema (or adapt if you want all schemas).
      //    We’ll skip system tables or other schemas by default.
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
            println(s"Table: $tableName")

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
                 |WHERE table_schema='public' AND table_name='$tableName'
               """.stripMargin

            val stmt2 = conn.createStatement()
            try {
              val rs2 = stmt2.executeQuery(columnsSQL)
              try {
                while (rs2.next()) {
                  val columnName = rs2.getString("column_name").toLowerCase
                  val columnType = rs2.getString("data_type") // e.g. "character varying", "integer", "numeric", ...
                  val isNullable = rs2.getString("is_nullable") // "YES" or "NO"
                  val nullable = isNullable == "YES"

                  val columnDefinition = ColumnDefinition
                    .newBuilder()
                    .setName(columnName)
                    .setType(toRaw(columnType, nullable))
                    .build()

                  descriptionBuilder.addColumns(columnDefinition)
                }
              } finally rs2.close()
            } finally stmt2.close()

            val tableDefinition = descriptionBuilder.build()
            // Add to the map
            tables += tableName -> new DASPostgresqlTable(this, tableDefinition)
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

  def estimate(query: String): TableEstimate = {
    // 3) Build an EXPLAIN statement in JSON format
    val explainSql = s"EXPLAIN (FORMAT JSON) $query"
    val mapper = new ObjectMapper

    // 4) Execute EXPLAIN
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      try {
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
          val obj = rs.getObject(i)

          val valueBuilder = Value.newBuilder()
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

          val column = Column
            .newBuilder()
            .setName(columnName)
            .setData(valueBuilder)
            .build()

          rowBuilder.addColumns(column)
        }

        rowBuilder.build()
      }
    }
  }
}
