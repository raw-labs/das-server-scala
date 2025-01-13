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

package com.rawlabs.das.sqlite
import java.sql.{Connection, DriverManager, SQLException}
import java.time.LocalDate

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._

class SqliteBackend(file: String) {
  val dbUrl = s"jdbc:sqlite:$file" // Adjust to your DB file path

  def tables(): Map[String, DASSqliteTable] = {
    val tables = scala.collection.mutable.Map.empty[String, DASSqliteTable]
    var conn: Connection = null
    try {
      // 1) Open connection
      conn = DriverManager.getConnection(dbUrl)

      // 2) List all user tables (ignore internal "sqlite_%" tables)
      val listTablesSQL =
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(listTablesSQL)
        try {
          while (rs.next()) {
            val tableName = rs.getString("name").toLowerCase
            println(s"Table: $tableName")
            val description = TableDefinition.newBuilder()
            description
              .setTableId(TableId.newBuilder().setName(tableName).build())
              .setDescription(s"Table $tableName")

            // 3) For each table, show column details via PRAGMA table_info
            val pragmaSQL = s"PRAGMA table_info($tableName)"
            val stmt2 = conn.createStatement()
            try {
              val rs2 = stmt2.executeQuery(pragmaSQL)
              try {
                while (rs2.next()) {
                  val columnName = rs2.getString("name").toLowerCase
                  val columnType = rs2.getString("type")
                  val notNull = rs2.getInt("notnull")
                  val columnDescription = ColumnDefinition
                    .newBuilder()
                    .setName(columnName)
                    .setType(toRaw(columnType, notNull != 0))
                  description.addColumns(columnDescription.build())
                }
              } finally {
                rs2.close()
              }
            } finally {
              stmt2.close()
            }
            tables += (tableName -> new DASSqliteTable(this, description.build()))
          }
          tables.toMap
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
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
  private def toRaw(strType: String, nullable: Boolean): Type = {
    val builder = Type.newBuilder()
    strType match {
      case "INTEGER" => builder.setDouble(DoubleType.newBuilder().setNullable(nullable))
      case "TEXT"    => builder.setString(StringType.newBuilder().setNullable(nullable))
      case "REAL"    => builder.setDouble(DoubleType.newBuilder().setNullable(nullable))
      case "DATE"    => builder.setDate(DateType.newBuilder().setNullable(nullable))
    }
    builder.build()
  }

  def execute(query: String): DASExecuteResult = {
    val conn = DriverManager.getConnection(dbUrl)
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
          finally {
            conn.close()
          }
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
        // We already advanced once in hasNext()
        nextFetched = false

        val rowBuilder = Row.newBuilder()
        for (i <- 1 to metadata.getColumnCount) {
          val valueBuilder = Value.newBuilder()
          val colType = metadata.getColumnType(i)

          if (rs.getObject(i) == null) {
            // handle null (maybe skip or set some "NULL" marker)
          } else {
            colType match {
              case java.sql.Types.VARCHAR =>
                valueBuilder.setString(ValueString.newBuilder().setV(rs.getString(i)))
              case java.sql.Types.INTEGER | java.sql.Types.DOUBLE | java.sql.Types.FLOAT =>
                valueBuilder.setDouble(ValueDouble.newBuilder().setV(rs.getDouble(i)))
              case java.sql.Types.DATE =>
                val strDate = rs.getString(i)
                val date = LocalDate.parse(strDate)
                valueBuilder.setDate(
                  ValueDate
                    .newBuilder()
                    .setYear(date.getYear)
                    .setMonth(date.getMonthValue)
                    .setDay(date.getDayOfMonth))
            }
          }

          val columnName = metadata.getColumnName(i).toLowerCase
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
