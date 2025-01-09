/**
 * Copyright 2024 RAW Labs S.A. All rights reserved.
 *
 * This source code is the property of RAW Labs S.A. It contains proprietary and confidential information that is
 * protected by applicable intellectual property and other laws. Unauthorized use, reproduction, or distribution of this
 * code, or any portion of it, may result in severe civil and criminal penalties and will be prosecuted to the maximum
 * extent possible under the law.
 */

package com.rawlabs.das.mock

import com.rawlabs.das.sdk.scala._
import com.rawlabs.protocol.das.v1.functions._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

class DASMock(options: Map[String, String]) extends DASSdk with StrictLogging {

  options.keys.foreach(key => logger.info(s"Option: $key = ${options(key)}"))

  private val dasMockStorage = new DASMockStorage("column1")

  override def tableDefinitions: Seq[TableDefinition] = {
    val bigTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("big"))
      .setDescription("A mock large table")
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column1")
          .setDescription("The first column - int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column2")
          .setDescription("The second column - string")
          .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
          .build())
      .setStartupCost(1000)
      .build()
    val smallTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("small"))
      .setDescription("A mock small table")
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column1")
          .setDescription("The first column - int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column2")
          .setDescription("The second column - string")
          .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
          .build())
      .setStartupCost(1000)
      .build()
    val slowTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("slow"))
      .setDescription("A mock slow table")
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column1")
          .setDescription("The first column - int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column2")
          .setDescription("The second column - string")
          .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
          .build())
      .setStartupCost(1000)
      .build()
    val brokenTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("broken"))
      .setDescription("A mock broken table")
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column1")
          .setDescription("The first column - int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column2")
          .setDescription("The second column - string")
          .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
          .build())
      .setStartupCost(1000)
      .build()
    val inMemoryTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("in_memory"))
      .setDescription("A mock in memory table")
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column1")
          .setDescription("The first column - int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("column2")
          .setDescription("The second column - string")
          .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
          .build())
      .setStartupCost(2000)
      .build()
    val allTypesTable = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName("all_types"))
      .setDescription("All types including nested types")
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("byte_col")
        .setType(Type.newBuilder().setByte(ByteType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("short_col")
        .setType(Type.newBuilder().setShort(ShortType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("int_col")
        .setType(Type.newBuilder().setInt(IntType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("long_col")
        .setType(Type.newBuilder().setLong(LongType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("float_col")
        .setType(Type.newBuilder().setFloat(FloatType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("double_col")
        .setType(Type.newBuilder().setDouble(DoubleType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("decimal_col")
        .setType(Type.newBuilder().setDecimal(DecimalType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("string_col")
        .setType(Type.newBuilder().setString(StringType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("binary_col")
        .setType(Type.newBuilder().setBinary(BinaryType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("bool_col")
        .setType(Type.newBuilder().setBool(BoolType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("date_col")
        .setType(Type.newBuilder().setDate(DateType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("time_col")
        .setType(Type.newBuilder().setTime(TimeType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("timestamp_col")
        .setType(Type.newBuilder().setTimestamp(TimestampType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("interval_col")
        .setType(Type.newBuilder().setInterval(IntervalType.newBuilder()).build())
        .build())
      .addColumns(ColumnDefinition
        .newBuilder()
        .setName("any_col")
        .setType(Type.newBuilder().setAny(AnyType.newBuilder()).build()))
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("strings_col")
          .setType(
            Type
              .newBuilder()
              .setList(ListType
                .newBuilder()
                .setInnerType(Type.newBuilder().setString(StringType.newBuilder()).build())
                .build())
              .build()))
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("timestamps_col")
          .setType(
            Type
              .newBuilder()
              .setList(ListType
                .newBuilder()
                .setInnerType(Type.newBuilder().setTimestamp(TimestampType.newBuilder()).build())
                .build())
              .build()))
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("record_col")
          .setType(
            Type
              .newBuilder()
              .setRecord(
                RecordType
                  .newBuilder()
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("intField")
                    .setTipe(Type.newBuilder().setInt(IntType.newBuilder()).build())
                    .build())
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("binaryField")
                    .setTipe(Type.newBuilder().setBinary(BinaryType.newBuilder()).build())
                    .build())
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("timestampField")
                    .setTipe(Type.newBuilder().setTimestamp(TimestampType.newBuilder()).build())
                    .build())
                  .build())
              .build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("str_record_col")
          .setType(
            Type
              .newBuilder()
              .setRecord(
                RecordType
                  .newBuilder()
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("str1")
                    .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                    .build())
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("str2")
                    .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                    .build())
                  .addAtts(AttrType
                    .newBuilder()
                    .setName("str3")
                    .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                    .build())
                  .build())
              .build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("str_records_col")
          .setType(
            Type
              .newBuilder()
              .setList(
                ListType
                  .newBuilder()
                  .setInnerType(
                    Type
                      .newBuilder()
                      .setRecord(
                        RecordType
                          .newBuilder()
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("str1")
                            .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                            .build())
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("str2")
                            .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                            .build())
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("str3")
                            .setTipe(Type.newBuilder().setString(StringType.newBuilder()).build())
                            .build())
                          .build())
                      .build())
                  .build())
              .build())
          .build())
      .addColumns(
        ColumnDefinition
          .newBuilder()
          .setName("records_col")
          .setType(
            Type
              .newBuilder()
              .setList(
                ListType
                  .newBuilder()
                  .setInnerType(
                    Type
                      .newBuilder()
                      .setRecord(
                        RecordType
                          .newBuilder()
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("intField")
                            .setTipe(Type.newBuilder().setInt(IntType.newBuilder()).build())
                            .build())
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("binaryField")
                            .setTipe(Type.newBuilder().setBinary(BinaryType.newBuilder()).build())
                            .build())
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("timestampField")
                            .setTipe(Type.newBuilder().setTimestamp(TimestampType.newBuilder()).build())
                            .build())
                          .addAtts(AttrType
                            .newBuilder()
                            .setName("timeField")
                            .setTipe(Type.newBuilder().setTime(TimeType.newBuilder()).build()))
                          .build())
                      .build())
                  .build())))
      .setStartupCost(1000)
      .build()
    Seq(bigTable, smallTable, slowTable, brokenTable, inMemoryTable, allTypesTable)
  }

  override def functionDefinitions: Seq[FunctionDefinition] = {
    Seq.empty
  }

  override def getTable(name: String): Option[DASTable] = {
    name match {
      case "big"       => Some(new DASMockTable(2000000000))
      case "small"     => Some(new DASMockTable(100))
      case "in_memory" => Some(new DASMockInMemoryTable(dasMockStorage))
      case "all_types" => Some(new DASMockAllTypesTable(100))
      case "slow"      => Some(new DASMockTable(10, sleepPerRowMills = 500))
      case "broken"    => Some(new DASMockTable(10, breakOnRow = 5))
      case _           => None
    }
  }

  override def getFunction(name: String): Option[DASFunction] = {
    None
  }

}
