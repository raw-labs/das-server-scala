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

package com.rawlabs.das.mock.functions

import scala.jdk.CollectionConverters.IterableHasAsJava

import com.google.protobuf.ByteString
import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

protected case class RecordItem(name: String, t: Type, v: Value) {
  def asParameter: ParameterDefinition =
    ParameterDefinition.newBuilder().setName(name).setType(t).setDefaultValue(v).build()
  def asAttrType: AttrType = AttrType.newBuilder().setName(name).setTipe(t).build()
  def asAtt(value: Option[Value]): ValueRecordAttr =
    ValueRecordAttr.newBuilder().setName(name).setValue(value.getOrElse(v)).build()
}

class AllTypesFunction extends DASMockFunction {

  override def definition: FunctionDefinition = {

    val parameters = recordItems.map(_.asParameter)
    val outputFields = recordItems.map(_.asAttrType)

    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("all_types_f").build())
      .addAllParams(parameters.asJava)
      .setReturnType(
        Type.newBuilder().setRecord(RecordType.newBuilder().addAllAtts(outputFields.asJava).build()).build())
      .build()
  }

  protected def recordItems: Seq[RecordItem] = Seq(
    // Byte
    RecordItem(
      "byte_value",
      Type
        .newBuilder()
        .setByte(ByteType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setByte(ValueByte.newBuilder().setV(42)) // sample value
        .build()),

    // Short
    RecordItem(
      "short_value",
      Type
        .newBuilder()
        .setShort(ShortType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setShort(ValueShort.newBuilder().setV(123)) // sample value
        .build()),

    // Int
    RecordItem(
      "int_value",
      Type
        .newBuilder()
        .setInt(IntType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setInt(ValueInt.newBuilder().setV(14))
        .build()),

    // Long
    RecordItem(
      "long_value",
      Type
        .newBuilder()
        .setLong(LongType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setLong(ValueLong.newBuilder().setV(9999999999L))
        .build()),

    // Float
    RecordItem(
      "float_value",
      Type
        .newBuilder()
        .setFloat(FloatType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setFloat(ValueFloat.newBuilder().setV(12.34f))
        .build()),

    // Double
    RecordItem(
      "double_value",
      Type
        .newBuilder()
        .setDouble(DoubleType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setDouble(ValueDouble.newBuilder().setV(56.78))
        .build()),

    // Decimal
    RecordItem(
      "decimal_value",
      Type
        .newBuilder()
        .setDecimal(DecimalType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setDecimal(ValueDecimal.newBuilder().setV("12345.6789"))
        .build()),

    // Bool
    RecordItem(
      "bool_value",
      Type
        .newBuilder()
        .setBool(BoolType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setBool(ValueBool.newBuilder().setV(true))
        .build()),

    // String
    RecordItem(
      "string_value",
      Type
        .newBuilder()
        .setString(StringType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setString(ValueString.newBuilder().setV("tralala"))
        .build()),

    // Binary
    RecordItem(
      "binary_value",
      Type
        .newBuilder()
        .setBinary(BinaryType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setBinary(
          ValueBinary
            .newBuilder()
            .setV(ByteString.copyFromUtf8("some bytes")))
        .build()),

    // Date
    RecordItem(
      "date_value",
      Type
        .newBuilder()
        .setDate(DateType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setDate(
          ValueDate
            .newBuilder()
            .setYear(2023)
            .setMonth(9)
            .setDay(15))
        .build()),

    // Time
    RecordItem(
      "time_value",
      Type
        .newBuilder()
        .setTime(TimeType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setTime(
          ValueTime
            .newBuilder()
            .setHour(12)
            .setMinute(34)
            .setSecond(56)
            .setNano(123000000) // e.g. 123 ms
        )
        .build()),

    // Timestamp
    RecordItem(
      "timestamp_value",
      Type
        .newBuilder()
        .setTimestamp(TimestampType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setTimestamp(
          ValueTimestamp
            .newBuilder()
            .setYear(2023)
            .setMonth(9)
            .setDay(15)
            .setHour(12)
            .setMinute(34)
            .setSecond(56)
            .setNano(123000000))
        .build()),

    // Interval
    RecordItem(
      "interval_value",
      Type
        .newBuilder()
        .setInterval(IntervalType.newBuilder().setNullable(true))
        .build(),
      Value
        .newBuilder()
        .setInterval(
          ValueInterval
            .newBuilder()
            .setYears(1)
            .setMonths(2)
            .setDays(3)
            .setHours(4)
            .setMinutes(5)
            .setSeconds(6)
            .setMicros(789))
        .build()),

    // Record
    {
      val recordType = RecordType.newBuilder().setNullable(true)
      recordType.addAtts(
        AttrType
          .newBuilder()
          .setName("first_name")
          .setTipe(Type
            .newBuilder()
            .setString(StringType.newBuilder().setNullable(true))))
      recordType.addAtts(
        AttrType
          .newBuilder()
          .setName("age")
          .setTipe(Type
            .newBuilder()
            .setInt(IntType.newBuilder().setNullable(true))))

      val recordValue = ValueRecord.newBuilder()
      recordValue.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("first_name")
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("Alice"))))
      recordValue.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("age")
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(42))))

      RecordItem(
        "record_value",
        Type
          .newBuilder()
          .setRecord(recordType)
          .build(),
        Value
          .newBuilder()
          .setRecord(recordValue)
          .build())
    },

    // Any
    {
      val recordValue = ValueRecord.newBuilder()
      recordValue.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("first_name")
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("Alice"))))
      recordValue.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("age")
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(42))))

      RecordItem(
        "any_value",
        Type
          .newBuilder()
          .setAny(AnyType.newBuilder())
          .build(),
        Value
          .newBuilder()
          .setRecord(recordValue)
          .build())
    },

    // List of bytes
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setByte(ByteType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setByte(ValueByte.newBuilder().setV(10)))
      listValue.addValues(Value.newBuilder().setByte(ValueByte.newBuilder().setV(20)))
      listValue.addValues(Value.newBuilder().setByte(ValueByte.newBuilder().setV(30)))
      RecordItem(
        "list_of_bytes_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of shorts
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setShort(ShortType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setShort(ValueShort.newBuilder().setV(100)))
      listValue.addValues(Value.newBuilder().setShort(ValueShort.newBuilder().setV(200)))
      listValue.addValues(Value.newBuilder().setShort(ValueShort.newBuilder().setV(300)))
      RecordItem(
        "list_of_shorts_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of ints
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(
        Type
          .newBuilder()
          .setInt(IntType.newBuilder().setNullable(true)))
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)))
      listValue.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(2)))
      listValue.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(3)))

      RecordItem(
        "list_of_ints_value",
        Type
          .newBuilder()
          .setList(listType)
          .build(),
        Value
          .newBuilder()
          .setList(listValue)
          .build())
    },

    // List of longs
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setLong(LongType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setLong(ValueLong.newBuilder().setV(100000L)))
      listValue.addValues(Value.newBuilder().setLong(ValueLong.newBuilder().setV(200000L)))
      listValue.addValues(Value.newBuilder().setLong(ValueLong.newBuilder().setV(300000L)))
      RecordItem(
        "list_of_longs_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of floats
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setFloat(FloatType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(1.23f)))
      listValue.addValues(Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(4.56f)))
      listValue.addValues(Value.newBuilder().setFloat(ValueFloat.newBuilder().setV(7.89f)))
      RecordItem(
        "list_of_floats_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of doubles
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setDouble(DoubleType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(1.23)))
      listValue.addValues(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(4.56)))
      listValue.addValues(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(7.89)))
      RecordItem(
        "list_of_doubles_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of decimals
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(
        Type
          .newBuilder()
          .setDecimal(DecimalType.newBuilder().setNullable(true))
          .build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV("1.23")))
      listValue.addValues(Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV("4.56")))
      listValue.addValues(Value.newBuilder().setDecimal(ValueDecimal.newBuilder().setV("7.89")))
      RecordItem(
        "list_of_decimals_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },
    // List of strings
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setString(StringType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setString(ValueString.newBuilder().setV("foo")))
      listValue.addValues(Value.newBuilder().setString(ValueString.newBuilder().setV("bar")))
      listValue.addValues(Value.newBuilder().setString(ValueString.newBuilder().setV("baz")))
      RecordItem(
        "list_of_strings_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of booleans
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setBool(BoolType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setBool(ValueBool.newBuilder().setV(true)))
      listValue.addValues(Value.newBuilder().setBool(ValueBool.newBuilder().setV(false)))
      listValue.addValues(Value.newBuilder().setBool(ValueBool.newBuilder().setV(true)))
      RecordItem(
        "list_of_bools_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of dates
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setDate(DateType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setDate(ValueDate.newBuilder().setYear(2022).setMonth(1).setDay(15)))
      listValue.addValues(Value.newBuilder().setDate(ValueDate.newBuilder().setYear(2022).setMonth(2).setDay(20)))
      listValue.addValues(Value.newBuilder().setDate(ValueDate.newBuilder().setYear(2022).setMonth(3).setDay(25)))
      RecordItem(
        "list_of_dates_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of times
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setTime(TimeType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(
        Value.newBuilder().setTime(ValueTime.newBuilder().setHour(10).setMinute(30).setSecond(15).setNano(0)))
      listValue.addValues(
        Value.newBuilder().setTime(ValueTime.newBuilder().setHour(12).setMinute(45).setSecond(30).setNano(0)))
      listValue.addValues(
        Value.newBuilder().setTime(ValueTime.newBuilder().setHour(15).setMinute(0).setSecond(0).setNano(0)))
      RecordItem(
        "list_of_times_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of timestamps
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setTimestamp(TimestampType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(
        Value
          .newBuilder()
          .setTimestamp(
            ValueTimestamp
              .newBuilder()
              .setYear(2022)
              .setMonth(1)
              .setDay(15)
              .setHour(10)
              .setMinute(30)
              .setSecond(15)
              .setNano(0)))
      listValue.addValues(
        Value
          .newBuilder()
          .setTimestamp(
            ValueTimestamp
              .newBuilder()
              .setYear(2022)
              .setMonth(2)
              .setDay(20)
              .setHour(12)
              .setMinute(45)
              .setSecond(30)
              .setNano(0)))
      listValue.addValues(
        Value
          .newBuilder()
          .setTimestamp(
            ValueTimestamp
              .newBuilder()
              .setYear(2022)
              .setMonth(3)
              .setDay(25)
              .setHour(15)
              .setMinute(0)
              .setSecond(0)
              .setNano(0)))
      RecordItem(
        "list_of_timestamps_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of intervals
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(
        Type
          .newBuilder()
          .setInterval(IntervalType.newBuilder().setNullable(true))
          .build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(
        Value
          .newBuilder()
          .setInterval(
            ValueInterval
              .newBuilder()
              .setYears(1)
              .setMonths(2)
              .setDays(3)
              .setHours(4)
              .setMinutes(5)
              .setSeconds(6)
              .setMicros(123456)))
      listValue.addValues(
        Value
          .newBuilder()
          .setInterval(
            ValueInterval
              .newBuilder()
              .setYears(10)
              .setMonths(2)
              .setDays(3)
              .setHours(4)
              .setMinutes(5)
              .setSeconds(6)
              .setMicros(123456)))
      listValue.addValues(
        Value
          .newBuilder()
          .setInterval(
            ValueInterval
              .newBuilder()
              .setYears(100)
              .setMonths(2)
              .setDays(3)
              .setHours(4)
              .setMinutes(5)
              .setSeconds(6)
              .setMicros(123456)))
      RecordItem(
        "list_of_intervals_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },
    // List of binary values
    {
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setBinary(BinaryType.newBuilder().setNullable(true)).build())
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setBinary(ValueBinary.newBuilder().setV(ByteString.copyFromUtf8("abc"))))
      listValue.addValues(Value.newBuilder().setBinary(ValueBinary.newBuilder().setV(ByteString.copyFromUtf8("def"))))
      listValue.addValues(Value.newBuilder().setBinary(ValueBinary.newBuilder().setV(ByteString.copyFromUtf8("ghi"))))
      RecordItem(
        "list_of_binarys_value",
        Type.newBuilder().setList(listType).build(),
        Value.newBuilder().setList(listValue).build())
    },

    // List of records
    {
      // Define the record type to embed in the list
      val recType = RecordType.newBuilder().setNullable(true)
      recType.addAtts(
        AttrType
          .newBuilder()
          .setName("x")
          .setTipe(Type
            .newBuilder()
            .setDouble(DoubleType.newBuilder().setNullable(true))))
      recType.addAtts(
        AttrType
          .newBuilder()
          .setName("y")
          .setTipe(Type
            .newBuilder()
            .setDouble(DoubleType.newBuilder().setNullable(true))))

      // The list type that wraps that record
      val listType = ListType.newBuilder().setNullable(true)
      listType.setInnerType(Type.newBuilder().setRecord(recType))

      // Build two record values for the list
      val recVal1 = ValueRecord.newBuilder()
      recVal1.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("x")
          .setValue(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(1.1))))
      recVal1.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("y")
          .setValue(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(2.2))))

      val recVal2 = ValueRecord.newBuilder()
      recVal2.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("x")
          .setValue(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(3.3))))
      recVal2.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("y")
          .setValue(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(4.4))))

      // The final list Value
      val listValue = ValueList.newBuilder()
      listValue.addValues(Value.newBuilder().setRecord(recVal1))
      listValue.addValues(Value.newBuilder().setRecord(recVal2))

      RecordItem(
        "list_of_records_value",
        Type
          .newBuilder()
          .setList(listType)
          .build(),
        Value
          .newBuilder()
          .setList(listValue)
          .build())
    },

    // List of any
    {
      // 1) Build the ListType whose inner type is "any"
      val listType = ListType
        .newBuilder()
        .setNullable(true)
        .setInnerType(
          Type
            .newBuilder()
            .setAny(AnyType.newBuilder()) // <-- Inner is AnyType
        )

      // 2) Build the ValueList
      val listValue = ValueList.newBuilder()

      // Add a record item
      val recordBuilder = ValueRecord.newBuilder()
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("first_name")
          .setValue(Value
            .newBuilder()
            .setString(ValueString.newBuilder().setV("Alice"))))
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("age")
          .setValue(Value
            .newBuilder()
            .setInt(ValueInt.newBuilder().setV(42))))
      // Build the record item and add to list
      listValue.addValues(Value.newBuilder().setRecord(recordBuilder).build())

      // Add a bool
      listValue.addValues(
        Value
          .newBuilder()
          .setBool(ValueBool.newBuilder().setV(true))
          .build())

      // Add a string
      listValue.addValues(
        Value
          .newBuilder()
          .setString(ValueString.newBuilder().setV("Hello"))
          .build())

      // Add an int
      listValue.addValues(
        Value
          .newBuilder()
          .setInt(ValueInt.newBuilder().setV(123))
          .build())

      // 3) Wrap into the final container or structure you're using
      //    (example: a "RecordItem" or similar)
      RecordItem(
        "list_of_any_value",
        Type
          .newBuilder()
          .setList(listType)
          .build(),
        Value
          .newBuilder()
          .setList(listValue)
          .build())
    },

    // list of lists
    {
      // 1) Build the ListType whose inner type is "list"
      val innerListType = Type
        .newBuilder()
        .setList(
          ListType
            .newBuilder()
            .setNullable(true)
            .setInnerType(Type
              .newBuilder()
              .setInt(IntType.newBuilder().setNullable(true))))
      val listType = ListType
        .newBuilder()
        .setNullable(true)
        .setInnerType(innerListType)

      // 2) Build the ValueList
      val listValue = ValueList.newBuilder()

      // Add a list of ints
      val innerListValue1 = ValueList.newBuilder()
      innerListValue1.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)))
      innerListValue1.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(2)))
      innerListValue1.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(3)))
      listValue.addValues(Value.newBuilder().setList(innerListValue1).build())

      // Add a list of ints
      val innerListValue2 = ValueList.newBuilder()
      innerListValue2.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(4)))
      innerListValue2.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(5)))
      innerListValue2.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(6)))
      listValue.addValues(Value.newBuilder().setList(innerListValue2).build())

      // 3) Wrap into the final container or structure you're using
      //    (example: a "RecordItem" or similar)
      RecordItem(
        "list_of_lists_value",
        Type
          .newBuilder()
          .setList(listType)
          .build(),
        Value
          .newBuilder()
          .setList(listValue)
          .build())
    })

  /**
   * Execute the function with the provided arguments.
   *
   * @param args a map from argument names to Values
   * @return the computed Value
   */
  override def execute(args: Map[String, Value]): Value = {
    // Returns a record with all the values, using the provided arguments if available.
    val values = recordItems.map { item =>
      val maybeArg = args.get(item.name)
      item.asAtt(maybeArg)
    }
    Value.newBuilder().setRecord(ValueRecord.newBuilder().addAllAtts(values.asJava).build()).build()
  }
}
