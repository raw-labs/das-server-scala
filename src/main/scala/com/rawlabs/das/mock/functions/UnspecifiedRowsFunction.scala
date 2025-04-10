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

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.common.Environment
import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class UnspecifiedRowsFunction extends DASMockFunction {

  def execute(args: Map[String, Value], maybeEnv: Option[Environment]): Value = {
    if (!args("n").hasInt) {
      throw new DASSdkInvalidArgumentException("The n argument must be an int")
    }
    val n = args("n").getInt.getV
    val builder = ValueList.newBuilder()
    // Add n rows to the list. Each row contains a string and an int
    for (i <- 0 until n) {
      val recordBuilder = ValueRecord.newBuilder()
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("str")
          .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV("str#" + i).build()).build())
          .build())
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("arg")
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(i).build()).build())
          .build())
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("pi*arg")
          .setValue(Value.newBuilder().setDouble(ValueDouble.newBuilder().setV(i * Math.PI).build()).build())
          .build())
      builder.addValues(Value.newBuilder().setRecord(recordBuilder))
    }
    Value.newBuilder().setList(builder.build()).build()
  }

  def definition: FunctionDefinition = {
    val rowType = Type
      .newBuilder()
      .setRecord(
        RecordType
          .newBuilder())
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("unknown_rows").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("n")
          .setDescription("an integer")
          .setType(Type.newBuilder().setInt(IntType.newBuilder().setNullable(false).build())))
      .setDescription("A function that returns n rows with unspecified columns")
      .setReturnType(Type.newBuilder().setList(ListType.newBuilder().setInnerType(rowType).build()))
      .build()
  }
}
