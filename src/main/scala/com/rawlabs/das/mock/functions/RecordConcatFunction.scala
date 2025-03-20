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

import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class RecordConcatFunction extends DASMockFunction {

  /* Concatenates the two records x and y */
  def execute(args: Map[String, Value]): Value = {
    val x = args("x").getRecord
    val y = args("y").getRecord
    val builder = ValueRecord.newBuilder()
    x.getAttsList.forEach(a => builder.addAtts(a))
    y.getAttsList.forEach(a => builder.addAtts(a))
    Value.newBuilder().setRecord(builder.build()).build()
  }

  def definition: FunctionDefinition = {
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("record_concat").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("x")
          .setDescription("any record you like")
          .setType(Type.newBuilder().setRecord(RecordType.newBuilder().build())))
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("y")
          .setDescription("any record you like")
          .setType(Type.newBuilder().setRecord(RecordType.newBuilder().build())))
      .setDescription("function that concatenates two records")
      .setReturnType(Type.newBuilder().setAny(AnyType.newBuilder()))
      .build()
  }
}
