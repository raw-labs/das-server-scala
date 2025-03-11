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
import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class RangeFunction extends DASMockFunction {

  def execute(args: Map[String, Value]): Value = {
    if (!args("n").hasInt) {
      throw new DASSdkInvalidArgumentException("The n argument must be a string")
    }
    val n = args("n").getInt.getV
    val builder = ValueList.newBuilder()
    // Add n rows to the list. Each row contains a string and an int
    for (i <- 0 until n) {
      builder.addValues(Value.newBuilder().setInt(ValueInt.newBuilder().setV(i)))
    }
    Value.newBuilder().setList(builder.build()).build()
  }

  def definition: FunctionDefinition = {
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("range").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("n")
          .setDescription("an integer")
          .setType(Type.newBuilder().setInt(IntType.newBuilder().setNullable(false).build())))
      .setDescription("A function that returns numbers from 0 to n")
      .setReturnType(Type
        .newBuilder()
        .setList(ListType.newBuilder().setInnerType(Type.newBuilder().setInt(IntType.newBuilder())).build()))
      .build()
  }
}
