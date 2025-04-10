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

import com.rawlabs.protocol.das.v1.common.Environment
import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class MultiplyStringFunction extends DASMockFunction {

  def execute(args: Map[String, Value], maybeEnv: Option[Environment]): Value = {
    val s = args("s").getString.getV
    val n = args("n").getInt.getV
    Value.newBuilder().setString(ValueString.newBuilder().setV(s * n)).build()
  }

  def definition: FunctionDefinition = {
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("multiply_string").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("s")
          .setDescription("a string")
          .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("n")
          .setDescription("an int")
          .setDefaultValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(1)).build())
          .setType(Type.newBuilder().setInt(IntType.newBuilder().setNullable(false).build())))
      .setDescription("A function that concatenates n times the string s")
      .setReturnType(Type.newBuilder().setString(StringType.newBuilder()))
      .build()
  }
}
