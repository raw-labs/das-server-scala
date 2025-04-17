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

class MultiplyIntFunction extends DASMockFunction {

  def execute(args: Map[String, Value], maybeEnv: Option[Environment]): Value = {
    val x = args("x").getInt.getV
    val y = args("y").getInt.getV
    Value.newBuilder().setInt(ValueInt.newBuilder().setV(x * y)).build()
  }

  def definition: FunctionDefinition = {
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("multiply").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("x")
          .setDescription("an int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder().setNullable(false).build())))
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("y")
          .setDescription("an int")
          .setType(Type.newBuilder().setInt(IntType.newBuilder().setNullable(false).build())))
      .setDescription("A function that multiplies its two arguments")
      .setReturnType(Type.newBuilder().setInt(IntType.newBuilder()))
      .build()
  }
}
