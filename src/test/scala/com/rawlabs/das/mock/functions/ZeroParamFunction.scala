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

class ZeroParamFunction extends DASMockFunction {

  def execute(args: Map[String, Value]): Value = {
    Value.newBuilder().setInt(ValueInt.newBuilder().setV(14)).build()
  }

  def definition: FunctionDefinition = {
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("noarg").build())
      .setDescription("A function that takes no arguments and returns 14")
      .setReturnType(Type.newBuilder().setInt(IntType.newBuilder()))
      .build()
  }
}
