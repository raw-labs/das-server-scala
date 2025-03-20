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

class RecipeListOfUntypedRecordsFunction extends RecipeListOfTypedRecordsFunction {

  override def definition: FunctionDefinition = {
    val rowType = Type
      .newBuilder()
      .setRecord(
        RecordType
          .newBuilder()
          .build())
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("recipe_untyped").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("dish")
          .setDescription("a dish name")
          .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
      .setDescription("A function that returns the recipe for a given dish")
      .setReturnType(Type.newBuilder().setList(ListType.newBuilder().setInnerType(rowType).build()))
      .build()
  }
}
