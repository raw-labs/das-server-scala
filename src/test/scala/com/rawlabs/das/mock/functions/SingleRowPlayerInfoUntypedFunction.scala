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

class SingleRowPlayerInfoUntypedFunction extends SingleRowPlayerInfoTypedFunction {

  /**
   * The function definition. This declares that the function "player_info_untyped" takes one parameter "name" (text) and
   * returns a record with unspecified fields
   */
  override def definition: FunctionDefinition = {
    // The anonymous record type.
    val rowType = Type
      .newBuilder()
      .setRecord(
        RecordType
          .newBuilder()
          .build())
      .build()

    // Build the function definition.
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("player_info_untyped").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("name")
          .setDescription("player name")
          .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build()))
          .build())
      .setDescription("Function that returns info about a player")
      .setReturnType(rowType)
      .build()
  }
}
