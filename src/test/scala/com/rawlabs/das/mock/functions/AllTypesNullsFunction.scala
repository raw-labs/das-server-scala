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

import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId}
import com.rawlabs.protocol.das.v1.types._

import scala.jdk.CollectionConverters.IterableHasAsJava

class AllTypesNullsFunction extends AllTypesFunction {

  override def definition: FunctionDefinition = {

    val parameters = recordItems.map(_.asParameter)
    val outputFields = recordItems.map(_.asAttrType)

    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("all_types_nulls_f").build())
      .addAllParams(parameters.asJava)
      .setReturnType(
        Type.newBuilder().setRecord(RecordType.newBuilder().addAllAtts(outputFields.asJava).build()).build())
      .build()
  }

  override protected def recordItems: Seq[RecordItem] = {
    super.recordItems.map(item => item.copy(v = Value.newBuilder().setNull(ValueNull.newBuilder()).build()))
  }
}
