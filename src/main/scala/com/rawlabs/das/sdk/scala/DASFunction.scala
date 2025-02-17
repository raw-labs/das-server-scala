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

package com.rawlabs.das.sdk.scala

import com.rawlabs.protocol.das.v1.types.Value

/**
 * Represents a function callable by the DAS.
 */
trait DASFunction {

  /**
   * Execute the function with the provided arguments.
   *
   * @param args a map from argument names to Values
   * @return the computed Value
   */
  def execute(args: Map[String, Value]): Value

}
