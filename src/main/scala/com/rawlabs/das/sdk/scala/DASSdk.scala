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

import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

trait DASSdk {

  /**
   * @return a list of table definitions.
   */
  def tableDefinitions: Seq[TableDefinition]

  /**
   * @return a list of function definitions.
   */
  def functionDefinitions: Seq[FunctionDefinition]

  /**
   * Retrieve a table by name.
   *
   * @param name table name
   * @return Optional containing the DASTable if present
   */
  def getTable(name: String): Option[DASTable]

  /**
   * Retrieve a function by name.
   *
   * @param name function name
   * @return Optional containing the DASFunction if present
   */
  def getFunction(name: String): Option[DASFunction]

}
