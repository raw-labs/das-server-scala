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

package com.rawlabs.das.sdk;

import com.rawlabs.protocol.das.v1.functions.FunctionDefinition;
import com.rawlabs.protocol.das.v1.tables.TableDefinition;
import java.util.List;
import java.util.Optional;

public interface DASSdk {

  /** @return a list of table definitions. */
  List<TableDefinition> getTableDefinitions();

  /** @return a list of function definitions. */
  List<FunctionDefinition> getFunctionDefinitions();

  /**
   * Retrieve a table by name.
   *
   * @param name table name
   * @return Optional containing the DASTable if present
   */
  Optional<DASTable> getTable(String name);

  /**
   * Retrieve a function by name.
   *
   * @param name function name
   * @return Optional containing the DASFunction if present
   */
  Optional<DASFunction> getFunction(String name);

  /**
   * Close the SDK.
   *
   * <p>This method can be used to perform any cleanup logic, such as releasing resources, closing
   * connections, etc.
   */
  default void close() {}

}
