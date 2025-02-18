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

import com.rawlabs.protocol.das.v1.tables.Row;
import java.io.Closeable;
import java.util.Iterator;

/** Represents an executable result set: an Iterator over Rows that must be closed. */
public interface DASExecuteResult extends Iterator<Row>, Closeable {
  // No additional methods; just a combination of Iterator<Row> and Closeable.
  // Implementations should properly handle resource cleanup in close().
}
