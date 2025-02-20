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

/**
 * Top-level Exception. Message contains information that WILL BE shared with the end-user, so
 * ensure it does not leak sensitive information.
 */
public class DASException extends RuntimeException {
  public DASException(String message) {
    super(message);
  }

  public DASException(String message, Throwable cause) {
    super(message, cause);
  }
}
