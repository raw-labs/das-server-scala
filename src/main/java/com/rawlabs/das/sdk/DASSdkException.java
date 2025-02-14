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

public class DASSdkException extends DASException {
  public DASSdkException(String message, Throwable cause) {
    super(message, cause);
  }

  public DASSdkException(String message) {
    super(message);
  }
}
