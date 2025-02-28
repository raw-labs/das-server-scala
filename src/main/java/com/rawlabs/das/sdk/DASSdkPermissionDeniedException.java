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
 * DASSdkPermissionDeniedException is thrown by DAS SDK methods when a permission denied error is to
 * be reported to a user (e.g. missing required permissions, etc.).
 */
public class DASSdkPermissionDeniedException extends RuntimeException {
  public DASSdkPermissionDeniedException(String message) {
    super(message);
  }

  public DASSdkPermissionDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
