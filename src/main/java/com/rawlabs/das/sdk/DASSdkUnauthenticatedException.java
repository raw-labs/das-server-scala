/*
 * Copyright 2025 RAW Labs S.A.
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
 * DASSdkUnauthenticatedException is thrown by DAS SDK methods when an unauthenticated error is to
 * be reported to a user (e.g. missing authentication token, invalid credentials, etc.).
 */
public class DASSdkUnauthenticatedException extends RuntimeException {
  public DASSdkUnauthenticatedException(String message) {
    super(message);
  }

  public DASSdkUnauthenticatedException(String message, Throwable cause) {
    super(message, cause);
  }
}
