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
 * DASSdkInvalidArgumentException is thrown by DAS SDK methods when an invalid argument error is to be reported
 * to a user (e.g. string too long during an INSERT, missing mandatory predicate, etc.).
 */
public class DASSdkInvalidArgumentException extends RuntimeException {
    public DASSdkInvalidArgumentException(String message) {
        super(message);
    }

    public DASSdkInvalidArgumentException(String message, Throwable cause) {
        super(message, cause);
    }
}
