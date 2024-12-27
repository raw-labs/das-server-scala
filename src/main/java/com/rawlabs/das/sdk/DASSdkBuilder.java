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

import java.util.Map;

public interface DASSdkBuilder {

  /** @return the DAS type. */
  String getDasType();

  /**
   * Build a DASSdk with the provided options and settings.
   *
   * @param options a map of options
   * @param settings the settings
   * @return the DASSdk
   */
  DASSdk build(Map<String, String> options, DASSettings settings);
}
