/**
 * Copyright 2024 RAW Labs S.A. All rights reserved.
 *
 * This source code is the property of RAW Labs S.A. It contains proprietary and confidential information that is
 * protected by applicable intellectual property and other laws. Unauthorized use, reproduction, or distribution of this
 * code, or any portion of it, may result in severe civil and criminal penalties and will be prosecuted to the maximum
 * extent possible under the law.
 */

package com.rawlabs.das.mock

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.sdk.scala.DASSdk
import com.rawlabs.das.sdk.scala.DASSdkBuilder

class DASMockBuilder extends DASSdkBuilder {

  override def dasType: String = "mock"

  override def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk = new DASMock(options)

}
