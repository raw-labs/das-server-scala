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

import java.util

import scala.jdk.CollectionConverters._

import com.rawlabs.das.sdk.DASSettings

/**
 * Represents a builder for a DASSdk, while providing a Scala-friendly API.
 */
trait DASSdkBuilder extends com.rawlabs.das.sdk.DASSdkBuilder {

  def dasType: String

  def build(options: Map[String, String])(implicit settings: DASSettings): DASSdk

  final override def getDasType: String = dasType

  final override def build(options: util.Map[String, String], settings: DASSettings): com.rawlabs.das.sdk.DASSdk =
    new DASSdkScalaToJavaBridge(build(options.asScala.toMap)(settings))

}
