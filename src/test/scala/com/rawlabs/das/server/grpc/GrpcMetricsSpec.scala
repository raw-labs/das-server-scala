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

package com.rawlabs.das.server.grpc

import kamon.Kamon
import kamon.testkit.InstrumentInspection.Syntax.{counterInstrumentInspection, distributionInstrumentInspection}
import kamon.testkit.MetricInspection
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

/** Exercises the generic GrpcMetrics trait. */
class GrpcMetricsSpec
  extends AnyWordSpec
    with Matchers
    with MetricInspection.Syntax
    with BeforeAndAfterAll {

  object DummySrv extends GrpcMetrics { override val serviceName = "DummySrv" }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Kamon.init()               // initialise Kamon for the test run
  }

  override protected def afterAll(): Unit = {
    Kamon.stop()
    super.afterAll()
  }

  "GrpcMetrics.withMetrics()" should {

    "increment the OK counter and record a latency" in {
      DummySrv.withMetrics("ping") { /** do nothing */ }

      // Counter
      Kamon
        .counter("grpc_requests_total")
        .withTag("grpc_service", "DummySrv")
        .withTag("grpc_method",  "ping")
        .withTag("grpc_status",  "OK")
        .value() shouldBe 1L

      // Histogram
      Kamon
        .histogram("grpc_requests_latency_millis")
        .withTag("grpc_service", "DummySrv")
        .withTag("grpc_method",  "ping")
        .withTag("grpc_status",  "OK")
        .distribution()
        .count shouldBe 1L
    }

    "increment the FAIL counter when an exception bubbles out" in {
      assertThrows[RuntimeException] {
        DummySrv.withMetrics("boom") { throw new RuntimeException("💥") }
      }

      Kamon
        .counter("grpc_requests_total")
        .withTag("grpc_service", "DummySrv")
        .withTag("grpc_method",  "boom")
        .withTag("grpc_status",  "FAIL")
        .value() shouldBe 1L
    }
  }
}
