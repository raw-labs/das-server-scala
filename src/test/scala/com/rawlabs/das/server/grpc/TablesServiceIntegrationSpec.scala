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

import java.io.File
import java.nio.file.Files

import scala.concurrent.ExecutionContext
import scala.util.Try

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.QueryResultCache
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query.Query
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._

import akka.actor.typed.scaladsl.Behaviors
// gRPC stubs
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.stream.Materializer
import akka.util.Timeout
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{ManagedChannel, Server, StatusRuntimeException}

class TablesServiceIntegrationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  private var server: Server = _
  private var channel: ManagedChannel = _
  implicit private val system: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, "test-system")

  // Because we’re using an in-process server, we need a unique name for each test suite
  private val inProcessServerName = "tables-service-test-" + System.currentTimeMillis()

  implicit private val settings: DASSettings = new DASSettings

  // 1) Real DASSdkManager
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  // 3) Derive or import the required Akka Streams implicits
  implicit val ec: ExecutionContext = system.executionContext
  implicit val mat: Materializer = Materializer(system)
  implicit val scheduler: Scheduler = system.scheduler
  implicit val timeout: Timeout = Timeout.create(java.time.Duration.ofSeconds(3))

  override def beforeAll(): Unit = {
    super.beforeAll()

    // 4) Build the service implementation
    val cache = new QueryResultCache(maxEntries = 10, maxChunksPerEntry = 10)
    val serviceImpl = new TableServiceGrpcImpl(dasSdkManager, cache)

    // 5) Start an in-process gRPC server
    server = InProcessServerBuilder
      .forName(inProcessServerName)
      .addService(serviceImpl)
      .directExecutor() // executes calls on the same thread (for simplicity)
      .build
      .start()

    // 6) Create a channel to connect to the in-process server
    channel = InProcessChannelBuilder
      .forName(inProcessServerName)
      .directExecutor()
      .build()
  }

  override def afterAll(): Unit = {
    Try {
      if (channel != null) channel.shutdownNow()
      if (server != null) server.shutdownNow()
      system.terminate()
    }
    super.afterAll()
  }

  // 7) Define your stubs (blocking or async) for testing
  private def blockingStub: TablesServiceGrpc.TablesServiceBlockingStub =
    TablesServiceGrpc.newBlockingStub(channel)

  private def asyncStub: TablesServiceGrpc.TablesServiceStub =
    TablesServiceGrpc.newStub(channel)

  private def createTempDir(prefix: String): File = {
    val dir = Files.createTempDirectory(prefix).toFile
    dir.mkdirs()
    dir
  }

  "TablesService" should {

    "return definitions for getTableDefinitions" in {
      val req = GetTableDefinitionsRequest.newBuilder().setDasId(DASId.newBuilder().setId("1")).build()
      val resp = blockingStub.getTableDefinitions(req)
      // Check that we got something
      resp.getDefinitionsCount should be > 0
      // Depending on your mock, you can assert specific definitions, etc.
    }

    "fail with INVALID_ARGUMENT if table does not exist for getTableEstimate" in {
      // We get INVALID_ARGUMENT if the table does not exist, because that's a user
      // visible error. NOT_FOUND is used to trigger re-registering a missing DAS.
      val req = GetTableEstimateRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("unknown_table"))
        .build()

      val thrown = intercept[StatusRuntimeException] {
        blockingStub.getTableEstimate(req)
      }
      thrown.getStatus.getCode shouldBe io.grpc.Status.INVALID_ARGUMENT.getCode
    }

    "executeTable in streaming mode for a known table" in {
      // 1) Build request
      val request = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("small"))
        .setPlanId("fakePlanId")
        .setQuery(
          Query
            .newBuilder()
            .addColumns("column1")
            .addColumns("column2")
            // add some Quals, SortKeys if you want
            // e.g. Qual, SortKey
            .build())
        .build()

      // 2) We’ll do a blocking read on the streaming call by using an iterator
      val rowIterator = blockingStub.executeTable(request)

      // 3) Collect all Rows from the stream
      var totalRows = 0
      while (rowIterator.hasNext) {
        val chunk = rowIterator.next()
        totalRows += chunk.getRowsCount
      }

      totalRows should be > 0
      // By default, our mock DAS might generate 10 rows or something
      // Assert or verify that the data is what you expect in your mock
    }

  }

}
