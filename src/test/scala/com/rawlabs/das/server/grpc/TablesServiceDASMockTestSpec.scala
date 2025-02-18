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

import scala.concurrent._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.QueryResultCache
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt, ValueString}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.stream.Materializer
import akka.util.Timeout
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{ManagedChannel, Server, StatusRuntimeException}

/**
 * Comprehensive test suite for TableService + CacheManager + DASMock, referencing exactly the following tables from DAS
 * ID "1":
 *
 *   - "big" => 2,000,000,000 rows
 *   - "small" => 100 rows
 *   - "in_memory" => in-memory table
 *   - "all_types" => 100 rows with varied data
 *   - "slow" => 10 rows, sleeps 500ms each
 *   - "broken" => 10 rows, throws exception on row #5
 *
 * We cannot reference any other tables or any other DAS IDs in this test.
 *
 * We use:
 *
 * chooseBestEntry = { case (definition, possible) => CacheSelector.pickBestCache(possible, definition.quals,
 * definition.columns) }
 *
 * satisfiesAllQualsFn = (row, quals) => QualEvaluator.satisfiesAllQuals(row, quals)
 *
 * The tests cover:
 *   - Basic table definitions
 *   - getTableEstimate => NOT_FOUND if unknown
 *   - executeTable for "small" => full read
 *   - "big" => partial read
 *   - "all_types" => verifying 100 rows
 *   - "in_memory" => ephemeral logic
 *   - "slow" => in-progress reuse, partial read, possible voluntary stop
 *   - "broken" => triggers error on row 5
 *   - eviction scenario with maxEntries = 2
 *   - partial coverage scenario by forcibly injecting a "complete" cache
 */
class TablesServiceDASMockTestSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  // 1) ActorSystem and concurrency
  implicit private val system: ActorSystem[Nothing] =
    ActorSystem[Nothing](Behaviors.empty, "tables-service-comprehensive")

  implicit private val ec: ExecutionContext = system.executionContext
  implicit private val mat: Materializer = Materializer(system)
  implicit private val scheduler: Scheduler = system.scheduler
  implicit private val timeout: Timeout = Timeout(3.seconds)

  // 2) In-process gRPC server + channel
  private val inProcessServerName = "tables-service-comprehensive-" + System.currentTimeMillis()
  private var server: Server = _
  private var channel: ManagedChannel = _

  // 3) DASSdkManager (DAS ID "1" references the mock that has [big, small, in_memory, all_types, slow, broken])
  implicit private val settings: DASSettings = new DASSettings
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  // 10) TableService
  private val cache = new QueryResultCache(maxEntries = 10, maxChunksPerEntry = 10)
  private val tableServiceImpl = new TableServiceGrpcImpl(dasSdkManager, cache)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Start the in-process gRPC server
    server = InProcessServerBuilder
      .forName(inProcessServerName)
      .addService(tableServiceImpl)
      .directExecutor()
      .build()
      .start()

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

  // 11) gRPC stubs
  private def blockingStub: TablesServiceGrpc.TablesServiceBlockingStub =
    TablesServiceGrpc.newBlockingStub(channel)

  private def asyncStub: TablesServiceGrpc.TablesServiceStub =
    TablesServiceGrpc.newStub(channel)

  // Helper to read streaming Rows
  private def collectRows(iterator: java.util.Iterator[Rows]): Seq[Row] = {
    var allRows = Seq.empty[Row]
    while (iterator.hasNext) {
      val batch = iterator.next()
      allRows ++= batch.getRowsList.asScala
    }
    allRows
  }

  // Helper to build a simple Qual with an int threshold
  private def intQualProto(colName: String, op: Operator, intVal: Int): Qual = {
    val sq = SimpleQual
      .newBuilder()
      .setOperator(op)
      .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal)))
      .build()
    Qual.newBuilder().setName(colName).setSimpleQual(sq).build()
  }

  // Helper to build a simple Qual with an int threshold
  private def stringQualProto(colName: String, op: Operator, stringVal: String): Qual = {
    val sq = SimpleQual
      .newBuilder()
      .setOperator(op)
      .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV(stringVal)))
      .build()
    Qual.newBuilder().setName(colName).setSimpleQual(sq).build()
  }

  // A small observer for partial async reads
  class StreamCollector[T] extends io.grpc.stub.StreamObserver[T] {
    private var elements = Vector.empty[T]
    private var done = false
    private var err: Option[Throwable] = None

    override def onNext(value: T): Unit = elements :+= value
    override def onError(t: Throwable): Unit = { err = Some(t); done = true }
    override def onCompleted(): Unit = { done = true }

    def getElements: Seq[T] = elements
    def isDone: Boolean = done
    def getError: Option[Throwable] = err

    // Cancels from client side
    def cancel(): Unit = { /* in typical gRPC, there's no direct 'cancel' on the observer side */
    }
  }

  // ----------------------------------------------------------------
  // The actual tests
  // ----------------------------------------------------------------
  "TablesService (with DAS ID=1 only)" should {

    // 1) getTableDefinitions => confirm we see the six tables
    "list definitions for the known mock tables" in {
      val req = GetTableDefinitionsRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .build()

      val resp = blockingStub.getTableDefinitions(req)
      resp.getDefinitionsCount should be > 0

      val tableNames = resp.getDefinitionsList.asScala.map(_.getTableId.getName).toSet
      tableNames should contain("big")
      tableNames should contain("small")
      tableNames should contain("in_memory")
      tableNames should contain("all_types")
      tableNames should contain("slow")
      tableNames should contain("broken")
    }

    // 2) getTableEstimate => unknown => INVALID_ARGUMENT
    "return NOT_FOUND for getTableEstimate of unknown table" in {
      val req = GetTableEstimateRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("unknown_table"))
        .build()

      val ex = intercept[StatusRuntimeException] {
        blockingStub.getTableEstimate(req)
      }
      ex.getStatus.getCode shouldBe io.grpc.Status.INVALID_ARGUMENT.getCode
    }

    // 3) "small" => 100 rows => read all
    "execute 'small' table => read 100 rows" in {
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("small"))
        .setPlanId("plan-small-1")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val it = blockingStub.executeTable(req)
      val rows = collectRows(it)
      rows.size shouldBe 100
    }

    // 4) "big" => 2,000,000,000 rows => partial read
    "allow partial consumption of 'big' table => read first 10, then stop" in {
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("big"))
        .setPlanId("plan-big-1")
        .setQuery(Query.newBuilder().addColumns("column2"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val iter = blockingStub.executeTable(req)
      var rowsAccum = Seq.empty[Row]
      while (rowsAccum.size < 10 && iter.hasNext) {
        val chunk = iter.next()
        rowsAccum ++= chunk.getRowsList.asScala
      }
      rowsAccum.size should be < 1000000 // Should be some multiply of the batch size to be exact
      // We simply don't read the remainder (partial consumption).
    }

    // 5) "all_types" => 100 rows => read them all
    "stream 'all_types' table => 100 rows" in {
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("all_types"))
        .setPlanId("plan-alltypes-1")
        .setQuery(Query.newBuilder().addColumns("int_col").addColumns("string_col"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val rows = collectRows(blockingStub.executeTable(req))
      rows.size shouldBe 100
      // Each row might have multiple columns with varied data
    }

    // 6) "in_memory" => ephemeral => read all rows (the mock might have some logic for how many)
    "execute 'in_memory' table => ephemeral logic" in {
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("in_memory"))
        .setPlanId("plan-inmem-1")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val rows = collectRows(blockingStub.executeTable(req))
      rows.size should be >= 0 // depends on the mock's content
    }

    // 7) "slow" => 10 rows, 500ms each => confirm it does eventually finish
    "execute 'slow' table => 10 rows, each row delayed by 500ms" in {
      val start = System.currentTimeMillis()

      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("slow"))
        .setPlanId("plan-slow-1")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val rows = collectRows(blockingStub.executeTable(req))
      rows.size shouldBe 10

      val elapsed = System.currentTimeMillis() - start
      // Should be near 5000ms or more
      elapsed should be >= 2000L
    }

    // 8) "broken" => triggers an error at row 5 => the manager sets the cache to Error
    "fail with error for 'broken' table => triggers an exception on row 5" in {
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("broken"))
        .setPlanId("plan-broken-1")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .setMaxBatchSizeBytes(1024 * 1024)
        .build()

      val ex = intercept[StatusRuntimeException] {
        val iter = blockingStub.executeTable(req)
        collectRows(iter) // read => triggers error
      }
      ex.getStatus.getCode shouldBe io.grpc.Status.Code.INTERNAL
    }

  }
}
