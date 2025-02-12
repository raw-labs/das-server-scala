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
import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.google.protobuf.UnknownFieldSet.Field
import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.iterator._
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.cache.manager.CacheManager.{InjectCacheEntry, ListCaches}
import com.rawlabs.das.server.cache.queue._
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt}

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.{AskPattern, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
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
class TablesServiceEvictOldCacheSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

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

  // 4) Catalog
  private val dbUrl = s"jdbc:sqlite:file:${Files.createTempFile("testdb", ".sqlite")}"
  private val catalog: CacheCatalog = new SqliteCacheCatalog(dbUrl)

  // 5) Chronicle base dir
  private val baseDir = createTempDir("cacheTestDirComprehensive")
  baseDir.mkdirs()

  // 6) The real chooseBestEntry
  private val chooseBestEntry: (CacheDefinition, List[CacheEntry]) => Option[(CacheEntry, Seq[Qual])] = {
    case (definition, possible) =>
      // Exactly the required function:
      CacheSelector.pickBestCache(possible, definition.quals, definition.columns)
  }

  // 7) The real satisfiesAllQualsFn
  private val satisfiesAllQualsFn: (Row, Seq[Qual]) => Boolean = (row, quals) =>
    QualEvaluator.satisfiesAllQuals(row, quals)

  // 8) settings
  private val maxEntries = 2
  private val batchSize = 1000
  private val gracePeriod = 5.minutes
  private val producerInterval = 5.millis

  // 9) Manager
  private val cacheManager: ActorRef[CacheManager.Command[Row]] =
    system.systemActorOf(
      CacheManager[Row](
        catalog,
        baseDir,
        maxEntries,
        batchSize,
        gracePeriod,
        producerInterval,
        chooseBestEntry,
        satisfiesAllQualsFn),
      "cacheManager-comprehensive")

  // 10) TableService
  private val tableServiceImpl = new TableServiceGrpcImpl(dasSdkManager)

  private def createTempDir(prefix: String): File = {
    val dir = Files.createTempDirectory(prefix).toFile
    dir.mkdirs()
    dir
  }

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
      baseDir.deleteOnExit()
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
  private def simpleQualProto(colName: String, op: Operator, intVal: Int): Qual = {
    val sq = SimpleQual
      .newBuilder()
      .setOperator(op)
      .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal)))
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

    // eviction test => we have maxEntries=2 => forcibly create 2 complete => create new => oldest evicted
    "evict oldest cache when maxEntries=2 is exceeded" in {
      val idA = UUID.randomUUID()
      val idB = UUID.randomUUID()

      cacheManager ! InjectCacheEntry(idA, "1", CacheDefinition("tblA", Nil, Nil, Nil), CacheState.Complete, Some(100L))
      cacheManager ! InjectCacheEntry(idB, "1", CacheDefinition("tblB", Nil, Nil, Nil), CacheState.Complete, Some(200L))

      // Now we do a new table => manager must evict the oldest
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("small"))
        .setPlanId("plan-evict")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .build()

      blockingStub.executeTable(req) // triggers creation => evicts the oldest

      val probe = TestProbe[List[CacheEntry]]()
      cacheManager ! ListCaches("1", probe.ref)
      val newList = probe.receiveMessage()
      newList.size shouldBe 2
      newList.map(_.cacheId) should not contain idA
    }

  }
}
