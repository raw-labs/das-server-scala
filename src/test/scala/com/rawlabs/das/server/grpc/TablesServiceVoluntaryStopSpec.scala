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

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.iterator._
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.cache.manager.CacheManager.ListCaches
import com.rawlabs.das.server.cache.queue._
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt}

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.stream.Materializer
import akka.util.Timeout
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{ManagedChannel, Server, StatusRuntimeException}

/**
 * Test suite focusing on verifying the "voluntary stop" scenario: If the only reader "goes away" (closes the gRPC
 * stream before reading everything), the producer sees 0 active readers, waits for `gracePeriod`, then marks the cache
 * as VoluntaryStop.
 *
 * We only test with DAS ID=1 and "slow" table => 10 rows @ 500ms each. We'll do a partial read with a blocking stub,
 * then break out early, causing the stream to end. After a wait > gracePeriod, we check the manager => should see
 * VoluntaryStop.
 */
class TablesServiceVoluntaryStopSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  // 1) ActorSystem
  implicit private val system: ActorSystem[Nothing] =
    ActorSystem[Nothing](Behaviors.empty, "tables-service-voluntary-stop")

  implicit private val ec: ExecutionContext = system.executionContext
  implicit private val mat: Materializer = Materializer(system)
  implicit private val scheduler: Scheduler = system.scheduler
  implicit private val timeout: Timeout = Timeout(3.seconds)

  // 2) In-process gRPC server + channel
  private val inProcessServerName = "tables-service-voluntary-stop-" + System.currentTimeMillis()
  private var server: Server = _
  private var channel: ManagedChannel = _

  // 3) Our DAS config: ID="1" => has [big, small, in_memory, all_types, slow, broken]
  implicit private val settings: DASSettings = new DASSettings
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  // 4) Catalog (SQLite)
  private val dbUrl = s"jdbc:sqlite:file:${Files.createTempFile("testdb", ".sqlite")}"
  private val catalog: CacheCatalog = new SqliteCacheCatalog(dbUrl)

  // 5) Chronicle base dir
  private val baseDir = createTempDir("cacheTestDirVoluntaryStop")
  baseDir.mkdirs()

  // 6) The real chooseBestEntry
  private val chooseBestEntry: (CacheDefinition, List[CacheEntry]) => Option[(CacheEntry, Seq[Qual])] = {
    case (definition, possible) =>
      CacheSelector.pickBestCache(possible, definition.quals, definition.columns)
  }

  // 7) The real satisfiesAllQuals
  private val satisfiesAllQualsFn: (Row, Seq[Qual]) => Boolean = (row, quals) =>
    QualEvaluator.satisfiesAllQuals(row, quals)

  // 8) We set up some smaller producer config to ensure it won't produce all data at once
  //    and has a short grace period => easy to test.
  private val maxEntries = 2
  private val batchSize = 1
  private val gracePeriod = 3.seconds // short grace to see voluntary stop quickly
  private val producerInterval = 1.second // produce 1 row per second

  // 9) CacheManager
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
      "cacheManager-voluntary-stop")

  // 10) TableService
  private val tableServiceImpl = new TableServiceGrpcImpl(dasSdkManager, cacheManager)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Start in-process gRPC
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

  // gRPC blocking stubs
  private def blockingStub: TablesServiceGrpc.TablesServiceBlockingStub =
    TablesServiceGrpc.newBlockingStub(channel)

  // Helper method for a new temp dir
  private def createTempDir(prefix: String): File = {
    val dir = Files.createTempDirectory(prefix).toFile
    dir.mkdirs()
    dir
  }

  // Helper to read a few rows from the blocking streaming call
  private def partialRead(iterator: java.util.Iterator[Rows], rowLimit: Int): Int = {
    var total = 0
    while (iterator.hasNext && total < rowLimit) {
      val chunk = iterator.next()
      total += chunk.getRowsCount
    }
    total
  }

  // The test
  "TablesService" should {
    "trigger voluntary stop when the only reader stops reading (slow table)" in {
      // We'll produce up to 10 rows, 1 row/sec, but we only read e.g. 3 rows => exit => the streaming call is done => manager sees no readers => after grace => VOLUNTARY_STOP

      // 1) Build request for the "slow" table in DAS ID=1
      val req = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("slow")) // 10 rows, 500ms each
        .setPlanId("plan-volStop-1")
        .setQuery(Query.newBuilder().addColumns("column1")) // or any columns
        .build()

      // 2) Start reading from the blocking stub => read partial => break
      val streamingIt = blockingStub.executeTable(req)
      val readCount = partialRead(streamingIt, rowLimit = 3)
      readCount shouldBe 3
      channel.shutdownNow()

      // 3) We don't read further => the streaming call returns => the server sees the consumer completed => activeConsumers=0
      //   => the producer will wait for gracePeriod and mark the cache as VOLUNTARY_STOP

      // 4) Wait for gracePeriod + a bit => we set grace=3s => let's wait 5s
      Thread.sleep(5000)

      // 5) Check the manager => the cache should be marked VOLUNTARY_STOP
      val probe = TestProbe[List[CacheEntry]]()
      cacheManager ! ListCaches("1", probe.ref)
      val caches = probe.receiveMessage()

      val maybeEntry = caches.find(_.definition.tableId == "slow")
      maybeEntry.isDefined shouldBe true
      maybeEntry.get.state shouldBe CacheState.VoluntaryStop
    }
  }
}
