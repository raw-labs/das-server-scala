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

import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Random, Try}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.iterator.QualEvaluator
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.cache.manager.CacheManager.Command
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, Query, SimpleQual}
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._
import com.rawlabs.protocol.das.v1.types.{Value, ValueInt}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.stream.{Materializer, SystemMaterializer}
import akka.util.Timeout
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{Context, ManagedChannel, Server}

/**
 * A high-concurrency test suite that exercises parallel calls to "executeTable" with overlapping qualifiers, partial
 * reads, cancellations, etc.
 *
 * Adjust concurrency levels and durations as needed for stress-testing.
 */
class TablesServiceHighConcurrencySpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Futures {

  // ----------------------------------------------------------------
  // 1) ActorSystem & concurrency
  // ----------------------------------------------------------------
  implicit private val system: ActorSystem[Nothing] =
    ActorSystem[Nothing](Behaviors.empty, "TablesServiceHighConcurrencySpec")

  implicit private val ec: ExecutionContext = system.executionContext
  implicit private val mat: Materializer = SystemMaterializer(system).materializer
  implicit private val scheduler: Scheduler = system.scheduler
  implicit private val timeout: Timeout = Timeout(5.seconds)

  // Increase if your environment is slow:
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(0.5, Seconds))

  // ----------------------------------------------------------------
  // 2) In-process server + channel
  // ----------------------------------------------------------------
  private val inProcessServerName = "tables-service-high-concurrency-" + System.currentTimeMillis()
  private var server: Server = _
  private var channel: ManagedChannel = _

  // ----------------------------------------------------------------
  // 3) DAS + CacheManager + TableService
  // ----------------------------------------------------------------
  implicit private val settings: DASSettings = new DASSettings
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  // Catalog + manager
  private val dbUrl = s"jdbc:sqlite:file:${Files.createTempFile("testdb", ".sqlite")}"
  private val catalog: CacheCatalog = new SqliteCacheCatalog(dbUrl)

  private val baseDir = {
    val dir = Files.createTempDirectory("cacheHighConcurrency").toFile
    dir.mkdirs()
    dir
  }

  // A standard coverage function
  private val chooseBestEntry: (CacheDefinition, List[CacheEntry]) => Option[(CacheEntry, Seq[Qual])] = {
    case (definition, possible) =>
      // Use your real coverage logic; for illustration we just pick the first complete or in-progress one
      com.rawlabs.das.server.cache.iterator.CacheSelector.pickBestCache(possible, definition.quals, definition.columns)
  }

  // Basic row-qual evaluation
  private val satisfiesAllQualsFn: (Row, Seq[Qual]) => Boolean =
    (row, quals) => QualEvaluator.satisfiesAllQuals(row, quals)

  // The concurrency test might create many new caches, so pick a bigger maxEntries
  private val maxEntries = 10
  private val batchSize = 1000
  private val gracePeriod = 5.seconds
  private val producerInterval = 5.millis

  private val cacheManager: ActorRef[Command[Row]] =
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
      "cacheManager-highConcurrency")

  private val serviceImpl = new TableServiceGrpcImpl(dasSdkManager, cacheManager)

  // ----------------------------------------------------------------
  // 4) Setup & Teardown
  // ----------------------------------------------------------------
  override def beforeAll(): Unit = {
    super.beforeAll()

    // Start in-process gRPC
    server = InProcessServerBuilder
      .forName(inProcessServerName)
      .addService(serviceImpl)
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

  private def blockingStub: TablesServiceGrpc.TablesServiceBlockingStub =
    TablesServiceGrpc.newBlockingStub(channel)

  // Helper to do partial reads on the streaming Rows
  private def partialRead(stubIterator: java.util.Iterator[Rows], limit: Int): Int = {
    var total = 0
    while (stubIterator.hasNext && total < limit) {
      val chunk = stubIterator.next()
      total += chunk.getRowsCount
    }
    total
  }

  // Helper to make a random Qual
  private def randomQual(): Qual = {
    val colName = "column1" // if (Random.nextBoolean()) "column1" else "column2"
    val op = if (Random.nextBoolean()) Operator.GREATER_THAN else Operator.LESS_THAN
    val rndInt = Random.nextInt(100)
    val sq = SimpleQual
      .newBuilder()
      .setOperator(op)
      .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(rndInt)))
      .build()
    Qual.newBuilder().setName(colName).setSimpleQual(sq).build()
  }

  // ----------------------------------------------------------------
  // 5) Actual concurrency test(s)
  // ----------------------------------------------------------------

  "TablesService under high concurrency" should {

    "handle multiple concurrent executeTable calls on the same table" in {
      // We'll pick the "small" table from DAS ID=1, which yields 100 rows.
      // Then run concurrency requests with different or same qualifiers.

      val concurrencyLevel = 20
      val dasId = DASId.newBuilder().setId("1").build()

      // We'll generate futures for all concurrent calls
      val concurrencyPool = Executors.newFixedThreadPool(concurrencyLevel)
      implicit val concEC = ExecutionContext.fromExecutor(concurrencyPool)

      val allFutures = (1 to concurrencyLevel).map { i =>
        Future {
          val stub = blockingStub
          val planId = s"plan-concurrent-$i-${UUID.randomUUID().toString.take(8)}"
          val randomQ = randomQual() // e.g. "column1 > 37"

          val request = ExecuteTableRequest
            .newBuilder()
            .setDasId(dasId)
            .setTableId(TableId.newBuilder().setName("small"))
            .setPlanId(planId)
            .setQuery(Query.newBuilder().addQuals(randomQ).addColumns("column1"))
            .setMaxBatchSizeBytes(1024 * 1024)
            .build()

          val it = stub.executeTable(request)

          // We only read partial to test partial coverage:
          val partialCount = partialRead(it, limit = Random.nextInt(30) + 1)
          partialCount
        }
      }

      // Wait for all to finish
      val aggregatedF: Future[Seq[Int]] = Future.sequence(allFutures)
      val results = Await.result(aggregatedF, 30.seconds)

      concurrencyPool.shutdown()
      concurrencyPool.awaitTermination(60, TimeUnit.SECONDS)

      // We don't know exactly how many rows each partial read got, but we can check:
      results.size shouldBe concurrencyLevel
      results.foreach { c =>
        c should be >= 0
        c should be <= 30
      }

      // Optionally, we can now check the manager to see how many caches exist, etc.
      // For instance, the "small" table might have 1 or 2 caches if partial coverage is recognized.
      // We skip that here or do something like:
      // (Left as an exercise to confirm final states in your cache manager.)
    }

    "handle concurrency with different table IDs" in {
      // In the mock DAS ID=1, we have multiple tables: "small", "big", "all_types", etc.
      // Let's run concurrency across them randomly.

      val concurrencyLevel = 15
      val tableNames = Seq("small", "big") // from your DAS mock
      val dasId = DASId.newBuilder().setId("1").build()

      val concurrencyPool = Executors.newFixedThreadPool(concurrencyLevel)
      implicit val concEC = ExecutionContext.fromExecutor(concurrencyPool)

      val futureWork = (1 to concurrencyLevel).map { i =>
        Future {
          val stub = blockingStub
          val tbl = tableNames(Random.nextInt(tableNames.size))
          val planId = s"plan-mixed-$i-${UUID.randomUUID().toString.take(8)}"
          val request = ExecuteTableRequest
            .newBuilder()
            .setDasId(dasId)
            .setTableId(TableId.newBuilder().setName(tbl))
            .setPlanId(planId)
            .setQuery(
              Query
                .newBuilder()
                .addColumns("column1")
                .addQuals(randomQual()) // random qual
            )
            .build()

          val it = stub.executeTable(request)
          val partialRows = partialRead(it, limit = 50) // read up to 50

          (tbl, partialRows)
        }
      }

      val aggregated = Future.sequence(futureWork)
      val results = Await.result(aggregated, 3.minutes)

      concurrencyPool.shutdown()
      concurrencyPool.awaitTermination(60, TimeUnit.SECONDS)

      // We expect each future to have read up to 50 rows from whichever table.
      // "big" might have billions, so partial read is definitely < 50. "small" has only 100 total, etc.
      results.size shouldBe concurrencyLevel
      results.foreach { case (tbl, count) =>
        // We can't know exactly how many rows were read, but can sanity check
        count should be >= 0
        count should be <= 50
      }

      // Additional validations:
      //  - Possibly query manager state or metrics to confirm # of caches spawned vs. reused
    }

    "randomly cancel some calls to test partial consumption" in {
      // We'll do partial cancellations by forcibly shutting down the blocking iterator early
      // or by gRPC channel shutdown for some calls.

      val concurrencyLevel = 10
      val dasId = DASId.newBuilder().setId("1").build()

      val concurrencyPool = Executors.newFixedThreadPool(concurrencyLevel)
      implicit val concEC = ExecutionContext.fromExecutor(concurrencyPool)

      val futures = (1 to concurrencyLevel).map { i =>
        Future {
          val stub = blockingStub
          val planId = s"plan-cancel-$i-${UUID.randomUUID().toString.take(8)}"

          val req = ExecuteTableRequest
            .newBuilder()
            .setDasId(dasId)
            .setTableId(TableId.newBuilder().setName("slow")) // or "big"
            .setPlanId(planId)
            .setQuery(Query.newBuilder().addColumns("column1"))
            .setMaxBatchSizeBytes(1024 * 1024)
            .build()

          val it = stub.executeTable(req)
          var localCount = 0

          // Decide if we want to cancel early
          val doCancel = Random.nextInt(100) < 50 // 50% chance

          try {
            while (it.hasNext && localCount < 10) {
              // read a few
              val chunk = it.next()
              localCount += chunk.getRowsCount
            }
            if (doCancel) {
              // we pretend the client forcibly closes the stream here
              // in a real environment, you'd do stub.withInterceptors(...) or handle concurrency differently
              throw new RuntimeException("simulate client-cancel")
            }
          } catch {
            case _: Throwable =>
              // This simulates a forced cancel from the client side
              localCount
          }
          localCount
        }
      }

      val aggregated = Future.sequence(futures)
      val results = Await.result(aggregated, 60.seconds)

      concurrencyPool.shutdown()
      concurrencyPool.awaitTermination(60, TimeUnit.SECONDS)

      // Some calls might have thrown exceptions or forcibly canceled; we mostly just ensure no deadlocks
      results.size shouldBe concurrencyLevel
      // We can check that partial reads < 10, etc.
      results.foreach { c =>
        c should be <= 10
      }

      // Then optionally check how many caches are left in 'InProgress', 'VoluntaryStop', etc.
      // E.g., if all readers canceled, some caches might end up VoluntaryStop, etc.
    }

  }

}
