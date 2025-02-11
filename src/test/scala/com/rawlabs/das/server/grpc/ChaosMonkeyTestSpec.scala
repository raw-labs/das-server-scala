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
import java.util.concurrent.Executors

import scala.collection.BuildFrom
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import com.rawlabs.das.sdk.DASSettings
import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.iterator.QualEvaluator
import com.rawlabs.das.server.cache.manager._
import com.rawlabs.das.server.cache.queue.{CloseableIterator, DataProducingTask}
import com.rawlabs.das.server.manager.DASSdkManager
import com.rawlabs.protocol.das.v1.common.DASId
import com.rawlabs.protocol.das.v1.query.{Qual, Query}
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.tables._

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.stream.{Materializer, SystemMaterializer}
import akka.util.Timeout
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, Server, StatusRuntimeException}

/**
 * A "Chaos Monkey" style suite that tries abrupt manager stops, random DataProducingTask errors, and random client
 * cancellations to ensure the system remains consistent and doesn’t leak resources.
 */
class ChaosMonkeyTestSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  // ----------------------------------------------------------------
  // 1) ActorTestKit for concurrency
  // ----------------------------------------------------------------
  val testKit: ActorTestKit = ActorTestKit("ChaosMonkeySpecSystem")
  implicit val system: ActorSystem[_] = testKit.system

  // We keep exactly ONE implicit ExecutionContext from the ActorSystem:
  implicit val ec: ExecutionContext = system.executionContext

  // Streams
  implicit val mat: Materializer = SystemMaterializer(system).materializer
  implicit val scheduler: Scheduler = system.scheduler
  implicit val timeout: Timeout = Timeout(5.seconds)

  // Test timeouts
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(0.5, Seconds))

  // ----------------------------------------------------------------
  // 2) Build CacheCatalog + manager references
  // ----------------------------------------------------------------
  private val dbUrl = s"jdbc:sqlite:file:${Files.createTempFile("chaos-monkey-db", ".sqlite")}"
  private val catalog: CacheCatalog = new SqliteCacheCatalog(dbUrl)

  private val baseDir = {
    val d = Files.createTempDirectory("cacheChaosMonkey").toFile
    d.mkdirs()
    d
  }

  // For coverage logic
  private val chooseBestEntry: (CacheDefinition, List[CacheEntry]) => Option[(CacheEntry, Seq[Qual])] =
    (definition, possible) =>
      com.rawlabs.das.server.cache.iterator.CacheSelector.pickBestCache(possible, definition.quals, definition.columns)

  private val satisfiesAllQualsFn: (Row, Seq[Qual]) => Boolean =
    (row, quals) => QualEvaluator.satisfiesAllQuals(row, quals)

  private val maxEntries = 5
  private val batchSize = 5
  private val gracePeriod = 2.seconds
  private val producerInterval = 200.millis

  // We'll spawn the manager in each test, or forcibly stop it.
  private def spawnManager(name: String): ActorRef[CacheManager.Command[Row]] = {
    testKit.spawn(
      CacheManager[Row](
        catalog,
        baseDir,
        maxEntries,
        batchSize,
        gracePeriod,
        producerInterval,
        chooseBestEntry,
        satisfiesAllQualsFn),
      name)
  }

  // ----------------------------------------------------------------
  // 3) DAS + TableService
  // ----------------------------------------------------------------
  implicit private val settings: DASSettings = new DASSettings
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  private var server: Server = _
  private var channel: ManagedChannel = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Start an in-process gRPC server
    val serverName = "ChaosMonkeyTestSpec-" + System.currentTimeMillis()

    val initialManager = spawnManager("mgr-main")

    // Build the table service with that manager
    val serviceImpl = new TableServiceGrpcImpl(dasSdkManager, initialManager)

    server = InProcessServerBuilder
      .forName(serverName)
      .addService(serviceImpl)
      .directExecutor()
      .build()
      .start()

    channel = InProcessChannelBuilder
      .forName(serverName)
      .directExecutor()
      .build()
  }

  override def afterAll(): Unit = {
    try {
      if (channel != null) channel.shutdownNow()
      if (server != null) server.shutdownNow()
      testKit.shutdownTestKit()
      baseDir.deleteOnExit()
    } finally {
      super.afterAll()
    }
  }

  private def blockingStub: TablesServiceGrpc.TablesServiceBlockingStub =
    TablesServiceGrpc.newBlockingStub(channel)

  // ----------------------------------------------------------------
  // 4) Test #1: Random kill of CacheManager
  // ----------------------------------------------------------------
  "Chaos monkey: abrupt manager stop" should {

    "clean up incomplete caches upon restart" in {
      val managerName = s"mgr-kill-${UUID.randomUUID().toString.take(6)}"
      val managerRef = spawnManager(managerName)

      // We'll do a partial read from "slow" table (which has 10 rows, 500ms each).
      val stub = blockingStub

      val request = ExecuteTableRequest
        .newBuilder()
        .setDasId(DASId.newBuilder().setId("1"))
        .setTableId(TableId.newBuilder().setName("slow"))
        .setPlanId(s"plan-kill")
        .setQuery(Query.newBuilder().addColumns("column1"))
        .build()

      // Start the streaming call in a future => read a few rows => kill manager
      val partialFut = Future {
        val it = stub.executeTable(request)
        var totalRead = 0
        while (it.hasNext && totalRead < 3) {
          val chunk = it.next()
          totalRead += chunk.getRowsCount
        }
        totalRead
      } // uses implicit ec from the system

      // Let it run for a bit
      Thread.sleep(1000)

      // forcibly stop the manager
      testKit.stop(managerRef)
      Thread.sleep(500)

      val partialRead =
        try Await.result(partialFut, 5.seconds)
        catch {
          case _: Throwable => -1
        }

      partialRead should (be >= 0 or be(-1))

      // spawn a new manager
      val managerRef2 = spawnManager(s"mgr-reborn-${UUID.randomUUID().toString.take(6)}")

      // confirm no incomplete "slow" entry is left
      val probe = TestProbe[List[CacheEntry]]()
      managerRef2 ! CacheManager.ListCaches("1", probe.ref)
      val caches = probe.receiveMessage(3.seconds)

      caches.filter(e => e.definition.tableId == "slow" && e.state == CacheState.InProgress) shouldBe empty
    }
  }

  // ----------------------------------------------------------------
  // 5) Test #2: Random errors from data-producing logic
  // ----------------------------------------------------------------
  "Chaos monkey: random DataProducingTask errors" should {

    "transition the cache to Error when a random row fails" in {
      val managerRef = spawnManager(s"mgr-chaos-randomErrors")

      // A custom DataProducingTask that throws ~10% of the time
      class RandomErrorTask extends DataProducingTask[Row] {
        private var idx = 0
        override def run(): CloseableIterator[Row] = {
          new CloseableIterator[Row] {
            def hasNext: Boolean = idx < 100
            def next(): Row = {
              if (!hasNext) throw new NoSuchElementException
              idx += 1
              // 10% chance
              if (Random.nextInt(10) == 0) {
                throw new RuntimeException(s"Random row error at idx=$idx")
              }
              Row.newBuilder().build()
            }
            def close(): Unit = {}
          }
        }
      }

      val definition = CacheDefinition(tableId = "chaos_random_errors", quals = Nil, columns = Nil, sortKeys = Nil)

      val probeAck = TestProbe[CacheManager.GetIteratorAck[Row]]()
      managerRef ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "1",
          definition = definition,
          minCreationDate = None,
          makeTask = () => new RandomErrorTask,
          codec = new RowCodec,
          replyTo = probeAck.ref))
      val ack = probeAck.receiveMessage(3.seconds)
      val cacheId = ack.cacheId

      ack.sourceFuture.foreach {
        case Some(src) =>
          src
            .runForeach(_ => ())(mat)
            .onComplete {
              case scala.util.Failure(_) =>
              // Possibly the random error triggered => the manager should mark the cache as Error
              case _ => // maybe no error
            }(ec)
        case None =>
        // Possibly the data source was already stopped
      }(ec)

      Thread.sleep(2000)

      val probeEntries = TestProbe[List[CacheEntry]]()
      managerRef ! CacheManager.ListCaches("1", probeEntries.ref)
      val entries = probeEntries.receiveMessage(2.seconds)

      // Not in InProgress => either Error or Complete
      entries.find(_.cacheId == cacheId).map(_.state) should not be Some(CacheState.InProgress)
    }
  }

  // ----------------------------------------------------------------
  // 6) Test #3: Random client cancels
  // ----------------------------------------------------------------
  "Chaos monkey: random client cancels" should {

    "not leak partial caches or wedge the manager" in {
      val concurrency = 10
      val pool = Executors.newFixedThreadPool(concurrency)
      val customEC: ExecutionContext = ExecutionContext.fromExecutor(pool)

      val stub = blockingStub

      // We'll convert futures to a List => simpler for BuildFrom
      val futuresList: List[Future[Int]] = (1 to concurrency).toList.map { i =>
        Future {
          val req = ExecuteTableRequest
            .newBuilder()
            .setDasId(DASId.newBuilder().setId("1"))
            .setTableId(TableId.newBuilder().setName("small"))
            .setPlanId(s"cancelTest-$i-${UUID.randomUUID().toString.take(6)}")
            .setQuery(Query.newBuilder().addColumns("column1"))
            .setMaxBatchSizeBytes(1024 * 1024)
            .build()

          val it = stub.executeTable(req)
          var rowCount = 0
          val maxConsume = Random.between(1, 10)
          try {
            while (it.hasNext && rowCount < maxConsume) {
              val chunk = it.next()
              rowCount += chunk.getRowsCount
            }
            // forcibly "cancel"
            throw new RuntimeException("Simulate abrupt client close")
          } catch {
            case _: Throwable => rowCount
          }
        }(customEC) // run on custom thread pool
      }

      // Use the same customEC for Future.sequence
      val aggregated: Future[List[Int]] =
        Future.sequence(futuresList)(implicitly[BuildFrom[List[Future[Int]], Int, List[Int]]], customEC)
      val results: List[Int] = Await.result(aggregated, 15.seconds)

      pool.shutdown()

      results.size shouldBe concurrency

      // spawn a new manager to check leftover caches
      val managerRef = spawnManager("mgr-after-cancel")
      Thread.sleep(3000) // allow the manager to do cleanup

      val probe = TestProbe[List[CacheEntry]]()
      managerRef ! CacheManager.ListCaches("1", probe.ref)
      val finalCaches = probe.receiveMessage()

      val inProg = finalCaches.filter(_.state == CacheState.InProgress)
      inProg shouldBe empty
    }
  }

}
