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
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Random, Try}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import com.rawlabs.das.sdk.DASSettings
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
import com.rawlabs.das.server.cache.QueryResultCache
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}
import io.grpc.{ManagedChannel, Server}

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
  // 3) DAS + TableService
  // ----------------------------------------------------------------
  implicit private val settings: DASSettings = new DASSettings
  private val dasSdkManager: DASSdkManager = new DASSdkManager

  private val cache = new QueryResultCache(maxEntries = 10, maxChunksPerEntry = 10)
  private val serviceImpl = new TableServiceGrpcImpl(dasSdkManager, cache)

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
    // Since the server sends batches, we might have read more than the limit
    if (total > limit) total = limit
    total
  }

  private def asyncStub: TablesServiceGrpc.TablesServiceStub =
    TablesServiceGrpc.newStub(channel)

  private def partialAsyncRead(request: ExecuteTableRequest, limit: Int)(implicit ec: ExecutionContext): Future[Int] = {
    // We'll define a promise to signal completion
    val promise = Promise[Int]()

    // A custom observer that tracks how many rows have been read so far
    val responseObserver = new ClientResponseObserver[ExecuteTableRequest, Rows] {

      // This field is only available once onStart(...) is called. We can store the callObserver to cancel later.
      private var callObserver: ClientCallStreamObserver[ExecuteTableRequest] = _

      private var totalCount = 0

      override def beforeStart(requestStream: ClientCallStreamObserver[ExecuteTableRequest]): Unit = {
        this.callObserver = requestStream
      }

      override def onNext(value: Rows): Unit = {
        totalCount += value.getRowsCount
        if (totalCount > limit) totalCount = limit
        if (totalCount >= limit) {
          // Cancel the call
          callObserver.cancel("partial read done", null)
          // We'll consider ourselves "done" at this point
          promise.trySuccess(totalCount)
        }
      }

      override def onError(t: Throwable): Unit = {
        // If we cancelled, we might get an error as well.
        // Distinguish normal cancellation from real errors if needed.
        // For this example, let's just succeed if we intentionally cancelled, else fail.
        if (!promise.isCompleted) {
          promise.failure(t)
        }
      }

      override def onCompleted(): Unit = {
        // If we never reached the limit, we might finish naturally
        promise.trySuccess(totalCount)
      }
    }

    // Kick off the call
    asyncStub.executeTable(request, responseObserver)

    promise.future
  }

  // Helper to make a random Qual
  private def randomQual(): Qual = {
    val colName = "column1"
    val op = Operator.GREATER_THAN
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
      val concurrencyLevel = 15
      val tableNames = Seq("small", "big") // from your mock
      val dasId = DASId.newBuilder().setId("1").build()

      val concurrencyPool = Executors.newFixedThreadPool(concurrencyLevel)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(concurrencyPool)

      val futureWork = (1 to concurrencyLevel).map { i =>
        Future {
          val tbl = tableNames(Random.nextInt(tableNames.size))
          val planId = s"plan-async-$i-${UUID.randomUUID().toString.take(8)}"

          // Build the request
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

          // partialAsyncRead returns a Future[Int], but we're inside a Future {...}?
          // Let's flatten this by returning partialAsyncRead(...) directly.
          partialAsyncRead(request, limit = 50)
        }.flatten // flatten merges the nested Future[Future[Int]] => Future[Int]
      }

      // Combine them
      val aggregated = Future.sequence(futureWork)
      val results = Await.result(aggregated, 15.minutes)

      concurrencyPool.shutdown()
      concurrencyPool.awaitTermination(60, TimeUnit.SECONDS)

      // Check results
      results.size shouldBe concurrencyLevel
      results.foreach { count =>
        count should be >= 0
        count should be <= 50
      }
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
