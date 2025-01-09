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

package com.rawlabs.das.server.cache.queue

import java.io.File
import java.nio.file.{Files, Path}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, ActorSystem, Terminated}
import akka.stream.scaladsl.Sink
import akka.stream.{Materializer, SystemMaterializer}
import net.openhft.chronicle.wire.{WireIn, WireOut}

class AkkaChronicleDataSourceSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually {

  // Increase patience for async tests
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  // Provide typed system + streams materializer
  implicit val typedSystem: ActorSystem[Nothing] =
    testKit.system
  implicit val mat: Materializer =
    SystemMaterializer(typedSystem).materializer
  implicit val ec: ExecutionContext =
    typedSystem.executionContext

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  private def createTempDir(prefix: String): File = {
    val dir: Path = Files.createTempDirectory(prefix)
    dir.toFile.deleteOnExit()
    dir.toFile
  }

  // ---------------------------------------------
  // Sample Codec[Int]
  // ---------------------------------------------
  val intCodec: Codec[Int] = new Codec[Int] {
    override def write(out: WireOut, value: Int): Unit = {
      out.write("int").int32(value)
    }
    override def read(in: WireIn): Int = {
      in.read("int").int32()
    }
  }

  // ---------------------------------------------
  // Sample DataProducingTask
  // ---------------------------------------------
  class RangeProducingTask(start: Int, end: Int) extends DataProducingTask[Int] {
    override def run(): CloseableIterator[Int] = {
      val it = (start to end).iterator
      new BasicCloseableIterator(it, () => ())
    }
  }

  "AkkaChronicleDataSource (typed)" should {

    "produce data to a single consumer and complete with EOF" in {
      val queueDir = createTempDir("test-single-consumer-")

      val dataSource = new AkkaChronicleDataSource[Int](
        task = new RangeProducingTask(1, 10),
        queueDir = queueDir,
        codec = intCodec,
        batchSize = 3,
        gracePeriod = 5.seconds,
        producerInterval = 100.millis)

      val futureSrcOpt = dataSource.getReader()
      val maybeSrc = futureSrcOpt.futureValue

      maybeSrc shouldBe defined
      val seqFut = maybeSrc.get.runWith(Sink.seq)
      seqFut.futureValue shouldBe (1 to 10)

      dataSource.close()
    }

    "allow multiple concurrent consumers, each reading from the beginning" in {
      val queueDir = createTempDir("test-multi-consumer-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 5),
        queueDir,
        intCodec,
        batchSize = 2,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      val srcF1 = dataSource.getReader()
      val srcF2 = dataSource.getReader()

      val resF1 = srcF1.flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Seq.empty[Int])
      }
      val resF2 = srcF2.flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Seq.empty[Int])
      }

      val c1 = resF1.futureValue
      val c2 = resF2.futureValue

      c1 shouldBe (1 to 5)
      c2 shouldBe (1 to 5)

      dataSource.close()
    }

    "allow a late-joining consumer to still read from the beginning" in {
      val queueDir = createTempDir("test-late-consumer-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 5),
        queueDir,
        intCodec,
        batchSize = 2,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      // #1 reads only first 2
      val partialF = dataSource.getReader().flatMap {
        case Some(src) => src.take(2).runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      partialF.futureValue shouldBe Seq(1, 2)

      // #2 arrives late => sees [1..5]
      val fullF2 = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      fullF2.futureValue shouldBe (1 to 5)

      // #1 can attach again => sees [1..5]
      val againF1 = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      againF1.futureValue shouldBe (1 to 5)

      dataSource.close()
    }

    "stop producing after grace period once all consumers have disconnected, then refuse new readers" in {
      val queueDir = createTempDir("test-grace-stop-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 999999),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 1.second,
        producerInterval = 50.millis)

      // #1 reads 5 items -> done
      val firstF = dataSource.getReader().flatMap {
        case Some(src) => src.take(5).runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      val c1Res = firstF.futureValue
      c1Res should have size 5

      Thread.sleep(1500) // let grace expire

      // next attempt => None
      dataSource.getReader().futureValue shouldBe None

      dataSource.close()
    }

    "producer signals EOF if the iterator ends, subsequent readers see the backlog" in {
      val queueDir = createTempDir("test-eof-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 3),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      val c1F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c1F.futureValue shouldBe Seq(1, 2, 3)

      // new consumer => same backlog
      val c2F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c2F.futureValue shouldBe Seq(1, 2, 3)

      dataSource.close()
    }

    "signal ERROR if the producer's iterator throws, causing readers to fail" in {
      val queueDir = createTempDir("test-producer-error-")

      class FaultyTask extends DataProducingTask[Int] {
        override def run(): CloseableIterator[Int] = new CloseableIterator[Int] {
          private val it = (1 to 5).iterator
          override def hasNext = it.hasNext
          override def next(): Int = {
            val n = it.next()
            if (n == 3) throw new RuntimeException("Boom on #3")
            n
          }
          override def close(): Unit = ()
        }
      }

      val dataSource = new AkkaChronicleDataSource[Int](
        new FaultyTask,
        queueDir,
        intCodec,
        batchSize = 2,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      val c1F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq).recover { case _ => Seq(-999) }
        case None      => Future.successful(Nil)
      }
      c1F.futureValue shouldBe Seq(-999)

      // In error => new consumer => None
      val c2F = dataSource.getReader().map {
        case Some(_) => fail("Expected no new consumer in ErrorState!")
        case None    => Seq(-111)
      }
      c2F.futureValue shouldBe Seq(-111)

      dataSource.close()
    }

    "prevent new consumers after a voluntary stop (no consumers -> grace -> stop)" in {
      val queueDir = createTempDir("test-voluntary-stop-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 1000000),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 1.second,
        producerInterval = 50.millis)

      Thread.sleep(1500) // let grace pass

      dataSource.getReader().futureValue shouldBe None
      dataSource.close()
    }

    "work with partial consumption by each consumer, not affecting others" in {
      val queueDir = createTempDir("test-partial-consumption-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 5),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      // #1 reads first 2
      val c1F = dataSource.getReader().flatMap {
        case Some(src) => src.take(2).runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c1F.futureValue shouldBe Seq(1, 2)

      // #2 reads all [1..5]
      val c2F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c2F.futureValue shouldBe (1 to 5)

      dataSource.close()
    }

    "not produce data indefinitely if no consumer, then forcibly stop on close()" in {
      val queueDir = createTempDir("test-no-consumers-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 1000000),
        queueDir,
        intCodec,
        batchSize = 10,
        gracePeriod = 5.minutes,
        producerInterval = 50.millis)

      // We'll watch termination via a probe that expects Terminated
      val termProbe = testKit.createTestProbe[Terminated]()
      termProbe.expectNoMessage(100.millis) // not yet

      // Expose dataSource.actorRef in your code if needed
      dataSource.close()

      // Wait for termination
      termProbe.expectTerminated(dataSource.actorRef, 3.seconds)
    }

    "allow reading after an EOF if the producer hasn't been forcibly closed" in {
      val queueDir = createTempDir("test-eof-backlog-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 2),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      val c1F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c1F.futureValue shouldBe Seq(1, 2)

      // new consumer => same backlog
      val c2F = dataSource.getReader().flatMap {
        case Some(src) => src.runWith(Sink.seq)
        case None      => Future.successful(Nil)
      }
      c2F.futureValue shouldBe Seq(1, 2)

      dataSource.close()
    }

    "reject new consumer if producer is in ErrorState or VoluntaryStop" in {
      val queueDir = createTempDir("test-stop-or-error-")

      class ImmediateErrorTask extends DataProducingTask[Int] {
        override def run(): CloseableIterator[Int] =
          throw new RuntimeException("Immediate explosion!")
      }

      val dataSource = new AkkaChronicleDataSource[Int](
        new ImmediateErrorTask,
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      Thread.sleep(500) // allow error

      dataSource.getReader().futureValue shouldBe None
      dataSource.close()
    }

    "support forced shutdown via close(), even if consumers are still attached" in {
      val queueDir = createTempDir("test-forced-shutdown-")

      val dataSource = new AkkaChronicleDataSource[Int](
        new RangeProducingTask(1, 100),
        queueDir,
        intCodec,
        batchSize = 5,
        gracePeriod = 5.seconds,
        producerInterval = 50.millis)

      // Start reading
      val c1F = dataSource.getReader().flatMap {
        case Some(src) =>
          src.throttle(1, 300.millis).runWith(Sink.seq)
        case None =>
          Future.successful(Nil)
      }

      val termProbe = testKit.createTestProbe[Terminated]()
      dataSource.close()

      // The reading might fail abruptly
      val outcome = c1F.failed.futureValue
      outcome shouldBe a[Throwable]

      // Now we expect the actor to terminate
      termProbe.expectTerminated(dataSource.actorRef, 3.seconds)
    }

  }
}
