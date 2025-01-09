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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.{ActorSystem, Terminated}
import akka.stream.scaladsl.Sink
import akka.stream.{Materializer, SystemMaterializer}
import akka.util.Timeout

/**
 * Rewritten spec for testing archived, read-only Chronicle data using the new AkkaChronicleDataSource to produce the
 * data, then ArchivedDataStore to consume it.
 */
class ArchivedDataStoreSpec
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
  implicit val typedSystem: ActorSystem[Nothing] = testKit.system
  implicit val mat: Materializer = SystemMaterializer(typedSystem).materializer
  implicit val ec: ExecutionContext = typedSystem.executionContext

  // Helper to create temp dirs
  private def createTempDir(prefix: String): File = {
    val dir: Path = Files.createTempDirectory(prefix)
    // ensure we eventually clean up
    dir.toFile.deleteOnExit()
    dir.toFile
  }

  // A simple integer codec (same as in AkkaChronicleDataSourceSpec)
  val intCodec: Codec[Int] = new Codec[Int] {
    override def write(out: net.openhft.chronicle.wire.WireOut, value: Int): Unit = {
      out.write("int").int32(value)
    }
    override def read(in: net.openhft.chronicle.wire.WireIn): Int = {
      in.read("int").int32()
    }
  }

  // A simple Range-based producer for [start..end]
  class RangeProducingTask(start: Int, end: Int) extends DataProducingTask[Int] {
    override def run(): CloseableIterator[Int] = {
      val it = (start to end).iterator
      new BasicCloseableIterator(it, () => ())
    }
  }

  // For typed ask pattern
  implicit val askTimeout: Timeout = 3.seconds

  // -----------------------------------------------------------------------------------
  // TEST #1: Multiple parallel readers of the archived store see [1..100].
  // -----------------------------------------------------------------------------------
  "ArchivedDataStore (read-only)" should {
    "allow multiple parallel readers to read [1..100] after the queue is finalized" in {
      val queueDir = createTempDir("archived-queue-test-")

      // 1) Produce [1..100] via AkkaChronicleDataSource
      val dataSource = new AkkaChronicleDataSource[Int](
        task = new RangeProducingTask(1, 100),
        queueDir = queueDir,
        codec = intCodec,
        batchSize = 10,
        gracePeriod = 5.seconds,
        producerInterval = 100.millis)

      // Force production by reading (and discarding) the entire stream
      val futOptSrc = dataSource.getReader() // returns Future[Option[Source[Int, _]]]
      val produceFut = futOptSrc.flatMap {
        case Some(src) => src.runWith(Sink.ignore) // read & discard
        case None      => Future.successful(())
      }

      // Wait for data production to finish
      produceFut.futureValue

      // Close the producer (which also closes the queue for writing)
      dataSource.close()

      // 2) Now open read-only / archived mode on the same directory
      val archivedData = new ArchivedDataStore[Int](queueDir, intCodec)

      try {
        // Create two parallel readers (each with its own tailer).
        val reader1Results = TrieMap[Int, Boolean]()
        val reader2Results = TrieMap[Int, Boolean]()

        val r1 = archivedData.newReader()
        val r2 = archivedData.newReader()

        // Read in parallel
        val f1 = Future {
          while (r1.hasNext) {
            val x = r1.next()
            reader1Results.put(x, true)
          }
          r1.close()
        }

        val f2 = Future {
          while (r2.hasNext) {
            val x = r2.next()
            reader2Results.put(x, true)
          }
          r2.close()
        }

        Await.result(Future.sequence(Seq(f1, f2)), 10.seconds)

        // Both readers see the entire 1..100 range
        reader1Results.keys.toList.sorted shouldBe (1 to 100).toList
        reader2Results.keys.toList.sorted shouldBe (1 to 100).toList

      } finally {
        archivedData.close()
      }

      // Clean up temp folder
      queueDir.delete()
    }

    // -----------------------------------------------------------------------------------
    // TEST #2: Confirm that EOF properly terminates archived readers.
    // -----------------------------------------------------------------------------------
    "stop reading upon an EOF control message, returning exactly the items produced" in {
      val queueDir = createTempDir("archived-queue-test-eof-")

      // 1) Produce [1..100] and finalize (EOF)
      val dataSource = new AkkaChronicleDataSource[Int](
        task = new RangeProducingTask(1, 100),
        queueDir = queueDir,
        codec = intCodec,
        batchSize = 10,
        gracePeriod = 5.seconds,
        producerInterval = 100.millis)

      // Force data production by reading them
      val produceFut = dataSource
        .getReader()
        .flatMap {
          case Some(src) =>
            // Actually collect them to ensure we read all 100
            src.runWith(Sink.seq)
          case None =>
            Future.successful(Seq.empty[Int])
        }

      produceFut.futureValue shouldBe (1 to 100)

      // Close producer => writes EOF
      dataSource.close()

      // 2) Now read from archived mode; expect exactly 100 items
      val archivedData = new ArchivedDataStore[Int](queueDir, intCodec)
      try {
        val r = archivedData.newReader()
        val results = ArrayBuffer[Int]()

        while (r.hasNext) {
          results += r.next()
        }
        r.close()

        results shouldBe (1 to 100)
      } finally {
        archivedData.close()
      }

      // Clean up
      queueDir.delete()
    }
  }
}
