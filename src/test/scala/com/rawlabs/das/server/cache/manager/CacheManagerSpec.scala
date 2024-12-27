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

package com.rawlabs.das.server.cache.manager

import java.io.File
import java.nio.file.Files
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.queue._

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.scaladsl.Source

object CacheManagerSpec {
  // A single test kit for the entire suite
  val testKit: ActorTestKit = ActorTestKit("CacheManagerSpecSystem")
}

class CacheManagerSpec extends AnyWordSpecLike with Matchers with Eventually with BeforeAndAfterAll {

  // Bring the testKit and system into scope
  import CacheManagerSpec.testKit
  implicit val system: ActorSystem[_] = testKit.system

  // Increase time if your environment is slow
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(0.2, Seconds))

  // After all tests finish, shut down the single ActorTestKit
  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  // ============================================================================
  // 1) The in-memory catalog, same as before
  // ============================================================================
  class InMemoryCacheCatalog extends CacheCatalog {
    private var entries: Map[UUID, CacheEntry] = Map.empty

    override def listByDasId(dasId: String): List[CacheEntry] =
      entries.values.filter(_.dasId == dasId).toList

    override def createCache(cacheId: UUID, dasId: String, description: String): Unit =
      entries += (cacheId -> CacheEntry(
        cacheId = cacheId,
        dasId = dasId,
        definition = description,
        state = CacheState.InProgress,
        stateDetail = None,
        creationDate = java.time.Instant.now(),
        lastAccessDate = None,
        numberOfTotalReads = 0,
        sizeInBytes = None))

    override def setCacheAsComplete(cacheId: UUID, sizeInBytes: Long): Unit =
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.Complete, sizeInBytes = Some(sizeInBytes)))
      }

    override def setCacheAsError(cacheId: UUID, msg: String): Unit =
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.Error, stateDetail = Some(msg)))
      }

    override def setCacheAsVoluntaryStop(cacheId: UUID): Unit =
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.VoluntaryStop))
      }

    override def addReader(cacheId: UUID): Unit =
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(numberOfTotalReads = e.numberOfTotalReads + 1))
      }

    override def removeReader(cacheId: UUID): Unit =
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(numberOfTotalReads = e.numberOfTotalReads - 1))
      }

    override def deleteCache(cacheId: UUID): Unit =
      entries -= cacheId

    // We'll treat Error/VoluntaryStop as "bad"
    override def listBadCaches(): List[(String, UUID)] =
      entries.values
        .filter(e => e.state == CacheState.Error || e.state == CacheState.VoluntaryStop)
        .map(e => (e.dasId, e.cacheId))
        .toList

    override def findCacheToDelete(): Option[UUID] =
      entries.headOption.map(_._1)

    override def close(): Unit = ()
  }

  // ============================================================================
  // 2) Helper methods
  // ============================================================================
  private def createTempDir(): File = {
    val dir = Files.createTempDirectory("cache-manager-spec").toFile
    dir.mkdirs()
    dir
  }

  private def chooseBestEntry(entries: List[CacheEntry]): Option[CacheEntry] =
    entries.headOption

  private val taskFactory: () => DataProducingTask[String] = () =>
    new DataProducingTask[String] {
      override def run() = {
        val it = List("Row1", "Row2", "Row3").iterator
        new BasicCloseableIterator(it, () => ())
      }
    }

  private val stringCodec = new Codec[String] {
    import net.openhft.chronicle.wire.{WireIn, WireOut}
    override def write(out: WireOut, value: String): Unit =
      out.write("value").text(value)
    override def read(in: WireIn): String =
      in.read("value").text()
  }

  // ============================================================================
  // 3) The test cases
  // ============================================================================
  "CacheManager" should {

    "remove bad caches on startup (CleanupBadCaches)" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()

      // 1) Inject a "bad" cache (Error or VoluntaryStop) BEFORE manager starts
      val badId = UUID.randomUUID()
      catalog.createCache(badId, "dasBad", "desc")
      catalog.setCacheAsError(badId, "some error")

      // 2) Spawn the manager => it should do CleanupBadCaches in `setup()`
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-1")

      // 3) Check that “bad” caches were removed
      val probe = TestProbe[List[CacheEntry]]()
      manager ! CacheManager.ListCaches("dasBad", probe.ref)
      eventually {
        probe.expectMessage(List.empty) // no caches
      }
    }

    "create a new cache if none is valid" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-2")

      // We ask for an iterator => manager should create a brand-new cache
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasA",
          taskDescription = "descA",
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = probe.ref))

      val ack = probe.receiveMessage()
      ack.cacheId.toString.length should be > 0
      ack.sourceFuture should not be null

      // Confirm it is now in the catalog
      val probeList = TestProbe[List[CacheEntry]]()
      manager ! CacheManager.ListCaches("dasA", probeList.ref)
      val listResp = probeList.receiveMessage()
      listResp.size shouldBe 1
    }

    "reuse a complete cache if available" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-3")

      // Step 1: Create a completed cache via manager's test command
      val preId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(preId, "dasA", "descA", CacheState.Complete, Some(999L))

      // Step 2: Now ask for a new iterator => should reuse the complete cache
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasA", "descX", taskFactory, stringCodec, probe.ref))
      val ack = probe.receiveMessage()
      ack.cacheId shouldBe preId

      // The future => Some(Source) for completed caches
      ack.sourceFuture.onComplete {
        case Success(Some(_)) => succeed
        case _                => fail("Expected a Source for a completed cache")
      }(system.executionContext)
    }

    "subscribe to an in-progress cache if available" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-4")

      // Step 1: Create an in-progress cache via manager
      val inProgId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(inProgId, "dasB", "descB", CacheState.InProgress)

      // Step 2: Ask for an iterator => manager should see the in-progress cache
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasB", "descX", taskFactory, stringCodec, probe.ref))

      val ack = probe.receiveMessage()
      ack.cacheId shouldBe inProgId // reuses the in-progress

      // The future => Some(Source) for an in-progress cache
      ack.sourceFuture.onComplete {
        case Success(Some(_)) => succeed
        case _                => fail("Expected a Source for an in-progress cache")
      }(system.executionContext)
    }

    "ignore error or voluntaryStop caches and create a new one" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-5")

      // Step 1: Create an error cache + a voluntaryStop cache
      val errorId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(errorId, "dasC", "descC", CacheState.Error)
      val stopId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(stopId, "dasC", "descC2", CacheState.VoluntaryStop)

      // Step 2: Now request an iterator => should ignore those and create new
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasC", "descNew", taskFactory, stringCodec, probe.ref))
      val ack = probe.receiveMessage()
      ack.cacheId should not be errorId
      ack.cacheId should not be stopId
    }

    "mark cache as complete when child data source signals CacheComplete" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-6")

      // Create an in-progress cache
      val cacheId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(cacheId, "dasX", "descX", CacheState.InProgress)

      // Send the CacheComplete message
      manager ! CacheManager.CacheComplete(cacheId, 12345L)

      // Eventually, the manager updates the catalog
      val probe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasX", probe.ref)
        val list = probe.receiveMessage()
        list.headOption.map(_.state) shouldBe Some(CacheState.Complete)
        list.headOption.flatMap(_.sizeInBytes) shouldBe Some(12345L)
      }
    }

    "mark cache as error when child data source signals CacheError" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-7")

      // Step 1: Create an in-progress
      val cacheId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(cacheId, "dasErr", "descErr", CacheState.InProgress)

      // Step 2: Manager receives CacheError
      manager ! CacheManager.CacheError(cacheId, "Boom!")

      // Step 3: Check catalog
      val probe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasErr", probe.ref)
        val list = probe.receiveMessage()
        list.head.state shouldBe CacheState.Error
        list.head.stateDetail shouldBe Some("Boom!")
      }
    }

    "mark cache as voluntary stop when child data source signals CacheVoluntaryStop" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-8")

      val cacheId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(cacheId, "dasStop", "descStop", CacheState.InProgress)

      // Manager gets CacheVoluntaryStop
      manager ! CacheManager.CacheVoluntaryStop(cacheId)

      val probe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasStop", probe.ref)
        val list = probe.receiveMessage()
        list.head.state shouldBe CacheState.VoluntaryStop
      }
    }

    "enforce maxEntries by deleting the oldest cache" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()

      // We'll set maxEntries = 1
      val manager = testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 1, chooseBestEntry), "manager-9")

      // Create 1 existing (complete) cache
      val oldId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(oldId, "dasOld", "descOld", CacheState.Complete, Some(111L))

      // Now request a new cache => manager enforces max=1, so oldId is deleted
      val probeAck = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasNew", "descNew", taskFactory, stringCodec, probeAck.ref))
      probeAck.receiveMessage() // Just to consume the ack

      // oldId should be gone
      val probeList = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasOld", probeList.ref)
        probeList.expectMessage(List.empty)
      }
    }

    "return an archived source for a completed cache" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager =
        testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-10")

      // Create a completed cache
      val cid = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(cid, "dasX", "descX", CacheState.Complete, Some(123L))

      // Now ask for an iterator => it should reuse the completed cache
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasX", "descX2", taskFactory, stringCodec, probe.ref))
      val ack = probe.receiveMessage()
      ack.cacheId shouldBe cid

      ack.sourceFuture.onComplete {
        case Success(opt) => opt should not be empty
        case Failure(ex)  => fail(s"Should have succeeded: $ex")
      }(system.executionContext)
    }

    "return None if the data source is already stopped" in {
      val catalog = new InMemoryCacheCatalog
      val baseDir = createTempDir()
      val manager =
        testKit.spawn(CacheManager[String](catalog, baseDir, maxEntries = 10, chooseBestEntry), "manager-11")

      // Create an in-progress cache
      val inProgId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(inProgId, "dasZ", "descZ", CacheState.InProgress)

      // Pretend the child is fully stopped (CacheVoluntaryStop)
      manager ! CacheManager.CacheVoluntaryStop(inProgId)

      // Now request a source => manager’s subscribe should yield AlreadyStopped => None
      val probe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator("dasZ", "descZ2", taskFactory, stringCodec, probe.ref))

      val ack = probe.receiveMessage()
      ack.sourceFuture.onComplete {
        case Success(None) => succeed
        case _             => fail("Expected None for AlreadyStopped")
      }(system.executionContext)
    }
  }
}
