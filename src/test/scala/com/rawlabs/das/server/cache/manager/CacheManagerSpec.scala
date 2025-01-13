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
import java.time.Instant
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
import com.rawlabs.protocol.das.v1.query.Qual

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.scaladsl.Source

class CacheManagerSpec extends AnyWordSpecLike with Matchers with Eventually with BeforeAndAfterAll {

  // ------------------------------------------------------------------
  // 1) The single ActorTestKit & ActorSystem for the entire suite
  // ------------------------------------------------------------------
  val testKit: ActorTestKit = ActorTestKit("CacheManagerSpecSystem")
  implicit val system: ActorSystem[_] = testKit.system

  // Increase time if your environment or build is slow
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(0.2, Seconds))

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  // ------------------------------------------------------------------
  // 2) An in-memory catalog with an extra method to tweak creationDate
  // ------------------------------------------------------------------
  class InMemoryCacheCatalog extends CacheCatalog {
    protected var entries: Map[UUID, CacheEntry] = Map.empty

    override def listAll(): List[CacheEntry] =
      entries.values.toList

    override def countAll(): Int =
      entries.size

    override def listByDasId(dasId: String): List[CacheEntry] =
      entries.values.filter(_.dasId == dasId).toList

    override def listCache(dasId: String, tableId: String): List[CacheEntry] =
      entries.values.filter(e => e.dasId == dasId && e.definition.tableId == tableId).toList

    override def createCache(cacheId: UUID, dasId: String, definition: CacheDefinition): Unit = {
      entries += (cacheId -> CacheEntry(
        cacheId = cacheId,
        dasId = dasId,
        definition = definition,
        state = CacheState.InProgress,
        stateDetail = None,
        creationDate = Instant.now(),
        lastAccessDate = None,
        activeReaders = 0,
        numberOfTotalReads = 0,
        sizeInBytes = None))
    }

    override def setCacheAsComplete(cacheId: UUID, sizeInBytes: Long): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.Complete, sizeInBytes = Some(sizeInBytes)))
      }
    }

    override def setCacheAsError(cacheId: UUID, errorMessage: String): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.Error, stateDetail = Some(errorMessage)))
      }
    }

    override def setCacheAsVoluntaryStop(cacheId: UUID): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(state = CacheState.VoluntaryStop))
      }
    }

    override def addReader(cacheId: UUID): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(
          activeReaders = e.activeReaders + 1,
          numberOfTotalReads = e.numberOfTotalReads + 1,
          lastAccessDate = Some(Instant.now())))
      }
    }

    override def removeReader(cacheId: UUID): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(activeReaders = e.activeReaders - 1))
      }
    }

    override def deleteCache(cacheId: UUID): Unit = {
      entries -= cacheId
    }

    override def listBadCaches(): List[(String, UUID)] = {
      entries.values
        .filter(e => e.state == CacheState.Error || e.state == CacheState.VoluntaryStop)
        .map(e => (e.dasId, e.cacheId))
        .toList
    }

    override def resetAllActiveReaders(): Unit = {
      entries = entries.map { case (k, v) => k -> v.copy(activeReaders = 0) }
    }

    // We'll just pick the first in .headOption for eviction
    override def findCacheToDelete(): Option[UUID] =
      entries.headOption.map(_._1)

    override def close(): Unit = {
      // no-op
    }

    // Additional helper for tests:
    def setCreationDate(cacheId: UUID, date: Instant): Unit = {
      entries.get(cacheId).foreach { e =>
        entries += (cacheId -> e.copy(creationDate = date))
      }
    }
  }

  // ------------------------------------------------------------------
  // 3) Helper methods, chooseBestEntry, etc.
  // ------------------------------------------------------------------
  private def createTempDir(): File = {
    val dir = Files.createTempDirectory("cache-manager-spec").toFile
    dir.mkdirs()
    dir
  }

  // Our simplistic choice for coverage => pick the first entry
  private def chooseBestEntry(
      definition: CacheDefinition,
      possible: List[CacheEntry]): Option[(CacheEntry, Seq[Qual])] = {
    possible.headOption.map { entry =>
      // We return diffQuals = Seq.empty for simplicity
      (entry, Seq.empty)
    }
  }

  private def satisfiesAllQuals(record: String, quals: Seq[Qual]): Boolean = {
    // Not relevant for these tests, we'll just say false or assume no filtering needed
    false
  }

  private val taskFactory: () => DataProducingTask[String] = () =>
    new DataProducingTask[String] {
      override def run() = {
        val it = Iterator("Row1", "Row2", "Row3")
        new BasicCloseableIterator(it, () => ())
      }
    }

  private val stringCodec: Codec[String] = new Codec[String] {
    import net.openhft.chronicle.wire.{WireIn, WireOut}
    override def write(out: WireOut, value: String): Unit =
      out.write("value").text(value)
    override def read(in: WireIn): String =
      in.read("value").text()
  }

  // Convenience for test: spawn the manager
  private def spawnManager(
      catalog: CacheCatalog,
      maxEntries: Int = 10,
      batchSize: Int = 10,
      nameSuffix: String = "test"): ActorRef[CacheManager.Command[String]] = {
    val baseDir = createTempDir()
    testKit.spawn(
      CacheManager[String](
        catalog = catalog,
        baseDirectory = baseDir,
        maxEntries = maxEntries,
        batchSize = batchSize,
        gracePeriod = 5.minutes,
        producerInterval = 500.millis,
        chooseBestEntry = chooseBestEntry,
        satisfiesAllQuals = satisfiesAllQuals),
      s"manager-$nameSuffix")
  }

  // ============================================================================
  // 4) The test suite
  // ============================================================================

  "CacheManager" should {

    "remove bad caches on startup (CleanupBadCaches)" in {
      val catalog = new InMemoryCacheCatalog

      // 1) Create a bad (Error) cache before manager starts
      val badId = UUID.randomUUID()
      catalog.createCache(badId, "dasBad", CacheDefinition("tableBad", Nil, Nil, Nil))
      catalog.setCacheAsError(badId, "some error")

      // 2) Create a good complete cache with active reader
      val goodId = UUID.randomUUID()
      catalog.createCache(goodId, "dasGood", CacheDefinition("tableGood", Nil, Nil, Nil))
      catalog.addReader(goodId)
      catalog.setCacheAsComplete(goodId, 123L)

      // 3) Spawn manager => should do cleanup of bad caches
      val manager = spawnManager(catalog, nameSuffix = "cleanup")

      // 4) Eventually, bad caches gone, good remains
      val probe = TestProbe[List[CacheEntry]]()

      eventually {
        manager ! CacheManager.ListCaches("dasBad", probe.ref)
        probe.expectMessage(Nil) // no bad cache
      }

      eventually {
        manager ! CacheManager.ListCaches("dasGood", probe.ref)
        val list = probe.receiveMessage()
        list.map(_.cacheId) shouldBe List(goodId)
        list.head.activeReaders shouldBe 0
        list.head.state shouldBe CacheState.Complete
      }
    }

    "create a new cache if none is valid" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "newCache")

      val probeAck = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasA",
          definition = CacheDefinition("tableA", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = probeAck.ref))

      val ack = probeAck.receiveMessage()
      ack.cacheId.toString.length should be > 0
      val listProbe = TestProbe[List[CacheEntry]]()
      manager ! CacheManager.ListCaches("dasA", listProbe.ref)
      val list = listProbe.receiveMessage()
      list should have size 1
      list.head.cacheId shouldBe ack.cacheId
    }

    "reuse a complete cache if available" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "reuseComplete")

      val preId = UUID.randomUUID()
      // Force-inject a complete cache
      manager ! CacheManager.InjectCacheEntry(
        cacheId = preId,
        dasId = "dasA",
        description = CacheDefinition("tableA", Nil, Nil, Nil),
        state = CacheState.Complete,
        sizeInBytes = Some(999L))

      // Wait until manager sees it
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasA", listProbe.ref)
        val caches = listProbe.receiveMessage()
        caches.exists(_.cacheId == preId) shouldBe true
      }

      // Now ask for an iterator => should reuse the complete cache
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasA",
          definition = CacheDefinition("tableA", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()
      ack.cacheId shouldBe preId

      ack.sourceFuture.onComplete {
        case Success(Some(_)) =>
          succeed
        case other =>
          fail(s"Expected Some(Source) but got $other")
      }(system.executionContext)
    }

    "subscribe to an in-progress cache if available" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "reuseInProgress")

      val inProgId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId = inProgId,
        dasId = "dasB",
        description = CacheDefinition("tableB", Nil, Nil, Nil),
        state = CacheState.InProgress)

      // Wait for manager
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasB", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.exists(_.cacheId == inProgId) shouldBe true
      }

      // ask for iterator => manager should reuse it
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasB",
          definition = CacheDefinition("tableB", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()
      ack.cacheId shouldBe inProgId
    }

    "ignore error or voluntaryStop caches and create a new one" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "ignoreErrorStop")

      val errId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId = errId,
        dasId = "dasC",
        description = CacheDefinition("tableC", Nil, Nil, Nil),
        state = CacheState.Error)

      val stopId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId = stopId,
        dasId = "dasC",
        description = CacheDefinition("tableC2", Nil, Nil, Nil),
        state = CacheState.VoluntaryStop)

      // Wait for manager
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasC", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.map(_.cacheId).toSet shouldBe Set(errId, stopId)
      }

      // Now ask => manager should create new
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasC",
          definition = CacheDefinition("tableCNew", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()
      ack.cacheId should not be errId
      ack.cacheId should not be stopId
    }

    "mark cache as complete when child data source signals CacheComplete" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "markComplete")

      val cid = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId = cid,
        dasId = "dasX",
        description = CacheDefinition("tableX", Nil, Nil, Nil),
        state = CacheState.InProgress)

      // Wait for manager
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasX", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == cid) shouldBe true
      }

      // Now send CacheComplete
      manager ! CacheManager.CacheComplete(cid, 12345L)

      eventually {
        manager ! CacheManager.ListCaches("dasX", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.headOption.map(_.state) shouldBe Some(CacheState.Complete)
        list.headOption.flatMap(_.sizeInBytes) shouldBe Some(12345L)
      }
    }

    "mark cache as error when child data source signals CacheError" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "markError")

      val cid = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId = cid,
        dasId = "dasErr",
        description = CacheDefinition("tableErr", Nil, Nil, Nil),
        state = CacheState.InProgress)

      // Wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasErr", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == cid) shouldBe true
      }

      // manager gets CacheError
      manager ! CacheManager.CacheError(cid, "Boom!")
      eventually {
        manager ! CacheManager.ListCaches("dasErr", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.head.state shouldBe CacheState.Error
        list.head.stateDetail shouldBe Some("Boom!")
      }
    }

    "mark cache as voluntary stop when child data source signals CacheVoluntaryStop" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "markStop")

      val cid = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cid,
        "dasStop",
        CacheDefinition("tableStop", Nil, Nil, Nil),
        CacheState.InProgress)

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasStop", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == cid) shouldBe true
      }

      // now send voluntaryStop
      manager ! CacheManager.CacheVoluntaryStop(cid)
      eventually {
        manager ! CacheManager.ListCaches("dasStop", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.head.state shouldBe CacheState.VoluntaryStop
      }
    }

    "enforce maxEntries by deleting the oldest cache" in {
      val catalog = new InMemoryCacheCatalog
      // set maxEntries = 1
      val manager = spawnManager(catalog, maxEntries = 1, nameSuffix = "maxEntries")

      val oldId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        oldId,
        "dasOld",
        CacheDefinition("tableOld", Nil, Nil, Nil),
        CacheState.Complete,
        Some(111L))

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasOld", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == oldId) shouldBe true
      }

      // Now request a new cache => manager enforces max=1 => oldId is deleted
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasNew",
          definition = CacheDefinition("tableNew", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      ackProbe.receiveMessage()

      eventually {
        manager ! CacheManager.ListCaches("dasOld", listProbe.ref)
        listProbe.expectMessage(Nil)
      }
    }

    "return an archived source for a completed cache" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "archivedSource")

      val cid = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cid,
        "dasX",
        CacheDefinition("tableX", Nil, Nil, Nil),
        CacheState.Complete,
        Some(123L))

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasX", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == cid) shouldBe true
      }

      // now ask => reuses that completed cache => archived source
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasX",
          definition = CacheDefinition("tableX", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()
      ack.cacheId shouldBe cid

      ack.sourceFuture.onComplete {
        case Success(Some(_)) => succeed
        case Failure(ex)      => fail(s"Expected an archived source, got $ex")
        case _                => fail("Expected Some(Source), got None")
      }(system.executionContext)
    }

    "return None if the data source is already stopped" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "alreadyStopped")

      // create an in-progress
      val inProgId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        inProgId,
        "dasZ",
        CacheDefinition("tableZ", Nil, Nil, Nil),
        CacheState.InProgress)

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasZ", listProbe.ref)
        listProbe.receiveMessage().exists(_.cacheId == inProgId) shouldBe true
      }

      // Mark it voluntarily stopped
      manager ! CacheManager.CacheVoluntaryStop(inProgId)
      eventually {
        manager ! CacheManager.ListCaches("dasZ", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.head.state shouldBe CacheState.VoluntaryStop
      }

      // Now ask => sourceFuture => None
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasZ",
          definition = CacheDefinition("tableZ", Nil, Nil, Nil),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()

      ack.sourceFuture.onComplete {
        case Success(None) => succeed
        case other         => fail(s"Expected None, got $other")
      }(system.executionContext)
    }
  }

  // ============================================================================
  // 5) Tests specifically for minCreationDate
  // ============================================================================
  "CacheManager (with minCreationDate)" should {

    "ignore caches older than minCreationDate and reuse a newer one if present" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "minDate-ignore")

      // 1) Inject two completed caches
      val olderId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        olderId,
        "dasDate",
        CacheDefinition("tableDate", Nil, Nil, Nil),
        CacheState.Complete,
        Some(100L))

      val newerId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        newerId,
        "dasDate",
        CacheDefinition("tableDate", Nil, Nil, Nil),
        CacheState.Complete,
        Some(200L))

      // 2) Wait for them to appear in manager
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasDate", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.map(_.cacheId).toSet shouldBe Set(olderId, newerId)
      }

      // 3) Manually tweak creation dates in the *catalog*
      val now = Instant.now()
      catalog.setCreationDate(olderId, now.minusSeconds(30)) // older
      catalog.setCreationDate(newerId, now.minusSeconds(5)) // newer

      // 4) We'll do an eventually check again => manager always calls .listByDasId(...) => should see updated creationDates
      eventually {
        manager ! CacheManager.ListCaches("dasDate", listProbe.ref)
        val list = listProbe.receiveMessage()
        val maybeOlder = list.find(_.cacheId == olderId)
        val maybeNewer = list.find(_.cacheId == newerId)
        maybeOlder
          .map(_.creationDate)
          .getOrElse(Instant.EPOCH) should be < maybeNewer.map(_.creationDate).getOrElse(Instant.MAX)
      }

      // 5) Now ask with minCreationDate => we want to skip olderId, pick newerId
      val cutoff = Some(now.minusSeconds(10)) // exclude older (30s), include newer (5s)
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasDate",
          definition = CacheDefinition("tableDate", Nil, Nil, Nil),
          minCreationDate = cutoff,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()

      // This should pick the "newerId"
      ack.cacheId shouldBe newerId
    }

    "reuse an older cache if its creationDate is still >= minCreationDate" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "minDate-reuse")

      val cacheId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        cacheId,
        "dasSame",
        CacheDefinition("tableSame", Nil, Nil, Nil),
        CacheState.Complete,
        Some(999L))

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasSame", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.exists(_.cacheId == cacheId) shouldBe true
      }

      // set creation date => now - 10s
      val now = Instant.now()
      catalog.setCreationDate(cacheId, now.minusSeconds(10))

      eventually {
        manager ! CacheManager.ListCaches("dasSame", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.find(_.cacheId == cacheId).map(_.creationDate) shouldBe Some(now.minusSeconds(10))
      }

      // Request minCreationDate = now - 15s => the existing cache is still newer => should reuse
      val cutoff = Some(now.minusSeconds(15))
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasSame",
          definition = CacheDefinition("tableSame", Nil, Nil, Nil),
          minCreationDate = cutoff,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()
      ack.cacheId shouldBe cacheId
    }

    "create a new cache if all existing caches are older than minCreationDate" in {
      val catalog = new InMemoryCacheCatalog
      val manager = spawnManager(catalog, nameSuffix = "minDate-new")

      val oldId = UUID.randomUUID()
      manager ! CacheManager.InjectCacheEntry(
        oldId,
        "dasNewDate",
        CacheDefinition("tableNewDate", Nil, Nil, Nil),
        CacheState.Complete,
        Some(111L))

      // wait
      val listProbe = TestProbe[List[CacheEntry]]()
      eventually {
        manager ! CacheManager.ListCaches("dasNewDate", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.exists(_.cacheId == oldId) shouldBe true
      }

      // set old creation date => now - 20s
      val now = Instant.now()
      catalog.setCreationDate(oldId, now.minusSeconds(20))

      eventually {
        manager ! CacheManager.ListCaches("dasNewDate", listProbe.ref)
        val list = listProbe.receiveMessage()
        list.find(_.cacheId == oldId).map(_.creationDate) shouldBe Some(now.minusSeconds(20))
      }

      // Now minCreationDate = now - 5s => oldId is older => ignore => create brand new
      val cutoff = Some(now.minusSeconds(5))
      val ackProbe = TestProbe[CacheManager.GetIteratorAck[String]]()
      manager ! CacheManager.WrappedGetIterator(
        CacheManager.GetIterator(
          dasId = "dasNewDate",
          definition = CacheDefinition("tableNewDate", Nil, Nil, Nil),
          minCreationDate = cutoff,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo = ackProbe.ref))
      val ack = ackProbe.receiveMessage()

      ack.cacheId should not be oldId
    }
  }
}
