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
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.queue._

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl._
import akka.stream.scaladsl.Source
import akka.util.Timeout

/**
 * A typed “CacheManager” actor for a specific data type T. It:
 *   - On startup, removes incomplete/error caches from the catalog & disk.
 *   - On GetIterator, it either reuses an existing entry or creates a new one.
 *   - On receiving lifecycle messages from child data sources (EOF, error, etc.), it updates the catalog.
 *   - It uses typed `ChronicleDataSource` children to produce data if needed.
 *   - Or it uses `ArchivedDataStore` to read from an existing cache.
 *   - `chooseBestEntry` is a user-supplied function that picks which entry (if any) to reuse.
 */
object CacheManager {

  // ================================================================
  // 1) Protocol (Messages)
  // ================================================================

  /** External request: "Give me an iterator/stream for the given (dasId, taskDescription)" */
  final case class GetIterator[T](
      dasId: String,
      taskDescription: String,
      makeTask: () => DataProducingTask[T],
      codec: Codec[T],
      replyTo: ActorRef[GetIteratorAck[T]])

  /** The manager replies with a chosen cacheId and a Future[Option[Source]] */
  final case class GetIteratorAck[T](cacheId: UUID, sourceFuture: Future[Option[Source[T, _]]])

  /**
   * A command that includes all messages we handle, including internal signals from children.
   */
  sealed trait Command[+T]

  // We wrap `GetIterator[T]` in a sealed trait so we have a single Command type.
  final case class WrappedGetIterator[T](msg: GetIterator[T]) extends Command[T]

  // On child data source completion/error/voluntary stop:
  final case class CacheComplete(cacheId: UUID, sizeInBytes: Long) extends Command[Nothing]
  final case class CacheError(cacheId: UUID, msg: String) extends Command[Nothing]
  final case class CacheVoluntaryStop(cacheId: UUID) extends Command[Nothing]

  // ------------------------------
  // TEST-ONLY commands (optional)
  // ------------------------------

  /** Ask manager to list all caches for a given dasId, then reply with the full list. */
  final case class ListCaches(dasId: String, replyTo: ActorRef[List[CacheEntry]]) extends Command[Nothing]

  /** Force-inject a “bad” cache into the catalog for startup testing, etc. */
  final case class InjectCacheEntry(
      cacheId: UUID,
      dasId: String,
      description: String,
      state: String,
      sizeInBytes: Option[Long] = None)
      extends Command[Nothing]

  // ================================================================
  // 2) Creating the manager
  // ================================================================
  def apply[T](
      catalog: CacheCatalog,
      baseDirectory: File,
      maxEntries: Int,
      chooseBestEntry: List[CacheEntry] => Option[CacheEntry]): Behavior[Command[T]] =
    Behaviors.setup { ctx =>
      // We store manager logic in a dedicated class for clarity
      new CacheManagerBehavior[T](ctx, catalog, baseDirectory, maxEntries, chooseBestEntry).create()
    }
}

/**
 * Internal class that holds the “mutable” state of the manager. We define a method `create()` that returns the
 * Behavior[Command].
 */
private class CacheManagerBehavior[T](
    ctx: ActorContext[CacheManager.Command[T]],
    catalog: CacheCatalog,
    baseDirectory: File,
    maxEntries: Int,
    chooseBestEntry: List[CacheEntry] => Option[CacheEntry]) {

  import ctx.executionContext // For futures

  // We'll keep track of child data source actors by cacheId
  private var dataSourceMap = Map.empty[UUID, ActorRef[ChronicleDataSource.ChronicleDataSourceCommand]]

  // On startup, cleanup bad caches and switch to running
  def create(): Behavior[CacheManager.Command[T]] = Behaviors.setup { ctx =>
    // do the cleanup now, synchronously, or in a blocking/future manner
    cleanupBadCaches()

    // once done, switch to the running behavior
    runningBehavior
  }

//  // On startup, schedule a “CleanupBadCaches”
//  def create(): Behavior[CacheManager.Command[T]] = Behaviors.setup { _ =>
//    ctx.self ! CacheManager.CleanupBadCaches
//    runningBehavior
//  }

  private def runningBehavior: Behavior[CacheManager.Command[T]] = Behaviors
    .receiveMessage[CacheManager.Command[T]] {

      case CacheManager.WrappedGetIterator(msg) =>
        val (cid, srcFut) = handleGetIterator(msg.dasId, msg.taskDescription, msg.makeTask, msg.codec)
        msg.replyTo ! CacheManager.GetIteratorAck(cid, srcFut)
        Behaviors.same

      // Child data source notifies completion
      case CacheManager.CacheComplete(cacheId, size) =>
        catalog.setCacheAsComplete(cacheId, size)
        ctx.log.info(s"Cache $cacheId marked COMPLETE, size=$size")
        Behaviors.same

      case CacheManager.CacheError(cacheId, message) =>
        catalog.setCacheAsError(cacheId, message)
        ctx.log.warn(s"Cache $cacheId marked ERROR: $message")
        Behaviors.same

      case CacheManager.CacheVoluntaryStop(cacheId) =>
        catalog.setCacheAsVoluntaryStop(cacheId)
        ctx.log.info(s"Cache $cacheId marked VOLUNTARY_STOP.")
        Behaviors.same

      // ============================
      // TEST-ONLY handling
      // ============================
      case CacheManager.ListCaches(dasId, replyTo) =>
        val result = catalog.listByDasId(dasId)
        replyTo ! result
        Behaviors.same

      case CacheManager.InjectCacheEntry(cacheId, dasId, desc, state, sizeOpt) =>
        // 1) Create the entry in the catalog if not present
        if (!catalog.listByDasId(dasId).exists(_.cacheId == cacheId)) {
          catalog.createCache(cacheId, dasId, desc)
        }
        // 2) Set the desired state
        state match {
          case CacheState.Complete      => catalog.setCacheAsComplete(cacheId, sizeOpt.getOrElse(0L))
          case CacheState.Error         => catalog.setCacheAsError(cacheId, "Injected error")
          case CacheState.VoluntaryStop => catalog.setCacheAsVoluntaryStop(cacheId)
          case CacheState.InProgress    => // do nothing, it’s already in-progress by default
        }
        Behaviors.same
    }
    .receiveSignal { case (_, PostStop) =>
      // Stop all children, close resources if needed
      dataSourceMap.values.foreach(ref => ctx.stop(ref))
      // Also close the catalog if you want (not strictly required if it’s a DB pool).
      Behaviors.same
    }

  // =====================================================
  // 1) Handling "GetIterator"
  // =====================================================
  private def handleGetIterator(
      dasId: String,
      taskDescription: String,
      makeTask: () => DataProducingTask[T],
      codec: Codec[T]): (UUID, Future[Option[Source[T, _]]]) = {

    // Find potential existing entries
    val possible = catalog
      .listByDasId(dasId)
      .filterNot(e => e.state == CacheState.Error || e.state == CacheState.VoluntaryStop)

    // Let the user pick
    chooseBestEntry(possible) match {
      case Some(entry) =>
        entry.state match {
          case CacheState.Complete =>
            // Return archived
            catalog.addReader(entry.cacheId)
            val futSrc = Future.successful(Some(buildArchivedSource(entry.cacheId, codec)))
            (entry.cacheId, futSrc)

          case CacheState.InProgress =>
            catalog.addReader(entry.cacheId)
            val dsRef = dataSourceMap.getOrElse(
              entry.cacheId, {
                val ref = spawnDataSource(entry.cacheId, makeTask, codec)
                dataSourceMap += (entry.cacheId -> ref)
                ref
              })
            val fut = subscribeForReader(dsRef, entry.cacheId, codec)
            (entry.cacheId, fut)

          case _ =>
            // Shouldn't happen if we filtered error/volStop
            (entry.cacheId, Future.successful(None))
        }

      case None =>
        // Create new
        enforceMaxEntries()
        val cacheId = UUID.randomUUID()
        catalog.createCache(cacheId, dasId, taskDescription)

        val dsRef = spawnDataSource(cacheId, makeTask, codec)
        dataSourceMap += (cacheId -> dsRef)

        catalog.addReader(cacheId)

        val fut = subscribeForReader(dsRef, cacheId, codec)
        (cacheId, fut)
    }
  }

  // =====================================================
  // 2) Spawning & Subscribing
  // =====================================================
  private def spawnDataSource(
      cacheId: UUID,
      makeTask: () => DataProducingTask[T],
      codec: Codec[T]): ActorRef[ChronicleDataSource.ChronicleDataSourceCommand] = {
    val dir = new File(baseDirectory, cacheId.toString)
    val storage = new ChronicleStorage[T](dir, codec)

    ctx.spawn(
      ChronicleDataSource[T](
        task = makeTask(),
        storage = storage,
        batchSize = 10,
        gracePeriod = 5.minutes,
        producerInterval = 500.millis,
        callbackRef = Some(ctx.messageAdapter[ChronicleDataSource.DataSourceLifecycleEvent] {
          case ChronicleDataSource.DataProductionComplete(size) => CacheManager.CacheComplete(cacheId, size)
          case ChronicleDataSource.DataProductionError(msg)     => CacheManager.CacheError(cacheId, msg)
          case ChronicleDataSource.DataProductionVoluntaryStop  => CacheManager.CacheVoluntaryStop(cacheId)
        })),
      s"datasource-$cacheId")
  }

  private def subscribeForReader(
      dsRef: ActorRef[ChronicleDataSource.ChronicleDataSourceCommand],
      cacheId: UUID,
      codec: Codec[T]): Future[Option[Source[T, _]]] = {

    implicit val timeout: Timeout = 3.seconds
    implicit val scheduler: Scheduler = ctx.system.scheduler

    dsRef
      .ask[ChronicleDataSource.SubscribeResponse](replyTo => ChronicleDataSource.RequestSubscribe(replyTo))
      .map {
        case ChronicleDataSource.Subscribed(consumerId) =>
          val dir = new File(baseDirectory, cacheId.toString)
          val tailerStorage = new ChronicleStorage[T](dir, codec)
          val stage = new ChronicleSourceGraphStage[T](dsRef, codec, tailerStorage, consumerId)
          Some(Source.fromGraph(stage))

        case ChronicleDataSource.AlreadyStopped =>
          None
      }
      .recover { case NonFatal(_) => None }
  }

  // =====================================================
  // 3) Build Archived
  // =====================================================
  private def buildArchivedSource(cacheId: UUID, codec: Codec[T]): Source[T, _] = {
    val dir = new File(baseDirectory, cacheId.toString)
    val archivedStore = new ArchivedDataStore[T](dir, codec)
    val reader = archivedStore.newReader()

    Source.fromIterator(() => reader).watchTermination() { (mat, doneF) =>
      doneF.onComplete(_ => reader.close())(executionContext)
      mat
    }
  }

  // =====================================================
  // 4) Cleanup & Eviction
  // =====================================================
  private def cleanupBadCaches(): Unit = {
    val bad = catalog.listBadCaches() // (dasId, cacheId)
    bad.foreach { case (_, cid) =>
      catalog.deleteCache(cid)
      dataSourceMap.get(cid).foreach(ctx.stop)
      dataSourceMap -= cid
      deleteCacheDir(cid)
    }
  }

  private def enforceMaxEntries(): Unit = {
    catalog.findCacheToDelete().foreach { oldestId =>
      catalog.deleteCache(oldestId)
      dataSourceMap.get(oldestId).foreach(ctx.stop)
      dataSourceMap -= oldestId
      deleteCacheDir(oldestId)
    }
  }

  private def deleteCacheDir(cacheId: UUID): Unit = {
    val dir = new File(baseDirectory, cacheId.toString)
    if (dir.isDirectory) {
      dir.listFiles().foreach(deleteRecursive)
    }
    dir.delete()
  }

  private def deleteRecursive(f: File): Unit = {
    if (f.isDirectory) {
      f.listFiles().foreach(deleteRecursive)
    }
    f.delete()
  }
}
