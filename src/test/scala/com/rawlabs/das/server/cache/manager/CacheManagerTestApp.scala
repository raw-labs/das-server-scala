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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import com.rawlabs.das.server.cache.catalog._
import com.rawlabs.das.server.cache.manager.CacheManager._
import com.rawlabs.das.server.cache.queue._
import com.rawlabs.protocol.das.v1.query.Qual

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.Sink
import akka.stream.{Materializer, SystemMaterializer}
import akka.util.Timeout

object CacheManagerTestApp extends App {

  // 1) Create ActorSystem
  implicit val system: ActorSystem[CacheManager.Command[String]] =
    ActorSystem(Behaviors.empty[CacheManager.Command[String]], "CacheManagerSystem")

  // 2) Materializer & ExecutionContext
  implicit val mat: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContext = system.executionContext
  implicit val timeout: Timeout = 3.seconds

  // Prepare catalog & baseDirectory
  val catalogDbUrl = "jdbc:sqlite:/tmp/mycache.db"
  val catalog = new SqliteCacheCatalog(catalogDbUrl)
  val baseDirectory = new File("/tmp/cacheData")

  // chooseBestEntry
  private def chooseBestEntry(
      cacheDefinition: CacheDefinition,
      possible: Seq[CacheEntry]): Option[(CacheEntry, Seq[Qual])] = {
    val best = possible.headOption
    best.map((_, Seq.empty))
  }

  // satisfiesAllQuals
  private def satisfiesAllQuals(row: String, quals: Seq[Qual]): Boolean = false

  // Spawn the manager actor
  val managerRef: ActorRef[CacheManager.Command[String]] = system.systemActorOf(
    CacheManager[String](
      catalog,
      baseDirectory,
      maxEntries = 10,
      batchSize = 10,
      gracePeriod = 5.minutes,
      producerInterval = 500.millis,
      chooseBestEntry,
      satisfiesAllQuals),
    "cacheManagerString")

  // Define a Task & Codec
  val taskFactory: () => DataProducingTask[String] = () =>
    new DataProducingTask[String] {
      override def run() = {
        val it = (1 to 5).map(i => s"Row $i").iterator
        new BasicCloseableIterator(it, () => println("Test iterator closed."))
      }
    }

  val stringCodec = new Codec[String] {
    def write(out: net.openhft.chronicle.wire.WireOut, value: String): Unit =
      out.write("value").text(value)

    def read(in: net.openhft.chronicle.wire.WireIn): String =
      in.read("value").text()
  }

  // Ask for an iterator
  // We wrap our GetIterator(...) inside WrappedGetIterator(...)
  val futureAck: Future[GetIteratorAck[String]] =
    managerRef.ask[GetIteratorAck[String]] { replyTo =>
      WrappedGetIterator(
        GetIterator(
          dasId = "testDas",
          definition = CacheDefinition("testTable", Seq.empty, Seq.empty, Seq.empty),
          minCreationDate = None,
          makeTask = taskFactory,
          codec = stringCodec,
          replyTo))
    }

  // Handle the future result
  futureAck.onComplete {
    case Success(GetIteratorAck(cacheId, srcFut)) =>
      println(s"Manager gave cacheId=$cacheId")
      srcFut.foreach {
        case Some(source) =>
          source
            .runWith(Sink.foreach(item => println(s"Got item: $item")))
            .onComplete { done =>
              println(s"Stream done: $done")
              system.terminate()
            }

        case None =>
          println("No source available (stopped or error).")
          system.terminate()
      }

    case Failure(ex) =>
      println(s"Failed to get iterator: ${ex.getMessage}")
      system.terminate()
  }
}
