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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.stream.scaladsl.Sink
import akka.stream.{Materializer, SystemMaterializer}

object ExampleTypedApp extends App {

  val guardian: Behavior[Nothing] = Behaviors.setup[Nothing] { ctx =>
    implicit val mat: Materializer = SystemMaterializer(ctx.system).materializer
    implicit val ec: ExecutionContext = ctx.executionContext
    import akka.util.Timeout
    implicit val askTimeout: Timeout = 3.seconds

    val myTask = new DataProducingTask[String] {
      override def run() = {
        val data = (1 to 5).map(i => s"Item $i").iterator
        new BasicCloseableIterator(data, () => println("Iterator closed!"))
      }
    }
    val myCodec = new Codec[String] {
      def write(out: net.openhft.chronicle.wire.WireOut, value: String): Unit =
        out.write("value").text(value)
      def read(in: net.openhft.chronicle.wire.WireIn): String =
        in.read("value").text()
    }

    val queueDir = new File("/tmp/chronicle-typed")
    val ds = new AkkaChronicleDataSource[String](
      task = myTask,
      queueDir,
      myCodec,
      batchSize = 3,
      gracePeriod = 5.seconds,
      producerInterval = 1.second)(ctx.system, mat, ec)

    ds.getReader().onComplete {
      case Success(Some(src)) =>
        println("Got a Source, let's run it!")
        src
          .runWith(Sink.foreach(elem => println(s"Got: $elem")))
          .onComplete { done =>
            println(s"Stream done: $done")
            ds.close()
            // The typed data source actor is now stopping, but the system remains alive
            // until we call system.terminate():
            ctx.system.terminate()
          }

      case Success(None) =>
        println("Data source already stopped.")
        ds.close()
        ctx.system.terminate()

      case Failure(ex) =>
        println(s"Failed to get reader: $ex")
        ds.close()
        ctx.system.terminate()
    }

    Behaviors.empty
  }

  val system = ActorSystem[Nothing](guardian, "MyTypedSystem")
}
