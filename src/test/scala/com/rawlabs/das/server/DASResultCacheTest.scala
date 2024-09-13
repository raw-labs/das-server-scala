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

package com.rawlabs.das.server

import com.rawlabs.protocol.das.{Row, Rows}
import com.rawlabs.protocol.raw.{Value, ValueInt}

import java.io.Closeable
import scala.collection.JavaConverters._

object DASResultCacheTest extends App {

  class TestIterator extends Iterator[Rows] with Closeable {
    var i = 0
    override def hasNext: Boolean = i < 10
    override def next(): Rows = {
      i += 1
      Rows
        .newBuilder()
        .addRows(
          Row
            .newBuilder()
            .putAllData(Map("col1" -> Value.newBuilder().setInt(ValueInt.newBuilder().setV(i)).build()).asJava)
        )
        .build()
    }
    override def close(): Unit = println("Closed")
  }

  println("CacheTest")

  val cache = new DASResultCache()
  cache.writeIfNotExists("1", new TestIterator)

  val reader1 = cache.read("1")
  val reader2 = cache.read("1")

  // Read data from both readers
  new Thread(() => {
    var i = 0
    reader1.foreach { v =>
      i += 1
      println("Thread 1 data: " + v.getRowsList.asScala.map(_.getDataMap.asScala))
    }
    println("Thread 1 read " + i + " rows")
    reader1.close()
  }).start()

  new Thread(() => {
    var i = 0
    reader2.foreach { v =>
      i += 1
      println("Thread 2 data: " + v.getRowsList.asScala.map(_.getDataMap.asScala))
    }
    println("Thread 2 read " + i + " rows")
    reader2.close()
  }).start()

  println("Done")

}
