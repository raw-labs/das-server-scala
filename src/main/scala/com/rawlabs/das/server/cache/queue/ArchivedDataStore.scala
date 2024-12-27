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

import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptTailer}
import net.openhft.chronicle.wire.DocumentContext

/**
 * A read-only wrapper around an on-disk ChronicleQueue that has already been fully produced.
 *
 * Multiple readers can be created in parallel (each with its own tailer). They will all start from the beginning (the
 * "start" of the queue).
 *
 * The queue is opened once in read-only mode and remains open for the lifetime of this ArchivedQueueData instance. Call
 * close() to release resources.
 */
class ArchivedDataStore[T](queueDir: File, codec: Codec[T]) extends AutoCloseable {

  /**
   * We open the ChronicleQueue in "readOnly" mode. This is recommended if you know you will not write to it anymore.
   */
  private val queue: ChronicleQueue =
    ChronicleQueue
      .singleBuilder(queueDir)
      .readOnly(true)
      .build()

  /**
   * Creates a new closeable iterator that starts reading from the very beginning of the queue. Each call to newReader()
   * returns a fresh tailer, so multiple readers can proceed in parallel.
   */
  def newReader(): CloseableIterator[T] = {
    new ArchivedDataStoreReader[T](queue.createTailer().toStart, codec)
  }

  /**
   * Closes the underlying ChronicleQueue, releasing any OS resources or file locks. After calling close(), no new
   * readers can be created reliably.
   */
  override def close(): Unit = {
    queue.close()
  }
}

/**
 * A read-only iterator that uses an ExcerptTailer to read from the queue. It stops reading upon:
 *   - Reaching a control message ("EOF", "ERROR", "VOLUNTARY_STOP"), or
 *   - Exhausting available data in the queue.
 */
class ArchivedDataStoreReader[T](private val tailer: ExcerptTailer, private val codec: Codec[T])
    extends CloseableIterator[T] {

  @volatile private var closed = false
  private var endReached = false
  private var error: Option[Throwable] = None
  private var nextItem: Option[T] = None

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    loadIfNecessary()
    // If an error was signaled, throw now
    error.foreach(e => throw e)
    nextItem.isDefined
  }

  override def next(): T = {
    if (!hasNext) {
      error.foreach(e => throw e)
      throw new NoSuchElementException("No more elements in ArchivedDataReader")
    }
    val elem = nextItem.get
    nextItem = None
    elem
  }

  /**
   * Attempt to read the next item, if we haven't already. This is a simpler approach than the concurrency-based
   * version, since no one is writing to the queue.
   */
  private def loadIfNecessary(): Unit = {
    if (endReached || nextItem.isDefined) return

    // Try reading the next document from the queue
    val dc: DocumentContext = tailer.readingDocument()
    if (!dc.isPresent) {
      // No more data in the queue
      dc.close()
      endReached = true
      return
    }

    try {
      if (dc.isData) {
        val wire = dc.wire()
        val sb = new java.lang.StringBuilder()
        wire.readEventName(sb)
        val eventName = sb.toString
        eventName match {
          case "data" =>
            // Normal data record
            wire.read("data").marshallable { inner =>
              nextItem = Some(codec.read(inner))
            }

          case "control" =>
            // Some 'final' message was stored (e.g. EOF, ERROR, STOP, etc.)
            val ctrl = wire.read("control").text()
            ctrl match {
              case "EOF" =>
                endReached = true
              case "ERROR" =>
                val msg = wire.read("message").text()
                error = Some(new RuntimeException(msg))
                endReached = true
              case "VOLUNTARY_STOP" =>
                endReached = true
              case unknown =>
                // If there's an unknown control, treat it as an end signal
                endReached = true
            }

          case _ =>
            // If there's an unexpected event name, skip or stop
            endReached = true
        }
      } else {
        // Something else we don't recognize, or metadata?
        endReached = true
      }
    } finally {
      dc.close()
    }
  }

  /**
   * Closes this reader. If called, subsequent hasNext()/next() calls will yield false or error.
   */
  override def close(): Unit = {
    closed = true
  }
}
