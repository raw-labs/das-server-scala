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

import com.rawlabs.protocol.das.Rows
import com.rawlabs.utils.core.RawUtils
import com.typesafe.scalalogging.StrictLogging

import java.io.Closeable
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

// TODO (msb): This is work-in-progress and not concluded.
//             What is missing:
//            - no limit to the number of cache entries, i.e. no eviction policy
//            - potentially exhausting the worker pool if too many cache entries are created
//            - potential memory exhaustion if the slowest reader has a very large lag to the producer
class DASResultCache {

  private val workerPool = Executors.newFixedThreadPool(10, RawUtils.newThreadFactory("cache"))

  private val cacheMap = mutable.HashMap[String, CacheEntry]()
  private val cacheMapLock = new Object

  def writeIfNotExists(cacheId: String, task: => Iterator[Rows] with Closeable): Unit = {
    cacheMapLock.synchronized {
      cacheMap.get(cacheId) match {
        case Some(_) => // Do nothing if cache already exists
        case None =>
          val cache = new CacheEntry(cacheId, task)
          cacheMap.put(cacheId, cache)
          workerPool.submit(new cache.WriterTask)
      }
    }
  }

  def read(cacheId: String): Iterator[Rows] with Closeable = {
    cacheMapLock.synchronized {
      cacheMap.get(cacheId) match {
        case Some(cache) => cache.read()
        case None => throw new AssertionError(s"Cache with id $cacheId does not exist")
      }
    }
  }

}

class CacheEntry(cacheId: String, task: Iterator[Rows] with Closeable) extends StrictLogging {

  private val maxBufferAhead = 10

  private val cache = mutable.ArrayBuffer[Rows]()

  // Index to track the producer's position
  @volatile private var producerIndex: Long = 0

  // Lock and Condition to protect reader positions and to signal producer
  private val readerLock = new ReentrantLock()
  private val readersUpdated = readerLock.newCondition()

  // Map to store reader positions (consumer name -> position)
  private val readerPositions = TrieMap[Int, Int]().withDefaultValue(0)

  private val readerCounter = new AtomicInteger(0)

  @volatile private var exception: Option[Throwable] = None

  class WriterTask extends Runnable {
    override def run(): Unit = {
      logger.trace(s"WriterTask started for cacheId $cacheId")
      try {
        while (true) {
          readerLock.lock()
          try {
            var fastestReaderPosition = getFastestReaderPosition

            logger.trace(
              s"Writer checking buffer ahead for cacheId $cacheId: producerIndex=$producerIndex, " +
                s"fastestReaderPosition=$fastestReaderPosition, " +
                s"bufferAhead=${producerIndex - fastestReaderPosition}"
            )

            while (producerIndex - fastestReaderPosition >= maxBufferAhead) {
              logger.trace(s"Writer waiting for readers to catch up for cacheId $cacheId")
              readersUpdated.await()
              fastestReaderPosition = getFastestReaderPosition
              logger.trace(
                s"Writer woke up for cacheId $cacheId: producerIndex=$producerIndex, " +
                  s"fastestReaderPosition=$fastestReaderPosition, " +
                  s"bufferAhead=${producerIndex - fastestReaderPosition}"
              )
            }
          } finally {
            readerLock.unlock()
          }

          // Produce more data
          try {
            if (task.hasNext) {
              val rows = task.next()
              readerLock.lock()
              try {
                cache.append(rows)
                producerIndex += 1
                logger.trace(s"Writer appended data for cacheId $cacheId: producerIndex=$producerIndex")
                readersUpdated.signalAll()
                logger.trace(s"Writer signaled readers after appending data for cacheId $cacheId")
              } finally {
                readerLock.unlock()
              }
            } else {
              task.close()
              readerLock.lock()
              try {
                cache.append(null)
                logger.trace(s"Writer reached end of data for cacheId $cacheId, appended null to cache")
                readersUpdated.signalAll()
                logger.trace(s"Writer signaled readers after appending end marker for cacheId $cacheId")
              } finally {
                readerLock.unlock()
              }
              logger.trace(s"WriterTask exiting for cacheId $cacheId")
              return
            }
          } catch {
            case e: Exception =>
              readerLock.lock()
              try {
                exception = Some(e)
                logger.error(s"Writer encountered exception for cacheId $cacheId: ${e.getMessage}", e)
                readersUpdated.signalAll()
                logger.trace(s"Writer signaled readers after exception for cacheId $cacheId")
              } finally {
                readerLock.unlock()
              }
              return
          }
        }
      } catch {
        case e: Exception =>
          readerLock.lock()
          try {
            exception = Some(e)
            logger.error(s"Writer encountered exception outside loop for cacheId $cacheId: ${e.getMessage}", e)
            readersUpdated.signalAll()
            logger.trace(s"Writer signaled readers after exception outside loop for cacheId $cacheId")
          } finally {
            readerLock.unlock()
          }
      }
    }
  }

  def read(): Iterator[Rows] with Closeable = {
    new Iterator[Rows] with Closeable {
      private val readerId = readerCounter.incrementAndGet()
      private var readerPosition: Int = 0
      private var lastFetchedData: Rows = _

      // Register reader's position when iterator is created
      readerLock.lock()
      try {
        readerPositions.update(readerId, readerPosition)
        logger.trace(
          s"Reader $readerId created for cacheId $cacheId: initial readerPosition=$readerPosition, " +
            s"current readerPositions=${readerPositions.toMap}"
        )
      } finally {
        readerLock.unlock()
      }

      override def hasNext: Boolean = {
        readerLock.lock()
        try {
          logger.trace(
            s"Reader $readerId checking hasNext for cacheId $cacheId: readerPosition=$readerPosition, " +
              s"cacheSize=${cache.size}, exception=$exception"
          )

          // Wait until data is available at the current position
          while (cache.size <= readerPosition && exception.isEmpty) {
            logger.trace(s"Reader $readerId waiting for data for cacheId $cacheId")
            readersUpdated.await() // Wait until producer adds data
            logger.trace(s"Reader $readerId woke up for cacheId $cacheId")
          }

          if (exception.nonEmpty) {
            // Producer has failed
            logger.error(s"Reader $readerId detected exception for cacheId $cacheId: ${exception.get.getMessage}")
            throw exception.get
          }

          if (cache.size <= readerPosition) {
            // Producer has stopped and cache has no more data
            logger.trace(s"Reader $readerId has no more data for cacheId $cacheId")
            return false
          }

          lastFetchedData = cache(readerPosition)
          val hasMore = lastFetchedData != null // Return true if data is available
          logger.trace(s"Reader $readerId hasNext result for cacheId $cacheId: $hasMore")
          hasMore
        } finally {
          readerLock.unlock()
        }
      }

      override def next(): Rows = {
        if (!hasNext) throw new NoSuchElementException("No more elements in the queue")
        if (lastFetchedData == null) throw new NoSuchElementException("End of data reached")

        // Update the consumer's position and return the fetched data
        readerPosition += 1
        updateReaderPosition(readerId, readerPosition)
        logger.trace(
          s"Reader $readerId advanced to readerPosition=$readerPosition for cacheId $cacheId, " +
            s"current readerPositions=${readerPositions.toMap}"
        )
        lastFetchedData
      }

      override def close(): Unit = {
        readerLock.lock()
        try {
          readerPositions.remove(readerId)
          logger.trace(
            s"Reader $readerId closed for cacheId $cacheId, removed from readerPositions, " +
              s"remaining readers=${readerPositions.keys}"
          )
          readersUpdated.signalAll() // Signal producer that a consumer has stopped
          logger.trace(s"Reader $readerId signaled producer after closing for cacheId $cacheId")
        } finally {
          readerLock.unlock()
        }
      }
    }
  }

  // Helper method to update a reader's position and signal the producer
  private def updateReaderPosition(readerId: Int, position: Int): Unit = {
    readerLock.lock()
    try {
      readerPositions.update(readerId, position)
      logger.trace(
        s"Reader $readerId updated position to $position for cacheId $cacheId, " +
          s"current readerPositions=${readerPositions.toMap}"
      )
      readersUpdated.signalAll() // Signal producer that a consumer has advanced
      logger.trace(s"Reader $readerId signaled producer after updating position for cacheId $cacheId")
    } finally {
      readerLock.unlock()
    }
  }

  // Helper method to get the fastest reader's position
  private def getFastestReaderPosition: Int = {
    readerLock.lock()
    try {
      val fastestPosition =
        if (readerPositions.nonEmpty) {
          readerPositions.values.max
        } else {
          producerIndex.toInt // Return producerIndex when no readers are present
        }
      logger.trace(
        s"Computed fastestReaderPosition=$fastestPosition for cacheId $cacheId, " +
          s"current readerPositions=${readerPositions.toMap}"
      )
      fastestPosition
    } finally {
      readerLock.unlock()
    }
  }

}
