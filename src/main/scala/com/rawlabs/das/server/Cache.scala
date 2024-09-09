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
import com.rawlabs.utils.core.RawService

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CancellationException, ConcurrentHashMap, Executors, TimeUnit}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
 * A cache service that stores rows of data, allowing concurrent access by multiple readers.
 * The service uses a bounded fullHistory for each cache entry and handles synchronization
 * between writers and readers using locks and condition variables.
 */
class Cache extends RawService {

  // Map to store cache entries, each consisting of a fullHistory, lock, and two conditions for synchronization
  private val cacheMap = new ConcurrentHashMap[String, (ListBuffer[Rows], ReentrantLock, Condition, Condition)]()

  // Map to track the completion status of cache writing tasks
  private val completionMap = new ConcurrentHashMap[String, Promise[Unit]]()

  // Map to track the number of active readers for each cache entry
  private val readerCountMap = new ConcurrentHashMap[String, AtomicInteger]()

  // Map to track the most advanced reader's position
  private val mostAdvancedReaderMap = new ConcurrentHashMap[String, AtomicInteger]()

  // Map to store the last access time for each cache entry, used for eviction purposes
  private val accessTimeMap = new ConcurrentHashMap[String, Long]()

  // Executor service to handle asynchronous tasks for writing data to the cache
  private val executor = Executors.newCachedThreadPool()

  // Maximum number of cache entries allowed in the cacheMap before evicting old entries
  private val maxCacheEntries = 100

  // Maximum difference between the writer and the most advanced reader before pausing writes
  private val maxAllowedDifference = 100

  // Timeout in milliseconds for a writer to wait for new readers before self-terminating
  private val writerTimeoutMillis = 30000L

  /**
   * Writes data to the cache if the specified cache entry does not already exist.
   *
   * @param cacheId The unique identifier for the cache entry.
   * @param task An iterator over rows to be written to the cache, which will be closed after use.
   * @return A Future that completes when the task is done or fails if an error occurs.
   */
  def writeIfNotExists(cacheId: String, task: => Iterator[Rows] with Closeable): Future[Unit] = {
    logger.debug("writeIfNotExists called for cacheId {}", cacheId)

    // Check if the number of cache entries exceeds the maximum allowed
    if (cacheMap.size() > maxCacheEntries) {
      // Sort cache entries by their last access time to find the least recently used entry
      logger.info("Cache size exceeded maxCacheEntries. Attempting to evict old entries.")
      val sortedCacheIds = accessTimeMap
        .entrySet()
        .toArray(Array.empty[java.util.Map.Entry[String, Long]])
        .sortBy(_.getValue)
        .map(_.getKey)

      var done = false
      // Attempt to evict the least recently used cache entry with no active readers
      for (cacheId <- sortedCacheIds if !done) {
        done = cancelIfNoReaders(cacheId)
      }
    }

    // Create a new cache entry if it does not already exist
    cacheMap.computeIfAbsent(
      cacheId,
      _ => {
        logger.debug("Creating new cache entry for id {}", cacheId)
        val lock = new ReentrantLock()
        val notFull = lock.newCondition() // Condition to signal that the fullHistory is not full
        val notEmpty = lock.newCondition() // Condition to signal that the fullHistory is not empty
        val fullHistory = ListBuffer.empty[Rows] // Buffer to hold rows of data

        val promise = Promise[Unit]() // Promise to track the completion status of the task
        completionMap.put(cacheId, promise)
        mostAdvancedReaderMap.put(cacheId, new AtomicInteger(0))

        // Update the access time whenever a new cache entry is created
        accessTimeMap.put(cacheId, System.currentTimeMillis())
        logger.debug("Cache entry created for id {}. Submitting task to executor.", cacheId)

        // Submit a task to the executor to handle writing data to the cache
        executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              val taskIterator = task
              try {
                // Iterate over the rows provided by the task and write them to the fullHistory
                for (chunk <- taskIterator) {
                  lock.lock()
                  try {
                    var mostAdvancedReader = mostAdvancedReaderMap.getOrDefault(cacheId, new AtomicInteger(0)).get()

                    // If the fullHistory is full, wait until there is space available or timeout
                    while ((fullHistory.size - mostAdvancedReader) >= maxAllowedDifference) {
                      logger.debug(
                        "Writer for cacheId {} waiting for readers to catch up. Most advanced reader: {}",
                        cacheId,
                        mostAdvancedReader
                      )
                      val awaitSuccess = notFull.await(writerTimeoutMillis, TimeUnit.MILLISECONDS)
                      // Check if the writer should self-terminate due to inactivity (no readers)
                      if (!awaitSuccess) {
                        val readerCount = readerCountMap.getOrDefault(cacheId, new AtomicInteger(0)).get()
                        if (readerCount == 0) {
                          throw new CancellationException(s"Writer self-terminated due to inactivity for id $cacheId")
                        } else {
                          logger.debug(
                            "Writer timed out waiting for readers for id {} but active readers. Reader count: {}",
                            cacheId,
                            readerCount
                          )
                        }
                      }
                      mostAdvancedReader = mostAdvancedReaderMap.getOrDefault(cacheId, new AtomicInteger(0)).get()
                    }
                    fullHistory.append(chunk) // Add the chunk of data to the fullHistory
                    notEmpty.signalAll() // Notify readers that new data is available
                    logger.debug(
                      "Data chunk added to fullHistory for cacheId {}. Buffer size now {}",
                      cacheId,
                      fullHistory.size
                    )
                  } finally {
                    lock.unlock()
                  }
                }
              } finally {
                // Ensure the task is closed when done or in case of an exception
                logger.debug("Closing task iterator for cacheId {}", cacheId)
                Try(taskIterator.close()).failed.foreach(_ => ()) // Ignore any exceptions from close
              }
            } catch {
              case ex: Exception =>
                logger.error("Exception occurred while writing to cacheId {}: {}", cacheId, ex.getMessage)
                promise.tryFailure(ex) // If an exception occurs, fail the promise
                lock.lock()
                try {
                  notEmpty.signalAll() // Notify all readers that the task has failed
                } finally {
                  lock.unlock()
                }
            } finally {
              lock.lock()
              try {
                if (!promise.isCompleted) {
                  logger.debug("Marking task as completed for cacheId {}", cacheId)
                  promise.trySuccess(()) // Mark the task as completed if not already done
                }
                notEmpty.signalAll() // Notify all readers that the task is complete
              } finally {
                lock.unlock()
              }
            }
          }
        })

        (fullHistory, lock, notFull, notEmpty) // Return the new cache entry
      }
    )

    // Return a future that completes when the task is done
    logger.debug("Returning future for cacheId {}", cacheId)
    completionMap.get(cacheId).future
  }

  /**
   * Reads data from the cache for the specified cache entry.
   *
   * @param cacheId The unique identifier for the cache entry to read from.
   * @return An iterator over the rows stored in the cache with closeable functionality.
   */
  def read(cacheId: String): Iterator[Rows] with Closeable = {
    logger.debug("read called for cacheId {}", cacheId)

    // Retrieve the cache entry for the specified ID or throw an exception if it doesn't exist
    val (fullHistory, lock, notFull, notEmpty) = Option(cacheMap.get(cacheId))
      .getOrElse {
        logger.warn("No cache found for id {}", cacheId)
        throw new NoSuchElementException(s"No cache found for id $cacheId")
      }

    // Retrieve the promise tracking the completion status of the cache writing task
    val promise = Option(completionMap.get(cacheId))
      .getOrElse {
        logger.warn("No task found for id {}", cacheId)
        throw new NoSuchElementException(s"No task found for id $cacheId")
      }

    // Increment the reader count for the cache entry
    val readerCount = readerCountMap.computeIfAbsent(cacheId, _ => new AtomicInteger(0))
    readerCount.incrementAndGet()
    logger.debug("Reader count incremented for cacheId {}. Current reader count: {}", cacheId, readerCount.get())

    // Update the access time whenever the cache entry is read
    accessTimeMap.put(cacheId, System.currentTimeMillis())

    val mostAdvancedReader = mostAdvancedReaderMap.getOrDefault(cacheId, new AtomicInteger(0))

    new Iterator[Rows] with Closeable {
      private var currentIndex = 0
      private var finished = false
      private var closed = false

      /**
       * Checks if more data is available in the cache.
       *
       * @return True if more data is available, false otherwise.
       */
      override def hasNext: Boolean = {
        if (closed) throw new IllegalStateException("Iterator has been closed")

        lock.lock() // Acquire the lock
        try {
          // Wait for data to be available if the fullHistory is empty and the task is not finished
          while (currentIndex >= fullHistory.size && !finished) {
            if (promise.isCompleted) {
              finished = true
              logger.debug("Task completed for cacheId {}. No more data to read.", cacheId)
            } else {
              logger.debug("Buffer empty for cacheId {}. Waiting for data.", cacheId)
              notEmpty.await() // Wait for data to be added to the fullHistory
            }
          }

          // If the task is complete and an exception occurred, rethrow it
          if (promise.isCompleted && currentIndex >= fullHistory.size && promise.future.value.exists(_.isFailure)) {
            val failure = promise.future.value.get.failed.get
            logger.error("Task failed for cacheId {}: {}", cacheId, failure.getMessage)
            throw failure // Rethrow the exception from the task
          }

          currentIndex < fullHistory.size // Check if there is more data in the fullHistory
        } finally {
          lock.unlock() // Release the lock after checking
        }
      }

      /**
       * Retrieves the next row from the cache.
       *
       * @return The next row of data.
       * @throws NoSuchElementException if there are no more elements.
       */
      override def next(): Rows = {
        if (!hasNext) throw new NoSuchElementException("No more elements")
        if (closed) throw new IllegalStateException("Iterator has been closed")

        lock.lock() // Acquire the lock before accessing the fullHistory
        try {
          val result = fullHistory(currentIndex)
          currentIndex += 1
          logger.debug("Returning next element for cacheId {}. Current index: {}", cacheId, currentIndex)
          // Notify the writer if this reader is the most advanced reader, as it can now write more data
          if (mostAdvancedReader.updateAndGet(oldPosition => math.max(oldPosition, currentIndex)) == currentIndex) {
            notFull.signalAll()
          }
          result
        } finally {
          lock.unlock() // Release the lock after processing
        }
      }

      /**
       * Closes the iterator, releasing resources and decrementing the reader count.
       */
      override def close(): Unit = {
        if (!closed) {
          logger.debug("Closing iterator for cacheId {}", cacheId)
          lock.lock()
          try {
            if (!closed) {
              closed = true
              readerCount.decrementAndGet()
              logger.debug(
                "Decrementing reader count for cacheId {}. Current reader count: {}",
                cacheId,
                readerCount.get()
              )
              // Notify the writer if this reader is the most advanced reader, as it can now write more data
              if (mostAdvancedReader.updateAndGet(oldPosition => math.max(oldPosition, currentIndex)) == currentIndex) {
                notFull.signalAll()
              }
            }
          } finally {
            lock.unlock()
          }
        }
      }
    }
  }

  /**
   * Cancels and removes a cache entry if there are no active readers.
   *
   * @param cacheId The unique identifier for the cache entry to cancel.
   * @return True if the entry was cancelled, false otherwise.
   */
  private def cancelIfNoReaders(cacheId: String): Boolean = {
    logger.debug("Attempting to cancel cacheId {} if no readers exist.", cacheId)

    // Get the number of active readers for the cache entry
    val readerCount = readerCountMap.getOrDefault(cacheId, new AtomicInteger(0))

    // If there are no active readers, proceed to cancel the cache entry
    if (readerCount.get() == 0) {
      logger.debug("No readers found for cacheId {}. Cancelling cache entry.", cacheId)

      // Try to fail the associated promise, indicating that the task was cancelled due to no readers
      Option(completionMap.get(cacheId)).foreach(_.tryFailure(new CancellationException("No readers left")))

      // Remove the cache entry and related metadata from all maps
      cacheMap.remove(cacheId)
      readerCountMap.remove(cacheId)
      completionMap.remove(cacheId)
      accessTimeMap.remove(cacheId) // Also remove the entry from the access time map

      logger.info("Cache entry {} successfully cancelled and removed.", cacheId)
      true // Return true indicating that the entry was cancelled
    } else {
      logger.debug("Active readers found for cacheId {}. Cache entry not cancelled.", cacheId)
      false // Return false if there are still active readers
    }
  }

  /**
   * Stops the cache service by shutting down the executor.
   * This method is typically called when the service is being stopped or terminated.
   */
  override def doStop(): Unit = {
    // Shut down the executor to stop accepting new tasks and terminate existing ones
    logger.info("Stopping Cache service. Shutting down executor.")
    executor.shutdown()
    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
      logger.warn("Executor did not terminate in the allotted time. Forcing shutdown.")
      executor.shutdownNow()
    } else {
      logger.info("Executor successfully terminated.")
    }
  }
}
