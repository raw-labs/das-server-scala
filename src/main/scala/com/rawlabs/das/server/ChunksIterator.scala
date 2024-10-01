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
import com.typesafe.scalalogging.StrictLogging

import java.io.Closeable
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Iterator implementation for processing query results in chunks.
 *
 * @param request The request containing query details (used to build a cache key).
 * @param resultIterator The iterator of query results.
 * @param maxChunkSize The maximum size of each chunk.
 */
class ChunksIterator(
    key: String,
    resultIterator: Iterator[Row] with Closeable,
    maxChunkSize: Int
) extends Iterator[Rows]
    with Closeable
    with StrictLogging {

  logger.debug(s"Initializing ChunksIterator with maxChunkSize: $maxChunkSize, key: $key")

  private val currentChunk = mutable.Buffer[Row]()
  private val maxChunksToCache = 5
  private var chunkCounter = 0
  private var eofReached = false

  private val completeChunkCache = mutable.Buffer[Rows]()

  /**
   * Checks if there are more rows to process.
   *
   * @return true if there are more rows, false otherwise.
   */
  override def hasNext: Boolean = {
    val hasNext = resultIterator.hasNext || currentChunk.nonEmpty
    logger.debug(s"hasNext() called. hasNext: $hasNext")
    if (!hasNext) {
      eofReached = true
      logger.debug("EOF reached in hasNext()")
    }
    hasNext
  }

  /**
   * Retrieves the next chunk of rows.
   *
   * @return The next chunk of rows.
   */
  override def next(): Rows = {
    if (!hasNext) {
      logger.debug("No more elements in next()")
      throw new NoSuchElementException("No more elements")
    }

    logger.debug(s"Fetching next chunk. Chunk counter: $chunkCounter")

    val nextChunk = getNextChunk()

    logger.debug(s"Next chunk fetched with ${nextChunk.getRowsCount} rows")

    // Cache the chunks up to a certain limit
    if (chunkCounter < maxChunksToCache) {
      // Append the chunk to the cache
      completeChunkCache.append(nextChunk)
      logger.debug(s"Appended chunk to cache. Cache size: ${completeChunkCache.size}")

      // If we reached the end of the result set (or this is the last chunk, since it's not complete),
      // cache the complete chunks read thus far for future use
      if (eofReached || nextChunk.getRowsCount < maxChunkSize) {
        logger.debug("Reached end of result set or last incomplete chunk. Caching complete chunks.")
        DASChunksCache.put(key, completeChunkCache)
        logger.debug("Chunks cached successfully.")
      }
    } else if (chunkCounter == maxChunksToCache) {
      // We bail out of trying to cache chunks because it's getting too big
      completeChunkCache.clear()
      logger.debug("Reached maxChunksToCache limit. Cleared completeChunkCache.")
    }

    chunkCounter += 1
    logger.debug(s"Incremented chunk counter to $chunkCounter")
    nextChunk
  }

  // Builds a chunk of rows by reading from the result iterator.
  private def getNextChunk(): Rows = {
    currentChunk.clear()
    logger.debug("Cleared currentChunk")
    while (resultIterator.hasNext && currentChunk.size < maxChunkSize) {
      currentChunk += resultIterator.next()
    }
    val rows = Rows.newBuilder().addAllRows(currentChunk.asJava).build()
    logger.debug(s"Built next chunk with ${currentChunk.size} rows")
    rows
  }

  /**
   * Closes the underlying result iterator.
   */
  override def close(): Unit = {
    resultIterator.close()
    logger.debug(s"Closed resultIterator for $key")
  }

}
