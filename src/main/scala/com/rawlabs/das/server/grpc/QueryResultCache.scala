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

package com.rawlabs.das.server.grpc

import scala.collection.mutable

import com.google.common.cache.{Cache, CacheBuilder, RemovalNotification}
import com.rawlabs.protocol.das.v1.services.ExecuteTableRequest
import com.rawlabs.protocol.das.v1.tables.Rows
import com.typesafe.scalalogging.StrictLogging

final case class QueryCacheKey(request: ExecuteTableRequest)

/**
 * ResultBuffer accumulates row chunks while the query stream is running. It stores up to maxSize chunks, and if that
 * limit is exceeded, it marks the result as overflowed. When the stream completes, if the result has not overflowed,
 * the accumulated chunks are registered into the cache.
 */
final class ResultBuffer(key: QueryCacheKey, maxSize: Int) {

  private val rows = mutable.Buffer.empty[Rows]
  // A flag indicating that the buffer has overflowed.
  private var full = false

  /**
   * Adds a new chunk of rows to the buffer. If the number of chunks exceeds maxSize, the buffer is cleared and marked
   * as full, meaning that the result will not be cached.
   */
  def addChunk(chunk: Rows): Unit = {
    if (!full && this.rows.size < maxSize) {
      this.rows += chunk
    } else {
      this.rows.clear()
      full = true
    }
  }

  /**
   * Called when the stream is finished. If the result did not overflow, register the accumulated chunks in the cache.
   */
  def register(): Unit = {
    if (!full) QueryResultCache.put(key, rows.toSeq)
  }

}

/**
 * QueryResultCache is a simple cache for query results using Guava Cache. It stores up to MAX_CACHES entries. Each
 * entry is a sequence of Rows (i.e. row chunks). A removal listener logs when an entry is discarded.
 */
object QueryResultCache extends StrictLogging {

  private val MAX_CACHES = 5
  private val MAX_CHUNKS_PER_CACHE = 10

  // Create a Guava cache with a maximum size and a removal listener to log evictions.
  private val cache: Cache[String, Seq[Rows]] = CacheBuilder
    .newBuilder()
    .maximumSize(MAX_CACHES)
    .removalListener((notification: RemovalNotification[String, Seq[Rows]]) => {
      logger.info(s"Entry for key [${notification.getKey}] removed due to ${notification.getCause}")
    })
    .build[String, Seq[Rows]]()

  /**
   * Creates a new ResultBuffer for a given query key. The buffer will accumulate up to MAX_CHUNKS_PER_CACHE chunks.
   */
  def newBuffer(key: QueryCacheKey): ResultBuffer = new ResultBuffer(key, MAX_CHUNKS_PER_CACHE)

  /**
   * Retrieves a cached result for the given key, if present. Returns an Iterator over the cached row chunks.
   */
  def get(key: QueryCacheKey): Option[Iterator[Rows]] = {
    val result = cache.getIfPresent(key.toString)
    if (result != null) {
      Some(result.iterator)
    } else {
      None
    }
  }

  /**
   * Registers the result (a sequence of row chunks) in the cache.
   */
  def put(key: QueryCacheKey, result: Seq[Rows]): Unit = {
    cache.put(key.toString, result)
  }

}
