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

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.Rows
import com.typesafe.scalalogging.StrictLogging

final case class QueryCacheKey(
    planId: String,
    quals: Seq[Qual],
    columns: Seq[String],
    sortKeys: Seq[SortKey],
    maybeLimit: Option[Long])

final class ResultCache(maxSize: Int) {

  private val rows = mutable.Buffer.empty[Rows]
  private var full = false

  def addChunk(chunk: Rows): Unit = {
    if (!full && this.rows.size < maxSize) {
      this.rows += chunk
    } else {
      this.rows.clear()
      full = true
    }
  }

  def content(): Option[Seq[Rows]] = {
    if (!full) {
      Some(rows.toSeq)
    } else {
      None
    }
  }

}

object QueryResultCache extends StrictLogging {

  private val MAX_CACHES = 5
  private val MAX_CHUNKS_PER_CACHE = 10
  private val cache: Cache[QueryCacheKey, Seq[Rows]] = CacheBuilder
    .newBuilder()
    .maximumSize(MAX_CACHES)
    .removalListener((notification: RemovalNotification[QueryCacheKey, Seq[Rows]]) => {
      logger.info(s"Entry for key [${notification.getKey}] removed due to ${notification.getCause}")
    })
    .build[QueryCacheKey, Seq[Rows]]()

  def newBuffer(): ResultCache = new ResultCache(MAX_CHUNKS_PER_CACHE)

  def get(key: QueryCacheKey): Option[Iterator[Rows]] = {
    val result = cache.getIfPresent(key)
    if (result != null) {
      Some(result.iterator)
    } else {
      None
    }
  }

  def register(key: QueryCacheKey, result: Seq[Rows]): Unit = {
    cache.put(key, result)
  }

}
