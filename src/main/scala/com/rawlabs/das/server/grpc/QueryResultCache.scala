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

import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.Rows
import com.typesafe.scalalogging.StrictLogging

final case class QueryCacheKey(
    planId: String,
    quals: Seq[Qual],
    columns: Seq[String],
    sortKeys: Seq[SortKey],
    maybeLimit: Option[Long])

final class CachedResult(maxSize: Int) {

  private val rows = mutable.Buffer.empty[Rows]
  private var full = false

  def add(chunk: Rows): Unit = {
    if (!full && this.rows.size < maxSize) {
      this.rows += chunk
    } else {
      this.rows.clear()
      full = true
    }
    this.rows += chunk
  }

  def overflowed: Boolean = full
  def content(): Seq[Rows] = rows.toSeq

}

object QueryResultCache extends StrictLogging {
  import java.util.concurrent.ConcurrentHashMap

  private val MAX_CHUNKS = 10
  private val data = new ConcurrentHashMap[QueryCacheKey, CachedResult]()

  def newBuffer(): CachedResult = new CachedResult(MAX_CHUNKS)

  def get(key: QueryCacheKey): Option[CachedResult] =
    Option(data.get(key))

  def put(key: QueryCacheKey, result: CachedResult): Unit = {
    if (result.overflowed) {
      logger.warn(s"Query cache overflowed for key: $key")
    } else {
      logger.debug(s"Query cache populated for key: $key")
      data.put(key, result)
    }
  }

  def remove(key: QueryCacheKey): Unit = {
    data.remove(key)
  }

}
