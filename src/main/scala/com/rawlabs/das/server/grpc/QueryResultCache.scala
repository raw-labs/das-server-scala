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

import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.Rows

final case class QueryCacheKey(
                                planId: String,
                                quals: Seq[Qual],
                                columns: Seq[String],
                                sortKeys: Seq[SortKey],
                                maybeLimit: Option[Long]
                              )

final case class CachedResult(batches: Seq[Rows])

object QueryResultCache {
  import java.util.concurrent.ConcurrentHashMap

  private val data = new ConcurrentHashMap[QueryCacheKey, CachedResult]()

  def get(key: QueryCacheKey): Option[CachedResult] =
    Option(data.get(key))

  def put(key: QueryCacheKey, value: CachedResult): Unit = {
    data.put(key, value)
  }

  def remove(key: QueryCacheKey): Unit = {
    data.remove(key)
  }
}
