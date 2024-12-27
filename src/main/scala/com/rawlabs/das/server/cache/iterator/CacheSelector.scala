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

package com.rawlabs.das.server.cache.iterator

import com.rawlabs.protocol.das.v1.query.Qual

/**
 * Represents a cache definition with:
 *  - cacheId: unique identifier for the cache
 *  - quals:   sequence of Qual objects
 */
case class CacheDef(cacheId: String, quals: Seq[Qual])

object CacheSelector {

  /**
   * Picks the "best" cache from a list of CacheDefs.
   * By "best," we mean the one whose `quals` list has the fewest elements.
   * If there's a tie, we pick the first among those tied for fewest.
   * If `cacheDefs` is empty, returns None.
   */
  def pickBestCache(cacheDefs: Seq[CacheDef]): Option[String] = {
    if (cacheDefs.isEmpty) {
      None
    } else {
      val best = cacheDefs.minBy(_.quals.size)
      Some(best.cacheId)
    }
  }
}
