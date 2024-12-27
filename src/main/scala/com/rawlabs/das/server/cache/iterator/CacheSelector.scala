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

import com.rawlabs.das.server.cache.catalog.{CacheDefinition, CacheEntry}
import com.rawlabs.protocol.das.v1.query.Qual

object CacheSelector {

  /**
   * Attempt to select a single cache from `cacheDefs` that can serve the new request:
   *
   *   1. The cache’s columns must cover the requested columns. (requestedColumns ⊆ cacheDef.columns) 2. The request’s
   *      quals must imply (=>) the cache’s quals, so that the cache doesn’t contain any data that fails the new
   *      request. We use `QualSelectivityAnalyzer.differenceIfMoreSelective(oldQuals, newQuals)`.
   *      - Here, 'oldQuals' = cacheDef.quals
   *      - 'newQuals' = requestedQuals
   *      - If `differenceIfMoreSelective(old, new)` => Some(diff), that means new=>old plus diff are the “extra”
   *        constraints to apply.
   *      - If it’s None, the cache is not valid for this request (the new request isn’t guaranteed to be covered). 3.
   *        Among all valid caches, pick the one with the fewest `cacheDef.quals.size` (tie => first). 4. Return
   *        `(cacheId, differenceQuals)` so the caller knows which cache to use and which extra quals to apply.
   *
   * @return None if no cache qualifies; Some(cacheId, differenceQuals) if found.
   */
  def pickBestCache(
      cacheEntries: Seq[CacheEntry],
      requestedQuals: Seq[Qual],
      requestedColumns: Seq[String]): Option[(CacheEntry, Seq[Qual])] = {

    // 1) Filter by columns => the cache must contain at least all requested columns
    val columnFiltered = cacheEntries.filter { e =>
      requestedColumns.toSet.subsetOf(e.definition.columns.toSet)
    }

    // 2) Among those, see if `requestedQuals` => cd.quals using differenceIfMoreSelective(cd.quals, requestedQuals)
    //    If it returns Some(diff), it means newQuals is strictly more selective than oldQuals.
    //    That’s valid. The 'diff' are the extra constraints we must apply on top of the cache.
    val validCandidates: Seq[(CacheEntry, Seq[Qual])] =
      columnFiltered.flatMap { ce =>
        QualSelectivityAnalyzer
          .differenceIfMoreSelective(ce.definition.quals, requestedQuals)
          .map { diff => (ce, diff) }
      }

    if (validCandidates.isEmpty) {
      None
    } else {
      // 3) Pick the cache with the fewest `cd.quals.size`
      val best = validCandidates.minBy { case (ce, _) => ce.definition.quals.size }
      // best is (cacheDef, differenceQuals)
      Some((best._1, best._2))
    }
  }
}
