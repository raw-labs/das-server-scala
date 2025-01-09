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

package com.rawlabs.das.server.cache.catalog

import java.time.Instant
import java.util.UUID

import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}

/**
 * A structured object that describes the data in this cache:
 *   - tableId : which table the cache is for
 *   - quals : the qualifiers used to produce the cache
 *   - columns : the columns stored in the cache
 *   - sortKeys : any sort ordering used
 */
case class CacheDefinition(tableId: String, quals: Seq[Qual], columns: Seq[String], sortKeys: Seq[SortKey])

/**
 * Represents a single row in `cache_catalog`.
 *
 * We store the entire "CacheDefinition" in the `definition` column (as JSON).
 */
case class CacheEntry(
    cacheId: UUID,
    dasId: String,
    definition: CacheDefinition, // <-- replaced the old 'String'
    state: String,
    stateDetail: Option[String],
    creationDate: Instant,
    lastAccessDate: Option[Instant],
    activeReaders: Int,
    numberOfTotalReads: Int,
    sizeInBytes: Option[Long])

object CacheState {
  val InProgress = "in_progress"
  val Complete = "complete"
  val Error = "error"
  val VoluntaryStop = "voluntary_stop"
}
