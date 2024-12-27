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

object CacheState {
  val InProgress = "in_progress"
  val Complete = "complete"
  val Error = "error"
  val VoluntaryStop = "voluntary_stop"
}

case class CacheEntry(
    cacheId: UUID,
    dasId: String,
    definition: String,
    state: String,
    stateDetail: Option[String],
    creationDate: Instant,
    lastAccessDate: Option[Instant],
    numberOfTotalReads: Int,
    sizeInBytes: Option[Long])
