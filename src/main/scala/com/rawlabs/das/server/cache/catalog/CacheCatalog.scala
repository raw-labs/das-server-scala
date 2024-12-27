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

import java.util.UUID

trait CacheCatalog {

  // ------------------------------------------------------------------
  // Public API Methods
  // ------------------------------------------------------------------

  /** Creates a new cache entry. */
  def createCache(cacheId: UUID, dasId: String, definition: String): Unit

  /** List all rows by dasId. */
  def listByDasId(dasId: String): List[CacheEntry]

  /**
   * Increments `numberOfTotalReads` and updates `lastAccessDate` to now.
   */
  def addReader(cacheId: UUID): Unit

  /** Decrements `numberOfTotalReads`. */
  def removeReader(cacheId: UUID): Unit

  /** Marks the cache as complete, sets `sizeInBytes`. */
  def setCacheAsComplete(cacheId: UUID, sizeInBytes: Long): Unit

  /** Marks the cache as error, sets `stateDetail`. */
  def setCacheAsError(cacheId: UUID, errorMessage: String): Unit

  /** Marks the cache as voluntary_stop. */
  def setCacheAsVoluntaryStop(cacheId: UUID): Unit

  /** Deletes the row with the given cacheId. */
  def deleteCache(cacheId: UUID): Unit

  /** Returns all rows that are NOT in state "complete". */
  def listBadCaches(): List[(String, UUID)]

  /**
   * Finds the cacheId for the oldest lastAccessDate. We treat NULL lastAccessDate as oldest.
   *
   * We'll do it with a small trick in SQL: ORDER BY (lastAccessDate IS NOT NULL), lastAccessDate ASC and take the first
   * row (LIMIT 1).
   */
  def findCacheToDelete(): Option[UUID]

  /** Closes the JDBC connection. */
  def close(): Unit

}
