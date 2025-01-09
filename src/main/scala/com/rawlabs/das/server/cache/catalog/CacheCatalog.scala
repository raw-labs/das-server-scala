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

/**
 * Defines methods for CRUD-like operations on the cache catalog.
 */
trait CacheCatalog {

  /** Creates a new cache entry with the given definition. */
  def createCache(cacheId: UUID, dasId: String, definition: CacheDefinition): Unit

  /** List all. */
  def listAll(): List[CacheEntry]

  /** Count all. */
  def countAll(): Int

  /** List all rows by dasId. */
  def listByDasId(dasId: String): List[CacheEntry]

  /**
   * List all rows that match the given dasId AND the given tableId (found within the definition). We'll parse each
   * definition and filter by `tableId`.
   */
  def listCache(dasId: String, tableId: String): List[CacheEntry]

  /** Increments `activeReaders`, `numberOfTotalReads` and updates `lastAccessDate` to now. */
  def addReader(cacheId: UUID): Unit

  /** Decrements `activeReaders`. */
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

  /** Reset all active readers to 0. */
  def resetAllActiveReaders(): Unit

  /** Finds the cacheId for the oldest lastAccessDate without active readers. */
  def findCacheToDelete(): Option[UUID]

  /** Closes the catalog. */
  def close(): Unit
}
