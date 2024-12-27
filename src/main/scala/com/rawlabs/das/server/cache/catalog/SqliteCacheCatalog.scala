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

import java.sql.{Connection, DriverManager, ResultSet}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import org.flywaydb.core.Flyway

class SqliteCacheCatalog(dbUrl: String) extends CacheCatalog {

  // 1) Ensure the JDBC driver is loaded
  Class.forName("org.sqlite.JDBC")

  // 2) Run Flyway migrations
  private def runMigrations(): Unit = {
    val flyway = Flyway
      .configure()
      .dataSource(dbUrl, null, null) // user/pass not typically needed for SQLite
      .load()
    flyway.migrate()
  }
  runMigrations()

  // 3) Establish a single connection for simplicity
  //    (Later we might prefer a connection pool.)
  private val conn: Connection = DriverManager.getConnection(dbUrl)
  conn.setAutoCommit(true)

  // ------------------------------------------------------------------
  // Utility Methods
  // ------------------------------------------------------------------

  private val dateFormatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)

  private def instantToString(inst: Instant): String =
    dateFormatter.format(inst)

  private def stringToInstant(s: String): Instant =
    Instant.from(dateFormatter.parse(s))

  /** Helper to read multiple rows as `List[CacheEntry]`. */
  private def resultSetToEntries(rs: ResultSet): List[CacheEntry] = {
    val buffer = scala.collection.mutable.ListBuffer[CacheEntry]()
    while (rs.next()) {
      val cacheId = UUID.fromString(rs.getString("cacheId"))
      val dasId = rs.getString("dasId")
      val definition = rs.getString("definition")
      val state = rs.getString("state")
      val stateDetail = Option(rs.getString("stateDetail")).filter(_ != null)
      val creationDate = stringToInstant(rs.getString("creationDate"))
      val lastAccessDateOpt = Option(rs.getString("lastAccessDate"))
        .filter(_ != null)
        .map(stringToInstant)
      val numberOfTotalReads = rs.getInt("numberOfTotalReads")
      val sizeInBytesOpt = {
        // getLong(...) returns 0 if the column is null or 0.
        // We can cross-check by wasNull() or store a separate "if 0, treat as None" logic:
        val raw = rs.getLong("sizeInBytes")
        if (rs.wasNull()) None else Some(raw)
      }

      buffer += CacheEntry(
        cacheId,
        dasId,
        definition,
        state,
        stateDetail,
        creationDate,
        lastAccessDateOpt,
        numberOfTotalReads,
        sizeInBytesOpt)
    }
    buffer.toList
  }

  // ------------------------------------------------------------------
  // Public API Methods
  // ------------------------------------------------------------------

  /** Creates a new cache entry. */
  override def createCache(cacheId: UUID, dasId: String, definition: String): Unit = {
    val now = Instant.now()
    val sql = """
      INSERT INTO cache_catalog
        (cacheId, dasId, definition, state, creationDate, numberOfTotalReads)
      VALUES (?, ?, ?, ?, ?, 0)
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, cacheId.toString)
      stmt.setString(2, dasId)
      stmt.setString(3, definition)
      stmt.setString(4, CacheState.InProgress)
      stmt.setString(5, instantToString(now))
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** List all rows by dasId. */
  override def listByDasId(dasId: String): List[CacheEntry] = {
    val sql = "SELECT * FROM cache_catalog WHERE dasId = ?"
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, dasId)
      val rs = stmt.executeQuery()
      resultSetToEntries(rs)
    } finally {
      stmt.close()
    }
  }

  /**
   * Increments `numberOfTotalReads` and updates `lastAccessDate` to now.
   */
  override def addReader(cacheId: UUID): Unit = {
    val nowStr = instantToString(Instant.now())
    val sql = """
      UPDATE cache_catalog
        SET numberOfTotalReads = numberOfTotalReads + 1,
            lastAccessDate = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, nowStr)
      stmt.setString(2, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Decrements `numberOfTotalReads`. */
  override def removeReader(cacheId: UUID): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET numberOfTotalReads = numberOfTotalReads - 1
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Marks the cache as complete, sets `sizeInBytes`. */
  override def setCacheAsComplete(cacheId: UUID, sizeInBytes: Long): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?, sizeInBytes = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, CacheState.Complete)
      stmt.setLong(2, sizeInBytes)
      stmt.setString(3, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Marks the cache as error, sets `stateDetail`. */
  override def setCacheAsError(cacheId: UUID, errorMessage: String): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?, stateDetail = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, CacheState.Error)
      stmt.setString(2, errorMessage)
      stmt.setString(3, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Marks the cache as voluntary_stop. */
  override def setCacheAsVoluntaryStop(cacheId: UUID): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, CacheState.VoluntaryStop)
      stmt.setString(2, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Deletes the row with the given cacheId. */
  override def deleteCache(cacheId: UUID): Unit = {
    val sql = "DELETE FROM cache_catalog WHERE cacheId = ?"
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  /** Returns all rows that are NOT in state "complete". */
  override def listBadCaches(): List[(String, UUID)] = {
    val sql = """
      SELECT dasId, cacheId
      FROM cache_catalog
      WHERE state != ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, CacheState.Complete)
      val rs = stmt.executeQuery()
      val buffer = scala.collection.mutable.ListBuffer[(String, UUID)]()
      while (rs.next()) {
        val dasId = rs.getString("dasId")
        val cacheId = UUID.fromString(rs.getString("cacheId"))
        buffer += ((dasId, cacheId))
      }
      buffer.toList
    } finally {
      stmt.close()
    }
  }

  /**
   * Finds the cacheId for the oldest lastAccessDate. We treat NULL lastAccessDate as oldest.
   *
   * We'll do it with a small trick in SQL: ORDER BY (lastAccessDate IS NOT NULL), lastAccessDate ASC and take the first
   * row (LIMIT 1).
   */
  override def findCacheToDelete(): Option[UUID] = {
    // Note: In SQLite, "lastAccessDate IS NOT NULL" yields 0 or 1 as an integer, not a boolean.
    // We'll do a direct SQL string:
    val sql = """
      SELECT cacheId
      FROM cache_catalog
      ORDER BY (lastAccessDate IS NOT NULL), lastAccessDate ASC
      LIMIT 1
    """
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      if (rs.next()) {
        Some(UUID.fromString(rs.getString("cacheId")))
      } else {
        None
      }
    } finally {
      stmt.close()
    }
  }

  /** Closes the JDBC connection. */
  override def close(): Unit = {
    conn.close()
  }
}
