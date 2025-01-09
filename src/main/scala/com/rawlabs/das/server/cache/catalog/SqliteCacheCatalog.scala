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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.{Connection, DriverManager, ResultSet}
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import scala.collection.mutable

import org.flywaydb.core.Flyway

import com.rawlabs.das.server.cache.catalog.CacheState._
import com.rawlabs.protocol.das.v1.query.{Qual, SortKey}

/**
 * SQLite-based implementation of CacheCatalog. We'll store:
 *   - tableId in TEXT
 *   - columns in TEXT (comma-separated)
 *   - quals in BLOB (repeated Qual) via parseDelimitedFrom
 *   - sortKeys in BLOB (repeated SortKeys) via parseDelimitedFrom
 */
class SqliteCacheCatalog(dbUrl: String) extends CacheCatalog {

  // 1) Load driver
  Class.forName("org.sqlite.JDBC")

  // 2) Flyway migrations
  private def runMigrations(): Unit = {
    val flyway = Flyway.configure().dataSource(dbUrl, null, null).load()
    flyway.migrate()
  }
  runMigrations()

  // 3) Single connection
  private val conn: Connection = DriverManager.getConnection(dbUrl)
  conn.setAutoCommit(true)

  // ------------------------------------------------------------------
  // Utility: date/time formatting
  // ------------------------------------------------------------------
  private val dateFormatter = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)

  private def instantToString(inst: Instant): String =
    dateFormatter.format(inst)

  private def stringToInstant(s: String): Instant =
    Instant.from(dateFormatter.parse(s))

  // ------------------------------------------------------------------
  // Utility: columns => comma-separated
  // ------------------------------------------------------------------
  private def encodeColumns(cols: Seq[String]): String =
    cols.mkString(",")

  private def decodeColumns(str: String): Seq[String] =
    if (str.isEmpty) Seq.empty else str.split(",").toSeq

  // ------------------------------------------------------------------
  // Utility: storing repeated Qual in BLOB
  // ------------------------------------------------------------------

  /**
   * Writes repeated Qual messages to a single BLOB using delimited format: for (q <- quals) { q.writeDelimitedTo(...) }
   */
  private def encodeQuals(quals: Seq[Qual]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    quals.foreach { q =>
      q.writeDelimitedTo(baos)
    }
    baos.toByteArray
  }

  /**
   * Reads repeated Qual messages from a single BLOB using parseDelimitedFrom(...)
   */
  private def decodeQuals(blob: Array[Byte]): Seq[Qual] = {
    if (blob == null || blob.isEmpty) return Seq.empty
    val bais = new ByteArrayInputStream(blob)
    val out = mutable.Buffer[Qual]()
    while (bais.available() > 0) {
      val q = Qual.parseDelimitedFrom(bais)
      if (q == null) {
        // no more messages
        return out.toSeq
      }
      out += q
    }
    out.toSeq
  }

  // ------------------------------------------------------------------
  // Similarly for repeated SortKeys
  // ------------------------------------------------------------------
  private def encodeSortKeys(sks: Seq[SortKey]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    sks.foreach { sk =>
      sk.writeDelimitedTo(baos)
    }
    baos.toByteArray
  }

  private def decodeSortKeys(blob: Array[Byte]): Seq[SortKey] = {
    if (blob == null || blob.isEmpty) return Seq.empty
    val bais = new ByteArrayInputStream(blob)
    val out = mutable.Buffer[SortKey]()
    while (bais.available() > 0) {
      val sk = SortKey.parseDelimitedFrom(bais)
      if (sk == null) {
        // no more messages
        return out.toSeq
      }
      out += sk
    }
    out.toSeq
  }

  // ------------------------------------------------------------------
  // Helper to read multiple rows into a List[CacheEntry].
  // ------------------------------------------------------------------
  private def resultSetToEntries(rs: ResultSet): List[CacheEntry] = {
    val buf = mutable.ListBuffer[CacheEntry]()
    while (rs.next()) {
      val cacheId = UUID.fromString(rs.getString("cacheId"))
      val dasId = rs.getString("dasId")
      val tableId = rs.getString("tableId")
      val columnsStr = rs.getString("columns")

      val qualsBlob = rs.getBytes("quals")
      val sortKeysBlob = rs.getBytes("sortKeys")

      val state = rs.getString("state")
      val stateDetail = Option(rs.getString("stateDetail")).filter(_ != null)
      val creationDate = stringToInstant(rs.getString("creationDate"))

      val lastAccessDateOpt = Option(rs.getString("lastAccessDate"))
        .filter(_ != null)
        .map(stringToInstant)

      val activeReaders = rs.getInt("activeReaders")
      val numberOfTotalReads = rs.getInt("numberOfTotalReads")

      val sizeInBytesOpt = {
        val raw = rs.getLong("sizeInBytes")
        if (rs.wasNull()) None else Some(raw)
      }

      // decode
      val colSeq = decodeColumns(columnsStr)
      val qSeq = decodeQuals(qualsBlob)
      val skSeq = decodeSortKeys(sortKeysBlob)

      val entry = CacheEntry(
        cacheId = cacheId,
        dasId = dasId,
        definition = CacheDefinition(tableId = tableId, columns = colSeq, quals = qSeq, sortKeys = skSeq),
        state = state,
        stateDetail = stateDetail,
        creationDate = creationDate,
        lastAccessDate = lastAccessDateOpt,
        activeReaders = activeReaders,
        numberOfTotalReads = numberOfTotalReads,
        sizeInBytes = sizeInBytesOpt)
      buf += entry
    }
    buf.toList
  }

  // ------------------------------------------------------------------
  // Public API Methods
  // ------------------------------------------------------------------

  override def createCache(cacheId: UUID, dasId: String, cacheDefinition: CacheDefinition): Unit = {
    val now = Instant.now()

    val sql = """
      INSERT INTO cache_catalog
        (cacheId, dasId, tableId, columns, quals, sortKeys, state, creationDate, activeReaders, numberOfTotalReads)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, cacheId.toString)
      stmt.setString(2, dasId)
      stmt.setString(3, cacheDefinition.tableId)
      stmt.setString(4, encodeColumns(cacheDefinition.columns))

      stmt.setBytes(5, encodeQuals(cacheDefinition.quals))
      stmt.setBytes(6, encodeSortKeys(cacheDefinition.sortKeys))

      stmt.setString(7, InProgress)
      stmt.setString(8, instantToString(now))

      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  override def listAll(): List[CacheEntry] = {
    val sql = "SELECT * FROM cache_catalog"
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      resultSetToEntries(rs)
    } finally {
      stmt.close()
    }
  }

  override def countAll(): Int = {
    val sql = "SELECT COUNT(*) FROM cache_catalog"
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      rs.next()
      rs.getInt(1)
    } finally {
      stmt.close()
    }
  }

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

  override def listCache(dasId: String, tableId: String): List[CacheEntry] = {
    val sql = "SELECT * FROM cache_catalog WHERE dasId = ? AND tableId = ?"
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, dasId)
      stmt.setString(2, tableId)
      val rs = stmt.executeQuery()
      resultSetToEntries(rs)
    } finally {
      stmt.close()
    }
  }

  override def addReader(cacheId: UUID): Unit = {
    val nowStr = instantToString(Instant.now())
    val sql = """
      UPDATE cache_catalog
        SET activeReaders = activeReaders + 1,
            numberOfTotalReads = numberOfTotalReads + 1,
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

  override def removeReader(cacheId: UUID): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET activeReaders = activeReaders - 1
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

  override def setCacheAsComplete(cacheId: UUID, sizeInBytes: Long): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?, sizeInBytes = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, Complete)
      stmt.setLong(2, sizeInBytes)
      stmt.setString(3, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  override def setCacheAsError(cacheId: UUID, errorMessage: String): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?, stateDetail = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, Error)
      stmt.setString(2, errorMessage)
      stmt.setString(3, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  override def setCacheAsVoluntaryStop(cacheId: UUID): Unit = {
    val sql = """
      UPDATE cache_catalog
        SET state = ?
      WHERE cacheId = ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, VoluntaryStop)
      stmt.setString(2, cacheId.toString)
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

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

  override def listBadCaches(): List[(String, UUID)] = {
    val sql = """
      SELECT dasId, cacheId
      FROM cache_catalog
      WHERE state != ?
    """
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.setString(1, Complete)
      val rs = stmt.executeQuery()
      val buf = mutable.ListBuffer[(String, UUID)]()
      while (rs.next()) {
        val dId = rs.getString("dasId")
        val cId = UUID.fromString(rs.getString("cacheId"))
        buf += ((dId, cId))
      }
      buf.toList
    } finally {
      stmt.close()
    }
  }

  override def resetAllActiveReaders(): Unit = {
    val sql = "UPDATE cache_catalog SET activeReaders = 0"
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  override def findCacheToDelete(): Option[UUID] = {
    val sql = """
      SELECT cacheId
      FROM cache_catalog
      WHERE activeReaders = 0
      ORDER BY (lastAccessDate IS NOT NULL), lastAccessDate ASC
      LIMIT 1
    """
    val stmt = conn.prepareStatement(sql)
    try {
      val rs = stmt.executeQuery()
      if (rs.next()) Some(UUID.fromString(rs.getString("cacheId")))
      else None
    } finally {
      stmt.close()
    }
  }

  override def close(): Unit = {
    conn.close()
  }
}
