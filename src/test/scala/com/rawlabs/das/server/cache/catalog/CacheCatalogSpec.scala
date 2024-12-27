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

import java.nio.file.{Files, Path}
import java.util.UUID

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CacheCatalogSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // Temporary file for SQLite database
  private var tempDbFile: Path = _
  private var catalog: CacheCatalog = _

  override def beforeAll(): Unit = {
    // Create a temporary file for the SQLite database
    tempDbFile = Files.createTempFile("testdb", ".sqlite")
    val dbUrl = s"jdbc:sqlite:${tempDbFile.toAbsolutePath.toString}"

    // Instantiate the catalog, which runs Flyway migrations
    catalog = new SqliteCacheCatalog(dbUrl)
  }

  // Close the DB after all tests.
  override def afterAll(): Unit = {
    // Close the catalog (closes the SQLite connection)
    if (catalog != null) catalog.close()

    // Delete the temporary database file
    if (tempDbFile != null) Files.deleteIfExists(tempDbFile)
  }

  test("create and retrieve a new cache entry") {
    val cacheId = UUID.randomUUID()
    val definition = CacheDefinition("tableId-123", Seq.empty, Seq.empty, Seq.empty)
    catalog.createCache(cacheId, "dasId-123", definition)

    val all = catalog.listAll()
    all should have size 1
    all.head.cacheId shouldBe cacheId

    val count = catalog.countAll()
    count shouldBe 1

    val fetched = catalog.listByDasId("dasId-123")

    fetched should have size 1
    fetched.head.cacheId shouldBe cacheId
    fetched.head.definition shouldBe definition
    fetched.head.state shouldBe CacheState.InProgress
    fetched.head.activeReaders shouldBe 0
    fetched.head.numberOfTotalReads shouldBe 0

    catalog.deleteCache(cacheId)
  }

  test("add reader updates lastAccessDate and increments read count") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(cacheId, "dasId-xyz", CacheDefinition("tableId-xyz", Seq.empty, Seq.empty, Seq.empty))

    // Initially zero readers
    val initialEntry = catalog.listByDasId("dasId-xyz").head
    initialEntry.activeReaders shouldBe 0
    initialEntry.numberOfTotalReads shouldBe 0
    initialEntry.lastAccessDate shouldBe None

    // Add a reader
    catalog.addReader(cacheId)

    val updatedEntry = catalog.listByDasId("dasId-xyz").head
    updatedEntry.activeReaders shouldBe 1
    updatedEntry.numberOfTotalReads shouldBe 1
    updatedEntry.lastAccessDate should not be empty

    catalog.deleteCache(cacheId)
  }

  test("remove reader decrements read count") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(
      cacheId,
      "dasId-removeTest",
      CacheDefinition("tableId-removeTest", Seq.empty, Seq.empty, Seq.empty))

    // Add two readers
    catalog.addReader(cacheId)
    catalog.addReader(cacheId)

    val withTwoReaders = catalog.listByDasId("dasId-removeTest").head
    withTwoReaders.activeReaders shouldBe 2
    withTwoReaders.numberOfTotalReads shouldBe 2

    // Remove one
    catalog.removeReader(cacheId)
    val withOneReader = catalog.listByDasId("dasId-removeTest").head
    withOneReader.activeReaders shouldBe 1
    withOneReader.numberOfTotalReads shouldBe 2

    catalog.deleteCache(cacheId)
  }

  test("set cache as complete") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(cacheId, "dasId-complete", CacheDefinition("tableId-complete", Seq.empty, Seq.empty, Seq.empty))

    // Mark as complete with size 1234
    catalog.setCacheAsComplete(cacheId, 1234L)

    val updated = catalog.listByDasId("dasId-complete").head
    updated.state shouldBe CacheState.Complete
    updated.sizeInBytes shouldBe Some(1234L)

    catalog.deleteCache(cacheId)
  }

  test("set cache as error") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(cacheId, "dasId-error", CacheDefinition("tableId-error", Seq.empty, Seq.empty, Seq.empty))

    // Mark as error
    catalog.setCacheAsError(cacheId, "Something went wrong")

    val updated = catalog.listByDasId("dasId-error").head
    updated.state shouldBe CacheState.Error
    updated.stateDetail shouldBe Some("Something went wrong")

    catalog.deleteCache(cacheId)
  }

  test("list bad caches (all non-complete)") {
    val completeCacheId = UUID.randomUUID()
    val errorCacheId = UUID.randomUUID()

    // Insert a "complete" cache
    catalog.createCache(completeCacheId, "dasId-bad1", CacheDefinition("tableId-bad1", Seq.empty, Seq.empty, Seq.empty))
    catalog.setCacheAsComplete(completeCacheId, 500L)

    // Insert an "error" cache
    catalog.createCache(errorCacheId, "dasId-bad2", CacheDefinition("tableId-bad2", Seq.empty, Seq.empty, Seq.empty))
    catalog.setCacheAsError(errorCacheId, "Test error")

    // Insert an "in_progress" cache (not complete)
    val inProgressCacheId = UUID.randomUUID()
    catalog.createCache(
      inProgressCacheId,
      "dasId-bad3",
      CacheDefinition("tableId-bad3", Seq.empty, Seq.empty, Seq.empty))

    // Now list all bad caches
    val badCaches = catalog.listBadCaches()

    // We expect that "completeCacheId" is NOT in the bad caches
    // but "errorCacheId" and "inProgressCacheId" are.
    badCaches.map(_._2) should contain(errorCacheId)
    badCaches.map(_._2) should contain(inProgressCacheId)
    badCaches.map(_._2) should not contain completeCacheId

    catalog.deleteCache(completeCacheId)
    catalog.deleteCache(errorCacheId)
    catalog.deleteCache(inProgressCacheId)
  }

  test("find oldest cache to delete (NULL lastAccessDate is oldest)") {
    // Create one cache with no lastAccessDate
    val oldCacheId = UUID.randomUUID()
    catalog.createCache(oldCacheId, "dasId-old", CacheDefinition("tableId-old", Seq.empty, Seq.empty, Seq.empty))

    // Add a new one with lastAccessDate (simulate a read)
    val newCacheId = UUID.randomUUID()
    catalog.createCache(newCacheId, "dasId-new", CacheDefinition("tableId-new", Seq.empty, Seq.empty, Seq.empty))
    catalog.addReader(newCacheId) // sets lastAccessDate

    // The oldest should be oldCacheId, because its lastAccessDate is still NULL
    val oldest = catalog.findCacheToDelete()
    oldest shouldBe Some(oldCacheId)

    catalog.deleteCache(oldCacheId)
    catalog.deleteCache(newCacheId)
  }

  test("delete a cache entry") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(cacheId, "dasId-delete", CacheDefinition("tableId-delete", Seq.empty, Seq.empty, Seq.empty))

    val beforeDelete = catalog.listByDasId("dasId-delete")
    beforeDelete should have size 1

    // Delete
    catalog.deleteCache(cacheId)

    val afterDelete = catalog.listByDasId("dasId-delete")
    afterDelete shouldBe empty

    catalog.deleteCache(cacheId)
  }

  test("resetAllActiveReaders sets all activeReaders to 0") {
    val cacheId = UUID.randomUUID()
    catalog.createCache(cacheId, "dasId-reset", CacheDefinition("tableId-reset", Seq.empty, Seq.empty, Seq.empty))
    catalog.addReader(cacheId)
    catalog.addReader(cacheId)
    catalog.addReader(cacheId)

    val beforeReset = catalog.listByDasId("dasId-reset").head
    beforeReset.activeReaders shouldBe 3

    catalog.resetAllActiveReaders()

    val afterReset = catalog.listByDasId("dasId-reset").head
    afterReset.activeReaders shouldBe 0

    catalog.deleteCache(cacheId)
  }

}
