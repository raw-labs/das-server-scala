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

import java.time.Instant
import java.util.UUID

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.rawlabs.das.server.cache.catalog.{CacheDefinition, CacheEntry}
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.types._

class CacheSelectorSpec extends AnyFunSpec with Matchers {

  private def buildCacheDef(tableId: String, quals: Seq[Qual], columns: Seq[String]) = {
    CacheEntry(
      UUID.randomUUID(),
      "dasId",
      CacheDefinition(tableId, quals = quals, columns = columns, sortKeys = Nil),
      state = "complete",
      stateDetail = None,
      creationDate = Instant.now(),
      lastAccessDate = None,
      activeReaders = 0,
      numberOfTotalReads = 0,
      sizeInBytes = None)
  }

  // A small helper to build a SimpleQual with custom operator
  private def buildSimpleQual(colName: String, op: Operator, intVal: Int): Qual = {
    Qual
      .newBuilder()
      .setName(colName)
      .setSimpleQual(
        SimpleQual
          .newBuilder()
          .setOperator(op)
          .setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(intVal))))
      .build()
  }

  // A helper to build a SimpleQual for demonstration
  private def buildSimpleQual(colName: String, intVal: Int): Qual = {
    buildSimpleQual(colName, Operator.GREATER_THAN, intVal)
  }

  describe("CacheSelector.pickBestCache") {

    it("should return None if no cache covers requested columns") {

      val c1 = buildCacheDef("c1", quals = Nil, columns = Seq("colA"))
      val c2 = buildCacheDef("c2", quals = Nil, columns = Seq("colB"))

      val requestedQuals = Nil
      val requestedCols = Seq("colX") // none has colX

      val result = CacheSelector.pickBestCache(Seq(c1, c2), requestedQuals, requestedCols)
      result shouldBe None
    }

    it("should return None if none of the caches pass the 'newQuals => oldQuals' check") {
      // Let’s say c1 has oldQuals = [colA>10], c2 has oldQuals = [colB>100]
      // The new request is [colA>5, colB>50] => we want new=>old for either c1 or c2
      // But [colA>5,colB>50] doesn't imply [colA>10] alone or [colB>100] alone if we must satisfy them all
      // => likely differenceIfMoreSelective(...) returns None for each.
      val c1 = buildCacheDef("c1", quals = Seq(buildSimpleQual("colA", 10)), columns = Seq("colA", "colB"))
      val c2 = buildCacheDef("c2", quals = Seq(buildSimpleQual("colB", 100)), columns = Seq("colA", "colB"))

      val newQuals = Seq(
        buildSimpleQual("colA", 5), // colA>5
        buildSimpleQual("colB", 50) // colB>50
      )
      val requestedCols = Seq("colA", "colB")

      // differenceIfMoreSelective(c1.quals, newQuals) => do colA>10 => colA>5,colB>50 hold?
      // Probably returns None unless colB>50 => colB>?? etc. (we won't dive too deep, but it's presumably invalid)
      // same for c2
      val result = CacheSelector.pickBestCache(Seq(c1, c2), newQuals, requestedCols)
      result shouldBe None
    }

    it("should pick a cache that covers columns and pass newQuals => oldQuals, returning the difference") {
      // oldQuals = [x>10,y>2], newQuals = [x>20,y>2,z<3]
      // from your example => difference = [x>20, z<3]
      // Also let's set columns so c1 has colX,colY => covers x,y, c2 has colX,colY,colZ => covers x,y,z
      // We'll see which one we pick: we also want the fewest oldQuals if multiple are valid.

      val oldQ1 = Seq(buildSimpleQual("x", 10), buildSimpleQual("y", 2))
      val c1 = buildCacheDef("cacheA", quals = oldQ1, columns = Seq("x", "y")) // 2 oldQuals

      val oldQ2 = Seq(buildSimpleQual("x", 10)) // only x>10
      val c2 = buildCacheDef("cacheB", quals = oldQ2, columns = Seq("x", "y", "z")) // 1 oldQual

      // The new request:
      //  newQuals = [x>20, y>2, z<3]
      //  requestedColumns = [x, y, z]
      val newQuals = Seq(
        buildSimpleQual("x", Operator.GREATER_THAN, 20),
        buildSimpleQual("y", Operator.GREATER_THAN, 2),
        buildSimpleQual("z", Operator.LESS_THAN, 3))
      val requestedCols = Seq("x", "y", "z")

      // differenceIfMoreSelective(c1.quals, newQuals) => is new => old? (x>20,y>2) => (x>10,y>2)? Possibly yes => difference might be [x>20], plus z<3 not covered => so difference= [x>20, z<3]
      // But c1 only has columns x,y => doesn't have z => fails the column coverage check => c1 is invalid for columns "z".
      // differenceIfMoreSelective(c2.quals, newQuals) => old= x>10, new= x>20,y>2,z<3 => if x>20,y>2 => x>10 => new => old is true => difference might be [x>20, y>2, z<3], because c2's oldQual is just x>10.
      // c2 covers columns x,y,z => so it's valid
      // c2 has oldQuals.size=1 => if c1 had been valid, c2 would still be chosen if c1 had more oldQuals, but c1 fails the column check anyway.

      val result = CacheSelector.pickBestCache(Seq(c1, c2), newQuals, requestedCols)

      // We expect some(cacheB, difference).
      result.isDefined shouldBe true
      val (chosenCacheId, diff) = result.get
      chosenCacheId shouldBe c2

      // Let's do a quick sanity check that diff includes x>20, y>2, z<3 if your selectivity logic returns them as new constraints
      diff.map(_.getName).toSet shouldBe Set("x", "y", "z")
    }

    it("should pick the cache with the fewest oldQuals if multiple are valid") {
      // Suppose both caches pass the columns and differenceIfMoreSelective test, we choose the one with fewer oldQuals
      val c1 = buildCacheDef(
        "cache1",
        quals = Seq(buildSimpleQual("a", 10), buildSimpleQual("b", 20)),
        columns = Seq("a", "b"))
      val c2 = buildCacheDef("cache2", quals = Seq(buildSimpleQual("a", 5)), columns = Seq("a", "b"))

      // newQuals => c1.quals => possible, newQuals => c2.quals => possible
      // We'll do a trivial newQuals that definitely implies those oldQuals, for example newQuals = [a>10,b>20]
      val newQuals = Seq(buildSimpleQual("a", 10), buildSimpleQual("b", 20))
      val requestedCols = Seq("a", "b")

      val result = CacheSelector.pickBestCache(Seq(c1, c2), newQuals, requestedCols)
      // c1 has oldQuals.size=2, c2 has oldQuals.size=1 => c2 wins
      result.map(_._1).get.cacheId shouldBe c2.cacheId
    }
  }
}
