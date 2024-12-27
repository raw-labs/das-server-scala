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

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.rawlabs.protocol.das.v1.query._
import com.rawlabs.protocol.das.v1.types._

class CacheSelectorSpec extends AnyFunSpec with Matchers {

  describe("CacheSelector") {

    it("should return None if the list is empty") {
      val emptyList = Seq.empty[CacheDef]
      val result = CacheSelector.pickBestCache(emptyList)
      result shouldBe None
    }

    it("should pick the cache with the fewest qualifiers") {
      // build some dummy Quals
      def buildSimpleQual(count: Int): Qual = {
        // Just produce something unique per count, for demonstration
        Qual.newBuilder()
          .setName(s"col$count")
          .setSimpleQual(
            SimpleQual.newBuilder()
              .setOperator(Operator.EQUALS)
              .setValue(
                Value.newBuilder().setInt(ValueInt.newBuilder().setV(count))
              )
          )
          .build()
      }

      val cacheDefs = Seq(
        CacheDef("cacheA", Seq(buildSimpleQual(1), buildSimpleQual(2))), // 2 quals
        CacheDef("cacheB", Seq(buildSimpleQual(3))),                     // 1 qual
        CacheDef("cacheC", Seq(buildSimpleQual(4), buildSimpleQual(5), buildSimpleQual(6))) // 3 quals
      )

      val result = CacheSelector.pickBestCache(cacheDefs)
      result shouldBe Some("cacheB")  // because cacheB has only 1 qual
    }

    it("should pick the first in case of a tie") {
      val cacheDefs = Seq(
        CacheDef("cacheA", Seq.empty), // 0 quals
        CacheDef("cacheB", Seq.empty), // 0 quals
        CacheDef("cacheC", Seq.empty)  // 0 quals
      )

      val result = CacheSelector.pickBestCache(cacheDefs)
      // minBy will find that all have 0, so it returns the first: "cacheA"
      result shouldBe Some("cacheA")
    }

    it("should handle a single CacheDef in the list") {
      val single = CacheDef("cacheX", Seq.empty)
      CacheSelector.pickBestCache(Seq(single)) shouldBe Some("cacheX")
    }

    it("should work with non-empty qualifiers across multiple caches") {
      // Some random qualifiers
      val qual1 = Qual.newBuilder().setName("col1").build()
      val qual2 = Qual.newBuilder().setName("col2").build()

      val def1 = CacheDef("id1", Seq(qual1, qual2))  // 2 quals
      val def2 = CacheDef("id2", Seq(qual1))         // 1 qual
      val def3 = CacheDef("id3", Seq(qual1, qual2))  // 2 quals

      val result = CacheSelector.pickBestCache(Seq(def1, def2, def3))
      result shouldBe Some("id2") // has only 1 qual, fewest
    }
  }
}
