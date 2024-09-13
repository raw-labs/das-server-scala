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

package com.rawlabs.das.server

import com.google.common.cache.{Cache, CacheBuilder}
import com.rawlabs.protocol.das.Rows
import com.rawlabs.protocol.das.services.ExecuteRequest
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable

object DASChunksCache extends StrictLogging {
  // Maximum number of entries cache
  private val N = 1000

  // Initialize the cache with a LRU eviction policy
  private val cache: Cache[String, mutable.Buffer[Rows]] = CacheBuilder
    .newBuilder()
    .maximumSize(N)
    .build()

  def put(request: ExecuteRequest, all: mutable.Buffer[Rows]): Unit = {
    logger.debug(s"Putting request in cache: $request")
    cache.put(request.toString, all)
  }

  def get(request: ExecuteRequest): Option[mutable.Buffer[Rows]] = {
    logger.debug(s"Getting request from cache: $request")
    val r = Option(cache.getIfPresent(request.toString))
    logger.debug(s"Cache hit: ${r.isDefined}")
    r
  }
}
