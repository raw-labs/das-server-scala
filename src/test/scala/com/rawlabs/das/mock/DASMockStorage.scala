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

package com.rawlabs.das.mock

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.rawlabs.protocol.das.v1.tables.Row

/**
 * Mock storage for DAS. It uses a TreeMap to store the data in memory. All methods are synchronized to ensure thread
 * safety.
 *
 * @param key the name of the column that serves as the unique row identifier (rowId)
 */
class DASMockStorage(val key: String) {

  private val treeMap: mutable.TreeMap[String, Row] =
    new mutable.TreeMap[String, Row]()
  private val treeMapLock = new Object

  /**
   * Adds a row to the storage.
   *
   * @param row the row to add
   * @return the row added
   */
  def add(row: Row): Row = treeMapLock.synchronized {
    val rowId = row.getColumnsList.asScala.collectFirst {
      case c if c.getName == key => c.getData.getInt.getV
    } match {
      case Some(rowId) => rowId.toString
      case None        => throw new IllegalArgumentException(s"Row does not contain a column with name $key")
    }
    treeMap.put(rowId, row)
    row
  }

  /**
   * Updates a row in the storage.
   *
   * @param rowId the id of the row to update
   * @param row the new row
   * @return the updated row
   */
  def update(rowId: String, row: Row): Row = treeMapLock.synchronized {
    treeMap.put(rowId, row)
    row
  }

  /**
   * Removes a row from the storage.
   *
   * @param rowId the id of the row to remove
   */
  def remove(rowId: String): Unit = treeMapLock
    .synchronized(treeMap.remove(rowId))

  /**
   * Returns the number of elements in the storage.
   *
   * @return the number of elements
   */
  def size: Int = treeMapLock.synchronized(treeMap.size)

  /**
   * Returns an iterator over the elements in this collection. The storage is first cloned to avoid concurrent
   * modification exceptions.
   *
   * @return an Iterator.
   */
  def iterator: Iterator[Row] = treeMapLock
    .synchronized(treeMap.clone().valuesIterator)

}
