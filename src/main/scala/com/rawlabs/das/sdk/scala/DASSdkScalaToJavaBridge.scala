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

package com.rawlabs.das.sdk.scala

import java.util

import scala.jdk.CollectionConverters._

import com.rawlabs.das.sdk.DASExecuteResult
import com.rawlabs.das.sdk.DASTable.TableEstimate
import com.rawlabs.protocol.das.v1.query.{PathKey, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.Row
import com.rawlabs.protocol.das.v1.types.Value

class DASSdkScalaToJavaBridge(scalaSdk: DASSdk) extends com.rawlabs.das.sdk.DASSdk {

  final override def init(): Unit = scalaSdk.init()

  final override def close(): Unit = scalaSdk.close()

  final override def getTableDefinitions: util.List[com.rawlabs.protocol.das.v1.tables.TableDefinition] =
    scalaSdk.tableDefinitions.asJava

  final override def getFunctionDefinitions: util.List[com.rawlabs.protocol.das.v1.functions.FunctionDefinition] =
    scalaSdk.functionDefinitions.asJava

  final override def getTable(name: String): util.Optional[com.rawlabs.das.sdk.DASTable] =
    scalaSdk.getTable(name) match {
      case None        => util.Optional.empty()
      case Some(table) => util.Optional.of(new DASTableScalaToJavaBridge(table))
    }

  final override def getFunction(name: String): util.Optional[com.rawlabs.das.sdk.DASFunction] =
    scalaSdk.getFunction(name) match {
      case None           => util.Optional.empty()
      case Some(function) => util.Optional.of(new DASFunctionScalaToJavaBridge(function))
    }

}

class DASTableScalaToJavaBridge(scalaTable: DASTable) extends com.rawlabs.das.sdk.DASTable {

  final override def getTableEstimate(quals: util.List[Qual], columns: util.List[String]): TableEstimate = {
    val DASTable.TableEstimate(expectedNumberOfRows, avgRowWidthBytes) =
      scalaTable.tableEstimate(quals.asScala.toSeq, columns.asScala.toSeq)
    new TableEstimate(expectedNumberOfRows, avgRowWidthBytes)
  }

  final override def getTableSortOrders(sortKeys: util.List[SortKey]): util.List[SortKey] = {
    scalaTable.getTableSortOrders(sortKeys.asScala.toSeq).asJava
  }

  final override def getTablePathKeys: util.List[PathKey] = {
    scalaTable.getTablePathKeys.asJava
  }

  final override def explain(
      quals: util.List[Qual],
      columns: util.List[String],
      sortKeys: util.List[SortKey],
      maybeLimit: java.lang.Long): util.List[String] = scalaTable
    .explain(quals.asScala.toSeq, columns.asScala.toSeq, sortKeys.asScala.toSeq, Option(maybeLimit).map(_.toLong))
    .asJava

  final override def execute(
      quals: util.List[Qual],
      columns: util.List[String],
      sortKeys: util.List[SortKey],
      maybeLimit: java.lang.Long): DASExecuteResult =
    scalaTable.execute(
      quals.asScala.toSeq,
      columns.asScala.toSeq,
      sortKeys.asScala.toSeq,
      Option(maybeLimit).map(_.toLong))

  final override def uniqueColumn: String = scalaTable.uniqueColumn

  final override def bulkInsertBatchSize: Int = scalaTable.bulkInsertBatchSize

  final override def insert(row: Row): Row = scalaTable.insert(row)

  final override def bulkInsert(rows: util.List[Row]): util.List[Row] =
    scalaTable.bulkInsert(rows.asScala.toSeq).asJava

  final override def update(rowId: Value, newValues: Row): Row = scalaTable
    .update(rowId, newValues)

  final override def delete(rowId: Value): Unit = scalaTable.delete(rowId)

}

class DASFunctionScalaToJavaBridge(scalaFunction: DASFunction) extends com.rawlabs.das.sdk.DASFunction {

  final override def execute(args: util.Map[String, Value]): Value =
    scalaFunction.execute(args.asScala.toMap)

}
