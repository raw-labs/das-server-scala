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

import java.time.LocalDate

import scala.collection.mutable

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkInvalidArgumentException}
import com.rawlabs.protocol.das.v1.common.Environment
import com.rawlabs.protocol.das.v1.query.{Operator, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{Column, Row}
import com.rawlabs.protocol.das.v1.types.{Value, ValueDate, ValueString}
import com.typesafe.scalalogging.StrictLogging

class DASMockEventTable extends DASTable with StrictLogging {

  private val content = mutable.Buffer(
    "event1" -> LocalDate.of(2021, 1, 1),
    "event2" -> LocalDate.of(2022, 1, 1),
    "event3" -> LocalDate.of(2023, 1, 1),
    "event4" -> LocalDate.of(2024, 1, 1))

  private def update(key: String, value: LocalDate): Unit = {
    content.update(content.indexWhere(_._1 == key), key -> value)
  }

  private def insert(key: String, value: LocalDate): Unit = {
    content += key -> value
  }

  private def remove(key: String): Unit = {
    content.remove(content.indexWhere(_._1 == key))
  }

  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate =
    DASTable.TableEstimate(content.size, 8)

  override def uniqueColumn: String = "event"

  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long],
      maybeEnv: Option[Environment]): Seq[String] = {
    val dateFilters = extractDateFilters(quals)
    Seq(s"Applying ${dateFilters.size} date filters")
  }

  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long],
      maybeEnv: Option[Environment]): DASExecuteResult = {
    logger.info(s"Executing query with quals: $quals, columns: $columns, sortKeys: $sortKeys, limit: $maybeLimit")

    val dateFilters = extractDateFilters(quals)

    new DASExecuteResult {
      private val iterator = content.iterator.filter(row => dateFilters.forall(f => f(row._2)))
      override def close(): Unit = {}
      // hasNext is called multiple times, before next is called
      override def hasNext: Boolean = iterator.hasNext
      override def next(): Row = {
        val (key, value) = iterator.next()
        Row
          .newBuilder()
          .addColumns(
            Column
              .newBuilder()
              .setName("event")
              .setData(Value.newBuilder().setString(ValueString.newBuilder().setV(key).build()).build())
              .build())
          .addColumns(
            Column
              .newBuilder()
              .setName("date")
              .setData(
                Value
                  .newBuilder()
                  .setDate(
                    ValueDate
                      .newBuilder()
                      .setYear(value.getYear)
                      .setMonth(value.getMonthValue)
                      .setDay(value.getDayOfMonth)
                      .build())
                  .build())
              .build())
          .build()
      }
    }
  }

  override def insert(row: Row): Row = {
    val key = row.getColumnsList.stream().filter(_.getName == "event").findFirst().get().getData.getString.getV
    val value = checkAndMkDate(
      row.getColumnsList.stream().filter(_.getName == "date").findFirst().get().getData.getDate)
    insert(key, value)
    row
  }

  override def bulkInsert(rows: Seq[Row]): Seq[Row] = rows.map(insert)

  override def update(rowId: Value, newValues: Row): Row = {
    val key = rowId.getString.getV
    val value = checkAndMkDate(
      newValues.getColumnsList.stream().filter(_.getName == "date").findFirst().get().getData.getDate)
    update(key, value)
    newValues
  }

  override def delete(rowId: Value): Unit = remove(rowId.getString.getV)

  private def checkAndMkDate(v: ValueDate) = {
    // Introduce an artificial limitation to only support dates > 2000
    if (v.getYear <= 2000) throw new DASSdkInvalidArgumentException(s"Invalid date: $v (only > 2000 supported)")
    LocalDate.of(v.getYear, v.getMonth, v.getDay)
  }

  private def extractDateFilters(quals: Iterable[Qual]) = {
    quals
      .collect { case q if q.getName == "date" && q.hasSimpleQual => q.getSimpleQual }
      .map { q =>
        val dateValue = checkAndMkDate(q.getValue.getDate)
        q.getOperator match {
          case Operator.GREATER_THAN          => (d: LocalDate) => d.isAfter(dateValue)
          case Operator.LESS_THAN             => (d: LocalDate) => d.isBefore(dateValue)
          case Operator.EQUALS                => (d: LocalDate) => d.equals(dateValue)
          case Operator.NOT_EQUALS            => (d: LocalDate) => !d.equals(dateValue)
          case Operator.GREATER_THAN_OR_EQUAL => (d: LocalDate) => d.isEqual(dateValue) || d.isAfter(dateValue)
          case Operator.LESS_THAN_OR_EQUAL    => (d: LocalDate) => d.isEqual(dateValue) || d.isBefore(dateValue)
          case _ => throw new IllegalArgumentException(s"Unsupported operator: ${q.getOperator}")
        }
      }
  }

}
