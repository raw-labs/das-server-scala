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

package com.rawlabs.das.postgresql

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition

class DASPostgresql(options: Map[String, String]) extends DASSdk {

  private val backend = {
    val host = options("host")
    val port = options.get("port").map(_.toInt).getOrElse(5432)
    val user = options("user")
    val password = options.getOrElse("password", "")
    val database = options.getOrElse("database", user)
    val url = s"jdbc:postgresql://$host:$port/$database"
    val schema = options.getOrElse("schema", "public")
    new PostgresqlBackend(url, user, password, schema)
  }
  private val tables = backend.tables()

  /**
   * @return a list of table definitions.
   */
  override def tableDefinitions: Seq[TableDefinition] = tables.values.map(_.definition).toSeq

  /**
   * @return a list of function definitions.
   */
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /**
   * Retrieve a table by name.
   *
   * @param name table name
   * @return Optional containing the DASTable if present
   */
  override def getTable(name: String): Option[DASTable] = tables.get(name)

  /**
   * Retrieve a function by name.
   *
   * @param name function name
   * @return Optional containing the DASFunction if present
   */
  override def getFunction(name: String): Option[DASFunction] = None
}
