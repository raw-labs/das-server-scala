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

import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types.{Type, Value}
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters.CollectionHasAsScala

class FunctionImport extends StrictLogging {

  private case class Parameter(name: String, pgType: String, pgDefault: Option[String])

  def innerStatements(schema: String, definition: FunctionDefinition): Either[String, Seq[String]] = {
    val functionName = innerFunctionName(definition.getFunctionId.getName)
    logger.info(s"Generating SQL statements for inner function $schema.$functionName")
    val qFunctionName = quoteIdentifier(functionName)
    val qSchema = quoteIdentifier(schema)
    val statements: Either[String, Seq[String]] = dasTypeToPostgres(definition.getReturnType) match {
      case Left(error) => Left(error)
      case Right(plainReturnType) =>
        convertParameters(definition.getParamsList.asScala) match {
          case Left(error) => Left(error)
          case Right(parameters) =>
            val paramList = parameters.map { p =>
              p.pgDefault match {
                case Some(default) => s"${p.name} ${p.pgType} DEFAULT $default"
                case None          => s"${p.name} ${p.pgType}"
              }
            }

            val rawArgList = paramList.mkString(", ")
            Right(Seq(s"""CREATE OR REPLACE FUNCTION $qSchema.$qFunctionName(
                 |  $rawArgList
                 |)
                 |RETURNS $plainReturnType
                 |LANGUAGE C
                 |AS 'multicorn', 'my_c_entrypoint';""".stripMargin))
        }
    }
    statements
  }

  private def innerFunctionName(functionName: String): String = {
    s"${functionName}_inner"
  }

  private def convertParameters(paramDef: Iterable[ParameterDefinition]): Either[String, Iterable[Parameter]] = {
    val parameters: Iterable[Either[String, Parameter]] = paramDef.map { param =>
      dasTypeToPostgres(param.getType).flatMap { plainType =>
        val qParamName = quoteIdentifier(param.getName)
        val qParamType = plainType
        if (param.hasDefaultValue) {
          mkPgValue(param.getDefaultValue).map(default => Parameter(qParamName, qParamType, Some(default)))
        } else {
          Right(Parameter(qParamName, qParamType, None))
        }
      }
    }
    val errorList = parameters.collect { case Left(a) => a }
    if (errorList.nonEmpty) {
      val errors = errorList.mkString(", ")
      Left(s"Failed to convert parameters to Postgres types: $errors")
    } else {
      Right(parameters.collect { case Right(d) => d })
    }

  }

  /*
  def outerStatements(dasId: DASId, schema: String, definition: FunctionDefinition): (String, Seq[String]) = {
    // The body depends on the return type of the function.

    if (returnType.hasList) {
      // The inner/system function returns an array of ... something.
      // We'll `unnest` that array to turn the array into a set of items (a TABLE).
    }

    // 9) Build the friendly function CREATE statement.
    val createFriendly =
      s"""CREATE OR REPLACE FUNCTION $qSchema.$qFunctionName(
         |  $friendlyArgList
         |)
         |RETURNS $returnType
         |LANGUAGE sql
         |AS $$$$
         |  $body;
         |$$$$;""".stripMargin



  }
   */

  // TODO dummy
  def mkPgValue(value: Value): Either[String, String] = {
    // Dummy implementation; you would need to handle all types of values.
    if (value.hasString) Right(value.getString.getV)
    else if (value.hasInt) Right(value.getInt.getV.toString)
    else Left(s"unknown value: $value")
  }

  // TODO dummy
  private def quoteIdentifier(name: String): String =
    s""""$name""""

  // TODO dummy
  private def dasTypeToPostgres(tipe: Type): Either[String, String] = {
    // Dummy implementation; you would need to handle all types of values.
    if (tipe.hasInt) Right("integer")
    else if (tipe.hasString) Right("text")
    else if (tipe.hasRecord) Right("jsonb")
    else if (tipe.hasAny) Right("jsonb")
    else if (tipe.hasList) {
      val innerType = tipe.getList.getInnerType
      dasTypeToPostgres(innerType).map(t => s"$t[]")
    } else Left("unknown")
  }

}
