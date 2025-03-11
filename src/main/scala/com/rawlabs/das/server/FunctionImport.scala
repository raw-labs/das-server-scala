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

import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types.{AttrType, Type, Value}
import com.typesafe.scalalogging.StrictLogging

class FunctionImport extends StrictLogging {

  private case class Parameter(name: String, pgType: String, pgDefault: Option[String])

  def innerStatements(schema: String, definition: FunctionDefinition): Either[String, Seq[String]] = {
    val functionName = innerFunctionName(definition.getFunctionId.getName)
    logger.info(s"Generating SQL statements for inner function $schema.$functionName")
    val qFunctionName = quoteIdentifier(functionName)
    val qSchema = quoteIdentifier(schema)
    for (
      plainReturnType <- dasTypeToPostgres(definition.getReturnType);
      parameters <- convertParameters(definition.getParamsList.asScala)
    ) yield {
      val paramList = parameters.map { p =>
        s"${p.name} ${p.pgType}"
      }
      val argList = (List("options text[]", "arg_names text[]") ++ paramList).mkString(", ")
      Seq(s"""CREATE OR REPLACE FUNCTION $qSchema.$qFunctionName(
             |  $argList
             |)
             |RETURNS $plainReturnType
             |LANGUAGE C
             |AS 'multicorn', 'my_c_entrypoint';""".stripMargin)
    }
  }

  def outerStatements(
      schema: String,
      options: Map[String, String],
      definition: FunctionDefinition): Either[String, Seq[String]] = {
    val functionName = outerFunctionName(definition.getFunctionId.getName)
    logger.info(s"Generating SQL statements for outer function $schema.$functionName")
    val qFunctionName = quoteIdentifier(functionName)
    val qSchema = quoteIdentifier(schema)
    for (
      returnType <- outerFunctionReturnType(definition.getReturnType);
      parameters <- convertParameters(definition.getParamsList.asScala);
      body <- outerFunctionBody(schema, options, definition)
    ) yield {

      val paramList = parameters.map { p =>
        p.pgDefault match {
          case Some(default) => s"${p.name} ${p.pgType} DEFAULT $default"
          case None          => s"${p.name} ${p.pgType}"
        }
      }

      val argList = paramList.mkString(", ")

      Seq(s"""CREATE OR REPLACE FUNCTION $qSchema.$qFunctionName(
               |  $argList
               |)
               |RETURNS $returnType
               |LANGUAGE sql
               |AS $$$$
               |  $body;
               |$$$$;""".stripMargin)
    }
  }

  private def outerFunctionBody(schema: String, options: Map[String, String], definition: FunctionDefinition) = {
    {
      val qSchema = quoteIdentifier(schema)
      val qFunctionName = quoteIdentifier(definition.getFunctionId.getName)
      val qInnerFunctionName = quoteIdentifier(innerFunctionName(definition.getFunctionId.getName))
      val optionsArrayLiteral = dictToArrayOfKV(options)
      val argNamesList = definition.getParamsList.asScala.map(_.getName).toSeq
      val argNamesArray = listOfStringsToTextArray(argNamesList)
      val row = qFunctionName
      val innerCall = {
        // 8) Build a comma-separated list of argument references for the SQL body.
        val passUserArgs = if (argNamesList.isEmpty) "" else argNamesList.map(quoteIdentifier).mkString(", ")
        s"""$qSchema.$qInnerFunctionName(
           |      $optionsArrayLiteral,
           |      $argNamesArray${if (passUserArgs.nonEmpty) s", $passUserArgs" else ""}
           |    )""".stripMargin
      }

      if (definition.getReturnType.hasList) {
        // We have a list of items. Turn it into a table by unnesting the function output.
        val call = "unnest(" + innerCall + s") AS $row"
        val innerType = definition.getReturnType.getList.getInnerType

        if (innerType.hasRecord && innerType.getRecord.getAttsCount > 0) {
          // It's a list of records, and we know the fields. We can expose the whole function
          // output as a TABLE of records.
          // Extract all fields from the inner jsonb
          attributesToJsonbExtraction(innerType.getRecord.getAttsList.asScala, row).map { extractions =>
            s"SELECT ${extractions.mkString(",")} FROM $call"
          }
        } else {
          // It's a list of something else. Expose the whole function output as a TABLE of one column.
          Right(s"SELECT * FROM $call")
        }
      } else if (definition.getReturnType.hasRecord && definition.getReturnType.getRecord.getAttsCount > 0) {
        // It's a record, and we know the fields. We can expose the whole function output
        // as a TABLE of records (one record).
        // Extract all fields from the inner jsonb
        attributesToJsonbExtraction(definition.getReturnType.getRecord.getAttsList.asScala, row).map { extractions =>
          s"SELECT ${extractions.mkString(",")} FROM $innerCall AS $row"
        }
      } else {
        // It's a scalar. Expose the whole function output as is.
        Right(s"SELECT * FROM $innerCall AS $row")
      }

    }
  }

  private def dictToArrayOfKV(options: Map[String, String]): String = {
    // Convert each key/value pair to "key=value" and then build an array literal.
    val items = options.map { case (k, v) => s"'$k=$v'" }.mkString(", ")
    s"ARRAY[$items]::text[]"
  }

  private def listOfStringsToTextArray(strings: Iterable[String]): String = {
    val items = strings.map(s => s"'$s'").mkString(", ")
    s"ARRAY[$items]::text[]"
  }

  private def innerFunctionName(functionName: String): String = {
    s"${functionName}_inner"
  }

  private def outerFunctionName(functionName: String): String = {
    functionName
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

  private def outerFunctionReturnType(tipe: Type): Either[String, String] = {
    val itemName = "item"
    if (tipe.hasInt) Right("integer")
    else if (tipe.hasString) Right("text")
    else if (tipe.hasRecord) {
      // make it a table with the record fields
      attributesToColumnTypes(tipe.getRecord.getAttsList.asScala).map { columns =>
        if (columns.isEmpty) "JSONB" else s"TABLE(${columns.mkString(", ")})"
      }
    } else if (tipe.hasAny) Right("jsonb")
    else if (tipe.hasList) {
      val innerType = tipe.getList.getInnerType
      if (innerType.hasRecord) {
        // make it a table with the record fields
        attributesToColumnTypes(innerType.getRecord.getAttsList.asScala).map { columns =>
          if (columns.isEmpty) s"TABLE($itemName JSONB)"
          else s"TABLE(${columns.mkString(", ")})"
        }
      } else {
        // make it an array of the inner type
        dasTypeToPostgres(innerType).map { itemType =>
          s"TABLE($itemName $itemType)"
        }
      }
    } else Left("unknown")
  }

  private def attributesToColumnTypes(attrTypes: Iterable[AttrType]): Either[String, Iterable[String]] = {
    val columns = attrTypes.map { attr =>
      dasTypeToPostgres(attr.getTipe).map { pgType =>
        quoteIdentifier(attr.getName) + " " + pgType
      }
    }
    val errors = columns.collect { case Left(error) => error }
    if (errors.nonEmpty) {
      val errorMessage = errors.mkString(", ")
      Left(errorMessage)
    } else {
      Right(columns.collect { case Right(col) => col })
    }
  }

  private def attributesToJsonbExtraction(
      attrTypes: Iterable[AttrType],
      rowName: String): Either[String, Iterable[String]] = {
    val extractions = attrTypes.map { attrType =>
      val fieldName = attrType.getName
      val fieldType = attrType.getTipe

      val jsonb = s"($rowName->'$fieldName')"
      if (fieldType.hasString) {
        // Extract a string field from the jsonb as TEXT
        Right(s"($rowName->>'$fieldName')")
      } else if (fieldType.hasInt) {
        // Cast an integer field from the jsonb to INTEGER
        Right(s"$jsonb::integer")
      } else if (fieldType.hasRecord) {
        // Leave the field as is.
        Right(jsonb)
      } else if (fieldType.hasList) {
        // We expect a list. We'll `unnest` the underlying array and cast each item to the inner type.
        // We don't recurse, everything complex becomes a jsonb.
        val innerType = fieldType.getList.getInnerType
        dasTypeToPostgres(innerType).map { innerPgType =>
          val items = s"jsonb_array_elements_text($jsonb)"
          s"SELECT i::$innerPgType FROM $items i"
        }
      } else {
        Left("unsupported type " + fieldType)
      }
    }
    val errors = extractions.collect { case Left(error) => error }
    if (errors.nonEmpty) {
      val errorMessage = errors.mkString(", ")
      Left(errorMessage)
    } else {
      Right(extractions.collect { case Right(extraction) => extraction })
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
  private def mkPgValue(value: Value): Either[String, String] = {
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
