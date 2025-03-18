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

  //
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
             |AS 'multicorn', 'multicorn_function_execute';""".stripMargin)
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

      val argList = paramList.mkString(",\n    ")

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

  private def outerFunctionBody(
      schema: String,
      options: Map[String, String],
      definition: FunctionDefinition): Either[String, String] = {
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
        val passUserArgs = if (argNamesList.isEmpty) "" else argNamesList.map(quoteIdentifier).mkString(",\n    ")
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
          // It's a list of records, and we know the fields. It comes as a JSONB but we can expose the whole function
          // output as a TABLE of records.
          val tryToExtract = innerType.getRecord.getAttsList.asScala.map(attr =>
            attributesToJsonbExtraction(row, attr))
          val errors = tryToExtract.collect { case Left(error) => error }
          if (errors.nonEmpty) {
            val errorMessage = errors.mkString(", ")
            Left(errorMessage)
          } else {
            val extractions = tryToExtract.collect { case Right(extraction) => extraction }
            // Extract all fields from the inner jsonb
            Right(s"SELECT ${extractions.mkString(",\n    ")} FROM $call")
          }
        } else {
          // It's a list of something else. Expose the whole function output as a TABLE of one column.
          Right(s"SELECT * FROM $call")
        }
      } else if (definition.getReturnType.hasRecord && definition.getReturnType.getRecord.getAttsCount > 0) {
        // It's a record, and we know the fields. We can expose the whole function output
        // as a TABLE of records (one record).
        // Extract all fields from the inner jsonb
        val tryToExtract = definition.getReturnType.getRecord.getAttsList.asScala.map(attr =>
          attributesToJsonbExtraction(row, attr))
        val errors = tryToExtract.collect { case Left(error) => error }
        if (errors.nonEmpty) {
          val errorMessage = errors.mkString(", ")
          Left(errorMessage)
        } else {
          val extractions = tryToExtract.collect { case Right(extraction) => extraction }
          // Extract all fields from the inner jsonb
          Right(s"SELECT ${extractions.mkString(",\n    ")} FROM $innerCall $row")
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
          dasValueToPostgresConst(param.getDefaultValue, param.getType).map(default =>
            Parameter(qParamName, qParamType, Some(default)))
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
    } else Left("outerFunctionReturnType: unsupported type " + tipe)
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

  private def attributesToJsonbExtraction(row: String, attrType: AttrType): Either[String, String] = {
    val fieldName = attrType.getName
    val fieldType = attrType.getTipe
    val jsonb = s"($row->'$fieldName')"
    val asText = s"($row->>'$fieldName')"
    if (fieldType.hasString) {
      // Extract a string field from the jsonb as TEXT.
      Right(asText)
    } else if (fieldType.hasByte || fieldType.hasShort) {
      // Cast byte and short fields as smallint.
      Right(s"$asText::smallint")
    } else if (fieldType.hasInt) {
      // Cast integer field.
      Right(s"$asText::integer")
    } else if (fieldType.hasLong) {
      // Cast long field.
      Right(s"$asText::bigint")
    } else if (fieldType.hasFloat) {
      // Cast float field.
      Right(s"$asText::real")
    } else if (fieldType.hasDouble) {
      // Cast double field.
      Right(s"$asText::double precision")
    } else if (fieldType.hasDecimal) {
      Right(s"$asText::numeric")
    } else if (fieldType.hasBool) {
      // Cast boolean field.
      Right(s"$asText::boolean")
    } else if (fieldType.hasBinary) {
      // Cast binary field as bytea.
      Right(s"$asText::bytea")
    } else if (fieldType.hasDate) {
      Right(s"$asText::date")
    } else if (fieldType.hasTime) {
      Right(s"$asText::time")
    } else if (fieldType.hasTimestamp) {
      Right(s"$asText::timestamp")
    } else if (fieldType.hasInterval) {
      Right(s"$asText::interval")
    } else if (fieldType.hasRecord) {
      // For records, we leave the JSONB expression as is, returning a NULL value if it's null.
      Right(s"(SELECT CASE WHEN $jsonb = 'null'::jsonb THEN NULL ELSE $jsonb END)")
    } else if (fieldType.hasList) {
      // For lists, unnest the JSONB array and cast each element to the inner type.
      val innerType = fieldType.getList.getInnerType
      val arrayExpression =
        if (innerType.hasRecord || innerType.hasList || innerType.hasAny) {
          // We extract the inner JSONB and leave them untouched
          Right(s"ARRAY(SELECT i FROM jsonb_array_elements($jsonb) i)")
        } else {
          dasTypeToPostgres(innerType).map { innerPgType =>
            // Extract elements as STRING and CAST them to the inner type.
            val items = s"jsonb_array_elements_text($jsonb)"
            s"ARRAY(SELECT i::$innerPgType FROM $items i)"
          }
        }
      arrayExpression.map(expression => s"(SELECT CASE WHEN $jsonb = 'null'::jsonb THEN NULL ELSE $expression END)")
    } else if (fieldType.hasAny) {
      // We leave the JSONB expression as is, returning a NULL value if it's null.
      Right(s"(SELECT CASE WHEN $jsonb = 'null'::jsonb THEN NULL ELSE $jsonb END)")
    } else {
      Left(s"jsonExtraction: unsupported type $fieldType")
    }
  }

  private def dasValueToPostgresConst(value: Value, t: Type): Either[String, String] = {
    if (value.hasNull) {
      Right("NULL")
    } else
      t.getTypeCase match {
        case Type.TypeCase.BYTE =>
          if (value.hasByte) Right(value.getByte.getV.toString)
          else Left(s"Expected byte value but got $value")
        case Type.TypeCase.SHORT =>
          if (value.hasShort) Right(value.getShort.getV.toString)
          else Left(s"Expected short value but got $value")
        case Type.TypeCase.INT =>
          if (value.hasInt) Right(value.getInt.getV.toString)
          else Left(s"Expected int value but got $value")
        case Type.TypeCase.LONG =>
          if (value.hasLong) Right(value.getLong.getV.toString)
          else Left(s"Expected long value but got $value")
        case Type.TypeCase.FLOAT =>
          if (value.hasFloat) Right(value.getFloat.getV.toString)
          else Left(s"Expected float value but got $value")
        case Type.TypeCase.DOUBLE =>
          if (value.hasDouble) Right(value.getDouble.getV.toString)
          else Left(s"Expected double value but got $value")
        case Type.TypeCase.DECIMAL =>
          if (value.hasDecimal) Right(value.getDecimal.getV)
          else Left(s"Expected decimal value but got $value")
        case Type.TypeCase.BOOL =>
          if (value.hasBool) Right(if (value.getBool.getV) "true" else "false")
          else Left(s"Expected bool value but got $value")
        case Type.TypeCase.STRING =>
          if (value.hasString) {
            val s = value.getString.getV
            Right(s"'${s.replace("'", "''")}'")
          } else Left(s"Expected string value but got $value")
        case Type.TypeCase.BINARY =>
          if (value.hasBinary) {
            val bytes = value.getBinary.getV.toByteArray
            val hex = bytes.map("%02x".format(_)).mkString
            Right(s"E'\\\\x$hex'::bytea")
          } else Left(s"Expected binary value but got $value")
        case Type.TypeCase.DATE =>
          if (value.hasDate) {
            val d = value.getDate
            // Format as YYYY-MM-DD
            Right(f"'${d.getYear}%04d-${d.getMonth}%02d-${d.getDay}%02d'::date")
          } else Left(s"Expected date value but got $value")
        case Type.TypeCase.TIME =>
          if (value.hasTime) {
            val tVal = value.getTime
            Right(f"'${tVal.getHour}%02d:${tVal.getMinute}%02d:${tVal.getSecond}%02d'::time")
          } else Left(s"Expected time value but got $value")
        case Type.TypeCase.TIMESTAMP =>
          if (value.hasTimestamp) {
            val ts = value.getTimestamp
            Right(
              f"'${ts.getYear}%04d-${ts.getMonth}%02d-${ts.getDay}%02d ${ts.getHour}%02d:${ts.getMinute}%02d:${ts.getSecond}%02d'::timestamp")
          } else Left(s"Expected timestamp value but got $value")
        case Type.TypeCase.INTERVAL =>
          if (value.hasInterval) {
            val iv = value.getInterval
            Right(
              s"'P${iv.getYears}Y${iv.getMonths}M${iv.getDays}DT${iv.getHours}H${iv.getMinutes}M${iv.getSeconds}S'::interval")
          } else Left(s"Expected interval value but got $value")
        case Type.TypeCase.RECORD =>
          if (value.hasRecord) {
            val jsonStr = valueToJson(value)
            Right(s"'${escapeJsonb(jsonStr)}'::jsonb")
          } else Left(s"Expected record value but got $value")
        case Type.TypeCase.LIST =>
          if (value.hasList) {
            val lst = value.getList
            val innerType = t.getList.getInnerType
            val elems = lst.getValuesList.asScala.map { elem =>
              dasArrayValueToPostgresConst(elem, innerType)
            }
            val errors = elems.collect { case Left(err) => err }
            if (errors.nonEmpty) {
              Left(errors.mkString(", "))
            } else {
              val values = elems.collect { case Right(v) => v }
              Right("ARRAY[" + values.mkString(", ") + "]")
            }
          } else Left(s"Expected list value but got $value")
        case Type.TypeCase.ANY =>
          // Fallback to JSONB
          val jsonStr = valueToJson(value)
          Right(s"jsonb '${escapeJsonb(jsonStr)}'")
        case _ =>
          Left(s"Unsupported type: $t")
      }
  }

  private def dasArrayValueToPostgresConst(value: Value, t: Type): Either[String, String] = {
    t.getTypeCase match {
      case Type.TypeCase.BYTE =>
        if (value.hasByte) Right(value.getByte.getV.toString)
        else Left(s"Expected byte value but got $value")
      case Type.TypeCase.SHORT =>
        if (value.hasShort) Right(value.getShort.getV.toString)
        else Left(s"Expected short value but got $value")
      case Type.TypeCase.INT =>
        if (value.hasInt) Right(value.getInt.getV.toString)
        else Left(s"Expected int value but got $value")
      case Type.TypeCase.LONG =>
        if (value.hasLong) Right(value.getLong.getV.toString)
        else Left(s"Expected long value but got $value")
      case Type.TypeCase.FLOAT =>
        if (value.hasFloat) Right(value.getFloat.getV.toString)
        else Left(s"Expected float value but got $value")
      case Type.TypeCase.DOUBLE =>
        if (value.hasDouble) Right(value.getDouble.getV.toString)
        else Left(s"Expected double value but got $value")
      case Type.TypeCase.DECIMAL =>
        if (value.hasDecimal) Right(value.getDecimal.getV)
        else Left(s"Expected decimal value but got $value")
      case Type.TypeCase.BOOL =>
        if (value.hasBool) Right(if (value.getBool.getV) "true" else "false")
        else Left(s"Expected bool value but got $value")
      case Type.TypeCase.STRING =>
        if (value.hasString) {
          val s = value.getString.getV
          Right(s"'${s.replace("'", "''")}'")
        } else Left(s"Expected string value but got $value")
      case Type.TypeCase.BINARY =>
        if (value.hasBinary) {
          val bytes = value.getBinary.getV.toByteArray
          val hex = bytes.map("%02x".format(_)).mkString
          Right(s"E'\\\\x$hex'::bytea")
        } else Left(s"Expected binary value but got $value")
      case Type.TypeCase.DATE =>
        if (value.hasDate) {
          val d = value.getDate
          // Format as YYYY-MM-DD
          Right(f"'${d.getYear}%04d-${d.getMonth}%02d-${d.getDay}%02d'::date")
        } else Left(s"Expected date value but got $value")
      case Type.TypeCase.TIME =>
        if (value.hasTime) {
          val tVal = value.getTime
          Right(f"'${tVal.getHour}%02d:${tVal.getMinute}%02d:${tVal.getSecond}%02d'::time")
        } else Left(s"Expected time value but got $value")
      case Type.TypeCase.TIMESTAMP =>
        if (value.hasTimestamp) {
          val ts = value.getTimestamp
          Right(
            f"'${ts.getYear}%04d-${ts.getMonth}%02d-${ts.getDay}%02d ${ts.getHour}%02d:${ts.getMinute}%02d:${ts.getSecond}%02d'::timestamp")
        } else Left(s"Expected timestamp value but got $value")
      case Type.TypeCase.INTERVAL =>
        if (value.hasInterval) {
          val iv = value.getInterval
          Right(
            s"'P${iv.getYears}Y${iv.getMonths}M${iv.getDays}DT${iv.getHours}H${iv.getMinutes}M${iv.getSeconds}S'::interval")
        } else Left(s"Expected interval value but got $value")
      case Type.TypeCase.RECORD =>
        if (value.hasRecord) {
          val jsonStr = valueToJson(value)
          Right(s"'${escapeJsonb(jsonStr)}'::jsonb")
        } else Left(s"Expected record value but got $value")
      case Type.TypeCase.ANY =>
        val jsonStr = valueToJson(value)
        Right(s"'${escapeJsonb(jsonStr)}'::jsonb")
      case _ =>
        // Fallback to JSONB
        val jsonStr = valueToJson(value)
        Right(s"'${escapeJsonb(jsonStr)}'::jsonb")
    }
  }

  // A helper function to escape a string for JSON.
  private def escapeJson(s: String): String = {
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c    => c.toString
    }
  }

  /**
   * Converts a DAS Value (from the proto) into a JSON string.
   */
  private def valueToJson(value: Value): String = {
    value.getValueCase match {
      case Value.ValueCase.NULL =>
        "null"
      case Value.ValueCase.BYTE =>
        value.getByte.getV.toString
      case Value.ValueCase.SHORT =>
        value.getShort.getV.toString
      case Value.ValueCase.INT =>
        value.getInt.getV.toString
      case Value.ValueCase.LONG =>
        value.getLong.getV.toString
      case Value.ValueCase.FLOAT =>
        value.getFloat.getV.toString
      case Value.ValueCase.DOUBLE =>
        value.getDouble.getV.toString
      case Value.ValueCase.DECIMAL =>
        // We assume the decimal is already a valid numeric literal.
        value.getDecimal.getV
      case Value.ValueCase.BOOL =>
        if (value.getBool.getV) "true" else "false"
      case Value.ValueCase.STRING =>
        "\"" + escapeJson(value.getString.getV) + "\""
      case Value.ValueCase.BINARY =>
        // Encode the binary as a base64 string.
        val bytes = value.getBinary.getV.toByteArray
        "\"" + java.util.Base64.getEncoder.encodeToString(bytes) + "\""
      case Value.ValueCase.DATE =>
        val d = value.getDate
        // Format as "YYYY-MM-DD"
        f""""${d.getYear}%04d-${d.getMonth}%02d-${d.getDay}%02d""""
      case Value.ValueCase.TIME =>
        val t = value.getTime
        // Format as "HH:MM:SS"
        f""""${t.getHour}%02d:${t.getMinute}%02d:${t.getSecond}%02d""""
      case Value.ValueCase.TIMESTAMP =>
        val ts = value.getTimestamp
        // Format as ISO8601 "YYYY-MM-DDTHH:MM:SS"
        f""""${ts.getYear}%04d-${ts.getMonth}%02d-${ts.getDay}%02dT${ts.getHour}%02d:${ts.getMinute}%02d:${ts.getSecond}%02d""""
      case Value.ValueCase.INTERVAL =>
        val iv = value.getInterval
        // Format using ISO8601 duration notation.
        s""""P${iv.getYears}Y${iv.getMonths}M${iv.getDays}DT${iv.getHours}H${iv.getMinutes}M${iv.getSeconds}S""""
      case Value.ValueCase.RECORD =>
        val rec = value.getRecord
        // For each attribute in the record, recursively convert its value.
        val fields = rec.getAttsList.asScala.map { attr =>
          val name = attr.getName
          val fieldValue = valueToJson(attr.getValue)
          s""""${escapeJson(name)}": $fieldValue"""
        }
        "{" + fields.mkString(", ") + "}"
      case Value.ValueCase.LIST =>
        val lst = value.getList
        // Recursively convert each element.
        val elems = lst.getValuesList.asScala.map(valueToJson)
        "[" + elems.mkString(", ") + "]"
      case other =>
        throw new IllegalArgumentException(s"Unsupported Value type: $other")
    }
  }

  /**
   * A helper to escape a JSON string literal (very simple implementation).
   */
  private def escapeJsonb(s: String): String = {
    s.replace("'", "''")
  }

  // TODO dummy
  private def quoteIdentifier(name: String): String =
    s""""$name""""

  private def dasTypeToPostgres(tipe: Type): Either[String, String] = {
    tipe.getTypeCase match {
      case Type.TypeCase.BYTE      => Right("smallint")
      case Type.TypeCase.SHORT     => Right("smallint")
      case Type.TypeCase.INT       => Right("integer")
      case Type.TypeCase.LONG      => Right("bigint")
      case Type.TypeCase.FLOAT     => Right("real")
      case Type.TypeCase.DOUBLE    => Right("double precision")
      case Type.TypeCase.DECIMAL   => Right("numeric")
      case Type.TypeCase.BOOL      => Right("boolean")
      case Type.TypeCase.STRING    => Right("text")
      case Type.TypeCase.BINARY    => Right("bytea")
      case Type.TypeCase.DATE      => Right("date")
      case Type.TypeCase.TIME      => Right("time")
      case Type.TypeCase.TIMESTAMP => Right("timestamp")
      case Type.TypeCase.INTERVAL  => Right("interval")
      case Type.TypeCase.RECORD    => Right("jsonb")
      case Type.TypeCase.ANY       => Right("jsonb")
      case Type.TypeCase.LIST =>
        tipe.getList.getInnerType.getTypeCase match {
          case Type.TypeCase.BYTE      => Right("smallint[]")
          case Type.TypeCase.SHORT     => Right("smallint[]")
          case Type.TypeCase.INT       => Right("integer[]")
          case Type.TypeCase.LONG      => Right("bigint[]")
          case Type.TypeCase.FLOAT     => Right("real[]")
          case Type.TypeCase.DOUBLE    => Right("double precision[]")
          case Type.TypeCase.DECIMAL   => Right("numeric[]")
          case Type.TypeCase.BOOL      => Right("boolean[]")
          case Type.TypeCase.STRING    => Right("text[]")
          case Type.TypeCase.BINARY    => Right("bytea[]")
          case Type.TypeCase.DATE      => Right("date[]")
          case Type.TypeCase.TIME      => Right("time[]")
          case Type.TypeCase.TIMESTAMP => Right("timestamp[]")
          case Type.TypeCase.INTERVAL  => Right("interval[]")
          case _                       => Right("jsonb[]")
        }
      case _ =>
        Left(s"dasTypeToPostgres: unsupported type: $tipe")
    }
  }
}
