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

package com.rawlabs.das.util

import scala.jdk.CollectionConverters._

import com.rawlabs.protocol.das.v1.common.{DASDefinition, DASId}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.services._
import com.rawlabs.protocol.das.v1.types.{AttrType, Type, Value}

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

object DumpRegisterStatements extends App {

  // Parse command-line arguments into a Map[String,String]
  private val dasOptions: Map[String, String] = args.map { arg =>
    val parts = arg.split("=")
    parts(0) -> parts(1)
  }.toMap

  // Get the DAS URL (must be provided)
  val dasUrl = dasOptions.getOrElse("das_url", throw new IllegalArgumentException("das_url must be specified"))

  // Create a gRPC channel towards the DAS server.
  val channel: ManagedChannel =
    ManagedChannelBuilder.forTarget(dasUrl).usePlaintext().build(); // assuming an insecure channel

  // Create a blocking stub for the RegistrationService.
  val registrationStub = RegistrationServiceGrpc.newBlockingStub(channel)

  // Get (or register) a DAS ID.
  // If the options map does not contain "das_id", then we call Register.
  val dasId: String = dasOptions.get("das_id") match {
    case Some(id) =>
      id
    case None =>
      // "das_type" must be provided as well.
      val dasType = dasOptions.getOrElse(
        "das_type",
        throw new IllegalArgumentException("das_type must be specified when das_id is not provided"))
      // Build a DASDefinition message; note that the options are provided as a Java Map.
      val defOptions = dasOptions.asJava
      val definition: DASDefinition = DASDefinition
        .newBuilder()
        .setType(dasType)
        .putAllOptions(defOptions)
        .build()
      val registerReq: RegisterRequest = RegisterRequest
        .newBuilder()
        .setDefinition(definition)
        .build()
      val registerResp: RegisterResponse = registrationStub.register(registerReq)
      if (registerResp.hasError) {
        throw new RuntimeException(s"Registration failed: ${registerResp.getError}")
      } else {
        registerResp.getId.getId
      }
  }

  val fullOptions = dasOptions + ("das_id" -> dasId)

  // Create a blocking stub for the FunctionsService.
  val functionsStub = FunctionsServiceGrpc.newBlockingStub(channel)

  // Build and send the GetFunctionDefinitionsRequest using the DAS ID.
  val getFuncReq = GetFunctionDefinitionsRequest
    .newBuilder()
    .setDasId(DASId.newBuilder().setId(dasId).build())
    .build()

  val getFuncResp: GetFunctionDefinitionsResponse = functionsStub.getFunctionDefinitions(getFuncReq)

  // Print the function definitions
  println("Registered Functions:")
  val statements = for (funcDef <- getFuncResp.getDefinitionsList.asScala) yield {
    val funcName = funcDef.getFunctionId.getName
    val postgresCode = create("test", fullOptions + ("das_function_name" -> funcName), funcDef)
    funcName -> postgresCode
  }

  statements.foreach { case (funcName, postgresCode) =>
    println(s"-- Function: $funcName")
    postgresCode.foreach(println)
  }

  def create(schemaName: String, options: Map[String, String], definition: FunctionDefinition): Iterable[String] = {

    // Dummy implementations for helper functions.
    // In your real code these would have the actual logic to quote identifiers,
    // convert a Map[String,String] into a Postgres array literal of key=value strings, etc.
    def quoteIdentifier(name: String): String =
      s""""$name""""

    // 6) Get the friendly function name.
    val qFriendly = quoteIdentifier(definition.getFunctionId.getName)

    def extractJsonbField(rowName: String, attrType: AttrType): String = {
      val fieldName = attrType.getName
      val jsonb = s"($rowName->'$fieldName')"
      val fieldType = attrType.getTipe
      if (fieldType.hasString) {
        s"($rowName->>'$fieldName')"
      } else if (fieldType.hasInt) {
        s"$jsonb::integer"
      } else if (fieldType.hasRecord) {
        jsonb
      } else if (fieldType.hasList) {
        val items = s"jsonb_array_elements_text($jsonb)"
        val innerType = fieldType.getList.getInnerType
        s"SELECT i::${pgPlainType(innerType).getOrElse("jsonb")} FROM $items i"
      } else {
        ???
      }
    }

    def dictToArrayOfKV(options: Map[String, String]): String = {
      // Convert each key/value pair to "key=value" and then build an array literal.
      val items = options.map { case (k, v) => s"'$k=$v'" }.mkString(", ")
      s"ARRAY[$items]::text[]"
    }

    def listOfStringsToTextArray(strings: Iterable[String]): String = {
      val items = strings.map(s => s"'$s'").mkString(", ")
      s"ARRAY[$items]::text[]"
    }

    def mkPgValue(value: Value): Either[String, String] = {
      // Dummy implementation; you would need to handle all types of values.
      if (value.hasString) Right(value.getString.getV)
      else if (value.hasInt) Right(value.getInt.getV.toString)
      else Left("unknown")
    }

    def pgPlainType(value: Type): Either[String, String] = {
      // Dummy implementation; you would need to handle all types of values.
      if (value.hasInt) Right("integer")
      else if (value.hasString) Right("text")
      else if (value.hasRecord) Right("jsonb")
      else if (value.hasAny) Right("jsonb")
      else if (value.hasList) {
        val innerType = value.getList.getInnerType
        pgPlainType(innerType).map(t => s"$t[]")
      } else Left("unknown")
    }

    def mkPgReturnType(tipe: Type): Either[String, String] = {
      if (tipe.hasInt) Right("integer")
      else if (tipe.hasString) Right("text")
      else if (tipe.hasRecord) {
        // make it a table with the record fields
        val columns = tipe.getRecord.getAttsList.asScala.map(f =>
          quoteIdentifier(f.getName) + " " + pgPlainType(f.getTipe).getOrElse(
            throw new IllegalArgumentException(s"Unsupported type for ${f.getName}: ${f.getTipe}")))
        if (columns.isEmpty) Right("JSONB")
        else Right(s"TABLE(${columns.mkString(", ")})")
      } else if (tipe.hasAny) Right("jsonb")
      else if (tipe.hasList) {
        val innerType = tipe.getList.getInnerType
        if (innerType.hasRecord) {
          // make it a table with the record fields
          val columns = innerType.getRecord.getAttsList.asScala.map(f =>
            quoteIdentifier(f.getName) + " " + pgPlainType(f.getTipe).getOrElse(
              throw new IllegalArgumentException(s"Unsupported type for ${f.getName}: ${f.getTipe}")))
          if (columns.isEmpty) Right(s"TABLE($qFriendly JSONB)")
          else Right(s"TABLE(${columns.mkString(", ")})")
        } else {
          // make it an array of the inner type
          Right(
            s"TABLE($qFriendly ${pgPlainType(innerType).getOrElse(throw new IllegalArgumentException(s"Unsupported type for $innerType"))})")
        }
      } else Left("unknown")
    }

    // 1) Raw function name: functionName + "_raw"
    val rawFuncName = s"${definition.getFunctionId.getName}_raw"
    val parameters = definition.getParamsList.asScala
      .map { param =>
        val argName = param.getName
        val pgType = pgPlainType(param.getType) match {
          case Right(v) => v
          case Left(_)  => throw new IllegalArgumentException(s"Unsupported type for $argName: ${param.getType}")
        }
        val defaultVal: Option[String] = {
          if (param.hasDefaultValue) mkPgValue(param.getDefaultValue) match {
            case Right(v) => Some(v)
            case Left(_) =>
              throw new IllegalArgumentException(s"Unsupported default value for $argName: ${param.getDefaultValue}")
          }
          else None
        }
        (argName, pgType, defaultVal)
      }

    val plainReturnType = pgPlainType(definition.getReturnType) match {
      case Right(v) => v
      case Left(_)  => throw new IllegalArgumentException(s"Unsupported return type: ${definition.getReturnType}")
    }

    val returnType = mkPgReturnType(definition.getReturnType) match {
      case Right(v) => v
      case Left(_)  => throw new IllegalArgumentException(s"Unsupported return type: ${definition.getReturnType}")
    }

    // 2) Build the raw function's argument list.
    // It always starts with "options text[]" and "arg_names text[]"
    val rawArgListParts = List("options text[]", "arg_names text[]") ++
      parameters.map { case (argName, pgType, _) =>
        s"${quoteIdentifier(argName)} $pgType"
      }
    val rawArgList = rawArgListParts.mkString(", ")

    // 3) Quote the schema and raw function names.
    val qSchema = quoteIdentifier(schemaName)
    val qRawFunc = quoteIdentifier(rawFuncName)

    // 4) Build the CREATE statement for the raw function.
    val createRaw =
      s"""CREATE OR REPLACE FUNCTION $qSchema.$qRawFunc(
         |  $rawArgList
         |)
         |RETURNS $plainReturnType
         |LANGUAGE C
         |AS 'multicorn', 'my_c_entrypoint';""".stripMargin

    // 5) Build the friendly function's argument list.
    // For each argument, include a DEFAULT clause if a default value is provided.
    val friendlyArgList = parameters
      .map { case (argName, pgType, maybeDefault) =>
        val defaultClause = maybeDefault.map(v => s" DEFAULT $v").getOrElse("")
        s"${quoteIdentifier(argName)} $pgType$defaultClause"
      }
      .mkString(", ")

    // 7) Build array literals:
    //    - options_array_literal from the options map.
    //    - arg_names_array from the list of argument names.
    val optionsArrayLiteral = dictToArrayOfKV(options)
    val argNamesList = definition.getParamsList.asScala.map(_.getName).toSeq
    val argNamesArray = listOfStringsToTextArray(argNamesList)

    // 8) Build a comma-separated list of argument references for the SQL body.
    val passUserArgs = if (argNamesList.isEmpty) "" else argNamesList.map(quoteIdentifier).mkString(", ")

    val body = {
      val row = qFriendly
      val functionCall = s"""$qSchema.$qRawFunc(
                          |      $optionsArrayLiteral,
                          |      $argNamesArray${if (passUserArgs.nonEmpty) s", $passUserArgs" else ""}
                          |    )""".stripMargin
      val call = if (definition.getReturnType.hasList) {
        "unnest(" + functionCall + s") AS $row"
      } else {
        functionCall + s" AS $row"
      }

      val projection = {
        val maybe = if (definition.getReturnType.hasList) {
          val innerType = definition.getReturnType.getList.getInnerType
          if (innerType.hasRecord && innerType.getRecord.getAttsCount > 0) {
            // project all fields from the inner jsonb
            Some(innerType.getRecord.getAttsList.asScala.map(f => extractJsonbField(row, f)))
          } else {
            None
          }
        } else if (definition.getReturnType.hasRecord) {
          // project all fields from the inner jsonb
          Some(definition.getReturnType.getRecord.getAttsList.asScala.map(f => extractJsonbField(row, f)))
        } else {
          None
        }
        maybe.map(_.mkString(", "))
      }
      s"SELECT ${projection.getOrElse("*")} FROM $call"
    }

    // 9) Build the friendly function CREATE statement.
    val createFriendly =
      s"""CREATE OR REPLACE FUNCTION $qSchema.$qFriendly(
         |  $friendlyArgList
         |)
         |RETURNS $returnType
         |LANGUAGE sql
         |AS $$$$
         |  $body;
         |$$$$;""".stripMargin

    // Return the two statements as an Iterable (for example, a List)
    List(createRaw, createFriendly)
  }

  // Shutdown the channel when done.
  channel.shutdownNow()
}
