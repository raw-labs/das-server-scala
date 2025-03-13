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

import com.rawlabs.das.server.FunctionImport
import com.rawlabs.protocol.das.v1.common.{DASDefinition, DASId}
import com.rawlabs.protocol.das.v1.services._

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
  val functionImport = new FunctionImport()
  val statements = for (funcDef <- getFuncResp.getDefinitionsList.asScala) yield {
    val funcName = funcDef.getFunctionId.getName
    val stmts =
      for (
        inner <- functionImport.innerStatements("test", funcDef);
        outer <- functionImport.outerStatements("test", fullOptions + ("das_function_name" -> funcName), funcDef)
      ) yield {
        inner ++ outer
      }
    funcName -> stmts
  }

  statements.foreach { case (funcName, statements) =>
    println(s"-- Function: $funcName")
    statements match {
      case Left(err)   => throw new IllegalArgumentException(err)
      case Right(list) => list.foreach(println)
    }
  }

  // Shutdown the channel when done.
  channel.shutdownNow()
}
