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

package com.rawlabs.das.server.webui

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object DASWebUIServer {

  def startHttpInterface(host: String, port: Int, debugService: DebugAppService)(implicit
      system: ActorSystem[_],
      mat: Materializer,
      ec: ExecutionContext): Unit = {

    val route =
      concat(pathSingleSlash {
        get {
          // Synchronous: debugService.renderOverviewPage() => HttpEntity
          complete(debugService.renderOverviewPage())
        }
      })

    val bindingF = Http().newServerAt(host, port).bind(route)

    bindingF.onComplete {
      case Success(binding) =>
        system.log.info(s"Web UI server online at http://$host:$port/")
      case Failure(ex) =>
        system.log.error(s"Failed to bind Web UI server: ${ex.getMessage}")
        system.terminate()
    }
  }
}
