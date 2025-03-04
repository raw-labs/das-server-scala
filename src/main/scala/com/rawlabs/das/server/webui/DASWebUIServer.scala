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

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.stream.Materializer

object DASWebUIServer {

  def startHttpInterface(host: String, port: Int, debugService: DebugAppService)(implicit
      system: ActorSystem[_],
      mat: Materializer,
      ec: ExecutionContext): Unit = {

    val route =
      concat(
        pathSingleSlash {
          get {
            // Synchronous: debugService.renderOverviewPage() => HttpEntity
            complete(debugService.renderOverviewPage())
          }
        },
        path("cache") {
          get {
            complete(debugService.renderCacheCatalog()) // GET /cache
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
