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

import akka.http.scaladsl.model._
import scalatags.Text.all._
import scalatags.Text.tags2.title

import scala.concurrent.ExecutionContext

/**
 * A service that uses the real CacheManager to fetch data, and returns the resulting HTML as a Future.
 */
class DebugAppService()(implicit
    ec: ExecutionContext,
    scheduler: akka.actor.typed.Scheduler) {

  // --------------------------------------------------------------------------
  // 2) RENDER “OVERVIEW” PAGE (SYNCHRONOUS EXAMPLE)
  //    This might not need manager data, so we do it directly.
  // --------------------------------------------------------------------------
  def renderOverviewPage(): HttpEntity.Strict = {
    val htmlContent = html(
      head(
        meta(charset := "UTF-8"),
        title("DAS Debug UI"),
        link(rel := "stylesheet", href := "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css")),
      body(cls := "bg-light")(
        div(cls := "container my-5")(
          h1(cls := "mb-4")("Welcome to DAS Debug UI"),
          p("Use the links below to see system status:"),
          ul(li(a(href := "/cache")("Cache Catalog")), li(a(href := "/actors")("Actors State"))))))
    htmlToEntity(htmlContent)
  }


  // --------------------------------------------------------------------------
  // UTILITY: Convert ScalaTags => HttpEntity
  // --------------------------------------------------------------------------
  private def htmlToEntity(content: Frag): HttpEntity.Strict = {
    HttpEntity(ContentTypes.`text/html(UTF-8)`, "<!DOCTYPE html>" + content.render)
  }
}
