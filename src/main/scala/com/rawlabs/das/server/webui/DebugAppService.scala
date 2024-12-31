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

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

import com.rawlabs.das.server.cache.catalog.CacheEntry
import com.rawlabs.das.server.cache.manager.CacheManager
import com.rawlabs.das.server.cache.manager.CacheManager.{
  ActionAck,
  ClearAllCaches,
  DataSourceInfo,
  ListAllCaches,
  ListDataSources
}
import com.rawlabs.protocol.das.v1.tables.Row

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.model._
import akka.util.Timeout
import scalatags.Text.all._
import scalatags.Text.tags2.title

/**
 * A service that uses the real CacheManager to fetch data, and returns the resulting HTML as a Future.
 */
class DebugAppService(cacheManager: ActorRef[CacheManager.Command[Row]])(implicit
    ec: ExecutionContext,
    scheduler: akka.actor.typed.Scheduler) {

  implicit private val timeout: Timeout = 3.seconds

  // --------------------------------------------------------------------------
  // 1) CLEAR ALL CACHE
  // --------------------------------------------------------------------------
  def clearAllCache(): Future[HttpEntity.Strict] = {
    // We send ClearAllCaches to the manager:
    val futureAck: Future[ActionAck] = cacheManager.ask(replyTo => ClearAllCaches(replyTo))

    futureAck.map { ack =>
      // After clearing, we might show a small "done" page, or redirect logic in the route.
      val htmlContent = html(
        head(meta(charset := "UTF-8"), title("Cleared All Cache")),
        body(h1("Clear All Cache"), p(s"Result: ${ack.message}"), a(href := "/cache")("Return to Cache Catalog")))
      htmlToEntity(htmlContent)
    }
  }

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
  // 3) RENDER CACHE CATALOG
  //    We'll ask the manager for all caches, then build HTML from them.
  // --------------------------------------------------------------------------
  def renderCacheCatalogPage(): Future[HttpEntity.Strict] = {
    // Ask for the entire list
    val futureCaches: Future[List[CacheEntry]] = cacheManager.ask(replyTo => ListAllCaches(replyTo))

    futureCaches.map { entries =>
      val tableRows = entries.map { e =>
        tr(td(e.cacheId.toString), td(e.definition.tableId), td(e.state), td(e.sizeInBytes.getOrElse(0L).toString))
      }

      val htmlContent = html(
        head(
          meta(charset := "UTF-8"),
          title("Cache Catalog"),
          link(rel := "stylesheet", href := "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css")),
        body(cls := "bg-light")(
          div(cls := "container my-5")(
            h1("Cache Catalog"),
            table(cls := "table table-bordered")(
              thead(tr(th("Cache ID"), th("Table ID"), th("State"), th("Size (bytes)"))),
              tbody(tableRows)),
            // Link to do “clear all.” We'll let the route handle the GET vs POST aspect.
            a(
              cls := "btn btn-danger me-3",
              href := "/cache/clear",
              attr("onclick") := "return confirm('Are you sure you want to clear all cache entries?')")(
              "Clear All Cache"),
            a(cls := "btn btn-secondary", href := "/")("Back"))))
      htmlToEntity(htmlContent)
    }
  }

  // --------------------------------------------------------------------------
  // 4) RENDER ACTORS PAGE: e.g. Data Sources
  // --------------------------------------------------------------------------
  def renderActorsPage(): Future[HttpEntity.Strict] = {
    val futureInfo: Future[List[DataSourceInfo]] = cacheManager.ask(replyTo => ListDataSources(replyTo))

    futureInfo.map { dataSources =>
      val listItems = dataSources.map { ds =>
        li(s"CacheId=${ds.cacheId}, state=${ds.state}, activeReaders=${ds.activeReaders}")
      }

      val htmlContent = html(
        head(
          meta(charset := "UTF-8"),
          title("Actors Info"),
          link(rel := "stylesheet", href := "https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css")),
        body(cls := "bg-light")(
          div(cls := "container my-5")(
            h1("Data Sources / Actors"),
            ul(listItems),
            a(cls := "btn btn-secondary", href := "/")("Back"))))
      htmlToEntity(htmlContent)
    }
  }

  // --------------------------------------------------------------------------
  // UTILITY: Convert ScalaTags => HttpEntity
  // --------------------------------------------------------------------------
  private def htmlToEntity(content: Frag): HttpEntity.Strict = {
    HttpEntity(ContentTypes.`text/html(UTF-8)`, "<!DOCTYPE html>" + content.render)
  }
}
