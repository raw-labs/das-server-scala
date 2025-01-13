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

package com.rawlabs.das.server.grpc

import scala.collection.mutable.ArrayBuffer

import com.rawlabs.protocol.das.v1.tables.Row

import akka.stream._
import akka.stream.stage._

/**
 * A GraphStage that groups Row elements by their total serialized size in bytes. Once the sum of sizes in the buffer
 * reaches the specified `maxBatchSizeBytes`, the batch is emitted and a new one starts.
 *
 * This version calls `row.toByteArray` inside the stage to measure each row's size. If a single row itself exceeds the
 * maximum, the stage fails.
 *
 * @param maxBatchSizeBytes the maximum allowed size in bytes for each emitted batch
 */
class SizeBasedBatcher(maxBatchCount: Long, maxBatchSizeBytes: Long) extends GraphStage[FlowShape[Row, List[Row]]] {

  val in: Inlet[Row] = Inlet("SizeBasedBatcher.in")
  val out: Outlet[List[Row]] = Outlet("SizeBasedBatcher.out")

  override def shape: FlowShape[Row, List[Row]] =
    FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private val buffer = ArrayBuffer.empty[Row]
      private var currentSize: Long = 0L
      private var currentRowCount: Long = 0L

      override def preStart(): Unit = {
        // Start pulling from the upstream as soon as we start
        pull(in)
      }

      // InHandler: how to handle incoming elements from `in`
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem: Row = grab(in)
            // Measure the row size in bytes
            val elemSize = elem.getSerializedSize.toLong

            // If a single row alone exceeds the limit, fail
            if (elemSize > maxBatchSizeBytes) {
              failStage(
                new IllegalArgumentException(
                  s"Single Row of size $elemSize bytes exceeds maxBatchSizeBytes = $maxBatchSizeBytes"))
              return
            }

            // If adding this row would exceed a batch limit, emit the current batch first
            if (currentSize + elemSize > maxBatchSizeBytes || currentRowCount > maxBatchCount) {
              emitBatch()
              buffer.clear()
              currentSize = 0L
              currentRowCount = 0L
            }

            // Add this row to the buffer
            buffer += elem
            currentSize += elemSize
            currentRowCount += 1

            // Pull the next element
            pull(in)
          }

          override def onUpstreamFinish(): Unit = {
            // If there's anything buffered, emit it before completing
            if (buffer.nonEmpty) {
              emitBatch()
            }
            completeStage()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            failStage(ex)
          }
        })

      // OutHandler: how to handle downstream "pull" demands
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            // We only emit from onPush or onUpstreamFinish, so no action here
          }
        })

      private def emitBatch(): Unit = {
        emit[List[Row]](out, buffer.toList)
      }
    }
}
