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

import com.rawlabs.das.server.cache.queue.Codec
import com.rawlabs.protocol.das.v1.tables.Row

import net.openhft.chronicle.wire.{WireIn, WireOut}

/**
 * Codec implementation for Row, encoding and decoding Row objects for Chronicle Queue using Protobuf serialization.
 */
class RowCodec extends Codec[Row] {

  /**
   * Serializes a Row into Chronicle WireOut using Protobuf serialization.
   *
   * @param out The WireOut to write the serialized data.
   * @param value The Row to serialize.
   */
  override def write(out: WireOut, value: Row): Unit = {
    require(value != null, "Row cannot be null")

    // Serialize Row to Protobuf byte array
    val serializedBytes = value.toByteArray

    // Write the serialized bytes to WireOut
    out.write("row").bytes(serializedBytes)
  }

  /**
   * Deserializes a Row from Chronicle WireIn using Protobuf serialization.
   *
   * @param in The WireIn to read the serialized data.
   * @return The deserialized Row.
   */
  override def read(in: WireIn): Row = {
    // Read the serialized bytes from WireIn
    val serializedBytes = in.read("row").bytes()

    // Deserialize Protobuf byte array into Row object
    Row.parseFrom(serializedBytes)
  }
}
