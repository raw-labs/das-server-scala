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

package com.rawlabs.das.mock.functions

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException
import com.rawlabs.protocol.das.v1.common.Environment
import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class RockAlbumsListOfRecordsWithNestedList extends DASMockFunction {

  private case class Album(albumName: String, band: String, songs: List[String])

  // Our internal dataset of rock albums.
  private val albums: List[Album] = List(
    Album("The Dark Side of the Moon", "Pink Floyd", List("Speak to Me", "Breathe", "On the Run", "Time", "Money")),
    Album(
      "Led Zeppelin IV",
      "Led Zeppelin",
      List("Black Dog", "Rock and Roll", "The Battle of Evermore", "Stairway to Heaven")),
    Album(
      "Back in Black",
      "AC/DC",
      List("Hells Bells", "Shoot to Thrill", "Back in Black", "You Shook Me All Night Long")),
    Album(
      "Abbey Road",
      "The Beatles",
      List("Come Together", "Something", "Maxwell's Silver Hammer", "Here Comes the Sun")))

  /**
   * The execute method.
   *
   * This function ignores any input arguments and returns a Value containing a list of album records.
   */
  def execute(args: Map[String, Value], maybeEnv: Option[Environment]): Value = {
    // Create a builder for a ValueList.
    val listBuilder = ValueList.newBuilder()
    // For each album in our dataset, build a record.
    albums.foreach { album =>
      val recordBuilder = ValueRecord.newBuilder()
      // Field "album_name"
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("album_name")
          .setValue(Value
            .newBuilder()
            .setString(ValueString.newBuilder().setV(album.albumName).build()))
          .build())
      // Field "band"
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("band")
          .setValue(Value
            .newBuilder()
            .setString(ValueString.newBuilder().setV(album.band).build()))
          .build())
      // Field "songs": a list of strings.
      val songsListBuilder = ValueList.newBuilder()
      album.songs.foreach { song =>
        songsListBuilder.addValues(
          Value
            .newBuilder()
            .setString(ValueString.newBuilder().setV(song).build()))
      }
      recordBuilder.addAtts(
        ValueRecordAttr
          .newBuilder()
          .setName("songs")
          .setValue(Value.newBuilder().setList(songsListBuilder.build()))
          .build())
      // Add the record to the list.
      listBuilder.addValues(Value.newBuilder().setRecord(recordBuilder.build()))
    }
    // Return the list wrapped in a Value.
    Value.newBuilder().setList(listBuilder.build()).build()
  }

  /**
   * The function definition.
   *
   * This declares a function "rock_albums" that takes no parameters and returns a list of records. Each record
   * contains:
   *   - album_name (text, non-nullable)
   *   - band (text, non-nullable)
   *   - songs (an array of text, non-nullable)
   */
  def definition: FunctionDefinition = {
    // Define the record type for a single album.
    val albumRecordType = Type
      .newBuilder()
      .setRecord(
        RecordType
          .newBuilder()
          .addAtts(
            AttrType
              .newBuilder()
              .setName("album_name")
              .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
          .addAtts(AttrType
            .newBuilder()
            .setName("band")
            .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
          .addAtts(
            AttrType
              .newBuilder()
              .setName("songs")
              .setTipe(
                Type
                  .newBuilder()
                  .setList(ListType
                    .newBuilder()
                    .setInnerType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build()))
                    .setNullable(false)
                    .build())))
          .build())
      .build()

    // The return type is a list of album records.
    val returnType = Type
      .newBuilder()
      .setList(
        ListType
          .newBuilder()
          .setInnerType(albumRecordType)
          .setNullable(false)
          .build())
      .build()

    // Build the function definition.
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("rock_albums").build())
      // No parameters in this example.
      .setDescription("Returns a list of rock albums with album name, band, and list of songs")
      .setReturnType(returnType)
      .build()
  }
}
