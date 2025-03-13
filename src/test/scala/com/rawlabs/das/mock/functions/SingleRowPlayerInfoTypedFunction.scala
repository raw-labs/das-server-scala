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

import com.rawlabs.protocol.das.v1.functions.{FunctionDefinition, FunctionId, ParameterDefinition}
import com.rawlabs.protocol.das.v1.types._

class SingleRowPlayerInfoTypedFunction extends DASMockFunction {

  import java.time.LocalDate

  case class BasketballPlayer(name: String, team: String, jerseyNumber: Int, birthDate: LocalDate)

  val players: List[BasketballPlayer] = List(
    BasketballPlayer("Michael Jordan", "Chicago Bulls", 23, LocalDate.of(1963, 2, 17)),
    BasketballPlayer("LeBron James", "Los Angeles Lakers", 23, LocalDate.of(1984, 12, 30)),
    BasketballPlayer("Kobe Bryant", "Los Angeles Lakers", 24, LocalDate.of(1978, 8, 23)),
    BasketballPlayer("Larry Bird", "Boston Celtics", 33, LocalDate.of(1956, 12, 7)),
    BasketballPlayer("Magic Johnson", "Los Angeles Lakers", 32, LocalDate.of(1959, 8, 14)),
    BasketballPlayer("Shaquille O'Neal", "Los Angeles Lakers", 34, LocalDate.of(1972, 3, 6)),
    BasketballPlayer("Tim Duncan", "San Antonio Spurs", 21, LocalDate.of(1976, 4, 25)),
    BasketballPlayer("Dirk Nowitzki", "Dallas Mavericks", 41, LocalDate.of(1978, 6, 19))
  )

  def execute(args: Map[String, Value]): Value = {
    // Extract player's name from the argument "name"
    val name = args("name").getString.getV

    // Build a record: we will include four fields.
    val recordBuilder = ValueRecord.newBuilder()

    // Field "name": return the name string
    recordBuilder.addAtts(
      ValueRecordAttr.newBuilder()
        .setName("name")
        .setValue(
          Value.newBuilder()
            .setString(ValueString.newBuilder().setV(name).build())
        ).build()
    )

    // Create attribute builders for the other fields.
    val teamAttrBuilder = ValueRecordAttr.newBuilder().setName("team")
    val jerseyAttrBuilder = ValueRecordAttr.newBuilder().setName("jersey_number")
    val birthDateAttrBuilder = ValueRecordAttr.newBuilder().setName("birth_date")

    // Look up the player in our dataset.
    players.find(_.name == name).foreach { player =>
      teamAttrBuilder.setValue(
        Value.newBuilder()
          .setString(ValueString.newBuilder().setV(player.team).build())
      )
      jerseyAttrBuilder.setValue(
        Value.newBuilder()
          .setInt(ValueInt.newBuilder().setV(player.jerseyNumber).build())
      )
      // Convert the LocalDate to ISO format ("YYYY-MM-DD")
      val birthDateStr = player.birthDate.toString
      // Here we choose to represent the date as a string.
      birthDateAttrBuilder.setValue(
        Value.newBuilder()
          .setString(ValueString.newBuilder().setV(birthDateStr).build())
      )
    }

    // Add the other fields to the record.
    recordBuilder.addAtts(teamAttrBuilder)
    recordBuilder.addAtts(jerseyAttrBuilder)
    recordBuilder.addAtts(birthDateAttrBuilder)

    // Build and return the final Value record.
    Value.newBuilder().setRecord(recordBuilder.build()).build()
  }

  /**
   * The function definition.
   * This declares that the function "player_info" takes one parameter "name" (text)
   * and returns a record with fields:
   *   - name         (text, non-nullable)
   *   - team         (text)
   *   - jersey_number (integer)
   *   - birth_date   (date)
   */
  def definition: FunctionDefinition = {
    // Build the record type.
    val rowType = Type.newBuilder()
      .setRecord(
        RecordType.newBuilder()
          .addAtts(
            AttrType.newBuilder()
              .setName("name")
              .setTipe(
                Type.newBuilder().setString(
                  StringType.newBuilder().setNullable(false).build()
                )
              )
          )
          .addAtts(
            AttrType.newBuilder()
              .setName("team")
              .setTipe(
                Type.newBuilder().setString(
                  StringType.newBuilder().setNullable(true).build()
                )
              )
          )
          .addAtts(
            AttrType.newBuilder()
              .setName("jersey_number")
              .setTipe(
                Type.newBuilder().setInt(
                  IntType.newBuilder().setNullable(true).build()
                )
              )
          )
          .addAtts(
            AttrType.newBuilder()
              .setName("birth_date")
              .setTipe(
                Type.newBuilder().setDate(
                  DateType.newBuilder().setNullable(true).build()
                )
              )
          )
          .build()
      )
      .build()

    // Build the function definition.
    FunctionDefinition.newBuilder()
      .setFunctionId(
        FunctionId.newBuilder().setName("player_info").build()
      )
      .addParams(
        ParameterDefinition.newBuilder()
          .setName("name")
          .setDescription("player name")
          .setType(
            Type.newBuilder().setString(
              StringType.newBuilder().setNullable(false).build()
            )
          )
          .build()
      )
      .setDescription("Function that returns the team, jersey number, and birth date of a player")
      .setReturnType(rowType)
      .build()
  }
}
