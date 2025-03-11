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

class SingleRecordFunction extends DASMockFunction {

  // A map of cities, mapped to their respective country and population
  private val cities = Map(
    "Paris" -> ("France", 2148000),
    "London" -> ("United Kingdom", 8908081),
    "Berlin" -> ("Germany", 3748148),
    "Madrid" -> ("Spain", 3223334),
    "Rome" -> ("Italy", 2872800),
    "Lisbon" -> ("Portugal", 505526),
    "Amsterdam" -> ("Netherlands", 821752),
    "Brussels" -> ("Belgium", 144784),
    "Vienna" -> ("Austria", 1897491),
    "Prague" -> ("Czech Republic", 1308573),
    "Warsaw" -> ("Poland", 1790658),
    "Budapest" -> ("Hungary", 1752286),
    "Athens" -> ("Greece", 664046),
    "Stockholm" -> ("Sweden", 975904),
    "Oslo" -> ("Norway", 673469),
    "Helsinki" -> ("Finland", 648042),
    "Copenhagen" -> ("Denmark", 602481),
    "Dublin" -> ("Ireland", 553165),
    "Reykjavik" -> ("Iceland", 123300),
    "Moscow" -> ("Russia", 12615882),
    "Istanbul" -> ("Turkey", 15462404),
    "Cairo" -> ("Egypt", 10230350),
    "Tunis" -> ("Tunisia", 638845),
    "Algiers" -> ("Algeria", 2917183),
    "Casablanca" -> ("Morocco", 3359818),
    "Dakar" -> ("Senegal", 1146053),
    "Lagos" -> ("Nigeria", 21000000),
    "Nairobi" -> ("Kenya", 4397073),
    "Cape Town" -> ("South Africa", 433688),
    "Johannesburg" -> ("South Africa", 4434827),
    "Mumbai" -> ("India", 12442373),
    "Delhi" -> ("India", 11034555),
    "Kolkata" -> ("India", 4631392),
    "Bangalore" -> ("India", 8443675))

  def execute(args: Map[String, Value]): Value = {
    val name = args("name").getString.getV
    val recordBuilder = ValueRecord.newBuilder()
    recordBuilder.addAtts(
      ValueRecordAttr
        .newBuilder()
        .setName("name")
        .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV(name).build())))
    val countryAttBuilder = ValueRecordAttr.newBuilder().setName("country")
    val populationAttBuilder = ValueRecordAttr.newBuilder().setName("population")
    cities.get(name).foreach { case (country, population) =>
      countryAttBuilder.setValue(Value.newBuilder().setString(ValueString.newBuilder().setV(country).build()))
      populationAttBuilder.setValue(Value.newBuilder().setInt(ValueInt.newBuilder().setV(population).build()))
    }
    recordBuilder.addAtts(countryAttBuilder).addAtts(populationAttBuilder)
    Value.newBuilder().setRecord(recordBuilder.build()).build()
  }

  def definition: FunctionDefinition = {
    val rowType = Type
      .newBuilder()
      .setRecord(
        RecordType
          .newBuilder()
          .addAtts(
            AttrType
              .newBuilder()
              .setName("name")
              .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
          .addAtts(AttrType
            .newBuilder()
            .setName("country")
            .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(true).build())))
          .addAtts(AttrType
            .newBuilder()
            .setName("population")
            .setTipe(Type.newBuilder().setInt(IntType.newBuilder().setNullable(true).build())))
          .build())
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("city_stat").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("name")
          .setDescription("a city")
          .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
      .setDescription("function that returns the country and population of a city")
      .setReturnType(rowType)
      .build()
  }
}
