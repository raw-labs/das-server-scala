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

class RecipeListOfTypedRecordsFunction extends DASMockFunction {

  // Define an Ingredient case class.
  private case class Ingredient(name: String, quantity: String) {
    def toValue: Value = {
      Value
        .newBuilder()
        .setRecord(
          ValueRecord
            .newBuilder()
            .addAtts(
              ValueRecordAttr
                .newBuilder()
                .setName("name")
                .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV(name).build()).build())
                .build())
            .addAtts(ValueRecordAttr
              .newBuilder()
              .setName("quantity")
              .setValue(Value.newBuilder().setString(ValueString.newBuilder().setV(quantity).build()).build())
              .build())
            .build())
        .build()
    }

  }

  // Map dish names to a list of ingredients.
  private val dishesAndIngredients: Map[String, List[Ingredient]] = Map(
    "Spaghetti Bolognese" -> List(
      Ingredient("Spaghetti", "200g"),
      Ingredient("Minced Beef", "150g"),
      Ingredient("Tomato Sauce", "100ml"),
      Ingredient("Onion", "1 medium"),
      Ingredient("Garlic", "2 cloves")),
    "Caesar Salad" -> List(
      Ingredient("Romaine Lettuce", "1 head"),
      Ingredient("Croutons", "50g"),
      Ingredient("Parmesan Cheese", "30g"),
      Ingredient("Caesar Dressing", "100ml"),
      Ingredient("Anchovies", "optional")),
    "Margherita Pizza" -> List(
      Ingredient("Pizza Dough", "1 base"),
      Ingredient("Tomato Sauce", "100ml"),
      Ingredient("Mozzarella", "150g"),
      Ingredient("Basil Leaves", "a handful"),
      Ingredient("Olive Oil", "2 tbsp")),
    "Tacos" -> List(
      Ingredient("Tortillas", "4 pieces"),
      Ingredient("Chicken", "200g"),
      Ingredient("Lettuce", "50g"),
      Ingredient("Cheddar Cheese", "50g"),
      Ingredient("Salsa", "50ml"),
      Ingredient("Sour Cream", "30ml"),
      Ingredient("Avocado", "1, sliced")))

  def execute(args: Map[String, Value], maybeEnv: Option[Environment]): Value = {
    if (!args("dish").hasString) {
      throw new DASSdkInvalidArgumentException("The dish argument must be a string")
    }
    val dish = args("dish").getString.getV
    val ingredients = dishesAndIngredients.getOrElse(dish, List.empty)
    val builder = ValueList.newBuilder()
    ingredients.foreach(p => builder.addValues(p.toValue))
    Value.newBuilder().setList(builder.build()).build()
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
              .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build()).build())
              .build())
          .addAtts(
            AttrType
              .newBuilder()
              .setName("quantity")
              .setTipe(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build()).build())
              .build()))
    FunctionDefinition
      .newBuilder()
      .setFunctionId(FunctionId.newBuilder().setName("recipe").build())
      .addParams(
        ParameterDefinition
          .newBuilder()
          .setName("dish")
          .setDescription("a dish name")
          .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(false).build())))
      .setDescription("A function that returns the recipe for a given dish")
      .setReturnType(Type.newBuilder().setList(ListType.newBuilder().setInnerType(rowType).build()))
      .build()
  }
}
