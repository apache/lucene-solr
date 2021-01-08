/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.analysis.hunspell;

import org.junit.BeforeClass;

public class TestCaseSensitive extends StemmerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    init("casesensitive.aff", "casesensitive.dic");
  }

  public void testAllPossibilities() {
    assertStemsTo("drink", "drink");
    assertStemsTo("drinks", "drink");
    assertStemsTo("drinkS", "drink");
    assertStemsTo("gooddrinks", "drink");
    assertStemsTo("Gooddrinks", "drink", "drink");
    assertStemsTo("GOODdrinks", "drink");
    assertStemsTo("gooddrinkS", "drink");
    assertStemsTo("GooddrinkS", "drink");
    assertStemsTo("gooddrink", "drink");
    assertStemsTo("Gooddrink", "drink", "drink");
    assertStemsTo("GOODdrink", "drink");
    assertStemsTo("Drink", "drink", "Drink");
    assertStemsTo("Drinks", "drink", "Drink");
    assertStemsTo("DrinkS", "Drink");
    assertStemsTo("goodDrinks", "Drink");
    assertStemsTo("GoodDrinks", "Drink");
    assertStemsTo("GOODDrinks", "Drink");
    assertStemsTo("goodDrinkS", "Drink");
    assertStemsTo("GoodDrinkS", "Drink");
    assertStemsTo("GOODDrinkS", "Drink");
    assertStemsTo("goodDrink", "Drink");
    assertStemsTo("GoodDrink", "Drink");
    assertStemsTo("GOODDrink", "Drink");
    assertStemsTo("DRINK", "DRINK", "drink", "Drink");
    assertStemsTo("DRINKs", "DRINK");
    assertStemsTo("DRINKS", "DRINK", "drink", "Drink");
    assertStemsTo("goodDRINKs", "DRINK");
    assertStemsTo("GoodDRINKs", "DRINK");
    assertStemsTo("GOODDRINKs", "DRINK");
    assertStemsTo("goodDRINKS", "DRINK");
    assertStemsTo("GoodDRINKS", "DRINK");
    assertStemsTo("GOODDRINKS", "DRINK", "drink", "drink");
    assertStemsTo("goodDRINK", "DRINK");
    assertStemsTo("GoodDRINK", "DRINK");
    assertStemsTo("GOODDRINK", "DRINK", "drink", "drink");
  }
}
