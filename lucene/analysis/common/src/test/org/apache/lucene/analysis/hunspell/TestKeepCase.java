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

public class TestKeepCase extends StemmerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    init("keepcase.aff", "keepcase.dic");
  }

  public void testPossibilities() {
    assertStemsTo("drink", "drink");
    assertStemsTo("Drink", "drink");
    assertStemsTo("DRINK", "drink");
    assertStemsTo("drinks", "drink");
    assertStemsTo("Drinks", "drink");
    assertStemsTo("DRINKS", "drink");
    assertStemsTo("walk", "walk");
    assertStemsTo("walks", "walk");
    assertStemsTo("Walk");
    assertStemsTo("Walks");
    assertStemsTo("WALKS");
    assertStemsTo("test", "test");
    assertStemsTo("Test");
    assertStemsTo("TEST");

    assertStemsTo("baz.", "baz.");
    assertStemsTo("Baz.");

    assertStemsTo("Quux.", "Quux.");
    assertStemsTo("QUUX.");
  }
}
