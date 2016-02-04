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

public class TestAlternateCasing extends StemmerTestBase {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    init("alternate-casing.aff", "alternate-casing.dic");
  }
  
  public void testPossibilities() {
    assertStemsTo("drink",   "drink");
    assertStemsTo("DRİNK",   "drink");
    assertStemsTo("DRINK");
    assertStemsTo("drinki",  "drink");
    assertStemsTo("DRİNKİ",  "drink");
    assertStemsTo("DRİNKI");
    assertStemsTo("DRINKI");
    assertStemsTo("DRINKİ");
    assertStemsTo("idrink",  "drink");
    assertStemsTo("İDRİNK",  "drink");
    assertStemsTo("IDRİNK");
    assertStemsTo("IDRINK");
    assertStemsTo("İDRINK");
    assertStemsTo("idrinki", "drink");
    assertStemsTo("İDRİNKİ", "drink");
    assertStemsTo("rıver",   "rıver");
    assertStemsTo("RIVER",   "rıver");
    assertStemsTo("RİVER");
    assertStemsTo("rıverı",  "rıver");
    assertStemsTo("RIVERI",  "rıver");
    assertStemsTo("RİVERI");
    assertStemsTo("RİVERİ");
    assertStemsTo("RIVERİ");
    assertStemsTo("ırıver",  "rıver");
    assertStemsTo("IRIVER",  "rıver");
    assertStemsTo("IRİVER");
    assertStemsTo("İRİVER");
    assertStemsTo("İRIVER");
    assertStemsTo("ırıverı",  "rıver");
    assertStemsTo("IRIVERI",  "rıver");
    assertStemsTo("Irıverı",  "rıver");
  }
}
