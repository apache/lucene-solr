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

public class TestCaseInsensitive extends StemmerTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    init(true, "simple.aff", "mixedcase.dic");
  }

  public void testCaseInsensitivity() {
    assertStemsTo("lucene", "lucene", "lucen");
    assertStemsTo("LuCeNe", "lucene", "lucen");
    assertStemsTo("mahoute", "mahout");
    assertStemsTo("MaHoUte", "mahout");
  }

  public void testSimplePrefix() {
    assertStemsTo("solr", "olr");
  }

  public void testRecursiveSuffix() {
    // we should not recurse here! as the suffix has no continuation!
    assertStemsTo("abcd");
  }

  // all forms unmunched from dictionary
  public void testAllStems() {
    assertStemsTo("ab", "ab");
    assertStemsTo("abc", "ab");
    assertStemsTo("apach", "apach");
    assertStemsTo("apache", "apach");
    assertStemsTo("foo", "foo", "foo");
    assertStemsTo("food", "foo");
    assertStemsTo("foos", "foo");
    assertStemsTo("lucen", "lucen");
    assertStemsTo("lucene", "lucen", "lucene");
    assertStemsTo("mahout", "mahout");
    assertStemsTo("mahoute", "mahout");
    assertStemsTo("moo", "moo");
    assertStemsTo("mood", "moo");
    assertStemsTo("olr", "olr");
    assertStemsTo("solr", "olr");
  }
  
  // some bogus stuff that should not stem (empty lists)!
  public void testBogusStems() {    
    assertStemsTo("abs");
    assertStemsTo("abe");
    assertStemsTo("sab");
    assertStemsTo("sapach");
    assertStemsTo("sapache");
    assertStemsTo("apachee");
    assertStemsTo("sfoo");
    assertStemsTo("sfoos");
    assertStemsTo("fooss");
    assertStemsTo("lucenee");
    assertStemsTo("solre");
  }
}
