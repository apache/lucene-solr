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

import static org.apache.lucene.analysis.hunspell.AffixCondition.ALWAYS_FALSE;
import static org.apache.lucene.analysis.hunspell.AffixCondition.ALWAYS_TRUE_KEY;
import static org.apache.lucene.analysis.hunspell.AffixKind.PREFIX;
import static org.apache.lucene.analysis.hunspell.AffixKind.SUFFIX;

import org.apache.lucene.util.LuceneTestCase;

public class TestAffixCondition extends LuceneTestCase {

  public void testPlainSuffixMatching() {
    AffixCondition condition = AffixCondition.compile(SUFFIX, "b", "ab", "");
    assertTrue(condition.acceptsStem("a"));
    assertFalse(condition.acceptsStem("b"));
    assertFalse(condition.acceptsStem("ab"));
  }

  public void testPlainPrefixMatching() {
    AffixCondition condition = AffixCondition.compile(PREFIX, "a", "ab", "");
    assertFalse(condition.acceptsStem("ab"));
    assertTrue(condition.acceptsStem("b"));
    assertFalse(condition.acceptsStem("a"));
  }

  public void testDotMatching() {
    AffixCondition condition = AffixCondition.compile(PREFIX, "", "wr.", "");
    assertTrue(condition.acceptsStem("wry"));
    assertTrue(condition.acceptsStem("wrong"));
    assertFalse(condition.acceptsStem("white"));
  }

  public void testUniqueKey() {
    assertNotEquals(
        AffixCondition.uniqueKey(PREFIX, "", "x"), AffixCondition.uniqueKey(SUFFIX, "", "x"));
    assertNotEquals(
        AffixCondition.uniqueKey(SUFFIX, "y", "x"), AffixCondition.uniqueKey(SUFFIX, "", "x"));
    assertEquals(ALWAYS_TRUE_KEY, AffixCondition.uniqueKey(PREFIX, "", "."));
    assertEquals(ALWAYS_TRUE_KEY, AffixCondition.uniqueKey(SUFFIX, "abc", "abc"));

    assertEquals(ALWAYS_TRUE_KEY, AffixCondition.uniqueKey(SUFFIX, "abc", "bc"));
    assertEquals(ALWAYS_TRUE_KEY, AffixCondition.uniqueKey(PREFIX, "abc", "ab"));
  }

  public void testConditionHasBracketsIntersectingWithStrip() {
    assertTrue(AffixCondition.compile(SUFFIX, "oj", "[io]j", "").acceptsStem("whatever"));
    assertTrue(AffixCondition.compile(SUFFIX, "oj", "o[ioj", "").acceptsStem("whatever"));
  }

  public void testImpossibleCondition() {
    assertEquals(ALWAYS_FALSE, AffixCondition.compile(SUFFIX, "a", "b", ""));
  }

  public void testNonHunspellPatternCharacters() {
    assertEquals(ALWAYS_FALSE, AffixCondition.compile(SUFFIX, "x", "(^ax)", ""));
    assertEquals(ALWAYS_FALSE, AffixCondition.compile(SUFFIX, "x", "(^.x)", ""));
    assertEquals(ALWAYS_FALSE, AffixCondition.compile(SUFFIX, "x", "[z](^ax)", ""));
    assertEquals(ALWAYS_FALSE, AffixCondition.compile(SUFFIX, "x", "(^ax)[z]", ""));
  }
}
