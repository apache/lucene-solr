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
  }
}
