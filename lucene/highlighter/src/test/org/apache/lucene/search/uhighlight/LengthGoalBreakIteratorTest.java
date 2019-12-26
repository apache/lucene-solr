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

package org.apache.lucene.search.uhighlight;

import java.io.IOException;
import java.text.BreakIterator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QueryBuilder;
import org.junit.Assert;

public class LengthGoalBreakIteratorTest extends LuceneTestCase {
  private static final String FIELD = "body";
  private static final float[] ALIGNS = {0.f, 0.5f, 1.f};

  // We test LengthGoalBreakIterator as it is used by the UnifiedHighlighter instead of directly, because it is
  //  not a general purpose BreakIterator.  A unit test of it directly wouldn't give as much confidence.

  private final Analyzer analyzer =
      new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase

  // We do a '.' BreakIterator and test varying the length goal.
  //                      0         1
  //                      01234567890123456789
  static final String CONTENT = "Aa bb. Cc dd. Ee ff";
  static final String CONTENT2 = "Aa bb Cc dd X Ee ff Gg hh.";

  public void testFragmentAlignmentConstructor() throws IOException {
    BreakIterator baseBI = new CustomSeparatorBreakIterator('.');
    // test fragmentAlignment validation
    float[] valid_aligns = {0.f, 0.3333f, 0.5f, 0.99f, 1.f};
    for (float alignment : valid_aligns) {
      LengthGoalBreakIterator.createClosestToLength(baseBI, 50, alignment);
    }
    float[] invalid_aligns = {-0.01f, -1.f, 1.5f, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY};
    for (float alignment : invalid_aligns) {
      try {
        LengthGoalBreakIterator.createClosestToLength(baseBI, 50, alignment);
        Assert.fail("Expected IllegalArgumentException for "+alignment);
      } catch (IllegalArgumentException e) {
        if (!e.getMessage().contains("fragmentAlignment")) {
          throw e;
        }
      }
    }
    // test backwards compatibility constructors
    String backwardCompString = LengthGoalBreakIterator.createClosestToLength(baseBI, 50).toString();
    assertTrue(backwardCompString, backwardCompString.contains("fragAlign=0.0"));
    backwardCompString = LengthGoalBreakIterator.createMinLength(baseBI, 50).toString();
    assertTrue(backwardCompString, backwardCompString.contains("fragAlign=0.0"));
  }

  public void testTargetLen() throws IOException {
    // "goal" means target length goal to find closest break

    // at first word:
    Query query = query("aa");
    for (float align : ALIGNS) { // alignment is not meaningful to boundary anchored matches
      assertEquals("almost two sent " + align,
          "<b>Aa</b> bb.", highlightClosestToLen(CONTENT, query, 9, align));
      assertEquals("barely two sent " + align,
          "<b>Aa</b> bb. Cc dd.", highlightClosestToLen(CONTENT, query, 11, align));
      assertEquals("long goal " + align,
          "<b>Aa</b> bb. Cc dd. Ee ff", highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), align));
    }

    // at some word not at start of passage
    query = query("dd");
    for (float align : ALIGNS) {
      // alignment is not meaningful when lengthGoal is less than match-fragment
      assertEquals("short goal " + align,
          " Cc <b>dd</b>.", highlightClosestToLen(CONTENT, query, random().nextInt(5), align));
      // alignment is not meaningful when target indexes are closer to than match-fragment than prev-next fragments
      assertEquals("almost two sent " + align,
          " Cc <b>dd</b>.", highlightClosestToLen(CONTENT, query, 9, align));
    }
    // preceding/following inclusion by alignment parameter
    assertEquals("barely two sent A",
        " Cc <b>dd</b>. Ee ff", highlightClosestToLen(CONTENT, query, 11, 0.f));
    assertEquals("barely two sent B",
        " Cc <b>dd</b>. Ee ff", highlightClosestToLen(CONTENT, query, 11, 0.5f));
    assertEquals("barely two sent C",
        "Aa bb. Cc <b>dd</b>.", highlightClosestToLen(CONTENT, query, 11, 1.f));
    assertEquals("long goal A",
        " Cc <b>dd</b>. Ee ff", highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), 0.f));
    assertEquals("long goal B",
        "Aa bb. Cc <b>dd</b>. Ee ff", highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), 0.5f));
    assertEquals("long goal C",
        "Aa bb. Cc <b>dd</b>. Ee ff", highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), 1.f));
  }

  public void testMinLen() throws IOException {
    // minLen mode is simpler than targetLen... just test a few cases

    Query query = query("dd");
    for (float align : ALIGNS) { // alignment is not meaningful when lengthGoal is less or equals than match-fragment
      assertEquals("almost two sent",
          " Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 7, align));
    }
    assertEquals("barely two sent A",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 8, 0.f));
    assertEquals("barely two sent B",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 8, 0.5f));
    assertEquals("barely two sent C",
        "Aa bb. Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 8, 1.f));
    assertEquals("barely two sent D",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 8, 0.55f));
    assertEquals("barely two sent D",
        "Aa bb. Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 9, 0.55f));
    assertEquals("barely two sent D",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 9, 0.45f));
  }

  public void testMinLenPrecision() throws IOException {
    Query queryX = query("x");
    assertEquals("test absolute minimal length",
        "<b>X</b> ", highlightMinLen(CONTENT2, queryX, 1, 0.f, ' '));
    assertEquals("test slightly above minimal length",
        "<b>X</b> Ee ", highlightMinLen(CONTENT2, queryX, 3, 0.f, ' '));
  }

  public void testDefaultSummaryTargetLen() throws IOException {
    Query query = query("zz");
    for (float align : ALIGNS) { // alignment is not used for creating default-summary
      assertEquals("Aa bb.",
          highlightClosestToLen(CONTENT, query, 6 + random().nextInt(4), align));
      assertEquals("Aa bb. Cc dd.",
          highlightClosestToLen(CONTENT, query, 12 + random().nextInt(4), align));
      assertEquals("Aa bb. Cc dd. Ee ff",
          highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), align));
    }
    assertEquals("Aa bb. Cc dd.",
        highlightClosestToLen(CONTENT, query, 6 + random().nextInt(4), 0.f, 2));
  }

  private Query query(String qStr) {
    return new QueryBuilder(analyzer).createBooleanQuery(FIELD, qStr);
  }

  private String highlightClosestToLen(String content, Query query, int lengthGoal, float fragAlign) throws IOException {
    return highlightClosestToLen(content, query, lengthGoal, fragAlign, 1);
  }

  private String highlightClosestToLen(String content, Query query, int lengthGoal, float fragAlign, int maxPassages) throws IOException {
    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
    highlighter.setBreakIterator(() -> LengthGoalBreakIterator.createClosestToLength(new CustomSeparatorBreakIterator('.'), lengthGoal, fragAlign));
    return highlighter.highlightWithoutSearcher(FIELD, query, content, maxPassages).toString();
  }

  private String highlightMinLen(String content, Query query, int lengthGoal, float fragAlign) throws IOException {
    return highlightMinLen(content, query, lengthGoal, fragAlign, '.');
  }

  private String highlightMinLen(String content, Query query, int lengthGoal, float fragAlign, char separator) throws IOException {
    // differs from above only by "createMinLength"
    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
    highlighter.setBreakIterator(() -> LengthGoalBreakIterator.createMinLength(new CustomSeparatorBreakIterator(separator), lengthGoal, fragAlign));
    return highlighter.highlightWithoutSearcher(FIELD, query, content, 1).toString();
  }
}