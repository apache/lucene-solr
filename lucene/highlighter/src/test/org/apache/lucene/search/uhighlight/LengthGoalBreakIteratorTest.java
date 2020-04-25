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
  static final String CONTENT3 = "Aa bbcc ddxyzee ffgg hh.";

  public void testFragmentAlignmentConstructor() throws IOException {
    BreakIterator baseBI = new CustomSeparatorBreakIterator('.');
    // test fragmentAlignment validation
    float[] valid_aligns = {0.f, 0.3333f, 0.5f, 0.99f, 1.f};
    for (float alignment : valid_aligns) {
      LengthGoalBreakIterator.createClosestToLength(baseBI, 50, alignment);
    }
    float[] invalid_aligns = {-0.01f, -1.f, 1.5f, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY};
    for (float alignment : invalid_aligns) {
      expectThrows(IllegalArgumentException.class, () -> {
        LengthGoalBreakIterator.createClosestToLength(baseBI, 50, alignment);
      });
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
    assertEquals("almost two sent A",
        "<b>Aa</b> bb.", highlightClosestToLen(CONTENT, query, 7, 0.f));
    assertEquals("almost two sent B",
        "<b>Aa</b> bb.", highlightClosestToLen(CONTENT, query, 15, 0.5f));
    assertEquals("almost two sent C",
        "<b>Aa</b> bb.", highlightClosestToLen(CONTENT, query, 64, 1.f));
    assertEquals("barely two sent A",
        "<b>Aa</b> bb. Cc dd.", highlightClosestToLen(CONTENT, query, 8, 0.f));
    assertEquals("barely two sent B",
        "<b>Aa</b> bb. Cc dd.", highlightClosestToLen(CONTENT, query, 16, 0.5f));
    assertEquals("long goal A",
        "<b>Aa</b> bb. Cc dd. Ee ff", highlightClosestToLen(CONTENT, query, 14 + random().nextInt(20), 0.f));
    assertEquals("long goal B",
        "<b>Aa</b> bb. Cc dd. Ee ff", highlightClosestToLen(CONTENT, query, 28 + random().nextInt(20), 0.5f));
    // at some word not at start of passage
    query = query("dd");
    for (float align : ALIGNS) {
      // alignment is not meaningful if fragsize is shorter than or closer to match-fragment boundaries
      assertEquals("short goal " + align,
          " Cc <b>dd</b>.", highlightClosestToLen(CONTENT, query, random().nextInt(4), align));
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
        "Aa bb. Cc <b>dd</b>.", highlightClosestToLen(CONTENT, query, 17 + random().nextInt(20), 1.f));

    query = query("ddxyzee");
    assertEquals("test fragment search from the middle of the match; almost including",
        "<b>ddxyzee</b> ", highlightClosestToLen(CONTENT3, query, 7, 0.5f, 1, ' '));
    assertEquals("test fragment search from the middle of the match; barely including",
        "bbcc <b>ddxyzee</b> ffgg ", highlightClosestToLen(CONTENT3, query, 14, 0.5f, 1, ' '));
  }

  public void testMinLen() throws IOException {
    // minLen mode is simpler than targetLen... just test a few cases

    Query query = query("dd");
    assertEquals("almost two sent A",
        " Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 0, 0.f));
    assertEquals("almost two sent B",
        " Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 1, 0.5f));
    assertEquals("almost two sent C",
        " Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 5, 1.f));

    assertEquals("barely two sent A",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 1, 0.f));
    assertEquals("barely two sent B",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 2, 0.5f));
    assertEquals("barely two sent C",
        "Aa bb. Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 7, 1.f));
    assertEquals("barely two sent D/a",
        " Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 2, 0.55f));
    assertEquals("barely two sent D/b",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 3, 0.55f));
    assertEquals("barely two sent E/a",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 10, 0.5f));
    assertEquals("barely two sent E/b",
        "Aa bb. Cc <b>dd</b>. Ee ff", highlightMinLen(CONTENT, query, 10, 0.7f));
    assertEquals("barely two sent E/c",
        "Aa bb. Cc <b>dd</b>.", highlightMinLen(CONTENT, query, 9, 0.9f));

    query = query("ddxyzee");
    assertEquals("test fragment search from the middle of the match; almost including",
        "<b>ddxyzee</b> ", highlightMinLen(CONTENT3, query, 7, 0.5f, ' '));
    assertEquals("test fragment search from the middle of the match; barely including",
        "bbcc <b>ddxyzee</b> ffgg ", highlightMinLen(CONTENT3, query, 8, 0.5f, ' '));
  }

  public void testMinLenPrecision() throws IOException {
    Query query = query("x");
    assertEquals("test absolute minimal length",
        "<b>X</b> ", highlightMinLen(CONTENT2, query, 1, 0.5f, ' '));
    assertEquals("test slightly above minimal length",
        "dd <b>X</b> Ee ", highlightMinLen(CONTENT2, query, 4, 0.5f, ' '));
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
    return highlightClosestToLen(content, query, lengthGoal, fragAlign, maxPassages, '.');
  }

  private String highlightClosestToLen(String content, Query query, int lengthGoal, float fragAlign, int maxPassages, char separator) throws IOException {
    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
    highlighter.setBreakIterator(() -> LengthGoalBreakIterator.createClosestToLength(new CustomSeparatorBreakIterator(separator), lengthGoal, fragAlign));
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