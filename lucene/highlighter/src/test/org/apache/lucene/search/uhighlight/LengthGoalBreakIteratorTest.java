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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.postingshighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QueryBuilder;

public class LengthGoalBreakIteratorTest extends LuceneTestCase {
  private static final String FIELD = "body";

  // We test LengthGoalBreakIterator as it is used by the UnifiedHighlighter instead of directly, because it is
  //  not a general purpose BreakIterator.  A unit test of it directly wouldn't give as much confidence.

  private final Analyzer analyzer =
      new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase

  // We do a '.' BreakIterator and test varying the length goal.
  //                      0         1
  //                      01234567890123456789
  final String content = "Aa bb. Cc dd. Ee ff";

  public void testTargetLen() throws IOException {
    // "goal" means target length goal to find closest break

    // at first word:
    Query query = query("aa");
    assertEquals("almost two sent",
        "<b>Aa</b> bb.", highlightClosestToLen(content, query, 9));
    assertEquals( "barely two sent",
        "<b>Aa</b> bb. Cc dd.", highlightClosestToLen(content, query, 10));
    assertEquals("long goal",
        "<b>Aa</b> bb. Cc dd. Ee ff", highlightClosestToLen(content, query, 17 + random().nextInt(20)));

    // at some word not at start of passage
    query = query("dd");
    assertEquals("short goal",
        " Cc <b>dd</b>.", highlightClosestToLen(content, query, random().nextInt(5)));
    assertEquals("almost two sent",
        " Cc <b>dd</b>.", highlightClosestToLen(content, query, 10));
    assertEquals("barely two sent",
        " Cc <b>dd</b>. Ee ff", highlightClosestToLen(content, query, 11));
    assertEquals("long goal",
        " Cc <b>dd</b>. Ee ff", highlightClosestToLen(content, query, 12 + random().nextInt(20)));
  }

  public void testMinLen() throws IOException {
    // minLen mode is simpler than targetLen... just test a few cases

    Query query = query("dd");
    assertEquals("almost two sent",
        " Cc <b>dd</b>.", highlightMinLen(content, query, 6));
    assertEquals("barely two sent",
        " Cc <b>dd</b>. Ee ff", highlightMinLen(content, query, 7));
  }

  public void testDefaultSummaryTargetLen() throws IOException {
    Query query = query("zz");
    assertEquals("Aa bb.",
        highlightClosestToLen(content, query, random().nextInt(10))); // < 10
    assertEquals("Aa bb. Cc dd.",
        highlightClosestToLen(content, query, 10 + 6)); // cusp of adding 3rd sentence
    assertEquals("Aa bb. Cc dd. Ee ff",
        highlightClosestToLen(content, query, 17 + random().nextInt(20))); // >= 14
  }

  private Query query(String qStr) {
    return new QueryBuilder(analyzer).createBooleanQuery(FIELD, qStr);
  }

  private String highlightClosestToLen(String content, Query query, int lengthGoal) throws IOException {
    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
    highlighter.setBreakIterator(() -> LengthGoalBreakIterator.createClosestToLength(new CustomSeparatorBreakIterator('.'), lengthGoal));
    return highlighter.highlightWithoutSearcher(FIELD, query, content, 1).toString();
  }

  private String highlightMinLen(String content, Query query, int lengthGoal) throws IOException {
    // differs from above only by "createMinLength"
    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, analyzer);
    highlighter.setBreakIterator(() -> LengthGoalBreakIterator.createMinLength(new CustomSeparatorBreakIterator('.'), lengthGoal));
    return highlighter.highlightWithoutSearcher(FIELD, query, content, 1).toString();
  }
}