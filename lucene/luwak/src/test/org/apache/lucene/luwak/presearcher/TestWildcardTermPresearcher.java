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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.matchers.SimpleMatcher;

public class TestWildcardTermPresearcher extends PresearcherTestBase {

  public void testFiltersWildcards() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("/hell.*/")));
      assertEquals(1,
          monitor.match(buildDoc("doc1", TEXTFIELD, "well hello there"), SimpleMatcher.FACTORY).getMatchCount("doc1"));

    }
  }

  public void testNgramsOnlyMatchWildcards() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("hello")));
      assertEquals(0, monitor.match(buildDoc("doc1", TEXTFIELD, "hellopolis"), SimpleMatcher.FACTORY).getQueriesRun());
    }
  }

  private static String repeat(String input, int size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      sb.append(input);
    }
    return sb.toString();
  }

  public void testLongTermsStillMatchWildcards() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("/a.*/")));

      InputDocument doc1 = InputDocument.builder("doc1")
          .addField(TEXTFIELD, repeat("a", WildcardNGramPresearcherComponent.DEFAULT_MAX_TOKEN_SIZE + 1), new KeywordAnalyzer())
          .build();

      Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertNotNull(matches.matches("1", "doc1"));
    }

  }

  public void testCaseSensitivity() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("foo")));
      assertEquals(1,
          monitor.match(buildDoc("doc1", TEXTFIELD, "Foo foo"), SimpleMatcher.FACTORY).getMatchCount("doc1"));
    }
  }

  @Override
  protected Presearcher createPresearcher() {
    return new TermFilteredPresearcher(new WildcardNGramPresearcherComponent());
  }

}
