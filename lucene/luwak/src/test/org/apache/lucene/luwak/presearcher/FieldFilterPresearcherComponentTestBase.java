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
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.search.MatchAllDocsQuery;

import static org.hamcrest.CoreMatchers.containsString;

public abstract class FieldFilterPresearcherComponentTestBase extends PresearcherTestBase {

  private static final Analyzer ANALYZER = new StandardAnalyzer();

  public void testBatchFiltering() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(
          mq("1", "test", "language", "en"),
          mq("2", "wahl", "language", "de"),
          mq("3", "wibble", "language", "en"),
          mq("4", "*:*", "language", "de"),
          mq("5", "*:*", "language", "es"));

      DocumentBatch enBatch = DocumentBatch.of(
          InputDocument.builder("en1")
              .addField(TEXTFIELD, "this is a test", ANALYZER)
              .addField("language", "en", ANALYZER)
              .build(),
          InputDocument.builder("en2")
              .addField(TEXTFIELD, "this is a wibble", ANALYZER)
              .addField("language", "en", ANALYZER)
              .build(),
          InputDocument.builder("en3")
              .addField(TEXTFIELD, "wahl is a misspelling of whale", ANALYZER)
              .addField("language", "en", ANALYZER)
              .build()
      );

      Matches<QueryMatch> matches = monitor.match(enBatch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("en1"));
      assertNotNull(matches.matches("1", "en1"));
      assertEquals(1, matches.getMatchCount("en2"));
      assertNotNull(matches.matches("3", "en2"));
      assertEquals(0, matches.getMatchCount("en3"));
      assertEquals(2, matches.getQueriesRun());
    }
  }

  public void testBatchesWithDissimilarFieldValuesThrowExceptions() throws IOException {

    DocumentBatch batch = DocumentBatch.of(
        InputDocument.builder("1").addField(TEXTFIELD, "test", ANALYZER).addField("language", "en", ANALYZER).build(),
        InputDocument.builder("2").addField(TEXTFIELD, "test", ANALYZER).addField("language", "de", ANALYZER).build()
    );

    try (Monitor monitor = newMonitor()) {
      IllegalArgumentException e
          = expectThrows(IllegalArgumentException.class, () -> monitor.match(batch, SimpleMatcher.FACTORY));
      assertThat(e.getMessage(), containsString("language:"));
    }
  }

  public void testFieldFiltering() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(
          new MonitorQuery("1", parse("test"), null, Collections.singletonMap("language", "en")),
          new MonitorQuery("2", parse("test"), null, Collections.singletonMap("language", "de")),
          new MonitorQuery("3", parse("wibble"), null, Collections.singletonMap("language", "en")),
          new MonitorQuery("4", parse("*:*"), null, Collections.singletonMap("language", "de")));

      InputDocument enDoc = InputDocument.builder("enDoc")
          .addField(TEXTFIELD, "this is a test", ANALYZER)
          .addField("language", "en", ANALYZER)
          .build();

      Matches<QueryMatch> en = monitor.match(enDoc, SimpleMatcher.FACTORY);
      assertEquals(1, en.getMatchCount("enDoc"));
      assertNotNull(en.matches("1", "enDoc"));
      assertEquals(1, en.getQueriesRun());

      InputDocument deDoc = InputDocument.builder("deDoc")
          .addField(TEXTFIELD, "das ist ein test", ANALYZER)
          .addField("language", "de", ANALYZER)
          .build();
      Matches<QueryMatch> de = monitor.match(deDoc, SimpleMatcher.FACTORY);
      assertEquals(2, de.getMatchCount("deDoc"));
      assertEquals(2, de.getQueriesRun());
      assertNotNull(de.matches("2", "deDoc"));
      assertNotNull(de.matches("4", "deDoc"));

      InputDocument bothDoc = InputDocument.builder("bothDoc")
          .addField(TEXTFIELD, "this is ein test", ANALYZER)
          .addField("language", "en", ANALYZER)
          .addField("language", "de", ANALYZER)
          .build();
      Matches<QueryMatch> both = monitor.match(bothDoc, SimpleMatcher.FACTORY);
      assertEquals(3, both.getMatchCount("bothDoc"));
      assertEquals(3, both.getQueriesRun());
    }
  }

  public void testFilteringOnMatchAllQueries() throws IOException {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", new MatchAllDocsQuery(), null, Collections.singletonMap("language", "de")));

      InputDocument doc = InputDocument.builder("enDoc")
          .addField(TEXTFIELD, "this is a test", ANALYZER)
          .addField("language", "en", ANALYZER)
          .build();
      Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("enDoc"));
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testDebugQueries() throws Exception {
    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("test"), null, Collections.singletonMap("language", "en")));

      InputDocument doc = InputDocument.builder("enDoc")
          .addField(TEXTFIELD, "this is a test", ANALYZER)
          .addField("language", "en", ANALYZER)
          .build();

      PresearcherMatches<QueryMatch> matches = monitor.debug(doc, SimpleMatcher.FACTORY);
      assertFalse(matches.match("1", "enDoc").presearcherMatches.isEmpty());
    }
  }

}
