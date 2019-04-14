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

package org.apache.lucene.luwak;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.LuceneTestCase;

public class TestMonitor extends LuceneTestCase {

  private static final String TEXTFIELD = "TEXTFIELD";

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  private Monitor newMonitor() throws IOException {
    return new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher());
  }

  public void testSingleTermQueryMatchesSingleDocument() throws IOException, UpdateException {

    String document = "This is a test document";

    DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
        .addField(TEXTFIELD, document, ANALYZER)
        .build());

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "test"));

      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertNotNull(matches.getMatches("doc1"));
      assertEquals(1, matches.getMatchCount("doc1"));
      assertNotNull(matches.matches("query1", "doc1"));
    }
  }

  public void testMatchStatisticsAreReported() throws IOException, UpdateException {
    String document = "This is a test document";
    DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
        .addField(TEXTFIELD, document, ANALYZER)
        .build());

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "test"));

      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertTrue(matches.getQueryBuildTime() > -1);
      assertTrue(matches.getSearchTime() > -1);
    }
  }

  public void testUpdatesOverwriteOldQueries() throws IOException, UpdateException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "this"));

      monitor.update(new MonitorQuery("query1", "that"));

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(TEXTFIELD, "that", ANALYZER).build());
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertNotNull(matches.matches("query1", "doc1"));
      assertEquals(1, matches.getQueriesRun());
    }
  }

  public void testCanDeleteById() throws IOException, UpdateException {

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "this"));
      monitor.update(new MonitorQuery("query2", "that"), new MonitorQuery("query3", "other"));
      assertEquals(3, monitor.getQueryCount());

      monitor.deleteById("query2", "query1");
      assertEquals(1, monitor.getQueryCount());

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(TEXTFIELD, "other things", ANALYZER).build());
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertNotNull(matches.matches("query3", "doc1"));
    }

  }

  public void testCanRetrieveQuery() throws IOException, UpdateException {

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("query1", "this"), new MonitorQuery("query2", "that"));
      assertEquals(2, monitor.getQueryCount());
      Set<String> expected = new HashSet<>(Arrays.asList("query1", "query2"));
      assertEquals(expected, monitor.getQueryIds());

      MonitorQuery mq = monitor.getQuery("query2");
      assertEquals(new MonitorQuery("query2", "that"), mq);
    }

  }

  public void testCanClearTheMonitor() throws IOException, UpdateException {
    try (Monitor monitor = newMonitor()) {
      monitor.update(
          new MonitorQuery("query1", "a"),
          new MonitorQuery("query2", "b"),
          new MonitorQuery("query3", "c"));
      assertEquals(3, monitor.getQueryCount());

      monitor.clear();
      assertEquals(0, monitor.getQueryCount());
    }
  }

  public void testMatchesAgainstAnEmptyMonitor() throws IOException {

    try (Monitor monitor = newMonitor()) {
      assertEquals(0, monitor.getQueryCount());

      InputDocument doc = InputDocument.builder("doc1").addField(TEXTFIELD, "other things", ANALYZER).build();
      Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(0, matches.getQueriesRun());
    }

  }

  public void testUpdateReporting() throws IOException, UpdateException {

    List<MonitorQuery> queries = new ArrayList<>(10400);
    for (int i = 0; i < 10355; i++) {
      queries.add(new MonitorQuery(Integer.toString(i), "test"));
    }

    final int[] expectedSizes = new int[]{5001, 5001, 353};
    final AtomicInteger callCount = new AtomicInteger();
    final AtomicInteger updateCount = new AtomicInteger();

    QueryIndexUpdateListener listener = new QueryIndexUpdateListener() {

      @Override
      public void afterUpdate(List<Indexable> updates) {
        int calls = callCount.getAndIncrement();
        updateCount.addAndGet(updates.size());
        assertEquals(expectedSizes[calls], updates.size());
      }
    };

    try (Monitor monitor = new Monitor(new LuceneQueryParser(TEXTFIELD, ANALYZER), new MatchAllPresearcher())) {
      monitor.addQueryIndexUpdateListener(listener);
      monitor.update(queries);
      assertEquals(10355, updateCount.get());
    }
  }

  public void testMatcherMetadata() throws IOException, UpdateException {
    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
      HashMap<String, String> metadataMap = new HashMap<>();
      metadataMap.put("key", "value");

      monitor.update(new MonitorQuery(Integer.toString(1), "+test " + 1, metadataMap));

      InputDocument doc = InputDocument.builder("1").addField("field", "test", ANALYZER).build();

      MatcherFactory<QueryMatch> testMatcherFactory = docs -> new CandidateMatcher<QueryMatch>(docs) {
        @Override
        protected void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
          assertEquals("value", metadata.get("key"));
        }

        @Override
        public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
          return null;
        }
      };

      monitor.match(doc, testMatcherFactory);
    }
  }

  public void testDocumentBatching() throws IOException, UpdateException {

    DocumentBatch batch = DocumentBatch.of(
        InputDocument.builder("doc1").addField(TEXTFIELD, "this is a test", ANALYZER).build(),
        InputDocument.builder("doc2").addField(TEXTFIELD, "this is a kangaroo", ANALYZER).build()
    );

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", "kangaroo"));

      Matches<QueryMatch> response = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(2, response.getBatchSize());
    }
  }

  public void testMutliValuedFieldWithNonDefaultGaps() throws IOException, UpdateException {

    Analyzer analyzer = new Analyzer() {
      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 1000;
      }

      @Override
      public int getOffsetGap(String fieldName) {
        return 2000;
      }

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new WhitespaceTokenizer());
      }
    };

    MonitorQuery mq = new MonitorQuery("query", TEXTFIELD + ":\"hello world\"~5");

    try (Monitor monitor = newMonitor()) {
      monitor.update(mq);

      InputDocument doc1 = InputDocument.builder("doc1")
          .addField(TEXTFIELD, "hello world", analyzer)
          .addField(TEXTFIELD, "goodbye", analyzer)
          .build();
      Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
      assertNotNull(matches.getMatches("doc1"));
      assertEquals(1, matches.getMatchCount("doc1"));
      assertNotNull(matches.matches("query", "doc1"));

      InputDocument doc2 = InputDocument.builder("doc2")
          .addField(TEXTFIELD, "hello", analyzer)
          .addField(TEXTFIELD, "world", analyzer)
          .build();
      matches = monitor.match(doc2, SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc2"));
    }

  }

}
