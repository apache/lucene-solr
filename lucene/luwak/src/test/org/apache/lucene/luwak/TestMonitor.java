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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TestMonitor extends MonitorTestBase {

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  public void testSingleTermQueryMatchesSingleDocument() throws IOException {

    String document = "This is a test document";

    DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
        .addField(FIELD, document, ANALYZER)
        .build());

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(FIELD, "test"))));

      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertNotNull(matches.getMatches("doc1"));
      assertEquals(1, matches.getMatchCount("doc1"));
      assertNotNull(matches.matches("query1", "doc1"));
    }
  }

  public void testMatchStatisticsAreReported() throws IOException {
    String document = "This is a test document";
    DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1")
        .addField(MonitorTestBase.FIELD, document, ANALYZER)
        .build());

    try (Monitor monitor = new Monitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "test"))));

      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertTrue(matches.getQueryBuildTime() > -1);
      assertTrue(matches.getSearchTime() > -1);
    }
  }

  public void testUpdatesOverwriteOldQueries() throws IOException {
    try (Monitor monitor = new Monitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "this"))));
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "that"))));

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(MonitorTestBase.FIELD, "that", ANALYZER).build());
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertNotNull(matches.matches("query1", "doc1"));
      assertEquals(1, matches.getQueriesRun());
    }
  }

  public void testCanDeleteById() throws IOException {

    try (Monitor monitor = new Monitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "this"))));
      monitor.register(
          new MonitorQuery("query2", new TermQuery(new Term(MonitorTestBase.FIELD, "that"))),
          new MonitorQuery("query3", new TermQuery(new Term(MonitorTestBase.FIELD, "other"))));
      assertEquals(3, monitor.getQueryCount());

      monitor.deleteById("query2", "query1");
      assertEquals(1, monitor.getQueryCount());

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").addField(MonitorTestBase.FIELD, "other things", ANALYZER).build());
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(1, matches.getQueriesRun());
      assertNotNull(matches.matches("query3", "doc1"));
    }

  }

  public void testCanClearTheMonitor() throws IOException {
    try (Monitor monitor = new Monitor()) {
      monitor.register(
          new MonitorQuery("query1", new MatchAllDocsQuery()),
          new MonitorQuery("query2", new MatchAllDocsQuery()),
          new MonitorQuery("query3", new MatchAllDocsQuery()));
      assertEquals(3, monitor.getQueryCount());

      monitor.clear();
      assertEquals(0, monitor.getQueryCount());
    }
  }

  public void testMatchesAgainstAnEmptyMonitor() throws IOException {

    try (Monitor monitor = new Monitor()) {
      assertEquals(0, monitor.getQueryCount());

      InputDocument doc = InputDocument.builder("doc1").addField(MonitorTestBase.FIELD, "other things", ANALYZER).build();
      Matches<QueryMatch> matches = monitor.match(doc, SimpleMatcher.FACTORY);
      assertEquals(0, matches.getQueriesRun());
    }

  }

  public void testUpdateReporting() throws IOException {

    List<MonitorQuery> queries = new ArrayList<>(10400);
    for (int i = 0; i < 10355; i++) {
      queries.add(new MonitorQuery(Integer.toString(i), MonitorTestBase.parse("test")));
    }

    final int[] expectedSizes = new int[]{5001, 5001, 353};
    final AtomicInteger callCount = new AtomicInteger();
    final AtomicInteger updateCount = new AtomicInteger();

    QueryIndexUpdateListener listener = new QueryIndexUpdateListener() {

      @Override
      public void afterUpdate(List<MonitorQuery> updates) {
        int calls = callCount.getAndIncrement();
        updateCount.addAndGet(updates.size());
        assertEquals(expectedSizes[calls], updates.size());
      }
    };

    try (Monitor monitor = new Monitor()) {
      monitor.addQueryIndexUpdateListener(listener);
      monitor.register(queries);
      assertEquals(10355, updateCount.get());
    }
  }

  public void testMatcherMetadata() throws IOException {
    try (Monitor monitor = new Monitor()) {
      HashMap<String, String> metadataMap = new HashMap<>();
      metadataMap.put("key", "value");

      monitor.register(new MonitorQuery(Integer.toString(1), MonitorTestBase.parse("+test " + 1), null, metadataMap));

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

  public void testDocumentBatching() throws IOException {

    DocumentBatch batch = DocumentBatch.of(
        InputDocument.builder("doc1").addField(MonitorTestBase.FIELD, "this is a test", ANALYZER).build(),
        InputDocument.builder("doc2").addField(MonitorTestBase.FIELD, "this is a kangaroo", ANALYZER).build()
    );

    try (Monitor monitor = new Monitor()) {
      monitor.register(new MonitorQuery("1", new TermQuery(new Term(MonitorTestBase.FIELD, "kangaroo"))));

      Matches<QueryMatch> response = monitor.match(batch, SimpleMatcher.FACTORY);
      assertEquals(2, response.getBatchSize());
    }
  }

  public void testMutliValuedFieldWithNonDefaultGaps() throws IOException {

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

    MonitorQuery mq = new MonitorQuery("query", MonitorTestBase.parse(MonitorTestBase.FIELD + ":\"hello world\"~5"));

    try (Monitor monitor = new Monitor()) {
      monitor.register(mq);

      InputDocument doc1 = InputDocument.builder("doc1")
          .addField(MonitorTestBase.FIELD, "hello world", analyzer)
          .addField(MonitorTestBase.FIELD, "goodbye", analyzer)
          .build();
      Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
      assertNotNull(matches.getMatches("doc1"));
      assertEquals(1, matches.getMatchCount("doc1"));
      assertNotNull(matches.matches("query", "doc1"));

      InputDocument doc2 = InputDocument.builder("doc2")
          .addField(MonitorTestBase.FIELD, "hello", analyzer)
          .addField(MonitorTestBase.FIELD, "world", analyzer)
          .build();
      matches = monitor.match(doc2, SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc2"));
    }

  }

}
