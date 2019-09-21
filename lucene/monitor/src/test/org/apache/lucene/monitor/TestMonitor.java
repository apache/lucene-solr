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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TestMonitor extends MonitorTestBase {

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  public void testSingleTermQueryMatchesSingleDocument() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(FIELD, "test"))));

      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.getMatches());
      assertEquals(1, matches.getMatchCount());
      assertNotNull(matches.matches("query1"));
    }
  }

  public void testMatchStatisticsAreReported() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "test"))));

      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getQueriesRun());
      assertTrue(matches.getQueryBuildTime() > -1);
      assertTrue(matches.getSearchTime() > -1);
    }
  }

  public void testUpdatesOverwriteOldQueries() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "that", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "this"))));
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "that"))));

      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.matches("query1"));
      assertEquals(1, matches.getQueriesRun());
    }
  }

  public void testCanDeleteById() throws IOException {

    Document doc = new Document();
    doc.add(newTextField(FIELD, "other things", Field.Store.NO));

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("query1", new TermQuery(new Term(MonitorTestBase.FIELD, "this"))));
      monitor.register(
          new MonitorQuery("query2", new TermQuery(new Term(MonitorTestBase.FIELD, "that"))),
          new MonitorQuery("query3", new TermQuery(new Term(MonitorTestBase.FIELD, "other"))));
      assertEquals(3, monitor.getQueryCount());

      monitor.deleteById("query2", "query1");
      assertEquals(1, monitor.getQueryCount());

      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getQueriesRun());
      assertNotNull(matches.matches("query3"));
    }

  }

  public void testCanClearTheMonitor() throws IOException {
    try (Monitor monitor = newMonitor()) {
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

    try (Monitor monitor = newMonitor()) {
      assertEquals(0, monitor.getQueryCount());
      Document doc = new Document();
      doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));
      MatchingQueries<QueryMatch> matches = monitor.match(doc, QueryMatch.SIMPLE_MATCHER);
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

    MonitorUpdateListener listener = new MonitorUpdateListener() {

      @Override
      public void afterUpdate(List<MonitorQuery> updates) {
        int calls = callCount.getAndIncrement();
        updateCount.addAndGet(updates.size());
        assertEquals(expectedSizes[calls], updates.size());
      }
    };

    try (Monitor monitor = new Monitor(ANALYZER)) {
      monitor.addQueryIndexUpdateListener(listener);
      monitor.register(queries);
      assertEquals(10355, updateCount.get());
    }
  }

  public void testMatcherMetadata() throws IOException {
    try (Monitor monitor = newMonitor()) {
      HashMap<String, String> metadataMap = new HashMap<>();
      metadataMap.put("key", "value");

      monitor.register(new MonitorQuery(Integer.toString(1), MonitorTestBase.parse("+test " + 1), null, metadataMap));

      Document doc = new Document();
      doc.add(newTextField(FIELD, "This is a test document", Field.Store.NO));

      MatcherFactory<QueryMatch> testMatcherFactory = docs -> new CandidateMatcher<QueryMatch>(docs) {
        @Override
        protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
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

    Document doc1 = new Document();
    doc1.add(newTextField(FIELD, "This is a test document", Field.Store.NO));
    Document doc2 = new Document();
    doc2.add(newTextField(FIELD, "This is a kangaroo document", Field.Store.NO));


    try (Monitor monitor = new Monitor(ANALYZER)) {
      monitor.register(new MonitorQuery("1", new TermQuery(new Term(MonitorTestBase.FIELD, "kangaroo"))));

      MultiMatchingQueries<QueryMatch> response = monitor.match(new Document[]{ doc1, doc2 }, QueryMatch.SIMPLE_MATCHER);
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

    try (Monitor monitor = new Monitor(analyzer)) {
      monitor.register(mq);

      Document doc1 = new Document();
      doc1.add(newTextField(FIELD, "hello world", Field.Store.NO));
      doc1.add(newTextField(FIELD, "goodbye", Field.Store.NO));

      MatchingQueries<QueryMatch> matches = monitor.match(doc1, QueryMatch.SIMPLE_MATCHER);
      assertNotNull(matches.getMatches());
      assertEquals(1, matches.getMatchCount());
      assertNotNull(matches.matches("query"));

      Document doc2 = new Document();
      doc2.add(newTextField(FIELD, "hello", Field.Store.NO));
      doc2.add(newTextField(FIELD, "world", Field.Store.NO));
      matches = monitor.match(doc2, QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
    }

  }

}
