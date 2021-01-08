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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

public class TestTermPresearcher extends PresearcherTestBase {

  public void testFiltersOnTermQueries() throws IOException {

    MonitorQuery query1 = new MonitorQuery("1", parse("furble"));
    MonitorQuery query2 = new MonitorQuery("2", parse("document"));
    MonitorQuery query3 =
        new MonitorQuery("3", parse("\"a document\"")); // will be selected but not match

    try (Monitor monitor = newMonitor()) {
      monitor.register(query1, query2, query3);

      Map<String, Long> timings = new HashMap<>();
      QueryTimeListener timeListener =
          (queryId, timeInNanos) ->
              timings.compute(queryId, (q, t) -> t == null ? timeInNanos : t + timeInNanos);
      MatchingQueries<QueryMatch> matches =
          monitor.match(
              buildDoc(TEXTFIELD, "this is a test document"),
              QueryTimeListener.timingMatcher(QueryMatch.SIMPLE_MATCHER, timeListener));
      assertEquals(1, matches.getMatchCount());
      assertNotNull(matches.matches("2"));
      assertEquals(2, matches.getQueriesRun());
      assertEquals(2, timings.size());
      assertTrue(timings.keySet().contains("2"));
      assertTrue(timings.keySet().contains("3"));
    }
  }

  public void testIgnoresTermsOnNotQueries() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("document -test")));

      MatchingQueries<QueryMatch> matches =
          monitor.match(buildDoc(TEXTFIELD, "this is a test document"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(1, matches.getQueriesRun());

      matches = monitor.match(buildDoc(TEXTFIELD, "weeble sclup test"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(0, matches.getMatchCount());
      assertEquals(0, matches.getQueriesRun());
    }
  }

  public void testMatchesAnyQueries() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(new MonitorQuery("1", parse("/hell./")));

      MatchingQueries<QueryMatch> matches =
          monitor.match(buildDoc(TEXTFIELD, "hello"), QueryMatch.SIMPLE_MATCHER);
      assertEquals(1, matches.getMatchCount());
      assertEquals(1, matches.getQueriesRun());
    }
  }

  @Override
  protected Presearcher createPresearcher() {
    return new TermFilteredPresearcher();
  }

  public void testAnyTermsAreCorrectlyAnalyzed() {

    QueryAnalyzer analyzer = new QueryAnalyzer();
    QueryTree qt =
        analyzer.buildTree(new MatchAllDocsQuery(), TermFilteredPresearcher.DEFAULT_WEIGHTOR);

    TermFilteredPresearcher presearcher = new TermFilteredPresearcher();
    Map<String, BytesRefHash> extractedTerms = presearcher.collectTerms(qt);
    assertEquals(1, extractedTerms.size());
  }

  public void testQueryBuilder() throws IOException {

    Presearcher presearcher = createPresearcher();

    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, iwc);
    MonitorConfiguration config =
        new MonitorConfiguration() {
          @Override
          public IndexWriter buildIndexWriter() {
            return writer;
          }
        };

    try (Monitor monitor = new Monitor(ANALYZER, presearcher, config)) {

      monitor.register(new MonitorQuery("1", parse("f:test")));

      try (IndexReader reader = DirectoryReader.open(writer, false, false)) {

        MemoryIndex mindex = new MemoryIndex();
        mindex.addField("f", "this is a test document", WHITESPACE);
        mindex.addField("g", "#######", ANALYZER); // analyzes away to empty field
        LeafReader docsReader = (LeafReader) mindex.createSearcher().getIndexReader();

        QueryIndex.QueryTermFilter termFilter = new QueryIndex.QueryTermFilter(reader);

        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(docsReader, termFilter);
        BooleanQuery expected =
            new BooleanQuery.Builder()
                .add(
                    should(
                        new BooleanQuery.Builder()
                            .add(should(new TermInSetQuery("f", new BytesRef("test"))))
                            .build()))
                .add(should(new TermQuery(new Term("__anytokenfield", "__ANYTOKEN__"))))
                .build();

        assertEquals(expected, q);
      }
    }
  }
}
