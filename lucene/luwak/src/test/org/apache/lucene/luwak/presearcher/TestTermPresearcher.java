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
import java.util.Map;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.QueryIndexConfiguration;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.QueryTermFilter;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.queryanalysis.QueryTree;
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

    MonitorQuery query1
        = new MonitorQuery("1", parse("furble"));
    MonitorQuery query2
        = new MonitorQuery("2", parse("document"));
    MonitorQuery query3 = new MonitorQuery("3", parse("\"a document\""));  // will be selected but not match

    try (Monitor monitor = newMonitor()) {
      monitor.update(query1, query2, query3);

      Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
      assertNotNull(matches.matches("2", "doc1"));
      assertEquals(2, matches.getQueriesRun());
      assertEquals(2, matches.getPresearcherHits().size());
      assertTrue(matches.getPresearcherHits().contains("2"));
      assertTrue(matches.getPresearcherHits().contains("3"));
    }
  }

  public void testIgnoresTermsOnNotQueries() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("document -test")));

      Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", TEXTFIELD, "this is a test document"), SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc1"));
      assertEquals(1, matches.getQueriesRun());

      matches = monitor.match(buildDoc("doc2", TEXTFIELD, "weeble sclup test"), SimpleMatcher.FACTORY);
      assertEquals(0, matches.getMatchCount("doc2"));
      assertEquals(0, matches.getQueriesRun());
    }

  }

  public void testMatchesAnyQueries() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.update(new MonitorQuery("1", parse("/hell./")));

      Matches<QueryMatch> matches = monitor.match(buildDoc("doc1", TEXTFIELD, "hello"), SimpleMatcher.FACTORY);
      assertEquals(1, matches.getMatchCount("doc1"));
      assertEquals(1, matches.getQueriesRun());
    }

  }

  @Override
  protected Presearcher createPresearcher() {
    return new TermFilteredPresearcher();
  }

  public void testAnyTermsAreCorrectlyAnalyzed() {

    TermFilteredPresearcher presearcher = new TermFilteredPresearcher();
    QueryTree qt = presearcher.extractor.buildTree(new MatchAllDocsQuery(), TermFilteredPresearcher.DEFAULT_WEIGHTOR);

    Map<String, BytesRefHash> extractedTerms = presearcher.collectTerms(qt);
    assertEquals(1, extractedTerms.size());

  }

  public void testQueryBuilder() throws IOException {

    Presearcher presearcher = createPresearcher();

    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    Directory dir = new ByteBuffersDirectory();
    IndexWriter writer = new IndexWriter(dir, iwc);
    QueryIndexConfiguration config = new QueryIndexConfiguration(){
      @Override
      public IndexWriter buildIndexWriter() {
        return writer;
      }
    };

    try (Monitor monitor = new Monitor(presearcher, config)) {

      monitor.update(new MonitorQuery("1", parse("f:test")));

      try (IndexReader reader = DirectoryReader.open(writer, false, false)) {

        DocumentBatch batch = DocumentBatch.of(
            InputDocument.builder("doc1").addField("f", "this is a test document", new StandardAnalyzer()).build()
        );

        BooleanQuery q = (BooleanQuery) presearcher.buildQuery(batch.getIndexReader(), new QueryTermFilter(reader));
        BooleanQuery expected = new BooleanQuery.Builder()
            .add(should(new BooleanQuery.Builder()
                .add(should(new TermInSetQuery("f", new BytesRef("test")))).build()))
            .add(should(new TermQuery(new Term("__anytokenfield", "__ANYTOKEN__"))))
            .build();

        assertEquals(expected, q);

      }

    }

  }
}
