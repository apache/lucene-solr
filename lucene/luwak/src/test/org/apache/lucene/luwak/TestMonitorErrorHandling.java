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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestMonitorErrorHandling extends LuceneTestCase {

  private static final String FIELD = "f";

  private static final Analyzer ANALYZER = new WhitespaceAnalyzer();

  private static class ThrowOnRewriteQuery extends Query {

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      throw new IOException("Error rewriting");
    }

    @Override
    public String toString(String field) {
      return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {

    }

    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static MonitorQueryParser createMockCache() {
    return (query, metadata) -> {
      if (query == null)
        return null;
      if ("unparseable".equals(query))
        throw new RuntimeException("Error parsing query [unparseable]");
      if ("error".equals(query))
        return new ThrowOnRewriteQuery();
      return new TermQuery(new Term(FIELD, query));
    };
  }

  public void testMonitorErrors() throws Exception {

    try (Monitor monitor = new Monitor(createMockCache(), new MatchAllPresearcher())) {

      UpdateException e = expectThrows(UpdateException.class,
          () -> monitor.update(
              new MonitorQuery("1", "unparseable"),
              new MonitorQuery("2", "test"),
              new MonitorQuery("3", "error")));
      assertEquals(1, e.errors.size());

      InputDocument doc = InputDocument.builder("doc").addField(FIELD, "test", ANALYZER).build();
      DocumentBatch batch = DocumentBatch.of(doc);
      Matches<QueryMatch> matches = monitor.match(batch, SimpleMatcher.FACTORY);

      assertEquals(1, matches.getErrors().size());
      assertEquals(1, matches.getMatchCount("doc"));
      assertEquals(2, matches.getQueriesRun());
    }
  }

  @Test
  public void testPresearcherErrors() throws Exception {

    Presearcher presearcher = new Presearcher() {

      int calls = 0;

      @Override
      public Query buildQuery(LeafReader reader, QueryTermFilter queryTermFilter) {
        return null;
      }

      @Override
      public Document indexQuery(Query query, Map<String, String> metadata) {
        calls++;
        if (calls == 2) {
          throw new UnsupportedOperationException("Oops");
        }
        return new Document();
      }
    };

    try (Monitor monitor = new Monitor(new LuceneQueryParser("f"), presearcher)) {
      UpdateException e = expectThrows(UpdateException.class, () -> monitor.update(
          new MonitorQuery("1", "1"),
          new MonitorQuery("2", "2"),
          new MonitorQuery("3", "3")
      ));
      assertEquals(1, e.errors.size());
      assertEquals("2", e.errors.get(0).query.getId());
      assertEquals(2, monitor.getQueryCount());
    }
  }

  public void testMonitorQueryNullValues() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      Map<String, String> metadata2 = new HashMap<>();
      metadata2.put("key", null);
      new MonitorQuery("id", "query", metadata2);
    });
    assertEquals("Null value for key key in metadata map", e.getMessage());
  }

}
