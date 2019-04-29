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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

public abstract class ConcurrentMatcherTestBase extends LuceneTestCase {

  private static final Analyzer ANALYZER = new StandardAnalyzer();

  protected abstract <T extends QueryMatch> MatcherFactory<T> matcherFactory(ExecutorService executor,
                                                                             MatcherFactory<T> factory, int threads);

  public void testAllMatchesAreCollected() throws Exception {

    ExecutorService executor = Executors.newFixedThreadPool(10, new NamedThreadFactory("matchers"));
    try (Monitor monitor = new Monitor(ANALYZER)) {
      List<MonitorQuery> queries = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        queries.add(new MonitorQuery(Integer.toString(i), MonitorTestBase.parse("+test " + i)));
      }
      monitor.register(queries);

      Document doc = new Document();
      doc.add(newTextField("field", "test", Field.Store.NO));

      MatchingQueries<QueryMatch> matches
          = monitor.match(doc, matcherFactory(executor, QueryMatch.SIMPLE_MATCHER, 10));

      assertEquals(1000, matches.getMatchCount());
    }
    finally {
      executor.shutdown();
    }
  }

  public void testMatchesAreDisambiguated() throws Exception {

    ExecutorService executor = Executors.newFixedThreadPool(4, new NamedThreadFactory("matchers"));

    try (Monitor monitor = new Monitor(ANALYZER)) {
      List<MonitorQuery> queries = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        queries.add(new MonitorQuery(Integer.toString(i), MonitorTestBase.parse("test^10 doc " + i)));
      }
      monitor.register(queries);
      assertEquals(30, monitor.getDisjunctCount());

      Document doc = new Document();
      doc.add(newTextField("field", "test doc doc", Field.Store.NO));

      MatchingQueries<ScoringMatch> matches
          = monitor.match(doc, matcherFactory(executor, ScoringMatch.DEFAULT_MATCHER, 10));

      assertEquals(20, matches.getQueriesRun());
      assertEquals(10, matches.getMatchCount());
      assertTrue(matches.getErrors().isEmpty());
      for (ScoringMatch match : matches.getMatches()) {
        // The queries are all split into three by the QueryDecomposer, and the
        // 'test' and 'doc' parts will match.  'test' will have a higher score,
        // because of it's lower termfreq.  We need to check that each query ends
        // up with the sum of the scores for the 'test' and 'doc' parts
        assertEquals(1.4874471f, match.getScore(), 0);
      }
    }
    finally {
      executor.shutdown();
    }
  }

}
