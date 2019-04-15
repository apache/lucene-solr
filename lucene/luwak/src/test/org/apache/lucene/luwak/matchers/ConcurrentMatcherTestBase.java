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

package org.apache.lucene.luwak.matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.InputDocument;
import org.apache.lucene.luwak.MatcherFactory;
import org.apache.lucene.luwak.Matches;
import org.apache.lucene.luwak.Monitor;
import org.apache.lucene.luwak.MonitorQuery;
import org.apache.lucene.luwak.QueryMatch;
import org.apache.lucene.luwak.TestSlowLog;
import org.apache.lucene.luwak.UpdateException;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.luwak.queryparsers.LuceneQueryParser;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;

public abstract class ConcurrentMatcherTestBase extends LuceneTestCase {

  private static final Analyzer ANALYZER = new StandardAnalyzer();

  protected abstract <T extends QueryMatch>
  MatcherFactory<T> matcherFactory(ExecutorService executor, MatcherFactory<T> factory, int threads);

  public void testAllMatchesAreCollected() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
      List<MonitorQuery> queries = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        queries.add(new MonitorQuery(Integer.toString(i), "+test " + i));
      }
      monitor.update(queries);

      ExecutorService executor = Executors.newFixedThreadPool(10);

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("1").addField("field", "test", ANALYZER).build());

      Matches<QueryMatch> matches
          = monitor.match(batch, matcherFactory(executor, SimpleMatcher.FACTORY, 10));

      assertEquals(1000, matches.getMatchCount("1"));

      executor.shutdown();
    }
  }

  public void testMatchesAreDisambiguated() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new LuceneQueryParser("field"), new MatchAllPresearcher())) {
      List<MonitorQuery> queries = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        queries.add(new MonitorQuery(Integer.toString(i), "test^10 doc " + i));
      }
      monitor.update(queries);
      assertEquals(30, monitor.getDisjunctCount());

      ExecutorService executor = Executors.newFixedThreadPool(4);

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("1")
          .addField("field", "test doc doc", ANALYZER)
          .build());

      Matches<ScoringMatch> matches
          = monitor.match(batch, matcherFactory(executor, ScoringMatcher.FACTORY, 10));

      assertEquals(10, matches.getMatchCount("1"));
      assertEquals(30, matches.getQueriesRun());
      assertTrue(matches.getErrors().isEmpty());
      for (ScoringMatch match : matches.getMatches("1")) {
        // The queries are all split into three by the QueryDecomposer, and the
        // 'test' and 'doc' parts will match.  'test' will have a higher score,
        // because of it's lower termfreq.  We need to check that each query ends
        // up with the sum of the scores for the 'test' and 'doc' parts
        assertEquals(1.4874471f, match.getScore(), 0);
      }

      executor.shutdown();
    }
  }

  public void testParallelSlowLog() throws IOException, UpdateException {

    ExecutorService executor = Executors.newCachedThreadPool();

    try (Monitor monitor = new Monitor(new TestSlowLog.SlowQueryParser(250), new MatchAllPresearcher())) {
      monitor.update(new MonitorQuery("1", "slow"), new MonitorQuery("2", "fast"), new MonitorQuery("3", "slow"));

      DocumentBatch batch = DocumentBatch.of(InputDocument.builder("doc1").build());

      MatcherFactory<QueryMatch> factory = matcherFactory(executor, SimpleMatcher.FACTORY, 10);

      Matches<QueryMatch> matches = monitor.match(batch, factory);
      assertEquals(3, matches.getMatchCount("doc1"));
      assertThat(matches.getSlowLog().toString(), containsString("1 ["));
      assertThat(matches.getSlowLog().toString(), containsString("3 ["));
      assertThat(matches.getSlowLog().toString(), not(containsString("2 [")));

      monitor.setSlowLogLimit(1);
      String slowlog = monitor.match(batch, factory).getSlowLog().toString();
      assertThat(slowlog, containsString("1 ["));
      assertThat(slowlog, containsString("2 ["));
      assertThat(slowlog, containsString("3 ["));

      monitor.setSlowLogLimit(2000000000000L);
      assertFalse(monitor.match(batch, factory).getSlowLog().iterator().hasNext());
    }

    executor.shutdown();
  }
}
