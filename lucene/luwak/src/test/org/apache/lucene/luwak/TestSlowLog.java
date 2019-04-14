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
import java.util.Map;

import org.apache.lucene.luwak.matchers.SimpleMatcher;
import org.apache.lucene.luwak.presearcher.MatchAllPresearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;

public class TestSlowLog extends LuceneTestCase {

  public static class SlowQueryParser implements MonitorQueryParser {

    final long delay;

    public SlowQueryParser(long delay) {
      this.delay = delay;
    }

    @Override
    public Query parse(String queryString, Map<String, String> metadata) {
      if (queryString.equals("slow")) {
        return new Query() {
          @Override
          public String toString(String s) {
            return "";
          }

          @Override
          public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            try {
              Thread.sleep(delay);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            return new MatchAllDocsQuery().createWeight(searcher, scoreMode, boost);
          }

          @Override
          public void visit(QueryVisitor visitor) {

          }

          @Override
          public boolean equals(Object o) {
            return false;
          }

          @Override
          public int hashCode() {
            return 0;
          }
        };
      }
      return new MatchAllDocsQuery();
    }
  }

  @Test
  public void testSlowLog() throws IOException, UpdateException {

    try (Monitor monitor = new Monitor(new SlowQueryParser(250), new MatchAllPresearcher())) {
      monitor.update(new MonitorQuery("1", "slow"), new MonitorQuery("2", "fast"), new MonitorQuery("3", "slow"));

      InputDocument doc1 = InputDocument.builder("doc1").build();

      Matches<QueryMatch> matches = monitor.match(doc1, SimpleMatcher.FACTORY);
      String slowlog = matches.getSlowLog().toString();
      assertThat(slowlog, containsString("1 ["));
      assertThat(slowlog, containsString("3 ["));
      assertThat(slowlog, not(containsString("2 [")));

      monitor.setSlowLogLimit(1);
      matches = monitor.match(doc1, SimpleMatcher.FACTORY);
      slowlog = matches.getSlowLog().toString();
      assertThat(slowlog, containsString("1 ["));
      assertThat(slowlog, containsString("2 ["));
      assertThat(slowlog, containsString("3 ["));

      monitor.setSlowLogLimit(2000000000000L);
      assertFalse(monitor.match(doc1, SimpleMatcher.FACTORY).getSlowLog().iterator().hasNext());
    }
  }
}
