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
import java.util.Map;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

abstract class CollectingMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

  private final ScoreMode scoreMode;

  CollectingMatcher(IndexSearcher searcher, ScoreMode scoreMode) {
    super(searcher);
    this.scoreMode = scoreMode;
  }

  @Override
  protected void matchQuery(final String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException {
    searcher.search(matchQuery, new MatchCollector(queryId, scoreMode));
  }

  /**
   * Called when a query matches a Document
   *
   * @param queryId the query ID
   * @param doc the index of the document in the DocumentBatch
   * @param scorer the Scorer for this query
   * @return a match object
   * @throws IOException on IO error
   */
  protected abstract T doMatch(String queryId, int doc, Scorable scorer) throws IOException;

  private class MatchCollector extends SimpleCollector {

    private final String queryId;
    private final ScoreMode scoreMode;
    private Scorable scorer;

    MatchCollector(String queryId, ScoreMode scoreMode) {
      this.queryId = queryId;
      this.scoreMode = scoreMode;
    }

    @Override
    public void collect(int doc) throws IOException {
      T match = doMatch(queryId, doc, scorer);
      if (match != null) {
        addMatch(match, doc);
      }
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public ScoreMode scoreMode() {
      return scoreMode;
    }
  }
}
