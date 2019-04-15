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
import java.util.Map;

import org.apache.lucene.search.*;
import org.apache.lucene.luwak.CandidateMatcher;
import org.apache.lucene.luwak.DocumentBatch;
import org.apache.lucene.luwak.QueryMatch;

/**
 * Extend this class to create matches that require a Scorer
 *
 * @param <T> the type of QueryMatch that this class returns
 */
public abstract class CollectingMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

  private final ScoreMode scoreMode;

  /**
   * Creates a new CollectingMatcher for the supplied DocumentBatch
   *
   * @param docs the documents to run queries against
   */
  public CollectingMatcher(DocumentBatch docs, ScoreMode scoreMode) {
    super(docs);
    this.scoreMode = scoreMode;
  }

  @Override
  protected void doMatchQuery(final String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {

    MatchCollector coll = new MatchCollector(queryId, scoreMode);

    long t = System.nanoTime();
    IndexSearcher searcher = docs.getSearcher();
    searcher.search(matchQuery, coll);
    t = System.nanoTime() - t;
    this.slowlog.addQuery(queryId, t);

  }

  /**
   * Called when a query matches an InputDocument
   *
   * @param queryId the query ID
   * @param docId   the docId for the InputDocument in the DocumentBatch
   * @param scorer  the Scorer for this query
   * @return a match object
   * @throws IOException on IO error
   */
  protected abstract T doMatch(String queryId, String docId, Scorable scorer) throws IOException;

  protected class MatchCollector extends SimpleCollector {

    private final String queryId;
    private final ScoreMode scoreMode;
    private Scorable scorer;

    public MatchCollector(String queryId, ScoreMode scoreMode) {
      this.queryId = queryId;
      this.scoreMode = scoreMode;
    }

    @Override
    public void collect(int doc) throws IOException {
      T match = doMatch(queryId, docs.resolveDocId(doc), scorer);
      if (match != null) {
        addMatch(match);
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
