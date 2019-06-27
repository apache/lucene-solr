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

import java.util.Objects;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Represents a match for a specific query and document
 * <p>
 * Derived classes may contain more information (such as scores, highlights, etc)
 *
 * @see ExplainingMatch
 * @see ScoringMatch
 * @see HighlightsMatch
 */
public class QueryMatch {

  private final String queryId;

  public static final MatcherFactory<QueryMatch> SIMPLE_MATCHER =
      searcher -> new CollectingMatcher<QueryMatch>(searcher, ScoreMode.COMPLETE_NO_SCORES) {
    @Override
    public QueryMatch resolve(QueryMatch match1, QueryMatch match2) {
      return match1;
    }

    @Override
    protected QueryMatch doMatch(String queryId, int doc, Scorable scorer) {
      return new QueryMatch(queryId);
    }
  };

  /**
   * Creates a new QueryMatch for a specific query and document
   *
   * @param queryId the query id
   */
  public QueryMatch(String queryId) {
    this.queryId = Objects.requireNonNull(queryId);
  }

  /**
   * @return the queryid of this match
   */
  public String getQueryId() {
    return queryId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof QueryMatch)) return false;
    QueryMatch that = (QueryMatch) o;
    return Objects.equals(queryId, that.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId);
  }

  @Override
  public String toString() {
    return "Match(query=" + queryId + ")";
  }
}
