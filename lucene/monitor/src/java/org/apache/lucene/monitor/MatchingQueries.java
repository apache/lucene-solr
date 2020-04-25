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

import java.util.*;

/**
 * Class to hold the results of matching a single {@link org.apache.lucene.document.Document}
 * against queries held in the Monitor
 *
 * @param <T> the type of QueryMatch returned
 */
public class MatchingQueries<T extends QueryMatch> {

  private final Map<String, T> matches;
  private final Map<String, Exception> errors;

  private final long queryBuildTime;
  private final long searchTime;
  private final int queriesRun;

  MatchingQueries(Map<String, T> matches, Map<String, Exception> errors,
                  long queryBuildTime, long searchTime, int queriesRun) {
    this.matches = Collections.unmodifiableMap(matches);
    this.errors = Collections.unmodifiableMap(errors);
    this.queryBuildTime = queryBuildTime;
    this.searchTime = searchTime;
    this.queriesRun = queriesRun;
  }

  /**
   * Returns the QueryMatch for the given query, or null if it did not match
   *
   * @param queryId the query id
   */
  public T matches(String queryId) {
    return matches.get(queryId);
  }

  /**
   * @return all matches
   */
  public Collection<T> getMatches() {
    return matches.values();
  }

  /**
   * @return the number of queries that matched
   */
  public int getMatchCount() {
    return matches.size();
  }

  /**
   * @return how long (in ms) it took to build the Presearcher query for the matcher run
   */
  public long getQueryBuildTime() {
    return queryBuildTime;
  }

  /**
   * @return how long (in ms) it took to run the selected queries
   */
  public long getSearchTime() {
    return searchTime;
  }

  /**
   * @return the number of queries passed to this CandidateMatcher during the matcher run
   */
  public int getQueriesRun() {
    return queriesRun;
  }

  /**
   * @return a List of any MatchErrors created during the matcher run
   */
  public Map<String, Exception> getErrors() {
    return errors;
  }

}
