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

import java.util.*;

/**
 * Class to hold a response from the Monitor
 *
 * @param <T> the type of QueryMatch returned
 */
public class MatchingQueries<T extends QueryMatch> {

  private final Map<String, T> matches;
  private final Set<String> presearcherHits;
  private final List<MatchError> errors;

  private final long queryBuildTime;
  private final long searchTime;
  private final int queriesRun;

  private final SlowLog slowlog;

  MatchingQueries(Map<String, T> matches, Set<String> presearcherHits, List<MatchError> errors,
                  long queryBuildTime, long searchTime, int queriesRun, SlowLog slowlog) {
    this.matches = Collections.unmodifiableMap(matches);
    this.errors = Collections.unmodifiableList(errors);
    this.presearcherHits = Collections.unmodifiableSet(presearcherHits);
    this.queryBuildTime = queryBuildTime;
    this.searchTime = searchTime;
    this.queriesRun = queriesRun;
    this.slowlog = slowlog;
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
   * @return ids of all queries selected by the presearcher
   */
  public Set<String> getPresearcherHits() {
    return presearcherHits;
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
  public List<MatchError> getErrors() {
    return errors;
  }

  /**
   * Return the slow log for this match run.
   * <p>
   * The slow log contains a list of all queries that took longer than the slow log
   * limit to run.
   *
   * @return the slow log
   */
  public SlowLog getSlowLog() {
    return slowlog;
  }

}
