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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiMatchingQueries<T extends QueryMatch> {

  private final List<Map<String, T>> matches;
  private final Set<String> presearcherHits;
  private final Map<String, Exception> errors;

  private final long queryBuildTime;
  private final long searchTime;
  private final int queriesRun;
  private final int batchSize;

  private final SlowLog slowlog;

  MultiMatchingQueries(List<Map<String, T>> matches, Set<String> presearcherHits, Map<String, Exception> errors,
                  long queryBuildTime, long searchTime, int queriesRun, int batchSize, SlowLog slowlog) {
    this.matches = Collections.unmodifiableList(matches);
    this.errors = Collections.unmodifiableMap(errors);
    this.presearcherHits = Collections.unmodifiableSet(presearcherHits);
    this.queryBuildTime = queryBuildTime;
    this.searchTime = searchTime;
    this.queriesRun = queriesRun;
    this.batchSize = batchSize;
    this.slowlog = slowlog;
  }

  /**
   * Returns the QueryMatch for the given query and document, or null if it did not match
   *
   * @param queryId the query id
   * @param docId   the doc id
   * @return the QueryMatch for the given query and document, or null if it did not match
   */
  public T matches(String queryId, int docId) {
    Map<String, T> docMatches = matches.get(docId);
    if (docMatches == null)
      return null;
    return docMatches.get(queryId);
  }

  /**
   * @param docId document id to check
   * @return all matches for a particular document
   */
  public Collection<T> getMatches(int docId) {
    return matches.get(docId).values();
  }

  /**
   * @return ids of all queries selected by the presearcher
   */
  public Set<String> getPresearcherHits() {
    return presearcherHits;
  }

  /**
   * @param docId document id to check
   * @return the number of queries that matched for a given document
   */
  public int getMatchCount(int docId) {
    Map<String, T> docMatches = matches.get(docId);
    if (docMatches == null)
      return 0;
    return docMatches.size();
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
   * @return the number of documents in the batch
   */
  public int getBatchSize() {
    return batchSize;
  }

  /**
   * @return a List of any MatchErrors created during the matcher run
   */
  public Map<String, Exception> getErrors() {
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

  MatchingQueries<T> singleton() {
    assert matches.size() == 1;
    return new MatchingQueries<>(matches.get(0), presearcherHits, errors, queryBuildTime, searchTime, queriesRun, slowlog);
  }
}
