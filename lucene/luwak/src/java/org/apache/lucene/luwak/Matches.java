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
public class Matches<T extends QueryMatch> implements Iterable<DocumentMatches<T>> {

  private final Map<String, DocumentMatches<T>> matches;
  private final Set<String> presearcherHits;
  private final List<MatchError> errors;

  private final long queryBuildTime;
  private final long searchTime;
  private final int queriesRun;
  private final int batchSize;

  private final SlowLog slowlog;

  Matches(Map<String, DocumentMatches<T>> matches, Set<String> presearcherHits, List<MatchError> errors,
          long queryBuildTime, long searchTime, int queriesRun, int batchSize, SlowLog slowlog) {
    this.matches = Collections.unmodifiableMap(matches);
    this.errors = Collections.unmodifiableList(errors);
    this.presearcherHits = Collections.unmodifiableSet(presearcherHits);
    this.queryBuildTime = queryBuildTime;
    this.searchTime = searchTime;
    this.queriesRun = queriesRun;
    this.batchSize = batchSize;
    this.slowlog = slowlog;
  }

  @Override
  public Iterator<DocumentMatches<T>> iterator() {
    return matches.values().iterator();
  }

  /**
   * Returns the QueryMatch for the given query and document, or null if it did not match
   *
   * @param queryId the query id
   * @param docId   the doc id
   * @return the QueryMatch for the given query and document, or null if it did not match
   */
  public T matches(String queryId, String docId) {
    DocumentMatches<T> docMatches = matches.get(docId);
    if (docMatches == null)
      return null;

    for (T match : docMatches) {
      if (match.getQueryId().equals(queryId))
        return match;
    }

    return null;
  }

  /**
   * @param docId document id to check
   * @return all matches for a particular document
   */
  public DocumentMatches<T> getMatches(String docId) {
    return matches.get(docId);
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
  public int getMatchCount(String docId) {
    DocumentMatches<T> docMatches = matches.get(docId);
    if (docMatches == null)
      return 0;
    return docMatches.getMatches().size();
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
