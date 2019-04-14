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
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.luwak.util.ForceNoBulkScoringQuery;

/**
 * Class used to match candidate queries selected by a Presearcher from a Monitor
 * query index.
 */
public abstract class CandidateMatcher<T extends QueryMatch> {

  private final Set<String> presearcherHits = new HashSet<>();
  protected final DocumentBatch docs;

  private final List<MatchError> errors = new LinkedList<>();
  private final Map<String, MatchHolder<T>> matches = new HashMap<>();

  private long queryBuildTime = -1;
  private long searchTime = System.nanoTime();
  private int queriesRun = -1;

  protected final SlowLog slowlog = new SlowLog();

  private static class MatchHolder<T> {
    Map<String, T> matches = new HashMap<>();
  }

  /**
   * Creates a new CandidateMatcher for the supplied DocumentBatch
   *
   * @param docs the documents to run queries against
   */
  public CandidateMatcher(DocumentBatch docs) {
    this.docs = docs;
  }

  /**
   * Runs the supplied query against this CandidateMatcher's DocumentBatch, storing any
   * resulting match, and recording the query in the presearcher hits
   *
   * @param queryId    the query id
   * @param matchQuery the query to run
   * @param metadata   the query metadata
   * @throws IOException on IO errors
   */
  public final void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
    presearcherHits.add(queryId);
    doMatchQuery(queryId, new ForceNoBulkScoringQuery(matchQuery), metadata);
  }

  /**
   * Override this method to actually run the query
   *
   * @param queryId    the query id
   * @param matchQuery the query to run
   * @param metadata   the query metadata
   * @throws IOException on error
   */
  protected abstract void doMatchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException;

  private void addMatch(String queryId, String docId, T match) {
    MatchHolder<T> docMatches = matches.computeIfAbsent(docId, k -> new MatchHolder<>());
    docMatches.matches.compute(queryId, (key, oldValue) -> {
      if (oldValue != null) {
        return resolve(match, oldValue);
      }
      return match;
    });
  }

  /**
   * Record a match
   *
   * @param match a QueryMatch object
   */
  protected void addMatch(T match) {
    addMatch(match.getQueryId(), match.getDocId(), match);
  }

  /**
   * If two matches from the same query are found (for example, two branches of a disjunction),
   * combine them.
   *
   * @param match1 the first match found
   * @param match2 the second match found
   * @return a Match object that combines the two
   */
  public abstract T resolve(T match1, T match2);

  /**
   * Called by the Monitor if running a query throws an Exception
   *
   * @param e the MatchError detailing the problem
   */
  public void reportError(MatchError e) {
    this.errors.add(e);
  }

  /**
   * Called when matching has finished
   *
   * @param buildTime  the time taken to construct the document disjunction
   * @param queryCount the number of queries run
   */
  public void finish(long buildTime, int queryCount) {
    this.queryBuildTime = buildTime;
    this.queriesRun = queryCount;
    this.searchTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - searchTime, TimeUnit.NANOSECONDS);
  }

  /*
   * Called by the Monitor to set the {@link SlowLog} limit
   */
  public void setSlowLogLimit(long t) {
    this.slowlog.setLimit(t);
  }

  /**
   * Returns the QueryMatch for the given document and query, or null if it did not match
   *
   * @param docId   the document id
   * @param queryId the query id
   * @return the QueryMatch for the given document and query, or null if it did not match
   */
  protected T matches(String docId, String queryId) {
    MatchHolder<T> docMatches = matches.get(docId);
    if (docMatches == null)
      return null;
    return docMatches.matches.get(queryId);
  }

  /**
   * @return the matches from this matcher
   */
  public Matches<T> getMatches() {
    Map<String, DocumentMatches<T>> results = new HashMap<>();
    for (InputDocument doc : docs) {
      String id = doc.getId();
      MatchHolder<T> matchHolder = matches.get(id);
      if (matchHolder != null)
        results.put(id, new DocumentMatches<>(id, matchHolder.matches.values()));
      else
        results.put(id, DocumentMatches.noMatches(id));
    }
    return new Matches<>(results, presearcherHits, errors, queryBuildTime, searchTime, queriesRun, docs.getBatchSize(), slowlog);
  }

  /**
   * Get a {@link LeafReader} over the documents in this matcher's {@link DocumentBatch}
   *
   * @return a {@link LeafReader} over the documents in this matcher's {@link DocumentBatch}
   * @throws IOException on I/O error
   */
  public LeafReader getIndexReader() throws IOException {
    return docs.getIndexReader();
  }
}
