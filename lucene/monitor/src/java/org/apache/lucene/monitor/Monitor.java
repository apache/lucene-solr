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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * A Monitor contains a set of {@link Query} objects with associated IDs, and efficiently
 * matches them against sets of {@link Document} objects.
 */
public class Monitor implements Closeable {

  protected final Presearcher presearcher;
  private final Analyzer analyzer;

  private final QueryIndex queryIndex;

  private final List<MonitorUpdateListener> listeners = new ArrayList<>();

  private final long commitBatchSize;

  private final ScheduledExecutorService purgeExecutor;

  private long lastPurged = -1;

  /**
   * Create a non-persistent Monitor instance with the default term-filtering Presearcher
   *
   * @param analyzer to analyze {@link Document}s at match time
   */
  public Monitor(Analyzer analyzer) throws IOException {
    this(analyzer, new TermFilteredPresearcher());
  }

  /**
   * Create a new non-persistent Monitor instance
   *
   * @param analyzer to analyze {@link Document}s at match time
   * @param presearcher the presearcher to use
   */
  public Monitor(Analyzer analyzer, Presearcher presearcher) throws IOException {
    this(analyzer, presearcher, new MonitorConfiguration());
  }

  /**
   * Create a new Monitor instance with a specific configuration
   *
   * @param analyzer to analyze {@link Document}s at match time
   * @param config   the configuration
   */
  public Monitor(Analyzer analyzer, MonitorConfiguration config) throws IOException {
    this(analyzer, new TermFilteredPresearcher(), config);
  }

  /**
   * Create a new Monitor instance
   *
   * @param analyzer      to analyze {@link Document}s at match time
   * @param presearcher   the presearcher to use
   * @param configuration the configuration
   */
  public Monitor(Analyzer analyzer, Presearcher presearcher,
                 MonitorConfiguration configuration) throws IOException {

    this.analyzer = analyzer;
    this.presearcher = presearcher;
    this.queryIndex = new QueryIndex(configuration, presearcher);

    long purgeFrequency = configuration.getPurgeFrequency();
    this.purgeExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("cache-purge"));
    this.purgeExecutor.scheduleAtFixedRate(() -> {
      try {
        purgeCache();
      } catch (Throwable e) {
        listeners.forEach(l -> l.onPurgeError(e));
      }
    }, purgeFrequency, purgeFrequency, configuration.getPurgeFrequencyUnits());

    this.commitBatchSize = configuration.getQueryUpdateBufferSize();
  }

  /**
   * Register a {@link MonitorUpdateListener} that will be notified whenever changes
   * are made to the Monitor's queryindex
   *
   * @param listener listener to register
   */
  public void addQueryIndexUpdateListener(MonitorUpdateListener listener) {
    listeners.add(listener);
  }

  /**
   * @return Statistics for the internal query index and cache
   */
  public QueryCacheStats getQueryCacheStats() {
    return new QueryCacheStats(queryIndex.numDocs(), queryIndex.cacheSize(), lastPurged);
  }

  /**
   * Statistics for the query cache and query index
   */
  public static class QueryCacheStats {

    /**
     * Total number of queries in the query index
     */
    public final int queries;

    /**
     * Total number of queries int the query cache
     */
    public final int cachedQueries;

    /**
     * Time the query cache was last purged
     */
    public final long lastPurged;

    public QueryCacheStats(int queries, int cachedQueries, long lastPurged) {
      this.queries = queries;
      this.cachedQueries = cachedQueries;
      this.lastPurged = lastPurged;
    }
  }

  /**
   * Remove unused queries from the query cache.
   * <p>
   * This is normally called from a background thread at a rate set by configurePurgeFrequency().
   *
   * @throws IOException on IO errors
   */
  public void purgeCache() throws IOException {
    queryIndex.purgeCache();
    lastPurged = System.nanoTime();
    listeners.forEach(MonitorUpdateListener::onPurge);
  }

  @Override
  public void close() throws IOException {
    purgeExecutor.shutdown();
    queryIndex.close();
  }

  /**
   * Add new queries to the monitor
   *
   * @param queries the MonitorQueries to add
   */
  public void register(Iterable<MonitorQuery> queries) throws IOException {
    List<MonitorQuery> updates = new ArrayList<>();
    for (MonitorQuery query : queries) {
      updates.add(query);
      if (updates.size() > commitBatchSize) {
        commit(updates);
        updates.clear();
      }
    }
    commit(updates);
  }

  private void commit(List<MonitorQuery> updates) throws IOException {
    queryIndex.commit(updates);
    listeners.forEach(l -> l.afterUpdate(updates));
  }

  /**
   * Add new queries to the monitor
   *
   * @param queries the MonitorQueries to add
   * @throws IOException     on IO errors
   */
  public void register(MonitorQuery... queries) throws IOException {
    register(Arrays.asList(queries));
  }

  /**
   * Delete queries from the monitor by ID
   *
   * @param queryIds the IDs to delete
   * @throws IOException on IO errors
   */
  public void deleteById(List<String> queryIds) throws IOException {
    queryIndex.deleteQueries(queryIds);
    listeners.forEach(l -> l.afterDelete(queryIds));
  }

  /**
   * Delete queries from the monitor by ID
   *
   * @param queryIds the IDs to delete
   * @throws IOException on IO errors
   */
  public void deleteById(String... queryIds) throws IOException {
    deleteById(Arrays.asList(queryIds));
  }

  /**
   * Delete all queries from the monitor
   *
   * @throws IOException on IO errors
   */
  public void clear() throws IOException {
    queryIndex.clear();
    listeners.forEach(MonitorUpdateListener::afterClear);
  }

  /**
   * Match an array of {@link Document}s against the queryindex, calling a {@link CandidateMatcher} produced by the
   * supplied {@link MatcherFactory} for each possible matching query.
   *
   * @param docs    the DocumentBatch to match
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of {@link QueryMatch} to return
   * @return a {@link MatchingQueries} object summarizing the match run.
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> MultiMatchingQueries<T> match(Document[] docs, MatcherFactory<T> factory) throws IOException {
    try (DocumentBatch batch = DocumentBatch.of(analyzer, docs)) {
      LeafReader reader = batch.get();
      CandidateMatcher<T> matcher = factory.createMatcher(new IndexSearcher(batch.get()));
      StandardQueryCollector<T> collector = new StandardQueryCollector<>(matcher);
      long buildTime = queryIndex.search(t -> presearcher.buildQuery(reader, t), collector);
      return matcher.finish(buildTime, collector.queryCount);
    }
  }

  /**
   * Match a single {@link Document} against the queryindex, calling a {@link CandidateMatcher} produced by the
   * supplied {@link MatcherFactory} for each possible matching query.
   *
   * @param doc     the InputDocument to match
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of {@link QueryMatch} to return
   * @return a {@link MatchingQueries} object summarizing the match run.
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> MatchingQueries<T> match(Document doc, MatcherFactory<T> factory) throws IOException {
    return match(new Document[]{ doc }, factory).singleton();
  }

  /**
   * Get the MonitorQuery for a given query id
   *
   * @param queryId the id of the query to get
   * @return the MonitorQuery stored for this id, or null if not found
   * @throws IOException           on IO errors
   * @throws IllegalStateException if queries are not stored in the queryindex
   */
  public MonitorQuery getQuery(final String queryId) throws IOException {
    return queryIndex.getQuery(queryId);
  }

  /**
   * @return the number of queries (after decomposition) stored in this Monitor
   */
  public int getDisjunctCount() {
    return queryIndex.numDocs();
  }

  /**
   * @return the number of queries stored in this Monitor
   * @throws IOException on IO errors
   */
  public int getQueryCount() throws IOException {
    return getQueryIds().size();
  }

  /**
   * @return the set of query ids of the queries stored in this Monitor
   * @throws IOException on IO errors
   */
  public Set<String> getQueryIds() throws IOException {
    final Set<String> ids = new HashSet<>();
    queryIndex.scan((id, query, dataValues) -> ids.add(id));
    return ids;
  }

  // For each query selected by the presearcher, pass on to a CandidateMatcher
  private static class StandardQueryCollector<T extends QueryMatch> implements QueryIndex.QueryCollector {

    final CandidateMatcher<T> matcher;
    int queryCount = 0;

    private StandardQueryCollector(CandidateMatcher<T> matcher) {
      this.matcher = matcher;
    }

    @Override
    public void matchQuery(String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
      if (query == null)
        return;
      try {
        queryCount++;
        matcher.matchQuery(id, query.matchQuery, query.metadata);
      } catch (Exception e) {
        matcher.reportError(id, e);
      }
    }

  }

  /**
   * Match a DocumentBatch against the queries stored in the Monitor, also returning information
   * about which queries were selected by the presearcher, and why.
   *
   * @param docs    a DocumentBatch to match against the index
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of QueryMatch produced by the CandidateMatcher
   * @return a {@link PresearcherMatches} object containing debug information
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> PresearcherMatches<T> debug(Document[] docs, MatcherFactory<T> factory)
      throws IOException {
    try (DocumentBatch batch = DocumentBatch.of(analyzer, docs)) {
      LeafReader reader = batch.get();
      IndexSearcher searcher = new IndexSearcher(reader);
      searcher.setQueryCache(null);
      PresearcherQueryCollector<T> collector = new PresearcherQueryCollector<>(factory.createMatcher(searcher));
      long buildTime = queryIndex.search(t -> new ForceNoBulkScoringQuery(presearcher.buildQuery(reader, t)), collector);
      return collector.getMatches(buildTime);
    }
  }

  /**
   * Match a single {@link Document} against the queries stored in the Monitor, also returning information
   * about which queries were selected by the presearcher, and why.
   *
   * @param doc     an InputDocument to match against the index
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of QueryMatch produced by the CandidateMatcher
   * @return a {@link PresearcherMatches} object containing debug information
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> PresearcherMatches<T> debug(Document doc, MatcherFactory<T> factory) throws IOException {
    return debug(new Document[]{doc}, factory);
  }

  private class PresearcherQueryCollector<T extends QueryMatch> extends StandardQueryCollector<T> {

    final Map<String, StringBuilder> matchingTerms = new HashMap<>();

    private PresearcherQueryCollector(CandidateMatcher<T> matcher) {
      super(matcher);
    }

    public PresearcherMatches<T> getMatches(long buildTime) {
      return new PresearcherMatches<>(matchingTerms, matcher.finish(buildTime, queryCount));
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    @Override
    public void matchQuery(final String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
      Weight w = ((Scorer)dataValues.scorer).getWeight();
      Matches matches = w.matches(dataValues.ctx, dataValues.scorer.docID());
      for (String field : matches) {
        MatchesIterator mi = matches.getMatches(field);
        while (mi.next()) {
          matchingTerms.computeIfAbsent(id, i -> new StringBuilder())
              .append(" ").append(mi.getQuery());
        }
      }
      super.matchQuery(id, query, dataValues);
    }

  }

}
