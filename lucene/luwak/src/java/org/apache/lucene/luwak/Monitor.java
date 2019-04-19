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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.luwak.presearcher.PresearcherMatches;
import org.apache.lucene.luwak.presearcher.TermFilteredPresearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * A Monitor contains a set of MonitorQuery objects, and runs them against
 * passed-in InputDocuments.
 */
public class Monitor implements Closeable {

  protected final Presearcher presearcher;

  private final QueryIndex queryIndex;

  private final List<QueryIndexUpdateListener> listeners = new ArrayList<>();

  protected long slowLogLimit = 2000000;

  private final long commitBatchSize;

  private final ScheduledExecutorService purgeExecutor;

  private long lastPurged = -1;

  /**
   * Create a non-persistent Monitor instance with the default term-filtering Presearcher
   */
  public Monitor() throws IOException {
    this(new TermFilteredPresearcher());
  }

  /**
   * Create a new Monitor instance
   *
   * @param presearcher   the presearcher to use
   * @param configuration the MonitorConfiguration
   */
  public Monitor(Presearcher presearcher,
                 QueryIndexConfiguration configuration) throws IOException {

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
   * Create a new non-persistent Monitor instance
   *
   * @param presearcher the presearcher to use
   */
  public Monitor(Presearcher presearcher) throws IOException {
    this(presearcher, new QueryIndexConfiguration());
  }

  /**
   * Create a new Monitor instance with a specific configuration
   */
  public Monitor(QueryIndexConfiguration config) throws IOException {
    this(new TermFilteredPresearcher(), config);
  }

  /**
   * Register a {@link QueryIndexUpdateListener} that will be notified whenever changes
   * are made to the Monitor's queryindex
   *
   * @param listener listener to register
   */
  public void addQueryIndexUpdateListener(QueryIndexUpdateListener listener) {
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
    listeners.forEach(QueryIndexUpdateListener::onPurge);
  }

  /**
   * Set the slow log limit
   * <p>
   * All queries that take longer than t nanoseconds to run will be recorded in
   * the slow log.  The default is 2,000,000 (2 milliseconds)
   *
   * @param limit the limit in nanoseconds
   * @see Matches#getSlowLog()
   */
  public void setSlowLogLimit(long limit) {
    this.slowLogLimit = limit;
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
   * @throws IOException     on IO errors
   * @throws UpdateException if any of the queries could not be added
   */
  public void update(Iterable<MonitorQuery> queries) throws IOException {
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
    listeners.forEach(l -> l.beforeUpdate(updates));
    queryIndex.commit(updates);
    listeners.forEach(l -> l.afterUpdate(updates));
  }

  /**
   * Add new queries to the monitor
   *
   * @param queries the MonitorQueries to add
   * @throws IOException     on IO errors
   * @throws UpdateException if any of the queries could not be added
   */
  public void update(MonitorQuery... queries) throws IOException {
    update(Arrays.asList(queries));
  }

  /**
   * Delete queries from the monitor by ID
   *
   * @param queryIds the IDs to delete
   * @throws IOException on IO errors
   */
  public void deleteById(Iterable<String> queryIds) throws IOException {
    listeners.forEach(QueryIndexUpdateListener::beforeDelete);
    queryIndex.deleteQueries(queryIds);
    listeners.forEach(QueryIndexUpdateListener::afterDelete);
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
    listeners.forEach(QueryIndexUpdateListener::beforeDelete);
    queryIndex.clear();
    listeners.forEach(QueryIndexUpdateListener::afterDelete);
  }

  /**
   * Match a {@link DocumentBatch} against the queryindex, calling a {@link CandidateMatcher} produced by the
   * supplied {@link MatcherFactory} for each possible matching query.
   *
   * @param docs    the DocumentBatch to match
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of {@link QueryMatch} to return
   * @return a {@link Matches} object summarizing the match run.
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> Matches<T> match(DocumentBatch docs, MatcherFactory<T> factory) throws IOException {
    CandidateMatcher<T> matcher = factory.createMatcher(docs);
    matcher.setSlowLogLimit(slowLogLimit);
    match(matcher);
    return matcher.getMatches();
  }

  /**
   * Match a single {@link InputDocument} against the queryindex, calling a {@link CandidateMatcher} produced by the
   * supplied {@link MatcherFactory} for each possible matching query.
   *
   * @param doc     the InputDocument to match
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of {@link QueryMatch} to return
   * @return a {@link Matches} object summarizing the match run.
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> Matches<T> match(InputDocument doc, MatcherFactory<T> factory) throws IOException {
    return match(DocumentBatch.of(doc), factory);
  }

  private class PresearcherQueryBuilder implements QueryIndex.QueryBuilder {

    final LeafReader batchIndexReader;

    private PresearcherQueryBuilder(LeafReader batchIndexReader) {
      this.batchIndexReader = batchIndexReader;
    }

    @Override
    public Query buildQuery(QueryTermFilter termFilter) throws IOException {
      return presearcher.buildQuery(batchIndexReader, termFilter);
    }
  }

  private <T extends QueryMatch> void match(CandidateMatcher<T> matcher) throws IOException {
    StandardQueryCollector<T> collector = new StandardQueryCollector<>(matcher);
    long buildTime = queryIndex.search(new PresearcherQueryBuilder(matcher.getIndexReader()), collector);
    matcher.finish(buildTime, collector.queryCount);
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
        matcher.reportError(new MatchError(id, e));
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
  public <T extends QueryMatch> PresearcherMatches<T> debug(final DocumentBatch docs, MatcherFactory<T> factory)
      throws IOException {
    PresearcherQueryCollector<T> collector = new PresearcherQueryCollector<>(factory.createMatcher(docs));
    QueryIndex.QueryBuilder queryBuilder = new PresearcherQueryBuilder(docs.getIndexReader()) {
      @Override
      public Query buildQuery(QueryTermFilter termFilter) throws IOException {
        return new ForceNoBulkScoringQuery(super.buildQuery(termFilter));
      }
    };
    queryIndex.search(queryBuilder, collector);
    return collector.getMatches();
  }

  /**
   * Match a single {@link InputDocument} against the queries stored in the Monitor, also returning information
   * about which queries were selected by the presearcher, and why.
   *
   * @param doc     an InputDocument to match against the index
   * @param factory a {@link MatcherFactory} to use to create a {@link CandidateMatcher} for the match run
   * @param <T>     the type of QueryMatch produced by the CandidateMatcher
   * @return a {@link PresearcherMatches} object containing debug information
   * @throws IOException on IO errors
   */
  public <T extends QueryMatch> PresearcherMatches<T> debug(InputDocument doc, MatcherFactory<T> factory) throws IOException {
    return debug(DocumentBatch.of(doc), factory);
  }

  private class PresearcherQueryCollector<T extends QueryMatch> extends StandardQueryCollector<T> {

    public final Map<String, StringBuilder> matchingTerms = new HashMap<>();

    private PresearcherQueryCollector(CandidateMatcher<T> matcher) {
      super(matcher);
    }

    public PresearcherMatches<T> getMatches() {
      return new PresearcherMatches<>(matchingTerms, matcher.getMatches());
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }

    @Override
    public void matchQuery(final String id, QueryCacheEntry query, QueryIndex.DataValues dataValues) throws IOException {
      Weight w = ((Scorer)dataValues.scorer).getWeight();
      org.apache.lucene.search.Matches matches = w.matches(dataValues.ctx, dataValues.scorer.docID());
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
