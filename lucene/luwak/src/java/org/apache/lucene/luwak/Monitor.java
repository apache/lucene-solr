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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.luwak.presearcher.PresearcherMatches;
import org.apache.lucene.luwak.util.SpanExtractor;
import org.apache.lucene.luwak.util.SpanRewriter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.luwak.util.RewriteException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.luwak.util.ForceNoBulkScoringQuery;

/**
 * A Monitor contains a set of MonitorQuery objects, and runs them against
 * passed-in InputDocuments.
 */
public class Monitor implements Closeable {

  protected final MonitorQueryParser queryParser;
  protected final Presearcher presearcher;
  protected final QueryDecomposer decomposer;

  private final QueryIndex queryIndex;

  private final List<QueryIndexUpdateListener> listeners = new ArrayList<>();

  protected long slowLogLimit = 2000000;

  private final long commitBatchSize;
  private final boolean storeQueries;

  public static final class FIELDS {
    public static final String id = "_id";
    public static final String del = "_del";
    public static final String hash = "_hash";
    public static final String mq = "_mq";
  }

  private final ScheduledExecutorService purgeExecutor;

  private long lastPurged = -1;

  /**
   * Create a new Monitor instance, using a passed in IndexWriter for its queryindex
   * <p>
   * Note that when the Monitor is closed, both the IndexWriter and its underlying
   * Directory will also be closed.
   *
   * @param queryParser   the query parser to use
   * @param presearcher   the presearcher to use
   * @param indexWriter   an indexWriter for the query index
   * @param configuration the MonitorConfiguration
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher,
                 IndexWriter indexWriter, QueryIndexConfiguration configuration) throws IOException {

    this.queryParser = queryParser;
    this.presearcher = presearcher;
    this.decomposer = configuration.getQueryDecomposer();

    this.queryIndex = new QueryIndex(indexWriter);

    this.storeQueries = configuration.storeQueries();
    prepareQueryCache(this.storeQueries);

    long purgeFrequency = configuration.getPurgeFrequency();
    this.purgeExecutor = Executors.newSingleThreadScheduledExecutor();
    this.purgeExecutor.scheduleAtFixedRate(() -> {
      try {
        purgeCache();
      } catch (Throwable e) {
        afterPurgeError(e);
      }
    }, purgeFrequency, purgeFrequency, configuration.getPurgeFrequencyUnits());

    this.commitBatchSize = configuration.getQueryUpdateBufferSize();
  }

  /**
   * Create a new Monitor instance, using a RAMDirectory and the default configuration
   *
   * @param queryParser the query parser to use
   * @param presearcher the presearcher to use
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher) throws IOException {
    this(queryParser, presearcher, defaultIndexWriter(new ByteBuffersDirectory()), new QueryIndexConfiguration());
  }

  /**
   * Create a new Monitor instance using a RAMDirectory
   *
   * @param queryParser the query parser to use
   * @param presearcher the presearcher to use
   * @param config      the monitor configuration
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, QueryIndexConfiguration config) throws IOException {
    this(queryParser, presearcher, defaultIndexWriter(new ByteBuffersDirectory()), config);
  }

  /**
   * Create a new Monitor instance, using the default QueryDecomposer and IndexWriter configuration
   *
   * @param queryParser the query parser to use
   * @param presearcher the presearcher to use
   * @param directory   the directory where the queryindex is stored
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory) throws IOException {
    this(queryParser, presearcher, defaultIndexWriter(directory), new QueryIndexConfiguration());
  }

  /**
   * Create a new Monitor instance
   *
   * @param queryParser the query parser to use
   * @param presearcher the presearcher to use
   * @param directory   the directory where the queryindex is to be stored
   * @param config      the monitor configuration
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, Directory directory, QueryIndexConfiguration config) throws IOException {
    this(queryParser, presearcher, defaultIndexWriter(directory), config);
  }

  /**
   * Create a new Monitor instance, using the default QueryDecomposer
   *
   * @param queryParser the query parser to use
   * @param presearcher the presearcher to use
   * @param indexWriter a {@link IndexWriter} for the Monitor's query index
   * @throws IOException on IO errors
   */
  public Monitor(MonitorQueryParser queryParser, Presearcher presearcher, IndexWriter indexWriter) throws IOException {
    this(queryParser, presearcher, indexWriter, new QueryIndexConfiguration());
  }

  // package-private for testing
  static IndexWriter defaultIndexWriter(Directory directory) throws IOException {

    IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = new TieredMergePolicy();
    mergePolicy.setSegmentsPerTier(4);
    iwc.setMergePolicy(mergePolicy);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

    return new IndexWriter(directory, iwc);

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

  private void prepareQueryCache(boolean storeQueries) throws IOException {

    if (storeQueries == false) {
      // we're not storing the queries, so ensure that the queryindex is empty
      // before we add any.
      clear();
      return;
    }

    // load any queries that have already been added to the queryindex
    final List<Exception> parseErrors = new LinkedList<>();
    final Set<BytesRef> seenHashes = new HashSet<>();
    final Set<String> seenIds = new HashSet<>();

    queryIndex.purgeCache(newCache -> queryIndex.scan((id, query, dataValues) -> {
      if (seenIds.contains(id)) {
        return;
      }
      seenIds.add(id);

      BytesRef serializedMQ = dataValues.mq.binaryValue();
      MonitorQuery mq = MonitorQuery.deserialize(serializedMQ);

      BytesRef hash = mq.hash();
      if (seenHashes.contains(hash)) {
        return;
      }
      seenHashes.add(hash);

      try {
        for (QueryCacheEntry ce : decomposeQuery(mq)) {
          newCache.put(ce.hash, ce);
        }
      } catch (Exception e) {
        parseErrors.add(e);
      }
    }));
    if (parseErrors.size() != 0)
      throw new IOException("Error populating cache - some queries couldn't be parsed:" + parseErrors);
  }

  private void commit(List<Indexable> updates) throws IOException {
    beforeCommit(updates);
    queryIndex.commit(updates);
    afterCommit(updates);
  }

  private void afterPurge() {
    for (QueryIndexUpdateListener listener : listeners) {
      listener.onPurge();
    }
  }

  private void afterPurgeError(Throwable t) {
    for (QueryIndexUpdateListener listener : listeners) {
      listener.onPurgeError(t);
    }
  }

  private void beforeCommit(List<Indexable> updates) {
    if (updates == null) {
      for (QueryIndexUpdateListener listener : listeners) {
        listener.beforeDelete();
      }
    } else {
      for (QueryIndexUpdateListener listener : listeners) {
        listener.beforeUpdate(updates);
      }
    }
  }

  private void afterCommit(List<Indexable> updates) {
    if (updates == null) {
      for (QueryIndexUpdateListener listener : listeners) {
        listener.afterDelete();
      }
    } else {
      for (QueryIndexUpdateListener listener : listeners) {
        listener.afterUpdate(updates);
      }
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
    queryIndex.purgeCache(newCache -> queryIndex.scan((id, query, dataValues) -> {
      if (query != null)
        newCache.put(BytesRef.deepCopyOf(query.hash), query);
    }));

    lastPurged = System.nanoTime();
    afterPurge();
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
    queryIndex.closeWhileHandlingException();
  }

  /**
   * Add new queries to the monitor
   *
   * @param queries the MonitorQueries to add
   * @throws IOException     on IO errors
   * @throws UpdateException if any of the queries could not be added
   */
  public void update(Iterable<MonitorQuery> queries) throws IOException, UpdateException {

    List<QueryError> errors = new ArrayList<>();
    List<Indexable> updates = new ArrayList<>();

    for (MonitorQuery query : queries) {
      try {
        for (QueryCacheEntry queryCacheEntry : decomposeQuery(query)) {
          updates.add(new Indexable(query.getId(), queryCacheEntry, buildIndexableQuery(query.getId(), query, queryCacheEntry)));
        }
      } catch (Exception e) {
        errors.add(new QueryError(query, e));
      }
      if (updates.size() > commitBatchSize) {
        commit(updates);
        updates.clear();
      }
    }
    commit(updates);

    if (errors.isEmpty() == false)
      throw new UpdateException(errors);
  }

  private Iterable<QueryCacheEntry> decomposeQuery(MonitorQuery query) throws Exception {

    Query q = queryParser.parse(query.getQuery(), query.getMetadata());

    BytesRef rootHash = query.hash();

    int upto = 0;
    List<QueryCacheEntry> cacheEntries = new LinkedList<>();
    for (Query subquery : decomposer.decompose(q)) {
      BytesRefBuilder subHash = new BytesRefBuilder();
      subHash.append(rootHash);
      subHash.append(new BytesRef("_" + upto++));
      cacheEntries.add(new QueryCacheEntry(subHash.toBytesRef(), subquery, query.getMetadata()));
    }

    return cacheEntries;
  }

  /**
   * Add new queries to the monitor
   *
   * @param queries the MonitorQueries to add
   * @throws IOException     on IO errors
   * @throws UpdateException if any of the queries could not be added
   */
  public void update(MonitorQuery... queries) throws IOException, UpdateException {
    update(Arrays.asList(queries));
  }

  /**
   * Delete queries from the monitor
   *
   * @param queries the queries to remove
   * @throws IOException on IO errors
   */
  public void delete(Iterable<MonitorQuery> queries) throws IOException {
    for (MonitorQuery mq : queries) {
      queryIndex.deleteDocuments(new Term(Monitor.FIELDS.del, mq.getId()));
    }
    commit(null);
  }

  /**
   * Delete queries from the monitor by ID
   *
   * @param queryIds the IDs to delete
   * @throws IOException on IO errors
   */
  public void deleteById(Iterable<String> queryIds) throws IOException {
    for (String queryId : queryIds) {
      queryIndex.deleteDocuments(new Term(FIELDS.del, queryId));
    }
    commit(null);
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
    queryIndex.deleteDocuments(new MatchAllDocsQuery());
    commit(null);
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
    if (storeQueries == false)
      throw new IllegalStateException("Cannot call getQuery() as queries are not stored");
    final MonitorQuery[] queryHolder = new MonitorQuery[]{null};
    queryIndex.search(new TermQuery(new Term(FIELDS.id, queryId)), (id, query, dataValues) -> {
      BytesRef serializedMQ = dataValues.mq.binaryValue();
      queryHolder[0] = MonitorQuery.deserialize(serializedMQ);
    });
    return queryHolder[0];
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

  /**
   * Build a lucene {@link Document} to be stored in the queryindex from a query entry
   *
   * @param id    the query id
   * @param mq    the MonitorQuery to be indexed
   * @param query the (possibly partial after decomposition) query to be indexed
   * @return a Document that will be indexed in the Monitor's queryindex
   */
  protected Document buildIndexableQuery(String id, MonitorQuery mq, QueryCacheEntry query) {
    Document doc = presearcher.indexQuery(query.matchQuery, mq.getMetadata());
    doc.add(new StringField(FIELDS.id, id, Field.Store.NO));
    doc.add(new StringField(FIELDS.del, id, Field.Store.NO));
    doc.add(new SortedDocValuesField(FIELDS.id, new BytesRef(id)));
    doc.add(new BinaryDocValuesField(FIELDS.hash, query.hash));
    if (storeQueries)
      doc.add(new BinaryDocValuesField(FIELDS.mq, MonitorQuery.serialize(mq)));
    return doc;
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
