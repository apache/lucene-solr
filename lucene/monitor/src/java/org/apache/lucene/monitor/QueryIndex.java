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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.IOUtils;

class QueryIndex implements Closeable {

  static final class FIELDS {
    static final String query_id = "_query_id";
    static final String cache_id = "_cache_id";
    static final String mq = "_mq";
  }

  private final IndexWriter writer;
  private final SearcherManager manager;
  private final QueryDecomposer decomposer;
  private final MonitorQuerySerializer serializer;
  private final Presearcher presearcher;

  /* Used to cache updates while a purge is ongoing */
  private volatile Map<String, QueryCacheEntry> purgeCache = null;

  /* Used to lock around the creation of the purgeCache */
  private final ReadWriteLock purgeLock = new ReentrantReadWriteLock();
  private final Object commitLock = new Object();

  /* The current query cache */
  private volatile ConcurrentMap<String, QueryCacheEntry> queries = new ConcurrentHashMap<>();
  // NB this is not final because it can be replaced by purgeCache()

  // package-private for testing
  final Map<IndexReader.CacheKey, QueryTermFilter> termFilters = new HashMap<>();

  QueryIndex(MonitorConfiguration config, Presearcher presearcher) throws IOException {
    this.writer = config.buildIndexWriter();
    this.manager = new SearcherManager(writer, true, true, new TermsHashBuilder());
    this.decomposer = config.getQueryDecomposer();
    this.serializer = config.getQuerySerializer();
    this.presearcher = presearcher;
    populateQueryCache(serializer, decomposer);
  }

  private void populateQueryCache(MonitorQuerySerializer serializer, QueryDecomposer decomposer) throws IOException {
    if (serializer == null) {
      // No query serialization happening here - check that the cache is empty
      IndexSearcher searcher = manager.acquire();
      try {
        if (searcher.count(new MatchAllDocsQuery()) != 0) {
          throw new IllegalStateException("Attempting to open a non-empty monitor query index with no MonitorQuerySerializer");
        }
      }
      finally {
        manager.release(searcher);
      }
      return;
    }
    Set<String> ids = new HashSet<>();
    List<Exception> errors = new ArrayList<>();
    purgeCache(newCache -> scan((id, cacheEntry, dataValues) -> {
      if (ids.contains(id)) {
        // this is a branch of a query that has already been reconstructed, but
        // then split by decomposition - we don't need to parse it again
        return;
      }
      ids.add(id);
      try {
        MonitorQuery mq = serializer.deserialize(dataValues.mq.binaryValue());
        for (QueryCacheEntry entry : QueryCacheEntry.decompose(mq, decomposer)) {
          newCache.put(entry.cacheId, entry);
        }
      }
      catch (Exception e) {
        errors.add(e);
      }
    }));
    if (errors.size() > 0) {
      IllegalStateException e = new IllegalStateException("Couldn't parse some queries from the index");
      for (Exception parseError : errors) {
        e.addSuppressed(parseError);
      }
      throw e;
    }
  }

  private class TermsHashBuilder extends SearcherFactory {
    @Override
    public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader) throws IOException {
      IndexSearcher searcher = super.newSearcher(reader, previousReader);
      searcher.setQueryCache(null);
      termFilters.put(reader.getReaderCacheHelper().getKey(), new QueryTermFilter(reader));
      reader.getReaderCacheHelper().addClosedListener(termFilters::remove);
      return searcher;
    }
  }

  void commit(List<MonitorQuery> updates) throws IOException {
    List<Indexable> indexables = buildIndexables(updates);
    synchronized (commitLock) {
      purgeLock.readLock().lock();
      try {
        if (indexables.size() > 0) {
          Set<String> ids = new HashSet<>();
          for (Indexable update : indexables) {
            ids.add(update.queryCacheEntry.queryId);
          }
          for (String id : ids) {
            writer.deleteDocuments(new Term(FIELDS.query_id, id));
          }
          for (Indexable update : indexables) {
            this.queries.put(update.queryCacheEntry.cacheId, update.queryCacheEntry);
            writer.addDocument(update.document);
            if (purgeCache != null)
              purgeCache.put(update.queryCacheEntry.cacheId, update.queryCacheEntry);
          }
        }
        writer.commit();
        manager.maybeRefresh();
      } finally {
        purgeLock.readLock().unlock();
      }
    }
  }

  private static class Indexable {
    final QueryCacheEntry queryCacheEntry;
    final Document document;

    private Indexable(QueryCacheEntry queryCacheEntry, Document document) {
      this.queryCacheEntry = queryCacheEntry;
      this.document = document;
    }
  }

  private static final BytesRef EMPTY = new BytesRef();

  private List<Indexable> buildIndexables(List<MonitorQuery> updates) {
    List<Indexable> indexables = new ArrayList<>();
    for (MonitorQuery mq : updates) {
      if (serializer != null && mq.getQueryString() == null) {
        throw new IllegalArgumentException("Cannot add a MonitorQuery with a null string representation to a non-ephemeral Monitor");
      }
      BytesRef serialized = serializer == null ? EMPTY : serializer.serialize(mq);
      for (QueryCacheEntry qce : QueryCacheEntry.decompose(mq, decomposer)) {
        Document doc = presearcher.indexQuery(qce.matchQuery, mq.getMetadata());
        doc.add(new StringField(FIELDS.query_id, qce.queryId, Field.Store.NO));
        doc.add(new SortedDocValuesField(FIELDS.cache_id, new BytesRef(qce.cacheId)));
        doc.add(new SortedDocValuesField(FIELDS.query_id, new BytesRef(qce.queryId)));
        doc.add(new BinaryDocValuesField(FIELDS.mq, serialized));
        indexables.add(new Indexable(qce, doc));
      }
    }
    return indexables;
  }

  interface QueryBuilder {
    Query buildQuery(BiPredicate<String, BytesRef> termAcceptor) throws IOException;
  }

  static class QueryTermFilter implements BiPredicate<String, BytesRef> {

    private final Map<String, BytesRefHash> termsHash = new HashMap<>();

    QueryTermFilter(IndexReader reader) throws IOException {
      for (LeafReaderContext ctx : reader.leaves()) {
        for (FieldInfo fi : ctx.reader().getFieldInfos()) {
          BytesRefHash terms = termsHash.computeIfAbsent(fi.name, f -> new BytesRefHash());
          Terms t = ctx.reader().terms(fi.name);
          if (t != null) {
            TermsEnum te = t.iterator();
            BytesRef term;
            while ((term = te.next()) != null) {
              terms.add(term);
            }
          }
        }
      }
    }

    @Override
    public boolean test(String field, BytesRef term) {
      BytesRefHash bytes = termsHash.get(field);
      if (bytes == null) {
        return false;
      }
      return bytes.find(term) != -1;
    }
  }

  MonitorQuery getQuery(String queryId) throws IOException {
    if (serializer == null) {
      throw new IllegalStateException("Cannot get queries from an index with no MonitorQuerySerializer");
    }
    BytesRef[] bytesHolder = new BytesRef[1];
    search(new TermQuery(new Term(FIELDS.query_id, queryId)),
        (id, query, dataValues) -> bytesHolder[0] = dataValues.mq.binaryValue());
    return serializer.deserialize(bytesHolder[0]);
  }

  void scan(QueryCollector matcher) throws IOException {
    search(new MatchAllDocsQuery(), matcher);
  }

  long search(final Query query, QueryCollector matcher) throws IOException {
    QueryBuilder builder = termFilter -> query;
    return search(builder, matcher);
  }

  long search(QueryBuilder queryBuilder, QueryCollector matcher) throws IOException {
    IndexSearcher searcher = null;
    try {
      Map<String, QueryCacheEntry> queries;

      purgeLock.readLock().lock();
      try {
        searcher = manager.acquire();
        queries = this.queries;
      } finally {
        purgeLock.readLock().unlock();
      }

      MonitorQueryCollector collector = new MonitorQueryCollector(queries, matcher);
      long buildTime = System.nanoTime();
      Query query = queryBuilder.buildQuery(termFilters.get(searcher.getIndexReader().getReaderCacheHelper().getKey()));
      buildTime = System.nanoTime() - buildTime;
      searcher.search(query, collector);
      return buildTime;
    } finally {
      if (searcher != null) {
        manager.release(searcher);
      }
    }
  }

  interface CachePopulator {
    void populateCacheWithIndex(Map<String, QueryCacheEntry> newCache) throws IOException;
  }

  void purgeCache() throws IOException {
    purgeCache(newCache -> scan((id, query, dataValues) -> {
      if (query != null)
        newCache.put(query.cacheId, query);
    }));
  }

  /**
   * Remove unused queries from the query cache.
   * <p>
   * This is normally called from a background thread at a rate set by configurePurgeFrequency().
   *
   * @throws IOException on IO errors
   */
  private synchronized void purgeCache(CachePopulator populator) throws IOException {

    // Note on implementation

    // The purge works by scanning the query index and creating a new query cache populated
    // for each query in the index.  When the scan is complete, the old query cache is swapped
    // for the new, allowing it to be garbage-collected.

    // In order to not drop cached queries that have been added while a purge is ongoing,
    // we use a ReadWriteLock to guard the creation and removal of an register log.  Commits take
    // the read lock.  If the register log has been created, then a purge is ongoing, and queries
    // are added to the register log within the read lock guard.

    // The purge takes the write lock when creating the register log, and then when swapping out
    // the old query cache.  Within the second write lock guard, the contents of the register log
    // are added to the new query cache, and the register log itself is removed.

    final ConcurrentMap<String, QueryCacheEntry> newCache = new ConcurrentHashMap<>();

    purgeLock.writeLock().lock();
    try {
      purgeCache = new ConcurrentHashMap<>();
    } finally {
      purgeLock.writeLock().unlock();
    }

    populator.populateCacheWithIndex(newCache);

    purgeLock.writeLock().lock();
    try {
      newCache.putAll(purgeCache);
      purgeCache = null;
      queries = newCache;
    } finally {
      purgeLock.writeLock().unlock();
    }
  }


  // ---------------------------------------------
  //  Proxy trivial operations...
  // ---------------------------------------------

  @Override
  public void close() throws IOException {
    IOUtils.close(manager, writer, writer.getDirectory());
  }

  int numDocs() {
    return writer.getDocStats().numDocs;
  }

  int cacheSize() {
    return queries.size();
  }

  void deleteQueries(Iterable<String> ids) throws IOException {
    for (String id : ids) {
      writer.deleteDocuments(new Term(FIELDS.query_id, id));
    }
    commit(Collections.emptyList());
  }

  void clear() throws IOException {
    writer.deleteAll();
    commit(Collections.emptyList());
  }

  interface QueryCollector {

    void matchQuery(String id, QueryCacheEntry query, DataValues dataValues) throws IOException;

    default ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

  }

  // ---------------------------------------------
  //  Helper classes...
  // ---------------------------------------------

  static final class DataValues {
    SortedDocValues queryId;
    SortedDocValues cacheId;
    BinaryDocValues mq;
    Scorable scorer;
    LeafReaderContext ctx;

    void advanceTo(int doc) throws IOException {
      assert scorer.docID() == doc;
      queryId.advanceExact(doc);
      cacheId.advanceExact(doc);
      if (mq != null) {
        mq.advanceExact(doc);
      }
    }
  }

  /**
   * A Collector that decodes the stored query for each document hit.
   */
  static final class MonitorQueryCollector extends SimpleCollector {

    private final Map<String, QueryCacheEntry> queries;
    private final QueryCollector matcher;
    private final DataValues dataValues = new DataValues();

    MonitorQueryCollector(Map<String, QueryCacheEntry> queries, QueryCollector matcher) {
      this.queries = queries;
      this.matcher = matcher;
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.dataValues.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      dataValues.advanceTo(doc);
      BytesRef cache_id = dataValues.cacheId.binaryValue();
      BytesRef query_id = dataValues.queryId.binaryValue();
      QueryCacheEntry query = queries.get(cache_id.utf8ToString());
      matcher.matchQuery(query_id.utf8ToString(), query, dataValues);
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
      this.dataValues.cacheId = context.reader().getSortedDocValues(FIELDS.cache_id);
      this.dataValues.queryId = context.reader().getSortedDocValues(FIELDS.query_id);
      this.dataValues.mq = context.reader().getBinaryDocValues(FIELDS.mq);
      this.dataValues.ctx = context;
    }

    @Override
    public ScoreMode scoreMode() {
      return matcher.scoreMode();
    }

  }
}
