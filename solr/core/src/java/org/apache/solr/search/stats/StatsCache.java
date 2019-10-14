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
package org.apache.solr.search.stats;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;

/**
 * This class represents a cache of global document frequency information for
 * selected terms. This information is periodically updated from all shards,
 * either through scheduled events of some kind, or on every request when there
 * is no global stats available for terms involved in the query (or if this
 * information is stale due to changes in the shards).
 * <p>
 * There are instances of this class at the aggregator node (where the partial
 * data from shards is aggregated), and on each core involved in a shard request
 * (where this data is maintained and updated from the aggregator's cache).
 * </p>
 */
public abstract class StatsCache implements PluginInfoInitialized {
  // TODO: decouple use in response from use in request context for these keys
  /**
   * Map of terms and {@link TermStats}.
   */
  public static final String TERM_STATS_KEY = "solr.stats.term";
  /**
   * Value of {@link CollectionStats}.
   */
  public static final String COL_STATS_KEY = "solr.stats.col";
  /**
   * List of terms in the query.
   */
  public static final String TERMS_KEY = "solr.stats.terms";
  /**
   * List of fields in the query.
   */
  public static final String FIELDS_KEY = "solr.stats.fields";

  public static final class StatsCacheMetrics {
    public final LongAdder lookups = new LongAdder();
    public final LongAdder retrieveStats = new LongAdder();
    public final LongAdder receiveGlobalStats = new LongAdder();
    public final LongAdder returnLocalStats = new LongAdder();
    public final LongAdder mergeToGlobalStats = new LongAdder();
    public final LongAdder sendGlobalStats = new LongAdder();
    public final LongAdder useCachedGlobalStats = new LongAdder();
    public final LongAdder missingGlobalTermStats = new LongAdder();
    public final LongAdder missingGlobalFieldStats = new LongAdder();

    public void clear() {
      lookups.reset();
      retrieveStats.reset();
      receiveGlobalStats.reset();
      returnLocalStats.reset();
      mergeToGlobalStats.reset();
      sendGlobalStats.reset();
      useCachedGlobalStats.reset();
      missingGlobalTermStats.reset();
      missingGlobalFieldStats.reset();
    }

    public void getSnapshot(BiConsumer<String, Object> consumer) {
      consumer.accept(SolrCache.LOOKUPS_PARAM, lookups.longValue());
      consumer.accept("retrieveStats", retrieveStats.longValue());
      consumer.accept("receiveGlobalStats", receiveGlobalStats.longValue());
      consumer.accept("returnLocalStats", returnLocalStats.longValue());
      consumer.accept("mergeToGlobalStats", mergeToGlobalStats.longValue());
      consumer.accept("sendGlobalStats", sendGlobalStats.longValue());
      consumer.accept("useCachedGlobalStats", useCachedGlobalStats.longValue());
      consumer.accept("missingGlobalTermStats", missingGlobalTermStats.longValue());
      consumer.accept("missingGlobalFieldStats", missingGlobalFieldStats.longValue());
    }

    public String toString() {
      Map<String, Object> map = new HashMap<>();
      getSnapshot(map::put);
      return map.toString();
    }
  }

  protected StatsCacheMetrics statsCacheMetrics = new StatsCacheMetrics();
  protected PluginInfo pluginInfo;

  public StatsCacheMetrics getCacheMetrics() {
    return statsCacheMetrics;
  }

  @Override
  public void init(PluginInfo info) {
    this.pluginInfo = info;
  }

  /**
   * Creates a {@link ShardRequest} to retrieve per-shard stats related to the
   * current query and the current state of the requester's {@link StatsCache}.
   * <p>This method updates the cache metrics and calls {@link #doRetrieveStatsRequest(ResponseBuilder)}.</p>
   *
   * @param rb contains current request
   * @return shard request to retrieve stats for terms in the current request,
   * or null if no additional request is needed (e.g. if the information
   * in global cache is already sufficient to satisfy this request).
   */
  public ShardRequest retrieveStatsRequest(ResponseBuilder rb) {
    statsCacheMetrics.retrieveStats.increment();
    return doRetrieveStatsRequest(rb);
  }

  protected abstract ShardRequest doRetrieveStatsRequest(ResponseBuilder rb);

  /**
   * Prepare a local (from the local shard) response to a "retrieve stats" shard
   * request.
   * <p>This method updates the cache metrics and calls {@link #doReturnLocalStats(ResponseBuilder, SolrIndexSearcher)}.</p>
   *
   * @param rb       response builder
   * @param searcher current local searcher
   */
  public void returnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher) {
    statsCacheMetrics.returnLocalStats.increment();
    doReturnLocalStats(rb, searcher);
  }

  protected abstract void doReturnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher);

  /**
   * Process shard responses that contain partial local stats. Usually this
   * entails combining per-shard stats for each term.
   * <p>This method updates the cache metrics and calls {@link #doMergeToGlobalStats(SolrQueryRequest, List)}.</p>
   *
   * @param req       query request
   * @param responses responses from shards containing local stats for each shard
   */
  public void mergeToGlobalStats(SolrQueryRequest req,
                                          List<ShardResponse> responses) {
    statsCacheMetrics.mergeToGlobalStats.increment();
    doMergeToGlobalStats(req, responses);
  }

  protected abstract void doMergeToGlobalStats(SolrQueryRequest req, List<ShardResponse> responses);

  /**
   * Receive global stats data from the master and update a local cache of global stats
   * with this global data. This event occurs either as a separate request, or
   * together with the regular query request, in which case this method is
   * called first, before preparing a {@link QueryCommand} to be submitted to
   * the local {@link SolrIndexSearcher}.
   * <p>This method updates the cache metrics and calls {@link #doReceiveGlobalStats(SolrQueryRequest)}.</p>
   *
   * @param req query request with global stats data
   */
  public void receiveGlobalStats(SolrQueryRequest req) {
    statsCacheMetrics.receiveGlobalStats.increment();
    doReceiveGlobalStats(req);
  }

  protected abstract void doReceiveGlobalStats(SolrQueryRequest req);

  /**
   * Prepare global stats data to be sent out to shards in this request.
   * <p>This method updates the cache metrics and calls {@link #doSendGlobalStats(ResponseBuilder, ShardRequest)}.</p>
   *
   * @param rb       response builder
   * @param outgoing shard request to be sent
   */
  public void sendGlobalStats(ResponseBuilder rb, ShardRequest outgoing) {
    statsCacheMetrics.sendGlobalStats.increment();
    doSendGlobalStats(rb, outgoing);
  }

  protected abstract void doSendGlobalStats(ResponseBuilder rb, ShardRequest outgoing);

  /**
   * Prepare a {@link StatsSource} that provides stats information to perform
   * local scoring (to be precise, to build a local {@link Weight} from the
   * query).
   * <p>This method updates the cache metrics and calls {@link #doGet(SolrQueryRequest)}.</p>
   *
   * @param req query request
   * @return an instance of {@link StatsSource} to use in creating a query
   * {@link Weight}
   */
  public StatsSource get(SolrQueryRequest req) {
    statsCacheMetrics.lookups.increment();
    return doGet(req);
  }

  protected abstract StatsSource doGet(SolrQueryRequest req);

  /**
   * Clear cached statistics.
   */
  public void clear() {
    statsCacheMetrics.clear();
  };

  /**
   * Check if the <code>statsSource</code> is missing some term or field statistics info,
   * which then needs to be retrieved.
   * <p>NOTE: this uses the local IndexReader for query rewriting, which may expand to less (or different)
   * terms as rewriting the same query on other shards' readers. This in turn may falsely fail to inform the consumers
   * about possibly missing stats, which may lead consumers to skip the fetching of full stats. Consequently
   * this would lead to incorrect global IDF data for the missing terms (because for these terms only local stats
   * would be used).</p>
   * @param rb request to evaluate against the statsSource
   * @param statsSource stats source to check
   * @param missingTermStats consumer of missing term stats
   * @param missingFieldStats consumer of missing field stats
   * @return approximate number of missing term stats and field stats combined
   */
  public int approxCheckMissingStats(ResponseBuilder rb, StatsSource statsSource, Consumer<Term> missingTermStats, Consumer<String> missingFieldStats) throws IOException {
    CheckingIndexSearcher checkingSearcher = new CheckingIndexSearcher(statsSource, rb.req.getSearcher().getIndexReader(), missingTermStats, missingFieldStats);
    Query q = rb.getQuery();
    q = checkingSearcher.rewrite(q);
    checkingSearcher.createWeight(q, ScoreMode.COMPLETE, 1);
    return checkingSearcher.missingFieldsCount + checkingSearcher.missingTermsCount;
  }

  static final class CheckingIndexSearcher extends IndexSearcher {
    final StatsSource statsSource;
    final Consumer<Term> missingTermStats;
    final Consumer<String> missingFieldStats;
    int missingTermsCount, missingFieldsCount;

    CheckingIndexSearcher(StatsSource statsSource, IndexReader reader, Consumer<Term> missingTermStats, Consumer<String> missingFieldStats) {
      super(reader);
      this.statsSource = statsSource;
      this.missingTermStats = missingTermStats;
      this.missingFieldStats = missingFieldStats;
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
      if (statsSource.termStatistics(null, term, docFreq, totalTermFreq) == null) {
        missingTermStats.accept(term);
        missingTermsCount++;
      }
      return super.termStatistics(term, docFreq, totalTermFreq);
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
      if (statsSource.collectionStatistics(null, field) == null) {
        missingFieldStats.accept(field);
        missingFieldsCount++;
      }
      return super.collectionStatistics(field);
    }
  }
}
