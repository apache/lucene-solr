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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements exact caching of statistics. It requires an additional
 * round-trip to parse query at shard servers, and return term statistics for
 * query terms (and collection statistics for term fields).
 * <p>Global statistics are cached in the current request's context and discarded
 * once the processing of the current request is complete. There's no support for
 * longer-term caching, and each request needs to build the global statistics from scratch,
 * even for repeating queries.</p>
 */
public class ExactStatsCache extends StatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CURRENT_GLOBAL_COL_STATS = "solr.stats.globalCol";
  private static final String CURRENT_GLOBAL_TERM_STATS = "solr.stats.globalTerm";
  private static final String PER_SHARD_TERM_STATS = "solr.stats.shardTerm";
  private static final String PER_SHARD_COL_STATS = "solr.stats.shardCol";

  @Override
  protected StatsSource doGet(SolrQueryRequest req) {
    @SuppressWarnings({"unchecked"})
    Map<String,CollectionStats> currentGlobalColStats = (Map<String,CollectionStats>) req.getContext().getOrDefault(CURRENT_GLOBAL_COL_STATS, Collections.emptyMap());
    @SuppressWarnings({"unchecked"})
    Map<String,TermStats> currentGlobalTermStats = (Map<String,TermStats>) req.getContext().getOrDefault(CURRENT_GLOBAL_TERM_STATS, Collections.emptyMap());
    if (log.isDebugEnabled()) {
      log.debug("Returning StatsSource. Collection stats={}, Term stats size= {}", currentGlobalColStats, currentGlobalTermStats.size());
    }
    return new ExactStatsSource(statsCacheMetrics, currentGlobalTermStats, currentGlobalColStats);
  }

  @Override
  protected ShardRequest doRetrieveStatsRequest(ResponseBuilder rb) {
    // always request shard statistics
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TERM_STATS;
    sreq.params = new ModifiableSolrParams(rb.req.getParams());
    // don't pass through any shards param
    sreq.params.remove(ShardParams.SHARDS);
    return sreq;
  }

  @Override
  protected void doMergeToGlobalStats(SolrQueryRequest req, List<ShardResponse> responses) {
    Set<Term> allTerms = new HashSet<>();
    for (ShardResponse r : responses) {
      if ("true".equalsIgnoreCase(req.getParams().get(ShardParams.SHARDS_TOLERANT)) && r.getException() != null) {
        // Can't expect stats if there was an exception for this request on any shard
        // this should only happen when using shards.tolerant=true
        log.debug("Exception shard response={}", r);
        continue;
      }
      if (log.isDebugEnabled()) {
        log.debug("Merging to global stats, shard={}, response={}", r.getShard(), r.getSolrResponse().getResponse());
      }
      // response's "shard" is really a shardURL, or even a list of URLs
      String shard = r.getShard();
      SolrResponse res = r.getSolrResponse();
      if (res.getException() != null) {
        log.debug("Exception response={}", res);
        continue;
      }
      if (res.getResponse().get(ShardParams.SHARD_NAME) != null) {
        shard = (String) res.getResponse().get(ShardParams.SHARD_NAME);
      }
      NamedList<Object> nl = res.getResponse();

      String termStatsString = (String) nl.get(TERM_STATS_KEY);
      if (termStatsString != null) {
        addToPerShardTermStats(req, shard, termStatsString);
      }
      Set<Term> terms = StatsUtil.termsFromEncodedString((String) nl.get(TERMS_KEY));
      allTerms.addAll(terms);
      String colStatsString = (String) nl.get(COL_STATS_KEY);
      Map<String,CollectionStats> colStats = StatsUtil.colStatsMapFromString(colStatsString);
      if (colStats != null) {
        addToPerShardColStats(req, shard, colStats);
      }
    }
    if (allTerms.size() > 0) {
      req.getContext().put(TERMS_KEY, StatsUtil.termsToEncodedString(allTerms));
    }
    if (log.isDebugEnabled()) printStats(req);
  }

  protected void addToPerShardColStats(SolrQueryRequest req, String shard, Map<String,CollectionStats> colStats) {
    @SuppressWarnings({"unchecked"})
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) req.getContext().computeIfAbsent(PER_SHARD_COL_STATS, o -> new HashMap<Object,Object>());
    perShardColStats.put(shard, colStats);
  }

  protected void printStats(SolrQueryRequest req) {
    @SuppressWarnings({"unchecked"})
    Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().getOrDefault(PER_SHARD_TERM_STATS, Collections.emptyMap());
    @SuppressWarnings({"unchecked"})
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) req.getContext().getOrDefault(PER_SHARD_COL_STATS, Collections.emptyMap());
    log.debug("perShardColStats={}, perShardTermStats={}", perShardColStats, perShardTermStats);
  }

  protected void addToPerShardTermStats(SolrQueryRequest req, String shard, String termStatsString) {
    Map<String,TermStats> termStats = StatsUtil.termStatsMapFromString(termStatsString);
    if (termStats != null) {
      @SuppressWarnings({"unchecked"})
      Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().computeIfAbsent(PER_SHARD_TERM_STATS, o -> new HashMap<Object,Object>());
      perShardTermStats.put(shard, termStats);
    }
  }

  @Override
  protected void doReturnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher) {
    Query q = rb.getQuery();
    try {
      Set<Term> additionalTerms = StatsUtil.termsFromEncodedString(rb.req.getParams().get(TERMS_KEY));
      Set<String> additionalFields = StatsUtil.fieldsFromString(rb.req.getParams().get(FIELDS_KEY));
      HashSet<Term> terms = new HashSet<>();
      HashMap<String,TermStats> statsMap = new HashMap<>();
      HashMap<String,CollectionStats> colMap = new HashMap<>();
      IndexSearcher statsCollectingSearcher = new IndexSearcher(searcher.getIndexReader()){
        @Override
        public CollectionStatistics collectionStatistics(String field) throws IOException {
          CollectionStatistics cs = super.collectionStatistics(field);
          if (cs != null) {
            colMap.put(field, new CollectionStats(cs));
          }
          return cs;
        }

        @Override
        public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
          TermStatistics ts = super.termStatistics(term, docFreq, totalTermFreq);
          terms.add(term);
          statsMap.put(term.toString(), new TermStats(term.field(), ts));
          return ts;
        }
      };
      statsCollectingSearcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
      for (String field : additionalFields) {
        if (colMap.containsKey(field)) {
          continue;
        }
        statsCollectingSearcher.collectionStatistics(field);
      }
      for (Term term : additionalTerms) {
        statsCollectingSearcher.createWeight(searcher.rewrite(new TermQuery(term)), ScoreMode.COMPLETE, 1);
      }

      CloudDescriptor cloudDescriptor = searcher.getCore().getCoreDescriptor().getCloudDescriptor();
      if (cloudDescriptor != null) {
        rb.rsp.add(ShardParams.SHARD_NAME, cloudDescriptor.getShardId());
      }
      if (!terms.isEmpty()) {
        rb.rsp.add(TERMS_KEY, StatsUtil.termsToEncodedString(terms));
      }
      if (!statsMap.isEmpty()) { //Don't add empty keys
        String termStatsString = StatsUtil.termStatsMapToString(statsMap);
        rb.rsp.add(TERM_STATS_KEY, termStatsString);
        if (log.isDebugEnabled()) {
          log.debug("termStats={}, terms={}, numDocs={}", termStatsString, terms, searcher.maxDoc());
        }
      }
      if (!colMap.isEmpty()) {
        String colStatsString = StatsUtil.colStatsMapToString(colMap);
        rb.rsp.add(COL_STATS_KEY, colStatsString);
        if (log.isDebugEnabled()) {
          log.debug("collectionStats={}, terms={}, numDocs={}", colStatsString, terms, searcher.maxDoc());
        }
      }
    } catch (IOException e) {
      log.error("Error collecting local stats, query='{}'", q, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error collecting local stats.", e);
    }
  }

  @Override
  protected void doSendGlobalStats(ResponseBuilder rb, ShardRequest outgoing) {
    ModifiableSolrParams params = outgoing.params;
    Set<Term> terms = StatsUtil.termsFromEncodedString((String) rb.req.getContext().get(TERMS_KEY));
    if (!terms.isEmpty()) {
      Set<String> fields = terms.stream().map(t -> t.field()).collect(Collectors.toSet());
      Map<String,TermStats> globalTermStats = new HashMap<>();
      Map<String,CollectionStats> globalColStats = new HashMap<>();
      // aggregate collection stats, only for the field in terms
      String collectionName = rb.req.getCore().getCoreDescriptor().getCollectionName();
      if (collectionName == null) {
        collectionName = rb.req.getCore().getCoreDescriptor().getName();
      }
      List<String> shards = new ArrayList<>();
      for (String shardUrl : rb.shards) {
        String shard = StatsUtil.shardUrlToShard(collectionName, shardUrl);
        if (shard == null) {
          log.warn("Can't determine shard from collectionName={} and shardUrl={}, skipping...", collectionName, shardUrl);
          continue;
        } else {
          shards.add(shard);
        }
      }
      for (String shard : shards) {
        Map<String,CollectionStats> s = getPerShardColStats(rb, shard);
        if (s == null) {
          continue;
        }
        for (Entry<String,CollectionStats> e : s.entrySet()) {
          if (!fields.contains(e.getKey())) { // skip non-relevant fields
            continue;
          }
          CollectionStats g = globalColStats.get(e.getKey());
          if (g == null) {
            g = new CollectionStats(e.getKey());
            globalColStats.put(e.getKey(), g);
          }
          g.add(e.getValue());
        }
      }
      params.add(COL_STATS_KEY, StatsUtil.colStatsMapToString(globalColStats));
      // sum up only from relevant shards
      params.add(TERMS_KEY, StatsUtil.termsToEncodedString(terms));
      for (Term t : terms) {
        String term = t.toString();
        for (String shard : shards) {
          TermStats termStats = getPerShardTermStats(rb.req, term, shard);
          if (termStats == null || termStats.docFreq == 0) {
            continue;
          }
          TermStats g = globalTermStats.get(term);
          if (g == null) {
            g = new TermStats(term);
            globalTermStats.put(term, g);
          }
          g.add(termStats);
        }
      }
      log.debug("terms={}, termStats={}", terms, globalTermStats);
      // need global TermStats here...
      params.add(TERM_STATS_KEY, StatsUtil.termStatsMapToString(globalTermStats));
    }
  }

  protected Map<String,CollectionStats> getPerShardColStats(ResponseBuilder rb, String shard) {
    @SuppressWarnings({"unchecked"})
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) rb.req.getContext().getOrDefault(PER_SHARD_COL_STATS, Collections.emptyMap());
    return perShardColStats.get(shard);
  }

  protected TermStats getPerShardTermStats(SolrQueryRequest req, String t, String shard) {
    @SuppressWarnings({"unchecked"})
    Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().getOrDefault(PER_SHARD_TERM_STATS, Collections.emptyMap());
    Map<String,TermStats> cache = perShardTermStats.get(shard);
    return (cache != null) ? cache.get(t) : null; //Term doesn't exist in shard
  }

  @Override
  protected void doReceiveGlobalStats(SolrQueryRequest req) {
    String globalTermStats = req.getParams().get(TERM_STATS_KEY);
    String globalColStats = req.getParams().get(COL_STATS_KEY);
    if (globalColStats != null) {
      Map<String,CollectionStats> colStats = StatsUtil.colStatsMapFromString(globalColStats);
      if (colStats != null) {
        for (Entry<String,CollectionStats> e : colStats.entrySet()) {
          addToGlobalColStats(req, e);
        }
      }
    }
    log.debug("Global collection stats={}", globalColStats);
    if (globalTermStats == null) return;
    Map<String,TermStats> termStats = StatsUtil.termStatsMapFromString(globalTermStats);
    if (termStats != null) {
      for (Entry<String,TermStats> e : termStats.entrySet()) {
        addToGlobalTermStats(req, e);
      }
    }
  }

  protected void addToGlobalColStats(SolrQueryRequest req,
                                     Entry<String,CollectionStats> e) {
    @SuppressWarnings({"unchecked"})
    Map<String,CollectionStats> currentGlobalColStats = (Map<String,CollectionStats>) req.getContext().computeIfAbsent(CURRENT_GLOBAL_COL_STATS, o -> new HashMap<Object,Object>());
    currentGlobalColStats.put(e.getKey(), e.getValue());
  }

  protected void addToGlobalTermStats(SolrQueryRequest req, Entry<String,TermStats> e) {
    @SuppressWarnings({"unchecked"})
    Map<String,TermStats> currentGlobalTermStats = (Map<String,TermStats>) req.getContext().computeIfAbsent(CURRENT_GLOBAL_TERM_STATS, o -> new HashMap<Object,Object>());
    currentGlobalTermStats.put(e.getKey(), e.getValue());
  }

  protected static class ExactStatsSource extends StatsSource {
    private final Map<String,TermStats> termStatsCache;
    private final Map<String,CollectionStats> colStatsCache;
    private final StatsCacheMetrics metrics;

    public ExactStatsSource(StatsCacheMetrics metrics, Map<String,TermStats> termStatsCache,
                            Map<String,CollectionStats> colStatsCache) {
      this.metrics = metrics;
      this.termStatsCache = termStatsCache;
      this.colStatsCache = colStatsCache;
    }

    public TermStatistics termStatistics(SolrIndexSearcher localSearcher, Term term, int docFreq, long totalTermFreq)
        throws IOException {
      TermStats termStats = termStatsCache.get(term.toString());
      // TermStats == null is also true if term has no docFreq anyway,
      // see returnLocalStats, if docFreq == 0, they are not added anyway
      // Not sure we need a warning here
      if (termStats == null) {
        log.debug("Missing global termStats info for term={}, using local stats", term);
        metrics.missingGlobalTermStats.increment();
        return localSearcher != null ? localSearcher.localTermStatistics(term, docFreq, totalTermFreq) : null;
      } else {
        return termStats.toTermStatistics();
      }
    }

    @Override
    public CollectionStatistics collectionStatistics(SolrIndexSearcher localSearcher, String field)
        throws IOException {
      CollectionStats colStats = colStatsCache.get(field);
      if (colStats == null) {
        log.debug("Missing global colStats info for field={}, using local", field);
        metrics.missingGlobalFieldStats.increment();
        return localSearcher != null ? localSearcher.localCollectionStatistics(field) : null;
      } else {
        return colStats.toCollectionStatistics();
      }
    }
  }
}
