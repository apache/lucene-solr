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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermStatistics;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
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
 */
public class ExactStatsCache extends StatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // experimenting with strategy that takes more RAM, but also doesn't share memory
  // across threads
  private static final String CURRENT_GLOBAL_COL_STATS = "org.apache.solr.stats.currentGlobalColStats";
  private static final String CURRENT_GLOBAL_TERM_STATS = "org.apache.solr.stats.currentGlobalTermStats";
  private static final String PER_SHARD_TERM_STATS = "org.apache.solr.stats.perShardTermStats";
  private static final String PER_SHARD_COL_STATS = "org.apache.solr.stats.perShardColStats";

  @Override
  public StatsSource get(SolrQueryRequest req) {
    Map<String,CollectionStats> currentGlobalColStats = (Map<String,CollectionStats>) req.getContext().get(CURRENT_GLOBAL_COL_STATS);
    Map<String,TermStats> currentGlobalTermStats = (Map<String,TermStats>) req.getContext().get(CURRENT_GLOBAL_TERM_STATS);
    if (currentGlobalColStats == null) {
     currentGlobalColStats = Collections.emptyMap();
    }
    if (currentGlobalTermStats == null) {
      currentGlobalTermStats = Collections.emptyMap();
    }
    log.debug("Returning StatsSource. Collection stats={}, Term stats size= {}", currentGlobalColStats, currentGlobalTermStats.size());
    return new ExactStatsSource(currentGlobalTermStats, currentGlobalColStats);
  }

  @Override
  public void init(PluginInfo info) {}

  @Override
  public ShardRequest retrieveStatsRequest(ResponseBuilder rb) {
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = ShardRequest.PURPOSE_GET_TERM_STATS;
    sreq.params = new ModifiableSolrParams(rb.req.getParams());
    // don't pass through any shards param
    sreq.params.remove(ShardParams.SHARDS);
    return sreq;
  }

  @Override
  public void mergeToGlobalStats(SolrQueryRequest req, List<ShardResponse> responses) {
    Set<Object> allTerms = new HashSet<>();
    for (ShardResponse r : responses) {
      log.debug("Merging to global stats, shard={}, response={}", r.getShard(), r.getSolrResponse().getResponse());
      String shard = r.getShard();
      SolrResponse res = r.getSolrResponse();
      NamedList<Object> nl = res.getResponse();

      // TODO: nl == null if not all shards respond (no server hosting shard)
      String termStatsString = (String) nl.get(TERM_STATS_KEY);
      if (termStatsString != null) {
        addToPerShardTermStats(req, shard, termStatsString);
      }
      List<Object> terms = nl.getAll(TERMS_KEY);
      allTerms.addAll(terms);
      String colStatsString = (String) nl.get(COL_STATS_KEY);
      Map<String,CollectionStats> colStats = StatsUtil.colStatsMapFromString(colStatsString);
      if (colStats != null) {
        addToPerShardColStats(req, shard, colStats);
      }
    }
    if (allTerms.size() > 0) {
      req.getContext().put(TERMS_KEY, Lists.newArrayList(allTerms));
    }
    if (log.isDebugEnabled()) printStats(req);
  }

  protected void addToPerShardColStats(SolrQueryRequest req, String shard, Map<String,CollectionStats> colStats) {
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) req.getContext().get(PER_SHARD_COL_STATS);
    if (perShardColStats == null) {
      perShardColStats = new HashMap<>();
      req.getContext().put(PER_SHARD_COL_STATS, perShardColStats);
    }
    perShardColStats.put(shard, colStats);
  }

  protected void printStats(SolrQueryRequest req) {
    Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().get(PER_SHARD_TERM_STATS);
    if (perShardTermStats == null) {
      perShardTermStats = Collections.emptyMap();
    }
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) req.getContext().get(PER_SHARD_COL_STATS);
    if (perShardColStats == null) {
      perShardColStats = Collections.emptyMap();
    }
    log.debug("perShardColStats={}, perShardTermStats={}", perShardColStats, perShardTermStats);
  }

  protected void addToPerShardTermStats(SolrQueryRequest req, String shard, String termStatsString) {
    Map<String,TermStats> termStats = StatsUtil.termStatsMapFromString(termStatsString);
    if (termStats != null) {
      Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().get(PER_SHARD_TERM_STATS);
      if (perShardTermStats == null) {
        perShardTermStats = new HashMap<>();
        req.getContext().put(PER_SHARD_TERM_STATS, perShardTermStats);
      }
      perShardTermStats.put(shard, termStats);
    }
  }

  @Override
  public void returnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher) {
    Query q = rb.getQuery();
    try {
      HashSet<Term> terms = new HashSet<>();
      searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1).extractTerms(terms);
      IndexReaderContext context = searcher.getTopReaderContext();
      HashMap<String,TermStats> statsMap = new HashMap<>();
      HashMap<String,CollectionStats> colMap = new HashMap<>();
      for (Term t : terms) {
        TermStates termStates = TermStates.build(context, t, true);

        if (!colMap.containsKey(t.field())) { // collection stats for this field
          CollectionStatistics collectionStatistics = searcher.localCollectionStatistics(t.field());
          if (collectionStatistics != null) {
            colMap.put(t.field(), new CollectionStats(collectionStatistics));
          }
        }

        TermStatistics tst = searcher.localTermStatistics(t, termStates);
        if (tst == null) { // skip terms that are not present here
          continue;
        }

        statsMap.put(t.toString(), new TermStats(t.field(), tst));
        rb.rsp.add(TERMS_KEY, t.toString());
      }
      if (statsMap.size() != 0) { //Don't add empty keys
        String termStatsString = StatsUtil.termStatsMapToString(statsMap);
        rb.rsp.add(TERM_STATS_KEY, termStatsString);
        if (log.isDebugEnabled()) {
          log.debug("termStats={}, terms={}, numDocs={}", termStatsString, terms, searcher.maxDoc());
        }
      }
      if (colMap.size() != 0){
        String colStatsString = StatsUtil.colStatsMapToString(colMap);
        rb.rsp.add(COL_STATS_KEY, colStatsString);
        if (log.isDebugEnabled()) {
          log.debug("collectionStats={}, terms={}, numDocs={}", colStatsString, terms, searcher.maxDoc());
        }
      }
    } catch (IOException e) {
      log.error("Error collecting local stats, query='" + q.toString() + "'", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error collecting local stats.", e);
    }
  }

  @Override
  public void sendGlobalStats(ResponseBuilder rb, ShardRequest outgoing) {
    outgoing.purpose |= ShardRequest.PURPOSE_SET_TERM_STATS;
    ModifiableSolrParams params = outgoing.params;
    List<String> terms = (List<String>) rb.req.getContext().get(TERMS_KEY);
    if (terms != null) {
      Set<String> fields = new HashSet<>();
      for (String t : terms) {
        String[] fv = t.split(":");
        fields.add(fv[0]);
      }
      Map<String,TermStats> globalTermStats = new HashMap<>();
      Map<String,CollectionStats> globalColStats = new HashMap<>();
      // aggregate collection stats, only for the field in terms

      for (String shard : rb.shards) {
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
      for (String t : terms) {
        params.add(TERMS_KEY, t);
        for (String shard : rb.shards) {
          TermStats termStats = getPerShardTermStats(rb.req, t, shard);
          if (termStats == null || termStats.docFreq == 0) {
            continue;
          }
          TermStats g = globalTermStats.get(t);
          if (g == null) {
            g = new TermStats(t);
            globalTermStats.put(t, g);
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
    Map<String,Map<String,CollectionStats>> perShardColStats = (Map<String,Map<String,CollectionStats>>) rb.req.getContext().get(PER_SHARD_COL_STATS);
    if (perShardColStats == null) {
      perShardColStats = Collections.emptyMap();
    }
    return perShardColStats.get(shard);
  }

  protected TermStats getPerShardTermStats(SolrQueryRequest req, String t, String shard) {
    Map<String,Map<String,TermStats>> perShardTermStats = (Map<String,Map<String,TermStats>>) req.getContext().get(PER_SHARD_TERM_STATS);
    if (perShardTermStats == null) {
      perShardTermStats = Collections.emptyMap();
    }
    Map<String,TermStats> cache = perShardTermStats.get(shard);
    return (cache != null) ? cache.get(t) : null; //Term doesn't exist in shard
  }

  @Override
  public void receiveGlobalStats(SolrQueryRequest req) {
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
    Map<String,CollectionStats> currentGlobalColStats = (Map<String,CollectionStats>) req.getContext().get(CURRENT_GLOBAL_COL_STATS);
    if (currentGlobalColStats == null) {
      currentGlobalColStats = new HashMap<>();
      req.getContext().put(CURRENT_GLOBAL_COL_STATS, currentGlobalColStats);
    }
    currentGlobalColStats.put(e.getKey(), e.getValue());
  }

  protected void addToGlobalTermStats(SolrQueryRequest req, Entry<String,TermStats> e) {
    Map<String,TermStats> currentGlobalTermStats = (Map<String,TermStats>) req.getContext().get(CURRENT_GLOBAL_TERM_STATS);
    if (currentGlobalTermStats == null) {
      currentGlobalTermStats = new HashMap<>();
      req.getContext().put(CURRENT_GLOBAL_TERM_STATS, currentGlobalTermStats);
    }
    currentGlobalTermStats.put(e.getKey(), e.getValue());
  }

  protected static class ExactStatsSource extends StatsSource {
    private final Map<String,TermStats> termStatsCache;
    private final Map<String,CollectionStats> colStatsCache;

    public ExactStatsSource(Map<String,TermStats> termStatsCache,
                            Map<String,CollectionStats> colStatsCache) {
      this.termStatsCache = termStatsCache;
      this.colStatsCache = colStatsCache;
    }

    public TermStatistics termStatistics(SolrIndexSearcher localSearcher, Term term, TermStates context)
        throws IOException {
      TermStats termStats = termStatsCache.get(term.toString());
      // TermStats == null is also true if term has no docFreq anyway,
      // see returnLocalStats, if docFreq == 0, they are not added anyway
      // Not sure we need a warning here
      if (termStats == null) {
        log.debug("Missing global termStats info for term={}, using local stats", term);
        return localSearcher.localTermStatistics(term, context);
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
        return localSearcher.localCollectionStatistics(field);
      } else {
        return colStats.toCollectionStatistics();
      }
    }
  }
}
