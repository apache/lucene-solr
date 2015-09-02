package org.apache.solr.search.stats;

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

import java.util.List;

import org.apache.lucene.search.Weight;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;
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
 * (where this data is maintained and updated from the central cache).
 * </p>
 */
public abstract class StatsCache implements PluginInfoInitialized {
  // TODO: decouple use in response from use in request context for these keys
  /**
   * Map of terms and {@link TermStats}.
   */
  public static final String TERM_STATS_KEY = "org.apache.solr.stats.termStats";
  /**
   * Value of {@link CollectionStats}.
   */
  public static final String COL_STATS_KEY = "org.apache.solr.stats.colStats";
  /**
   * List of terms in the query.
   */
  public static final String TERMS_KEY = "org.apache.solr.stats.terms";

  /**
   * Creates a {@link ShardRequest} to retrieve per-shard stats related to the
   * current query and the current state of the requester's {@link StatsCache}.
   *
   * @param rb contains current request
   * @return shard request to retrieve stats for terms in the current request,
   * or null if no additional request is needed (e.g. if the information
   * in global cache is already sufficient to satisfy this request).
   */
  public abstract ShardRequest retrieveStatsRequest(ResponseBuilder rb);

  /**
   * Prepare a local (from the local shard) response to a "retrieve stats" shard
   * request.
   *
   * @param rb       response builder
   * @param searcher current local searcher
   */
  public abstract void returnLocalStats(ResponseBuilder rb,
                                        SolrIndexSearcher searcher);

  /**
   * Process shard responses that contain partial local stats. Usually this
   * entails combining per-shard stats for each term.
   *
   * @param req       query request
   * @param responses responses from shards containing local stats for each shard
   */
  public abstract void mergeToGlobalStats(SolrQueryRequest req,
                                          List<ShardResponse> responses);

  /**
   * Receive global stats data from the master and update a local cache of stats
   * with this global data. This event occurs either as a separate request, or
   * together with the regular query request, in which case this method is
   * called first, before preparing a {@link QueryCommand} to be submitted to
   * the local {@link SolrIndexSearcher}.
   *
   * @param req query request with global stats data
   */
  public abstract void receiveGlobalStats(SolrQueryRequest req);

  /**
   * Prepare global stats data to be sent out to shards in this request.
   *
   * @param rb       response builder
   * @param outgoing shard request to be sent
   */
  public abstract void sendGlobalStats(ResponseBuilder rb, ShardRequest outgoing);

  /**
   * Prepare local {@link StatsSource} to provide stats information to perform
   * local scoring (to be precise, to build a local {@link Weight} from the
   * query).
   *
   * @param req query request
   * @return an instance of {@link StatsSource} to use in creating a query
   * {@link Weight}
   */
  public abstract StatsSource get(SolrQueryRequest req);

}
