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

import java.lang.invoke.MethodHandles;

import java.util.List;

import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation that simply ignores global term statistics, and always
 * uses local term statistics.
 */
public class LocalStatsCache extends StatsCache {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected StatsSource doGet(SolrQueryRequest req) {
    log.debug("## GET {}", req);
    return new LocalStatsSource(statsCacheMetrics);
  }

  // by returning null we don't create additional round-trip request.
  @Override
  protected ShardRequest doRetrieveStatsRequest(ResponseBuilder rb) {
    log.debug("## RSR {}", rb.req);
    // already incremented the stats - decrement it now
    statsCacheMetrics.retrieveStats.decrement();
    return null;
  }

  @Override
  protected void doMergeToGlobalStats(SolrQueryRequest req,
          List<ShardResponse> responses) {
    if (log.isDebugEnabled()) {
      log.debug("## MTGS {}", req);
      for (ShardResponse r : responses) {
        log.debug(" - {}", r);
      }
    }
  }

  @Override
  protected void doReturnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher) {
    log.debug("## RLS {}", rb.req);
  }

  @Override
  protected void doReceiveGlobalStats(SolrQueryRequest req) {
    log.debug("## RGS {}", req);
  }

  @Override
  protected void doSendGlobalStats(ResponseBuilder rb, ShardRequest outgoing) {
    log.debug("## SGS {}", outgoing);
  }
}
