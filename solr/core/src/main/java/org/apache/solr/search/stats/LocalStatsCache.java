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

import org.apache.solr.core.PluginInfo;
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
  public StatsSource get(SolrQueryRequest req) {
    log.debug("## GET {}", req);
    return new LocalStatsSource();
  }

  @Override
  public void init(PluginInfo info) {
  }

  // by returning null we don't create additional round-trip request.
  @Override
  public ShardRequest retrieveStatsRequest(ResponseBuilder rb) {
    log.debug("## RDR {}", rb.req);
    return null;
  }

  @Override
  public void mergeToGlobalStats(SolrQueryRequest req,
          List<ShardResponse> responses) {
    if (log.isDebugEnabled()) {
      log.debug("## MTGD {}", req);
      for (ShardResponse r : responses) {
        log.debug(" - {}", r);
      }
    }
  }

  @Override
  public void returnLocalStats(ResponseBuilder rb, SolrIndexSearcher searcher) {
    log.debug("## RLD {}", rb.req);
  }

  @Override
  public void receiveGlobalStats(SolrQueryRequest req) {
    log.debug("## RGD {}", req);
  }

  @Override
  public void sendGlobalStats(ResponseBuilder rb, ShardRequest outgoing) {
    log.debug("## SGD {}", outgoing);
  }
}
