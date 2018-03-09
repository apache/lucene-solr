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
package org.apache.solr.cloud.autoscaling;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class checks whether there are shards that have been inactive for a long
 * time (which usually means they are left-overs from shard splitting) and requests their removal
 * after their cleanup TTL period elapsed.
 * <p>Shard delete requests are put into the {@link ActionContext}'s properties
 * with the key name "operations". The value is a List of SolrRequest objects.</p>
 */
public class InactiveShardPlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String TTL_PROP = "ttl";

  public static final int DEFAULT_TTL_SECONDS = 3600 * 24 * 2;

  private int cleanupTTL;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    String cleanupStr = args.getOrDefault(TTL_PROP, String.valueOf(DEFAULT_TTL_SECONDS));
    try {
      cleanupTTL = Integer.parseInt(cleanupStr);
    } catch (Exception e) {
      log.warn("Invalid " + TTL_PROP + " value: '" + cleanupStr + "', using default " + DEFAULT_TTL_SECONDS);
      cleanupTTL = DEFAULT_TTL_SECONDS;
    }
    if (cleanupTTL < 0) {
      log.warn("Invalid " + TTL_PROP + " value: '" + cleanupStr + "', using default " + DEFAULT_TTL_SECONDS);
      cleanupTTL = DEFAULT_TTL_SECONDS;
    }
  }

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    SolrCloudManager cloudManager = context.getCloudManager();
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
    Map<String, List<String>> cleanup = new LinkedHashMap<>();
    Map<String, List<String>> inactive = new LinkedHashMap<>();
    state.forEachCollection(coll ->
      coll.getSlices().forEach(s -> {
        if (Slice.State.INACTIVE.equals(s.getState())) {
          inactive.computeIfAbsent(coll.getName(), c -> new ArrayList<>()).add(s.getName());
          String tstampStr = s.getStr(ZkStateReader.STATE_TIMESTAMP_PROP);
          if (tstampStr == null || tstampStr.isEmpty()) {
            return;
          }
          long timestamp = Long.parseLong(tstampStr);
          // this timestamp uses epoch time
          long currentTime = cloudManager.getTimeSource().getEpochTime();
          long delta = TimeUnit.NANOSECONDS.toSeconds(currentTime - timestamp);
          log.debug("{}/{}: tstamp={}, time={}, delta={}", coll.getName(), s.getName(), timestamp, currentTime, delta);
          if (delta > cleanupTTL) {
            log.debug("-- delete inactive {} / {}", coll.getName(), s.getName());
            List<SolrRequest> operations = (List<SolrRequest>)context.getProperties().computeIfAbsent("operations", k -> new ArrayList<>());
            operations.add(CollectionAdminRequest.deleteShard(coll.getName(), s.getName()));
            cleanup.computeIfAbsent(coll.getName(), c -> new ArrayList<>()).add(s.getName());
          }
        }
      })
    );
    if (!cleanup.isEmpty()) {
      Map<String, Object> results = new LinkedHashMap<>();
      results.put("inactive", inactive);
      results.put("cleanup", cleanup);
      context.getProperties().put(getName(), results);
    }
  }
}