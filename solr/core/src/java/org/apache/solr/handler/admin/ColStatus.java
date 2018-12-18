package org.apache.solr.handler.admin;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ColStatus {
  private static final Logger log = LoggerFactory.getLogger(ColStatus.class);

  private final ZkStateReader zkStateReader;
  private final ZkNodeProps props;
  private final SolrClientCache solrClientCache;

  public static final String FIELD_INFOS_PROP = "fieldInfos";
  public static final String SEGMENTS_PROP = "segments";
  public static final String DV_STATS_PROP = "dvStats";

  public ColStatus(HttpClient httpClient, ZkStateReader zkStateReader, ZkNodeProps props) {
    this.props = props;
    this.solrClientCache = new SolrClientCache(httpClient);
    this.zkStateReader = zkStateReader;
  }

  public void getColStatus(NamedList<Object> results)
      throws KeeperException, InterruptedException {
    ClusterState clusterState = zkStateReader.getClusterState();
    Collection<String> collections;
    String col = props.getStr(ZkStateReader.COLLECTION_PROP);
    if (col == null) {
      collections = new HashSet<>(clusterState.getCollectionsMap().keySet());
    } else {
      collections = Collections.singleton(col);
    }
    boolean withFieldInfos = props.getBool(FIELD_INFOS_PROP, false);
    boolean withSegments = props.getBool(SEGMENTS_PROP, false);
    boolean withDVStats = props.getBool(DV_STATS_PROP, false);
    for (String collection : collections) {
      DocCollection coll = clusterState.getCollectionOrNull(collection);
      if (coll == null) {
        continue;
      }
      SimpleOrderedMap<Object> colMap = new SimpleOrderedMap<>();
      if (coll.getReplicationFactor() != null) {
        colMap.add("replicationFactor", coll.getReplicationFactor());
      }
      colMap.add("stateFormat", coll.getStateFormat());
      colMap.add("znodeVersion", coll.getZNodeVersion());
      colMap.add("autoAddReplicas", coll.getAutoAddReplicas());
      colMap.add("maxShardsPerNode", coll.getMaxShardsPerNode());
      colMap.add("activeSlices", coll.getActiveSlices().size());
      colMap.add("inactiveSlices", coll.getSlices().size() - coll.getActiveSlices().size());
      results.add(collection, colMap);

      Set<String> nonCompliant = new TreeSet<>();

      SimpleOrderedMap<Object> slices = new SimpleOrderedMap<>();
      for (Slice s : coll.getSlices()) {
        SimpleOrderedMap<Object> sliceMap = new SimpleOrderedMap<>();
        slices.add(s.getName(), sliceMap);
        SimpleOrderedMap<Object> replicaMap = new SimpleOrderedMap<>();
        int totalReplicas = s.getReplicas().size();
        int activeReplicas = 0;
        int downReplicas = 0;
        int recoveringReplicas = 0;
        int recoveryFailedReplicas = 0;
        for (Replica r : s.getReplicas()) {
          switch (r.getState()) {
            case ACTIVE:
              activeReplicas++;
              break;
            case DOWN:
              downReplicas++;
              break;
            case RECOVERING:
              recoveringReplicas++;
              break;
            case RECOVERY_FAILED:
              recoveryFailedReplicas++;
              break;
          }
        }
        replicaMap.add("total", totalReplicas);
        replicaMap.add("active", activeReplicas);
        replicaMap.add("down", downReplicas);
        replicaMap.add("recovering", recoveringReplicas);
        replicaMap.add("recovery_failed", recoveryFailedReplicas);
        sliceMap.add("state", s.getState().toString());
        sliceMap.add("range", s.getRange().toString());
        Map<String, RoutingRule> rules = s.getRoutingRules();
        if (rules != null && !rules.isEmpty()) {
          sliceMap.add("routingRules", rules);
        }
        sliceMap.add("replicas", replicaMap);
        Replica leader = s.getLeader();
        if (leader == null) { // pick the first one
          leader = s.getReplicas().size() > 0 ? s.getReplicas().iterator().next() : null;
        }
        if (leader == null) {
          continue;
        }
        SimpleOrderedMap<Object> leaderMap = new SimpleOrderedMap<>();
        sliceMap.add("leader", leaderMap);
        leaderMap.add("coreNode", leader.getName());
        leaderMap.addAll(leader.getProperties());
        String url = ZkCoreNodeProps.getCoreUrl(leader);
        try (SolrClient client = solrClientCache.getHttpSolrClient(url)) {
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.add(CommonParams.QT, "/admin/segments");
          params.add("fieldInfos", "true");
          params.add(DV_STATS_PROP, String.valueOf(withDVStats));
          QueryRequest req = new QueryRequest(params);
          NamedList<Object> rsp = client.request(req);
          rsp.remove("responseHeader");
          leaderMap.add("segInfos", rsp);
          NamedList<Object> segs = (NamedList<Object>)rsp.get("segments");
          if (segs != null) {
            for (Map.Entry<String, Object> entry : segs) {
              NamedList<Object> fields = (NamedList<Object>)((NamedList<Object>)entry.getValue()).get("fields");
              if (fields != null) {
                for (Map.Entry<String, Object> fEntry : fields) {
                  Object nc = ((NamedList<Object>)fEntry.getValue()).get("nonCompliant");
                  if (nc != null) {
                    nonCompliant.add(fEntry.getKey());
                  }
                }
              }
              if (!withFieldInfos) {
                ((NamedList<Object>)entry.getValue()).remove("fields");
              }
            }
          }
          if (!withSegments) {
            rsp.remove("segments");
          }
        } catch (SolrServerException | IOException e) {
          log.warn("Error getting details of replica segments from " + url, e);
        }
      }
      if (nonCompliant.isEmpty()) {
        nonCompliant.add("(NONE)");
      }
      colMap.add("schemaNonCompliant", nonCompliant);
      colMap.add("slices", slices);
    }
  }
}
