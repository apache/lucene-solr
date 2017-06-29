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

package org.apache.solr.client.solrj.impl;


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.autoscaling.ClusterDataProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.ReplicaInfo;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.RemoteCallback;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that implements {@link ClusterStateProvider} accepting a SolrClient
 */
public class SolrClientDataProvider implements ClusterDataProvider, MapWriter {

  private final CloudSolrClient solrClient;
  private final Map<String, Map<String, Map<String, List<ReplicaInfo>>>> data = new HashMap<>();
  private Set<String> liveNodes;
  private Map<String, Object> snitchSession = new HashMap<>();
  private Map<String, Map> nodeVsTags = new HashMap<>();

  public SolrClientDataProvider(CloudSolrClient solrClient) {
    this.solrClient = solrClient;
    ZkStateReader zkStateReader = solrClient.getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    this.liveNodes = clusterState.getLiveNodes();
    Map<String, ClusterState.CollectionRef> all = clusterState.getCollectionStates();
    all.forEach((collName, ref) -> {
      DocCollection coll = ref.get();
      if (coll == null) return;
      coll.forEachReplica((shard, replica) -> {
        Map<String, Map<String, List<ReplicaInfo>>> nodeData = data.get(replica.getNodeName());
        if (nodeData == null) data.put(replica.getNodeName(), nodeData = new HashMap<>());
        Map<String, List<ReplicaInfo>> collData = nodeData.get(collName);
        if (collData == null) nodeData.put(collName, collData = new HashMap<>());
        List<ReplicaInfo> replicas = collData.get(shard);
        if (replicas == null) collData.put(shard, replicas = new ArrayList<>());
        replicas.add(new ReplicaInfo(replica.getName(), collName, shard, new HashMap<>()));
      });
    });
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    ClusterState.CollectionRef state = solrClient.getClusterStateProvider().getState(coll);
    return state == null || state.get() == null ? null : (String) state.get().getProperties().get("policy");
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    AutoScalingSnitch snitch = new AutoScalingSnitch();
    ClientSnitchCtx ctx = new ClientSnitchCtx(null, node, snitchSession, solrClient);
    snitch.getTags(node, new HashSet<>(tags), ctx);
    nodeVsTags.put(node, ctx.getTags());
    return ctx.getTags();
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    return data.getOrDefault(node, Collections.emptyMap());//todo fill other details
  }

  @Override
  public Collection<String> getNodes() {
    return liveNodes;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("liveNodes", liveNodes);
    ew.put("replicaInfo", Utils.getDeepCopy(data, 5));
    ew.put("nodeValues", nodeVsTags);

  }

  static class ClientSnitchCtx
      extends SnitchContext {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    ZkClientClusterStateProvider zkClientClusterStateProvider;
    CloudSolrClient solrClient;

    public ClientSnitchCtx(SnitchInfo perSnitch,
                           String node, Map<String, Object> session,
                           CloudSolrClient solrClient) {
      super(perSnitch, node, session);
      this.solrClient = solrClient;
      this.zkClientClusterStateProvider = (ZkClientClusterStateProvider) solrClient.getClusterStateProvider();
    }


    @Override
    public Map getZkJson(String path) throws KeeperException, InterruptedException {
      return Utils.getJson(zkClientClusterStateProvider.getZkStateReader().getZkClient(), path, true);
    }

    public void invokeRemote(String node, ModifiableSolrParams params, String klas, RemoteCallback callback) {

    }

    public SimpleSolrResponse invoke(String solrNode, String path, SolrParams params)
        throws IOException, SolrServerException {
      String url = zkClientClusterStateProvider.getZkStateReader().getBaseUrlForNodeName(solrNode);

      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.GET, path, params);
      try (HttpSolrClient client = new HttpSolrClient.Builder()
          .withHttpClient(solrClient.getHttpClient())
          .withBaseSolrUrl(url)
          .withResponseParser(new BinaryResponseParser())
          .build()) {
        NamedList<Object> rsp = client.request(request);
        request.response.nl = rsp;
        return request.response;
      }
    }

  }

  //uses metrics API to get node information
  static class AutoScalingSnitch extends ImplicitSnitch {
    @Override
    protected void getRemoteInfo(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
      ClientSnitchCtx snitchContext = (ClientSnitchCtx) ctx;
      readSysProps(solrNode, requestedTags, snitchContext);
      Set<String> groups = new HashSet<>();
      List<String> prefixes = new ArrayList<>();
      if (requestedTags.contains(DISK)) {
        groups.add("solr.node");
        prefixes.add("CONTAINER.fs.usableSpace");
      }
      if (requestedTags.contains(CORES)) {
        groups.add("solr.core");
        prefixes.add("CORE.coreName");
      }
      if (requestedTags.contains(SYSLOADAVG)) {
        groups.add("solr.jvm");
        prefixes.add("os.systemLoadAverage");
      }
      if (requestedTags.contains(HEAPUSAGE)) {
        groups.add("solr.jvm");
        prefixes.add("memory.heap.usage");
      }
      if (groups.isEmpty() || prefixes.isEmpty()) return;

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("group", StrUtils.join(groups, ','));
      params.add("prefix", StrUtils.join(prefixes, ','));

      try {
        SimpleSolrResponse rsp = snitchContext.invoke(solrNode, CommonParams.METRICS_PATH, params);
        Map m = rsp.nl.asMap(4);
        if (requestedTags.contains(DISK)) {
          Number n = (Number) Utils.getObjectByPath(m, true, "metrics/solr.node/CONTAINER.fs.usableSpace");
          if (n != null) ctx.getTags().put(DISK, n.doubleValue() / 1024.0d / 1024.0d / 1024.0d);
        }
        if (requestedTags.contains(CORES)) {
          int count = 0;
          Map cores = (Map) m.get("metrics");
          for (Object o : cores.keySet()) {
            if (o.toString().startsWith("solr.core.")) count++;
          }
          ctx.getTags().put(CORES, count);
        }
        if (requestedTags.contains(SYSLOADAVG)) {
          Number n = (Number) Utils.getObjectByPath(m, true, "metrics/solr.jvm/os.systemLoadAverage");
          if (n != null) ctx.getTags().put(SYSLOADAVG, n.doubleValue() * 100.0d);
        }
        if (requestedTags.contains(HEAPUSAGE)) {
          Number n = (Number) Utils.getObjectByPath(m, true, "metrics/solr.jvm/memory.heap.usage");
          if (n != null) ctx.getTags().put(HEAPUSAGE, n.doubleValue() * 100.0d);
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
    }

    private void readSysProps(String solrNode, Set<String> requestedTags, ClientSnitchCtx snitchContext) {
      List<String> prefixes = null;
      ModifiableSolrParams params;
      List<String> sysProp = null;
      for (String tag : requestedTags) {
        if (!tag.startsWith(SYSPROP)) continue;
        if (sysProp == null) {
          prefixes = new ArrayList<>();
          sysProp = new ArrayList<>();
          prefixes.add("system.properties");
        }
        sysProp.add(tag.substring(SYSPROP.length()));
      }

      if (sysProp == null) return;
      params = new ModifiableSolrParams();
      params.add("prefix", StrUtils.join(prefixes, ','));
      for (String s : sysProp) params.add("property", s);
      try {
        SimpleSolrResponse rsp = snitchContext.invoke(solrNode, CommonParams.METRICS_PATH, params);
        Map m = rsp.nl.asMap(6);
        for (String s : sysProp) {
          Object v = Utils.getObjectByPath(m, true,
              Arrays.asList("metrics", "solr.jvm", "system.properties", s));
          if (v != null) snitchContext.getTags().put("sysprop." + s, v);
        }

      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", e);

      }
    }
  }
}
