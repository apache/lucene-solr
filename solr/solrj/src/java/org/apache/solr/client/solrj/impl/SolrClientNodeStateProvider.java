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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.ReplicaInfo;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.cloud.rule.SnitchContext;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;

/**
 * The <em>real</em> {@link NodeStateProvider}, which communicates with Solr via SolrJ.
 */
public class SolrClientNodeStateProvider implements NodeStateProvider, MapWriter {
  public static final String METRICS_PREFIX = "metrics:";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  //only for debugging
  public static SolrClientNodeStateProvider INST;



  private final BaseCloudSolrClient solrClient;
  protected final Map<String, Map<String, Map<String, List<ReplicaInfo>>>> nodeVsCollectionVsShardVsReplicaInfo = new HashMap<>();
  private final HttpClient httpClient;
  private Map<String, Object> snitchSession = new HashMap<>();
  private Map<String, Map> nodeVsTags = new HashMap<>();
  private Map<String, String> withCollectionsMap = new HashMap<>();

  public SolrClientNodeStateProvider(BaseCloudSolrClient solrClient, HttpClient httpClient) {
    this.solrClient = solrClient;
    this.httpClient = httpClient;
    try {
      readReplicaDetails();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    if(log.isDebugEnabled()) INST = this;
  }

  protected ClusterStateProvider getClusterStateProvider() {
    return solrClient.getClusterStateProvider();
  }

  protected void readReplicaDetails() throws IOException {
    ClusterStateProvider clusterStateProvider = getClusterStateProvider();
    ClusterState clusterState = clusterStateProvider.getClusterState();
    if (clusterState == null) { // zkStateReader still initializing
      return;
    }
    Map<String, ClusterState.CollectionRef> all = clusterStateProvider.getClusterState().getCollectionStates();
    all.forEach((collName, ref) -> {
      DocCollection coll = ref.get();
      if (coll == null) return;
      if (coll.getProperties().get(CollectionAdminParams.WITH_COLLECTION) != null) {
        withCollectionsMap.put(coll.getName(), (String) coll.getProperties().get(CollectionAdminParams.WITH_COLLECTION));
      }
      coll.forEachReplica((shard, replica) -> {
        Map<String, Map<String, List<ReplicaInfo>>> nodeData = nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(replica.getNodeName(), k -> new HashMap<>());
        Map<String, List<ReplicaInfo>> collData = nodeData.computeIfAbsent(collName, k -> new HashMap<>());
        List<ReplicaInfo> replicas = collData.computeIfAbsent(shard, k -> new ArrayList<>());
        replicas.add(new ReplicaInfo(collName, shard, replica, new HashMap<>(replica.getProperties())));
      });
    });
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    ew.put("replicaInfo", Utils.getDeepCopy(nodeVsCollectionVsShardVsReplicaInfo, 5));
    ew.put("nodeValues", nodeVsTags);
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    Map<String, Object> tagVals = fetchTagValues(node, tags);
    nodeVsTags.put(node, tagVals);
    return tagVals;
  }

  protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
    MetricsFetchingSnitch snitch = new MetricsFetchingSnitch();
    ClientSnitchCtx ctx = new ClientSnitchCtx(null, node, snitchSession, solrClient, httpClient);
    snitch.getTags(node, new HashSet<>(tags), ctx);
    return ctx.getTags();
  }

  public void forEachReplica(String node, Consumer<ReplicaInfo> consumer){
    forEachReplica(nodeVsCollectionVsShardVsReplicaInfo.get(node), consumer);
  }

  public static void forEachReplica(Map<String, Map<String, List<ReplicaInfo>>> collectionVsShardVsReplicas, Consumer<ReplicaInfo> consumer) {
    collectionVsShardVsReplicas.forEach((coll, shardVsReplicas) -> shardVsReplicas
        .forEach((shard, replicaInfos) -> {
          for (int i = 0; i < replicaInfos.size(); i++) {
            ReplicaInfo r = replicaInfos.get(i);
            consumer.accept(r);
          }
        }));
  }

  @Override
  public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
    Map<String, Map<String, List<ReplicaInfo>>> result = nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(node, Utils.NEW_HASHMAP_FUN);
    if (!keys.isEmpty()) {
      Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica = new HashMap<>();
      forEachReplica(result, r -> {
        for (String key : keys) {
          if (r.getVariables().containsKey(key)) continue;// it's already collected
          String perReplicaMetricsKey = "solr.core." + r.getCollection() + "." + r.getShard() + "." + r.getName() + ":";
          String perReplicaValue = key;
          perReplicaMetricsKey += perReplicaValue;
          metricsKeyVsTagReplica.put(perReplicaMetricsKey, new Pair<>(key, r));
        }
      });

      if (!metricsKeyVsTagReplica.isEmpty()) {
        Map<String, Object> tagValues = fetchReplicaMetrics(node, metricsKeyVsTagReplica);
        tagValues.forEach((k, o) -> {
          Pair<String, ReplicaInfo> p = metricsKeyVsTagReplica.get(k);
          if (p.second() != null) p.second().getVariables().put(p.first(), o);
        });

      }
    }
    return result;
  }

  protected Map<String, Object> fetchReplicaMetrics(String node, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
    Map<String, Object> collect = metricsKeyVsTagReplica.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getKey));
    ClientSnitchCtx ctx = new ClientSnitchCtx(null, null, emptyMap(), solrClient, httpClient);
    fetchReplicaMetrics(node, ctx, collect);
    return ctx.getTags();

  }

  static void fetchReplicaMetrics(String solrNode, ClientSnitchCtx ctx, Map<String, Object> metricsKeyVsTag) {
    if (!ctx.isNodeAlive(solrNode)) return;
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("key", metricsKeyVsTag.keySet().toArray(new String[0]));
    try {
      SimpleSolrResponse rsp = ctx.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
      metricsKeyVsTag.forEach((key, tag) -> {
        Object v = Utils.getObjectByPath(rsp.nl, true, Arrays.asList("metrics", key));
        if (tag instanceof Function) {
          Pair<String, Object> p = (Pair<String, Object>) ((Function) tag).apply(v);
          ctx.getTags().put(p.first(), p.second());
        } else {
          if (v != null) ctx.getTags().put(tag.toString(), v);
        }
      });
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.warn("could not get tags from node {}", solrNode, e);
    }
  }

  @Override
  public void close() throws IOException {

  }

  //uses metrics API to get node information
  static class MetricsFetchingSnitch extends ImplicitSnitch {
    @Override
    protected void getRemoteInfo(String solrNode, Set<String> requestedTags, SnitchContext ctx) {
      if (!((ClientSnitchCtx)ctx).isNodeAlive(solrNode)) return;
      ClientSnitchCtx snitchContext = (ClientSnitchCtx) ctx;
      Map<String, Object> metricsKeyVsTag = new HashMap<>();
      for (String tag : requestedTags) {
        if (tag.startsWith(SYSPROP)) {
          metricsKeyVsTag.put("solr.jvm:system.properties:" + tag.substring(SYSPROP.length()), tag);
        } else if (tag.startsWith(METRICS_PREFIX)) {
          metricsKeyVsTag.put(tag.substring(METRICS_PREFIX.length()), tag);
        }
      }
      if (requestedTags.contains(ImplicitSnitch.DISKTYPE)) {
        metricsKeyVsTag.put("solr.node:CONTAINER.fs.coreRoot.spins", (Function<Object, Pair<String, Object>>) o -> {
          if("true".equals(String.valueOf(o))){
            return new Pair<>(ImplicitSnitch.DISKTYPE, "rotational");
          }
          if("false".equals(String.valueOf(o))){
            return new Pair<>(ImplicitSnitch.DISKTYPE, "ssd");
          }
          return new Pair<>(ImplicitSnitch.DISKTYPE,null);

        });
      }
      if (!metricsKeyVsTag.isEmpty()) {
        fetchReplicaMetrics(solrNode, snitchContext, metricsKeyVsTag);
      }

      Set<String> groups = new HashSet<>();
      List<String> prefixes = new ArrayList<>();
      if (requestedTags.contains(DISK)) {
        groups.add("solr.node");
        prefixes.add("CONTAINER.fs.usableSpace");
      }
      if (requestedTags.contains(Variable.TOTALDISK.tagName)) {
        groups.add("solr.node");
        prefixes.add("CONTAINER.fs.totalSpace");
      }
      if (requestedTags.contains(CORES)) {
        groups.add("solr.node");
        prefixes.add("CONTAINER.cores");
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
        SimpleSolrResponse rsp = snitchContext.invokeWithRetry(solrNode, CommonParams.METRICS_PATH, params);
        NamedList<?> metrics = (NamedList<?>) rsp.nl.get("metrics");

        if (requestedTags.contains(Variable.FREEDISK.tagName)) {
          Object n = Utils.getObjectByPath(metrics, true, "solr.node/CONTAINER.fs.usableSpace");
          if (n != null) ctx.getTags().put(Variable.FREEDISK.tagName, Variable.FREEDISK.convertVal(n));
        }
        if (requestedTags.contains(Variable.TOTALDISK.tagName)) {
          Object n = Utils.getObjectByPath(metrics, true, "solr.node/CONTAINER.fs.totalSpace");
          if (n != null) ctx.getTags().put(Variable.TOTALDISK.tagName, Variable.TOTALDISK.convertVal(n));
        }
        if (requestedTags.contains(CORES)) {
          NamedList<?> node = (NamedList<?>) metrics.get("solr.node");
          int count = 0;
          for (String leafCoreMetricName : new String[]{"lazy", "loaded", "unloaded"}) {
            Number n = (Number) node.get("CONTAINER.cores." + leafCoreMetricName);
            if (n != null) count += n.intValue();
          }
          ctx.getTags().put(CORES, count);
        }
        if (requestedTags.contains(SYSLOADAVG)) {
          Number n = (Number) Utils.getObjectByPath(metrics, true, "solr.jvm/os.systemLoadAverage");
          if (n != null) ctx.getTags().put(SYSLOADAVG, n.doubleValue() * 100.0d);
        }
        if (requestedTags.contains(HEAPUSAGE)) {
          Number n = (Number) Utils.getObjectByPath(metrics, true, "solr.jvm/memory.heap.usage");
          if (n != null) ctx.getTags().put(HEAPUSAGE, n.doubleValue() * 100.0d);
        }
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error getting remote info", e);
      }
    }
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  static class ClientSnitchCtx
      extends SnitchContext {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    ZkClientClusterStateProvider zkClientClusterStateProvider;
    BaseCloudSolrClient solrClient;

    HttpClient httpClient;

    public boolean isNodeAlive(String node) {
      if (zkClientClusterStateProvider != null) {
        return zkClientClusterStateProvider.getLiveNodes().contains(node);
      }
      return true;
    }
    public ClientSnitchCtx(SnitchInfo perSnitch,
                           String node, Map<String, Object> session,
                           BaseCloudSolrClient solrClient, HttpClient httpClient) {
      super(perSnitch, node, session);
      this.solrClient = solrClient;
      this.httpClient = httpClient;
      this.zkClientClusterStateProvider = (ZkClientClusterStateProvider) solrClient.getClusterStateProvider();
    }


    @Override
    public Map getZkJson(String path) throws KeeperException, InterruptedException {
      return Utils.getJson(zkClientClusterStateProvider.getZkStateReader().getZkClient(), path);
    }

    /**
     * Will attempt to call {@link #invoke(String, String, SolrParams)}, retrying on any IO Exceptions
     */
    public SimpleSolrResponse invokeWithRetry(String solrNode, String path, SolrParams params) throws InterruptedException, IOException, SolrServerException {
      int retries = 2;
      int cnt = 0;

      while (cnt++ < retries) {
        try {
          return invoke(solrNode, path, params);
        } catch (SolrException | SolrServerException | IOException e) {
          boolean hasIOExceptionCause = false;

          Throwable t = e;
          while (t != null) {
            if (t instanceof IOException) {
              hasIOExceptionCause = true;
              break;
            }
            t = t.getCause();
          }

          if (hasIOExceptionCause) {
            if (log.isInfoEnabled()) {
              log.info("Error on getting remote info, trying again: {}", e.getMessage());
            }
          } else {
            throw e;
          }
        }
      }

      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get remote info after " + cnt + " retries on NoHttpResponseException");
    }

    public SimpleSolrResponse invoke(String solrNode, String path, SolrParams params)
        throws IOException, SolrServerException {
      String url = zkClientClusterStateProvider.getZkStateReader().getBaseUrlForNodeName(solrNode);

      GenericSolrRequest request = new GenericSolrRequest(SolrRequest.METHOD.POST, path, params);
      try (HttpSolrClient client = new HttpSolrClient.Builder()
          .withHttpClient(httpClient)
          .withBaseSolrUrl(url)
          .withResponseParser(new BinaryResponseParser())
          .markInternalRequest()
          .build()) {
        NamedList<Object> rsp = client.request(request);
        request.response.nl = rsp;
        return request.response;
      }
    }

  }

  public enum Variable {
    FREEDISK("freedisk", null, Double.class),
    TOTALDISK("totaldisk", null, Double.class),
    CORE_IDX("INDEX.sizeInGB",  "INDEX.sizeInBytes", Double.class)
    ;


    public final String tagName, metricsAttribute;
    @SuppressWarnings("rawtypes")
    public final Class type;


    @SuppressWarnings("rawtypes")
    Variable(String tagName, String metricsAttribute, Class type) {
      this.tagName = tagName;
      this.metricsAttribute = metricsAttribute;
      this.type = type;
    }

    public Object convertVal(Object val) {
      if(val instanceof String) {
        return Double.valueOf((String)val);
      } else if (val instanceof Number) {
        Number num = (Number)val;
        return num.doubleValue();

      } else {
        throw new IllegalArgumentException("Unknown type : "+val);
      }

    }
  }
}
