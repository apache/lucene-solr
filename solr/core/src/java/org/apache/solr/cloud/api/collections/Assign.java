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
package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

// nocommit - this needs work, but lets not hit zk and other nodes if we dont need for base Assign
public class Assign {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static AtomicInteger REPLICA_CNT = new AtomicInteger(0);

  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   *
   * @return the assigned shard id
   */
  public static String assignShard(DocCollection collection, Integer numShards) {
    if (numShards == null) {
      numShards = 1;
    }
    String returnShardId = null;
    Map<String, Slice> sliceMap = collection != null ? collection.getActiveSlicesMap() : null;


    // TODO: now that we create shards ahead of time, is this code needed?  Esp since hash ranges aren't assigned when creating via this method?

    if (sliceMap == null) {
      return "s";
    }

    List<String> shardIdNames = new ArrayList<>(sliceMap.keySet());

    if (shardIdNames.size() < numShards) {
      return "s" + (shardIdNames.size() + 1);
    }

    // TODO: don't need to sort to find shard with fewest replicas!

    // else figure out which shard needs more replicas
    final Map<String, Integer> map = new HashMap<>();
    for (String shardId : shardIdNames) {
      int cnt = sliceMap.get(shardId).getReplicasMap().size();
      map.put(shardId, cnt);
    }

    Collections.sort(shardIdNames, (String o1, String o2) -> {
      Integer one = map.get(o1);
      Integer two = map.get(o2);
      return one.compareTo(two);
    });

    returnShardId = shardIdNames.get(0);
    return returnShardId;
  }

  private static String buildSolrCoreName(String collectionName, String shard, Replica.Type type, int replicaNum) {
    // TODO: Adding the suffix is great for debugging, but may be an issue if at some point we want to support a way to change replica type
    return String.format(Locale.ROOT, "%s_%s_r_%s%s", collectionName, shard, type.name().substring(0,1).toLowerCase(Locale.ROOT), replicaNum);
  }

  public static int defaultCounterValue(DocCollection coll, String shard) {

    if (coll == null) {
      throw new NullPointerException("DocCollection cannot be null");
    }

    if (coll.getSlice(shard) == null) {
      return 1;
    }

    if (coll.getSlice(shard).getReplicas() == null) {
      return 1;
    }

    return coll.getSlice(shard).getReplicas().size() + 1;
  }

  public static String buildSolrCoreName(DocCollection coll, String collectionName, String shard, Replica.Type type) {
    int defaultValue = defaultCounterValue(coll, shard);
    String coreName = buildSolrCoreName(collectionName, shard, type, defaultValue);

    return coreName;
  }

  public static List<String> getLiveOrLiveAndCreateNodeSetList(final Collection<String> liveNodes, final ZkNodeProps message, final Random random) {
    List<String> nodeList;
    final String createNodeSetStr = message.getStr(ZkStateReader.CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null) ? null :
        StrUtils.splitSmart((ZkStateReader.CREATE_NODE_SET_EMPTY.equals(createNodeSetStr) ?
            "" : createNodeSetStr), ",", true);

    if (createNodeList != null) {
      nodeList = new ArrayList<>(createNodeList);
      nodeList.retainAll(liveNodes);
      if (message.getBool(OverseerCollectionMessageHandler.CREATE_NODE_SET_SHUFFLE,
          OverseerCollectionMessageHandler.CREATE_NODE_SET_SHUFFLE_DEFAULT)) {
        Collections.shuffle(nodeList, random);
      }
    } else {
      nodeList = new ArrayList<>(liveNodes);
      Collections.shuffle(nodeList, random);
    }

    return nodeList;
  }

  static class ReplicaCount {
    public final String nodeName;
    public int thisCollectionNodes = 0;
    public int totalNodes = 0;

    ReplicaCount(String nodeName) {
      this.nodeName = nodeName;
    }

    public int weight() {
      return (thisCollectionNodes * 100) + totalNodes;
    }
  }

  // Only called from addReplica (and by extension createShard) (so far).
  //
  // Gets a list of candidate nodes to put the required replica(s) on. Throws errors if not enough replicas
  // could be created on live nodes given maxShardsPerNode, Replication factor (if from createShard) etc.
  @SuppressWarnings({"unchecked"})
  public static List<ReplicaPosition> getNodesForNewReplicas(ClusterState clusterState, DocCollection collection,
                                                          String shard, int nrtReplicas, int tlogReplicas, int pullReplicas,
                                                          Object createNodeSet, SolrCloudManager cloudManager) throws IOException, InterruptedException, AssignmentException {
    log.debug("getNodesForNewReplicas() shard: {} , nrtReplicas : {} , tlogReplicas: {} , pullReplicas: {} , createNodeSet {}"
        , shard, nrtReplicas, tlogReplicas, pullReplicas, createNodeSet);

    //int maxShardsPerNode = collection.getMaxShardsPerNode() == -1 ? Integer.MAX_VALUE : collection.getMaxShardsPerNode();
    List<String> createNodeList = null;

    if (createNodeSet instanceof List) {
      createNodeList = (List<String>) createNodeSet;
    } else {
      // deduplicate
      createNodeList = createNodeSet == null ? null : new ArrayList<>(new LinkedHashSet<>(StrUtils.splitSmart((String) createNodeSet, ",", true)));
    }
    String collectionName = collection.getName();
    HashMap<String, ReplicaCount> nodeNameVsShardCount = getNodeNameVsShardCount(collectionName, clusterState, cloudManager.getClusterStateProvider().getClusterState().getLiveNodes(), createNodeList);


    AssignRequest assignRequest = new AssignRequestBuilder()
        .forCollection(collectionName)
        .forShard(Collections.singletonList(shard))
        .assignNrtReplicas(nrtReplicas)
        .assignTlogReplicas(tlogReplicas)
        .assignPullReplicas(pullReplicas)
        .onNodes(createNodeList)
        .build();
    AssignStrategyFactory assignStrategyFactory = new AssignStrategyFactory(cloudManager);
    AssignStrategy assignStrategy = assignStrategyFactory.create();
    return assignStrategy.assign(cloudManager, assignRequest);
  }

  static HashMap<String, ReplicaCount> getNodeNameVsShardCount(String collectionName, ClusterState clusterState,
                                                                       Set<String> liveNodes, List<String> createNodeList) {

    List<String> nodeList = new ArrayList<>(liveNodes.size());
    nodeList.addAll(liveNodes);
    if (createNodeList != null) nodeList.retainAll(createNodeList);

    HashMap<String, ReplicaCount> nodeNameVsShardCount = new HashMap<>(nodeList.size());
    for (String s : nodeList) {
      nodeNameVsShardCount.put(s, new ReplicaCount(s));
    }
    if (createNodeList != null) { // Overrides petty considerations about maxShardsPerNode
      if (createNodeList.size() != nodeNameVsShardCount.size()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "At least one of the node(s) specified " + createNodeList + " are not currently active in "
                + nodeNameVsShardCount.keySet() + ", no action taken.");
      }
      return nodeNameVsShardCount;
    }

    Map<String, DocCollection> collections = clusterState.getCollectionsMap();
    for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
      if (entry.getValue() == null) continue;
      DocCollection c = entry.getValue();
      //identify suitable nodes  by checking the no:of cores in each of them
      for (Slice slice : c.getSlices()) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          ReplicaCount count = nodeNameVsShardCount.get(replica.getNodeName());
          if (count != null) {
            count.totalNodes++; // Used to "weigh" whether this node should be used later.
            if (entry.getKey().equals(collectionName)) {
              count.thisCollectionNodes++;
             // if (count.thisCollectionNodes >= maxShardsPerNode) nodeNameVsShardCount.remove(replica.getNodeName());
            }
          }
        }
      }
    }

    return nodeNameVsShardCount;
  }

  /**
   * Thrown if there is an exception while assigning nodes for replicas
   */
  public static class AssignmentException extends RuntimeException {
    public AssignmentException() {
    }

    public AssignmentException(String message) {
      super(message);
    }

    public AssignmentException(String message, Throwable cause) {
      super(message, cause);
    }

    public AssignmentException(Throwable cause) {
      super(cause);
    }

    public AssignmentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }
  }

  public interface AssignStrategy {
    List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, AssignRequest assignRequest)
        throws Assign.AssignmentException, IOException, InterruptedException;
  }

  public static class AssignRequest {
    public String collectionName;
    public List<String> shardNames;
    public List<String> nodes;
    public int numNrtReplicas;
    public int numTlogReplicas;
    public int numPullReplicas;

    public AssignRequest(String collectionName, List<String> shardNames, List<String> nodes, int numNrtReplicas, int numTlogReplicas, int numPullReplicas) {
      this.collectionName = collectionName;
      this.shardNames = shardNames;
      this.nodes = nodes;
      this.numNrtReplicas = numNrtReplicas;
      this.numTlogReplicas = numTlogReplicas;
      this.numPullReplicas = numPullReplicas;
    }
  }

  public static class AssignRequestBuilder {
    private String collectionName;
    private List<String> shardNames;
    private List<String> nodes;
    private int numNrtReplicas;
    private int numTlogReplicas;
    private int numPullReplicas;

    public AssignRequestBuilder forCollection(String collectionName) {
      this.collectionName = collectionName;
      return this;
    }

    public AssignRequestBuilder forShard(List<String> shardNames) {
      this.shardNames = shardNames;
      return this;
    }

    public AssignRequestBuilder onNodes(List<String> nodes) {
      this.nodes = nodes;
      return this;
    }

    public AssignRequestBuilder assignNrtReplicas(int numNrtReplicas) {
      this.numNrtReplicas = numNrtReplicas;
      return this;
    }

    public AssignRequestBuilder assignTlogReplicas(int numTlogReplicas) {
      this.numTlogReplicas = numTlogReplicas;
      return this;
    }

    public AssignRequestBuilder assignPullReplicas(int numPullReplicas) {
      this.numPullReplicas = numPullReplicas;
      return this;
    }

    public AssignRequest build() {
      Objects.requireNonNull(collectionName, "The collectionName cannot be null");
      Objects.requireNonNull(shardNames, "The shard names cannot be null");
      return new AssignRequest(collectionName, shardNames, nodes, numNrtReplicas,
          numTlogReplicas, numPullReplicas);
    }
  }

  public static class LegacyAssignStrategy implements AssignStrategy {
    @Override
    public List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, AssignRequest assignRequest) throws Assign.AssignmentException, IOException, InterruptedException {
      ClusterState clusterState = solrCloudManager.getClusterStateProvider().getClusterState();
      List<String> nodeList = assignRequest.nodes;

      HashMap<String, Assign.ReplicaCount> nodeNameVsShardCount = Assign.getNodeNameVsShardCount(assignRequest.collectionName,
          clusterState, solrCloudManager.getClusterStateProvider().getLiveNodes(),assignRequest.nodes);
      if (nodeList == null || nodeList.isEmpty()) {
        ArrayList<Assign.ReplicaCount> sortedNodeList = new ArrayList<>(nodeNameVsShardCount.values());
        sortedNodeList.sort(Comparator.comparingInt(Assign.ReplicaCount::weight));
        nodeList = sortedNodeList.stream().map(replicaCount -> replicaCount.nodeName).collect(Collectors.toList());
      }

      int i = 0;
      List<ReplicaPosition> result = new ArrayList<>(assignRequest.numNrtReplicas + assignRequest.numPullReplicas + assignRequest.numTlogReplicas);
      for (String aShard : assignRequest.shardNames)
        for (Map.Entry<Replica.Type, Integer> e : ImmutableMap.of(Replica.Type.NRT, assignRequest.numNrtReplicas,
            Replica.Type.TLOG, assignRequest.numTlogReplicas,
            Replica.Type.PULL, assignRequest.numPullReplicas
        ).entrySet()) {
          for (int j = 0; j < e.getValue(); j++) {
            result.add(new ReplicaPosition(aShard, j, e.getKey(), nodeList.get(i % nodeList.size())));
            i++;
          }
        }
      return result;
    }
  }

  public static class AssignStrategyFactory {
    public SolrCloudManager solrCloudManager;

    public AssignStrategyFactory(SolrCloudManager solrCloudManager) {
      this.solrCloudManager = solrCloudManager;
    }

    public AssignStrategy create() throws IOException, InterruptedException {

      return new LegacyAssignStrategy();

    }

    private enum Strategy {
      LEGACY, RULES;
    }
  }
}
