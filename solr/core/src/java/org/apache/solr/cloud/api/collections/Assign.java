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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.NumberUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.POLICY;
import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.CREATE_NODE_SET;
import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

public class Assign {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String getCounterNodePath(String collection) {
    return ZkStateReader.COLLECTIONS_ZKNODE + "/"+collection+"/counter";
  }

  public static int incAndGetId(DistribStateManager stateManager, String collection, int defaultValue) {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/"+collection;
    try {
      if (!stateManager.hasData(path)) {
        try {
          stateManager.makePath(path);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
      path += "/counter";
      if (!stateManager.hasData(path)) {
        try {
          stateManager.createData(path, NumberUtils.intToBytes(defaultValue), CreateMode.PERSISTENT);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating counter node in Zookeeper for collection:" + collection, e);
    } catch (IOException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating counter node in Zookeeper for collection:" + collection, e);
    }

    while (true) {
      try {
        int version = 0;
        int currentId = 0;
        VersionedData data = stateManager.getData(path, null);
        if (data != null) {
          currentId = NumberUtils.bytesToInt(data.getData());
          version = data.getVersion();
        }
        byte[] bytes = NumberUtils.intToBytes(++currentId);
        stateManager.setData(path, bytes, version);
        return currentId;
      } catch (BadVersionException e) {
        continue;
      } catch (IOException | KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error inc and get counter from Zookeeper for collection:"+collection, e);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error inc and get counter from Zookeeper for collection:" + collection, e);
      }
    }
  }

  public static String assignCoreNodeName(DistribStateManager stateManager, DocCollection collection) {
    // for backward compatibility;
    int defaultValue = defaultCounterValue(collection, false);
    String coreNodeName = "core_node" + incAndGetId(stateManager, collection.getName(), defaultValue);
    while (collection.getReplica(coreNodeName) != null) {
      // there is wee chance that, the new coreNodeName id not totally unique,
      // but this will be guaranteed unique for new collections
      coreNodeName = "core_node" + incAndGetId(stateManager, collection.getName(), defaultValue);
    }
    return coreNodeName;
  }

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
      return "shard1";
    }

    List<String> shardIdNames = new ArrayList<>(sliceMap.keySet());

    if (shardIdNames.size() < numShards) {
      return "shard" + (shardIdNames.size() + 1);
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

  public static String buildSolrCoreName(String collectionName, String shard, Replica.Type type, int replicaNum) {
    // TODO: Adding the suffix is great for debugging, but may be an issue if at some point we want to support a way to change replica type
    return String.format(Locale.ROOT, "%s_%s_replica_%s%s", collectionName, shard, type.name().substring(0,1).toLowerCase(Locale.ROOT), replicaNum);
  }

  private static int defaultCounterValue(DocCollection collection, boolean newCollection) {
    if (newCollection) return 0;
    int defaultValue = collection.getReplicas().size();
    if (collection.getReplicationFactor() != null) {
      // numReplicas and replicationFactor * numSlices can be not equals,
      // in case of many addReplicas or deleteReplicas are executed
      defaultValue = Math.max(defaultValue,
          collection.getReplicationFactor() * collection.getSlices().size());
    }
    return defaultValue * 20;
  }

  public static String buildSolrCoreName(DistribStateManager stateManager, DocCollection collection, String shard, Replica.Type type, boolean newCollection) {
    Slice slice = collection.getSlice(shard);
    int defaultValue = defaultCounterValue(collection, newCollection);
    int replicaNum = incAndGetId(stateManager, collection.getName(), defaultValue);
    String coreName = buildSolrCoreName(collection.getName(), shard, type, replicaNum);
    while (existCoreName(coreName, slice)) {
      replicaNum = incAndGetId(stateManager, collection.getName(), defaultValue);
      coreName = buildSolrCoreName(collection.getName(), shard, type, replicaNum);
    }
    return coreName;
  }

  public static String buildSolrCoreName(DistribStateManager stateManager, DocCollection collection, String shard, Replica.Type type) {
    return buildSolrCoreName(stateManager, collection, shard, type, false);
  }

  private static boolean existCoreName(String coreName, Slice slice) {
    if (slice == null) return false;
    for (Replica replica : slice.getReplicas()) {
      if (coreName.equals(replica.getStr(CORE_NAME_PROP))) {
        return true;
      }
    }
    return false;
  }

  public static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random) {
    // TODO: add smarter options that look at the current number of cores per
    // node?
    // for now we just go random (except when createNodeSet and createNodeSet.shuffle=false are passed in)

    List<String> nodeList;

    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null) ? null :
        StrUtils.splitSmart((OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY.equals(createNodeSetStr) ?
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

  public static List<ReplicaPosition> identifyNodes(SolrCloudManager cloudManager,
                                                    ClusterState clusterState,
                                                    List<String> nodeList,
                                                    String collectionName,
                                                    ZkNodeProps message,
                                                    List<String> shardNames,
                                                    int numNrtReplicas,
                                                    int numTlogReplicas,
                                                    int numPullReplicas) throws IOException, InterruptedException, AssignmentException {
    List<Map> rulesMap = (List) message.get("rule");
    String policyName = message.getStr(POLICY);
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();

    if (rulesMap == null && policyName == null && autoScalingConfig.getPolicy().getClusterPolicy().isEmpty()) {
      log.debug("Identify nodes using default");
      int i = 0;
      List<ReplicaPosition> result = new ArrayList<>();
      for (String aShard : shardNames)
        for (Map.Entry<Replica.Type, Integer> e : ImmutableMap.of(Replica.Type.NRT, numNrtReplicas,
            Replica.Type.TLOG, numTlogReplicas,
            Replica.Type.PULL, numPullReplicas
        ).entrySet()) {
          for (int j = 0; j < e.getValue(); j++){
            result.add(new ReplicaPosition(aShard, j, e.getKey(), nodeList.get(i % nodeList.size())));
            i++;
          }
        }
      return result;
    } else {
      if (numTlogReplicas + numPullReplicas != 0 && rulesMap != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            Replica.Type.TLOG + " or " + Replica.Type.PULL + " replica types not supported with placement rules");
      }
    }

    if (rulesMap != null && !rulesMap.isEmpty()) {
      List<Rule> rules = new ArrayList<>();
      for (Object map : rulesMap) rules.add(new Rule((Map) map));
      Map<String, Integer> sharVsReplicaCount = new HashMap<>();

      for (String shard : shardNames) sharVsReplicaCount.put(shard, numNrtReplicas);
      ReplicaAssigner replicaAssigner = new ReplicaAssigner(rules,
          sharVsReplicaCount,
          (List<Map>) message.get(SNITCH),
          new HashMap<>(),//this is a new collection. So, there are no nodes in any shard
          nodeList,
          cloudManager,
          clusterState);

      Map<ReplicaPosition, String> nodeMappings = replicaAssigner.getNodeMappings();
      return nodeMappings.entrySet().stream()
          .map(e -> new ReplicaPosition(e.getKey().shard, e.getKey().index, e.getKey().type, e.getValue()))
          .collect(Collectors.toList());
    } else  {
      if (message.getStr(CREATE_NODE_SET) == null)
        nodeList = Collections.emptyList();// unless explicitly specified do not pass node list to Policy
      return getPositionsUsingPolicy(collectionName,
          shardNames, numNrtReplicas, numTlogReplicas, numPullReplicas, policyName, cloudManager, nodeList);
    }
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

  // Only called from createShard and addReplica (so far).
  //
  // Gets a list of candidate nodes to put the required replica(s) on. Throws errors if not enough replicas
  // could be created on live nodes given maxShardsPerNode, Replication factor (if from createShard) etc.
  public static List<ReplicaCount> getNodesForNewReplicas(ClusterState clusterState, String collectionName,
                                                          String shard, int nrtReplicas, int tlogReplicas, int pullReplicas,
                                                          Object createNodeSet, SolrCloudManager cloudManager) throws IOException, InterruptedException, AssignmentException {
    log.debug("getNodesForNewReplicas() shard: {} , nrtReplicas : {} , tlogReplicas: {} , pullReplicas: {} , createNodeSet {}", shard, nrtReplicas, tlogReplicas, pullReplicas, createNodeSet );
    DocCollection coll = clusterState.getCollection(collectionName);
    Integer maxShardsPerNode = coll.getMaxShardsPerNode() == -1 ? Integer.MAX_VALUE : coll.getMaxShardsPerNode();
    List<String> createNodeList = null;

    if (createNodeSet instanceof List) {
      createNodeList = (List) createNodeSet;
    } else {
      // deduplicate
      createNodeList = createNodeSet == null ? null : new ArrayList<>(new LinkedHashSet<>(StrUtils.splitSmart((String) createNodeSet, ",", true)));
    }

     HashMap<String, ReplicaCount> nodeNameVsShardCount = getNodeNameVsShardCount(collectionName, clusterState, createNodeList);

    if (createNodeList == null) { // We only care if we haven't been told to put new replicas on specific nodes.
      long availableSlots = 0;
      for (Map.Entry<String, ReplicaCount> ent : nodeNameVsShardCount.entrySet()) {
        //ADDREPLICA can put more than maxShardsPerNode on an instance, so this test is necessary.
        if (maxShardsPerNode > ent.getValue().thisCollectionNodes) {
          availableSlots += (maxShardsPerNode - ent.getValue().thisCollectionNodes);
        }
      }
      if (availableSlots < nrtReplicas + tlogReplicas + pullReplicas) {
        throw new AssignmentException(
            String.format(Locale.ROOT, "Cannot create %d new replicas for collection %s given the current number of live nodes and a maxShardsPerNode of %d",
                nrtReplicas, collectionName, maxShardsPerNode));
      }
    }

    List l = (List) coll.get(DocCollection.RULE);
    List<ReplicaPosition> replicaPositions = null;
    if (l != null) {
      if (tlogReplicas + pullReplicas > 0)  {
        throw new AssignmentException(Replica.Type.TLOG + " or " + Replica.Type.PULL +
            " replica types not supported with placement rules");
      }
      // TODO: make it so that this method doesn't require access to CC
      replicaPositions = getNodesViaRules(clusterState, shard, nrtReplicas, cloudManager, coll, createNodeList, l);
    }
    String policyName = coll.getStr(POLICY);
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    if (policyName != null || !autoScalingConfig.getPolicy().getClusterPolicy().isEmpty()) {
      replicaPositions = Assign.getPositionsUsingPolicy(collectionName, Collections.singletonList(shard), nrtReplicas, tlogReplicas, pullReplicas,
          policyName, cloudManager, createNodeList);
    }

    if(replicaPositions != null){
      List<ReplicaCount> repCounts = new ArrayList<>();
      for (ReplicaPosition p : replicaPositions) {
        repCounts.add(new ReplicaCount(p.node));
      }
      return repCounts;
    }

    ArrayList<ReplicaCount> sortedNodeList = new ArrayList<>(nodeNameVsShardCount.values());
    Collections.sort(sortedNodeList, (x, y) -> (x.weight() < y.weight()) ? -1 : ((x.weight() == y.weight()) ? 0 : 1));
    return sortedNodeList;

  }

  public static List<ReplicaPosition> getPositionsUsingPolicy(String collName, List<String> shardNames,
                                                              int nrtReplicas,
                                                              int tlogReplicas,
                                                              int pullReplicas,
                                                              String policyName, SolrCloudManager cloudManager,
                                                              List<String> nodesList) throws IOException, InterruptedException, AssignmentException {
    log.debug("shardnames {} NRT {} TLOG {} PULL {} , policy {}, nodeList {}", shardNames, nrtReplicas, tlogReplicas, pullReplicas, policyName, nodesList);
    List<ReplicaPosition> replicaPositions = null;
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    try {
      Map<String, String> kvMap = Collections.singletonMap(collName, policyName);
      replicaPositions = PolicyHelper.getReplicaLocations(
          collName,
          autoScalingConfig,
          cloudManager,
          kvMap,
          shardNames,
          nrtReplicas,
          tlogReplicas,
          pullReplicas,
          nodesList);
      return replicaPositions;
    } catch (Exception e) {
      throw new AssignmentException("Error getting replica locations : " + e.getMessage(), e);
    } finally {
      if (log.isTraceEnabled()) {
        if (replicaPositions != null)
          log.trace("REPLICA_POSITIONS: " + Utils.toJSONString(Utils.getDeepCopy(replicaPositions, 7, true)));
        log.trace("AUTOSCALING_CONF: " + Utils.toJSONString(autoScalingConfig));
      }
    }
  }

  private static List<ReplicaPosition> getNodesViaRules(ClusterState clusterState, String shard, int numberOfNodes,
                                                        SolrCloudManager cloudManager, DocCollection coll, List<String> createNodeList, List l) {
    ArrayList<Rule> rules = new ArrayList<>();
    for (Object o : l) rules.add(new Rule((Map) o));
    Map<String, Map<String, Integer>> shardVsNodes = new LinkedHashMap<>();
    for (Slice slice : coll.getSlices()) {
      LinkedHashMap<String, Integer> n = new LinkedHashMap<>();
      shardVsNodes.put(slice.getName(), n);
      for (Replica replica : slice.getReplicas()) {
        Integer count = n.get(replica.getNodeName());
        if (count == null) count = 0;
        n.put(replica.getNodeName(), ++count);
      }
    }
    List snitches = (List) coll.get(SNITCH);
    List<String> nodesList = createNodeList == null ?
        new ArrayList<>(clusterState.getLiveNodes()) :
        createNodeList;
    Map<ReplicaPosition, String> positions = new ReplicaAssigner(
        rules,
        Collections.singletonMap(shard, numberOfNodes),
        snitches,
        shardVsNodes,
        nodesList, cloudManager, clusterState).getNodeMappings();

    return positions.entrySet().stream().map(e -> e.getKey().setNode(e.getValue())).collect(Collectors.toList());// getReplicaCounts(positions);
  }

  private static HashMap<String, ReplicaCount> getNodeNameVsShardCount(String collectionName,
                                                                       ClusterState clusterState, List<String> createNodeList) {
    Set<String> nodes = clusterState.getLiveNodes();

    List<String> nodeList = new ArrayList<>(nodes.size());
    nodeList.addAll(nodes);
    if (createNodeList != null) nodeList.retainAll(createNodeList);

    HashMap<String, ReplicaCount> nodeNameVsShardCount = new HashMap<>();
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
    DocCollection coll = clusterState.getCollection(collectionName);
    int maxShardsPerNode = coll.getMaxShardsPerNode() == -1 ? Integer.MAX_VALUE : coll.getMaxShardsPerNode();
    Map<String, DocCollection> collections = clusterState.getCollectionsMap();
    for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
      DocCollection c = entry.getValue();
      //identify suitable nodes  by checking the no:of cores in each of them
      for (Slice slice : c.getSlices()) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          ReplicaCount count = nodeNameVsShardCount.get(replica.getNodeName());
          if (count != null) {
            count.totalNodes++; // Used ot "weigh" whether this node should be used later.
            if (entry.getKey().equals(collectionName)) {
              count.thisCollectionNodes++;
              if (count.thisCollectionNodes >= maxShardsPerNode) nodeNameVsShardCount.remove(replica.getNodeName());
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
}
