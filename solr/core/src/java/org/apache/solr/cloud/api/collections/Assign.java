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
import java.util.Optional;
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
import org.apache.solr.common.params.CollectionAdminParams;
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
        Thread.currentThread().interrupt();
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

  private static int defaultCounterValue(DocCollection collection, boolean newCollection, String shard) {
    if (newCollection) return 0;

    int defaultValue;
    if (collection.getSlice(shard) != null && collection.getSlice(shard).getReplicas().isEmpty()) {
      return 0;
    } else {
      defaultValue = collection.getReplicas().size() * 2;
    }

    if (collection.getReplicationFactor() != null) {
      // numReplicas and replicationFactor * numSlices can be not equals,
      // in case of many addReplicas or deleteReplicas are executed
      defaultValue = Math.max(defaultValue,
          collection.getReplicationFactor() * collection.getSlices().size());
    }
    return defaultValue;
  }
  
  private static int defaultCounterValue(DocCollection collection, boolean newCollection) {
    if (newCollection) return 0;
    int defaultValue = collection.getReplicas().size();
    return defaultValue;
  }

  public static String buildSolrCoreName(DistribStateManager stateManager, DocCollection collection, String shard, Replica.Type type, boolean newCollection) {
    Slice slice = collection.getSlice(shard);
    int defaultValue = defaultCounterValue(collection, newCollection, shard);
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

  /**
   * <b>Note:</b> where possible, the {@link #usePolicyFramework(DocCollection, SolrCloudManager)} method should
   * be used instead of this method
   *
   * @return true if autoscaling policy framework should be used for replica placement
   */
  public static boolean usePolicyFramework(SolrCloudManager cloudManager) throws IOException, InterruptedException {
    Objects.requireNonNull(cloudManager, "The SolrCloudManager instance cannot be null");
    return usePolicyFramework(Optional.empty(), cloudManager);
  }

  /**
   * @return true if auto scaling policy framework should be used for replica placement
   * for this collection, otherwise false
   */
  public static boolean usePolicyFramework(DocCollection collection, SolrCloudManager cloudManager)
      throws IOException, InterruptedException {
    Objects.requireNonNull(collection, "The DocCollection instance cannot be null");
    Objects.requireNonNull(cloudManager, "The SolrCloudManager instance cannot be null");
    return usePolicyFramework(Optional.of(collection), cloudManager);
  }

  @SuppressWarnings({"unchecked"})
  private static boolean usePolicyFramework(Optional<DocCollection> collection, SolrCloudManager cloudManager) throws IOException, InterruptedException {
    boolean useLegacyAssignment = true;
    Map<String, Object> clusterProperties = cloudManager.getClusterStateProvider().getClusterProperties();
    if (clusterProperties.containsKey(CollectionAdminParams.DEFAULTS))  {
      Map<String, Object> defaults = (Map<String, Object>) clusterProperties.get(CollectionAdminParams.DEFAULTS);
      Map<String, Object> collectionDefaults = (Map<String, Object>) defaults.getOrDefault(CollectionAdminParams.CLUSTER, Collections.emptyMap());
      useLegacyAssignment = Boolean.parseBoolean(collectionDefaults.getOrDefault(CollectionAdminParams.USE_LEGACY_REPLICA_ASSIGNMENT, "true").toString());
    }

    if (!useLegacyAssignment) {
      // if legacy assignment is not selected then autoscaling is always available through the implicit policy/preferences
      return true;
    }

    // legacy assignment is turned on, which means we must look at the actual autoscaling config
    // to determine whether policy framework can be used or not for this collection

    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    // if no autoscaling configuration exists then obviously we cannot use the policy framework
    if (autoScalingConfig.getPolicy().isEmpty()) return false;
    // do custom preferences exist
    if (!autoScalingConfig.getPolicy().isEmptyPreferences()) return true;
    // does a cluster policy exist
    if (!autoScalingConfig.getPolicy().getClusterPolicy().isEmpty()) return true;
    // finally we check if the current collection has a policy
    return !collection.isPresent() || collection.get().getPolicyName() != null;
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
  public static List<ReplicaPosition> getNodesForNewReplicas(ClusterState clusterState, String collectionName,
                                                          String shard, int nrtReplicas, int tlogReplicas, int pullReplicas,
                                                          Object createNodeSet, SolrCloudManager cloudManager) throws IOException, InterruptedException, AssignmentException {
    log.debug("getNodesForNewReplicas() shard: {} , nrtReplicas : {} , tlogReplicas: {} , pullReplicas: {} , createNodeSet {}"
        , shard, nrtReplicas, tlogReplicas, pullReplicas, createNodeSet);
    DocCollection coll = clusterState.getCollection(collectionName);
    int maxShardsPerNode = coll.getMaxShardsPerNode() == -1 ? Integer.MAX_VALUE : coll.getMaxShardsPerNode();
    List<String> createNodeList = null;

    if (createNodeSet instanceof List) {
      createNodeList = (List<String>) createNodeSet;
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
            String.format(Locale.ROOT, "Cannot create %d new replicas for collection %s given the current number of eligible live nodes %d and a maxShardsPerNode of %d",
                nrtReplicas, collectionName, nodeNameVsShardCount.size(), maxShardsPerNode));
      }
    }

    AssignRequest assignRequest = new AssignRequestBuilder()
        .forCollection(collectionName)
        .forShard(Collections.singletonList(shard))
        .assignNrtReplicas(nrtReplicas)
        .assignTlogReplicas(tlogReplicas)
        .assignPullReplicas(pullReplicas)
        .onNodes(createNodeList)
        .build();
    AssignStrategyFactory assignStrategyFactory = new AssignStrategyFactory(cloudManager);
    AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, coll);
    return assignStrategy.assign(cloudManager, assignRequest);
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
        if (replicaPositions != null) {
          if (log.isTraceEnabled()) {
            log.trace("REPLICA_POSITIONS: {}", Utils.toJSONString(Utils.getDeepCopy(replicaPositions, 7, true)));
          }
        }
        if (log.isTraceEnabled()) {
          log.trace("AUTOSCALING_CONF: {}", Utils.toJSONString(autoScalingConfig));
        }
      }
    }
  }

  static HashMap<String, ReplicaCount> getNodeNameVsShardCount(String collectionName,
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
            count.totalNodes++; // Used to "weigh" whether this node should be used later.
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

  // throw an exception if all nodes in the supplied list are not live.
  // Empty list will also fail.
  // Returns the input
  private static List<String> checkAnyLiveNodes(List<String> createNodeList, ClusterState clusterState) {
    Set<String> liveNodes = clusterState.getLiveNodes();
    if (createNodeList == null) {
      createNodeList = Collections.emptyList();
    }
    boolean anyLiveNodes = false;
    for (String node : createNodeList) {
      anyLiveNodes |= liveNodes.contains(node);
    }
    if (!anyLiveNodes) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "None of the node(s) specified " + createNodeList + " are currently active in "
              + liveNodes + ", no action taken.");
    }
    return createNodeList; // unmodified, but return for inline use. Only modified if empty, and that will throw an error
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

      HashMap<String, Assign.ReplicaCount> nodeNameVsShardCount = Assign.getNodeNameVsShardCount(assignRequest.collectionName, clusterState, assignRequest.nodes);
      if (nodeList == null || nodeList.isEmpty()) {
        ArrayList<Assign.ReplicaCount> sortedNodeList = new ArrayList<>(nodeNameVsShardCount.values());
        sortedNodeList.sort(Comparator.comparingInt(Assign.ReplicaCount::weight));
        nodeList = sortedNodeList.stream().map(replicaCount -> replicaCount.nodeName).collect(Collectors.toList());
      }

      // Throw an error if there aren't any live nodes.
      checkAnyLiveNodes(nodeList, solrCloudManager.getClusterStateProvider().getClusterState());

      int i = 0;
      List<ReplicaPosition> result = new ArrayList<>();
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

  public static class RulesBasedAssignStrategy implements AssignStrategy {
    public List<Rule> rules;
    @SuppressWarnings({"rawtypes"})
    public List snitches;
    public ClusterState clusterState;

    public RulesBasedAssignStrategy(List<Rule> rules, @SuppressWarnings({"rawtypes"})List snitches, ClusterState clusterState) {
      this.rules = rules;
      this.snitches = snitches;
      this.clusterState = clusterState;
    }

    @Override
    public List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, AssignRequest assignRequest) throws Assign.AssignmentException, IOException, InterruptedException {
      if (assignRequest.numTlogReplicas + assignRequest.numPullReplicas != 0) {
        throw new Assign.AssignmentException(
            Replica.Type.TLOG + " or " + Replica.Type.PULL + " replica types not supported with placement rules or cluster policies");
      }

      Map<String, Integer> shardVsReplicaCount = new HashMap<>();
      for (String shard : assignRequest.shardNames) shardVsReplicaCount.put(shard, assignRequest.numNrtReplicas);

      Map<String, Map<String, Integer>> shardVsNodes = new LinkedHashMap<>();
      DocCollection docCollection = solrCloudManager.getClusterStateProvider().getClusterState().getCollectionOrNull(assignRequest.collectionName);
      if (docCollection != null) {
        for (Slice slice : docCollection.getSlices()) {
          LinkedHashMap<String, Integer> n = new LinkedHashMap<>();
          shardVsNodes.put(slice.getName(), n);
          for (Replica replica : slice.getReplicas()) {
            Integer count = n.get(replica.getNodeName());
            if (count == null) count = 0;
            n.put(replica.getNodeName(), ++count);
          }
        }
      }

      List<String> nodesList = assignRequest.nodes == null ? new ArrayList<>(clusterState.getLiveNodes()) : assignRequest.nodes;

      ReplicaAssigner replicaAssigner = new ReplicaAssigner(rules,
          shardVsReplicaCount,
          snitches,
          shardVsNodes,
          nodesList,
          solrCloudManager, clusterState);

      Map<ReplicaPosition, String> nodeMappings = replicaAssigner.getNodeMappings();
      return nodeMappings.entrySet().stream()
          .map(e -> new ReplicaPosition(e.getKey().shard, e.getKey().index, e.getKey().type, e.getValue()))
          .collect(Collectors.toList());
    }
  }

  public static class PolicyBasedAssignStrategy implements AssignStrategy {
    public String policyName;

    public PolicyBasedAssignStrategy(String policyName) {
      this.policyName = policyName;
    }

    @Override
    public List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, AssignRequest assignRequest) throws Assign.AssignmentException, IOException, InterruptedException {
      return Assign.getPositionsUsingPolicy(assignRequest.collectionName,
          assignRequest.shardNames, assignRequest.numNrtReplicas,
          assignRequest.numTlogReplicas, assignRequest.numPullReplicas,
          policyName, solrCloudManager, assignRequest.nodes);
    }
  }

  public static class AssignStrategyFactory {
    public SolrCloudManager solrCloudManager;

    public AssignStrategyFactory(SolrCloudManager solrCloudManager) {
      this.solrCloudManager = solrCloudManager;
    }

    public AssignStrategy create(ClusterState clusterState, DocCollection collection) throws IOException, InterruptedException {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<Map> ruleMaps = (List<Map>) collection.get("rule");
      String policyName = collection.getStr(POLICY);
      @SuppressWarnings({"rawtypes"})
      List snitches = (List) collection.get(SNITCH);

      Strategy strategy = null;
      if ((ruleMaps == null || ruleMaps.isEmpty()) && !usePolicyFramework(collection, solrCloudManager)) {
        strategy = Strategy.LEGACY;
      } else if (ruleMaps != null && !ruleMaps.isEmpty()) {
        strategy = Strategy.RULES;
      } else {
        strategy = Strategy.POLICY;
      }

      switch (strategy) {
        case LEGACY:
          return new LegacyAssignStrategy();
        case RULES:
          List<Rule> rules = new ArrayList<>();
          for (Object map : ruleMaps) rules.add(new Rule((Map) map));
          return new RulesBasedAssignStrategy(rules, snitches, clusterState);
        case POLICY:
          return new PolicyBasedAssignStrategy(policyName);
        default:
          throw new Assign.AssignmentException("Unknown strategy type: " + strategy);
      }
    }

    private enum Strategy {
      LEGACY, RULES, POLICY;
    }
  }
}
