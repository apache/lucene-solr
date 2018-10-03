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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.AddReplicaCmd;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.CreateShardCmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.api.collections.SplitShardCmd;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.zookeeper.CreateMode;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Simulated {@link ClusterStateProvider}.
 * <p>The following behaviors are supported:</p>
 *   <ul>
 *     <li>using autoscaling policy for replica placements</li>
 *     <li>maintaining and up-to-date list of /live_nodes and nodeAdded / nodeLost markers</li>
 *     <li>running a simulated leader election on collection changes (with throttling), when needed</li>
 *     <li>maintaining an up-to-date /clusterstate.json (single file format), which also tracks replica states,
 *     leader election changes, replica property changes, etc. Note: this file is only written,
 *     but never read by the framework!</li>
 *     <li>maintaining an up-to-date /clusterprops.json. Note: this file is only written, but never read by the
 *     framework!</li>
 *   </ul>
 */
public class SimClusterStateProvider implements ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final long DEFAULT_DOC_SIZE_BYTES = 500;

  private static final String BUFFERED_UPDATES = "__buffered_updates__";

  private final LiveNodesSet liveNodes;
  private final SimDistribStateManager stateManager;
  private final SimCloudManager cloudManager;

  private final Map<String, List<ReplicaInfo>> nodeReplicaMap = new ConcurrentHashMap<>();
  private final Map<String, Map<String, List<ReplicaInfo>>> colShardReplicaMap = new ConcurrentHashMap<>();
  private final Map<String, Object> clusterProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> collProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Map<String, Object>>> sliceProperties = new ConcurrentHashMap<>();

  private final ReentrantLock lock = new ReentrantLock();

  private final Map<String, Map<String, ActionThrottle>> leaderThrottles = new ConcurrentHashMap<>();

  // default map of: operation -> delay
  private final Map<String, Long> defaultOpDelays = new HashMap<>();
  // per-collection map of: collection -> op -> delay
  private final Map<String, Map<String, Long>> opDelays = new ConcurrentHashMap<>();


  private volatile int clusterStateVersion = 0;
  private volatile String overseerLeader = null;

  private Map<String, Object> lastSavedProperties = null;

  private AtomicReference<Map<String, DocCollection>> collectionsStatesRef = new AtomicReference<>();

  private Random bulkUpdateRandom = new Random(0);

  private transient boolean closed;

  /**
   * The instance needs to be initialized using the <code>sim*</code> methods in order
   * to ensure proper behavior, otherwise it will behave as a cluster with zero replicas.
   */
  public SimClusterStateProvider(LiveNodesSet liveNodes, SimCloudManager cloudManager) throws Exception {
    this.liveNodes = liveNodes;
    for (String nodeId : liveNodes.get()) {
      createEphemeralLiveNode(nodeId);
    }
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getSimDistribStateManager();
    // names are CollectionAction operation names, delays are in ms (simulated time)
    defaultOpDelays.put(CollectionParams.CollectionAction.MOVEREPLICA.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETEREPLICA.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.ADDREPLICA.name(), 500L);
    defaultOpDelays.put(CollectionParams.CollectionAction.SPLITSHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.CREATESHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETESHARD.name(), 5000L);
    defaultOpDelays.put(CollectionParams.CollectionAction.CREATE.name(), 500L);
    defaultOpDelays.put(CollectionParams.CollectionAction.DELETE.name(), 5000L);
  }

  // ============== SIMULATOR SETUP METHODS ====================

  /**
   * Initialize from an existing cluster state
   * @param initialState initial cluster state
   */
  public void simSetClusterState(ClusterState initialState) throws Exception {
    lock.lock();
    try {
      collProperties.clear();
      sliceProperties.clear();
      nodeReplicaMap.clear();
      liveNodes.clear();
      for (String nodeId : stateManager.listData(ZkStateReader.LIVE_NODES_ZKNODE)) {
        if (stateManager.hasData(ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeId)) {
          stateManager.removeData(ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeId, -1);
        }
        if (stateManager.hasData(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeId)) {
          stateManager.removeData(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeId, -1);
        }
      }
      liveNodes.addAll(initialState.getLiveNodes());
      for (String nodeId : liveNodes.get()) {
        createEphemeralLiveNode(nodeId);
      }
      initialState.forEachCollection(dc -> {
        collProperties.computeIfAbsent(dc.getName(), name -> new ConcurrentHashMap<>()).putAll(dc.getProperties());
        opDelays.computeIfAbsent(dc.getName(), c -> new HashMap<>()).putAll(defaultOpDelays);
        dc.getSlices().forEach(s -> {
          sliceProperties.computeIfAbsent(dc.getName(), name -> new ConcurrentHashMap<>())
              .computeIfAbsent(s.getName(), name -> new HashMap<>()).putAll(s.getProperties());
          s.getReplicas().forEach(r -> {
            ReplicaInfo ri = new ReplicaInfo(r.getName(), r.getCoreName(), dc.getName(), s.getName(), r.getType(), r.getNodeName(), r.getProperties());
            if (liveNodes.get().contains(r.getNodeName())) {
              nodeReplicaMap.computeIfAbsent(r.getNodeName(), rn -> new ArrayList<>()).add(ri);
            }
          });
        });
      });
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Reset the leader election throttles.
   */
  public void simResetLeaderThrottles() {
    leaderThrottles.clear();
  }

  private ActionThrottle getThrottle(String collection, String shard) {
    return leaderThrottles.computeIfAbsent(collection, coll -> new ConcurrentHashMap<>())
        .computeIfAbsent(shard, s -> new ActionThrottle("leader", 5000, cloudManager.getTimeSource()));
  }

  /**
   * Get random node id.
   * @return one of the live nodes
   */
  public String simGetRandomNode() {
    return simGetRandomNode(cloudManager.getRandom());
  }

  /**
   * Get random node id.
   * @param random instance of random.
   * @return one of the live nodes
   */
  public String simGetRandomNode(Random random) {
    if (liveNodes.isEmpty()) {
      return null;
    }
    List<String> nodes = new ArrayList<>(liveNodes.get());
    return nodes.get(random.nextInt(nodes.size()));
  }

  // todo: maybe hook up DistribStateManager /clusterstate.json watchers?

  private ReplicaInfo getReplicaInfo(Replica r) {
    List<ReplicaInfo> list = nodeReplicaMap.get(r.getNodeName());
    if (list == null) {
      return null;
    }
    for (ReplicaInfo ri : list) {
      if (r.getCoreName().equals(ri.getCore())) {
        return ri;
      }
    }
    return null;
  }

  /**
   * Add a new node to the cluster.
   * @param nodeId unique node id
   */
  public void simAddNode(String nodeId) throws Exception {
    ensureNotClosed();
    if (liveNodes.contains(nodeId)) {
      throw new Exception("Node " + nodeId + " already exists");
    }
    liveNodes.add(nodeId);
    createEphemeralLiveNode(nodeId);
    updateOverseerLeader();
    nodeReplicaMap.putIfAbsent(nodeId, new ArrayList<>());
  }

  /**
   * Remove node from a cluster. This is equivalent to a situation when a node is lost.
   * All replicas that were assigned to this node are marked as DOWN.
   * @param nodeId node id
   * @return true if a node existed and was removed
   */
  public boolean simRemoveNode(String nodeId) throws Exception {
    ensureNotClosed();
    lock.lockInterruptibly();
    try {
      Set<String> collections = new HashSet<>();
      // mark every replica on that node as down
      boolean res = liveNodes.remove(nodeId);
      setReplicaStates(nodeId, Replica.State.DOWN, collections);
      if (!collections.isEmpty()) {
        collectionsStatesRef.set(null);
      }
      // remove ephemeral nodes
      stateManager.getRoot().removeEphemeralChildren(nodeId);
      updateOverseerLeader();
      // create a nodeLost marker if needed
      AutoScalingConfig cfg = stateManager.getAutoScalingConfig(null);
      if (cfg.hasTriggerForEvents(TriggerEventType.NODELOST)) {
        stateManager.makePath(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + nodeId);
      }
      if (!collections.isEmpty()) {
        simRunLeaderElection(collections, true);
      }
      return res;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove all replica information related to dead nodes.
   */
  public void simRemoveDeadNodes() throws Exception {
    lock.lockInterruptibly();
    try {
      Set<String> myNodes = new HashSet<>(nodeReplicaMap.keySet());
      myNodes.removeAll(liveNodes.get());
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
  }

  private synchronized void updateOverseerLeader() throws Exception {
    if (overseerLeader != null && liveNodes.contains(overseerLeader)) {
      return;
    }
    String path = Overseer.OVERSEER_ELECT + "/leader";
    if (liveNodes.isEmpty()) {
      overseerLeader = null;
      // remove it from ZK
      try {
        cloudManager.getDistribStateManager().removeData(path, -1);
      } catch (NoSuchElementException e) {
        // ignore
      }
      return;
    }
    // pick first
    overseerLeader = liveNodes.iterator().next();
    log.debug("--- new Overseer leader: " + overseerLeader);
    // record it in ZK
    Map<String, Object> id = new HashMap<>();
    id.put("id", cloudManager.getTimeSource().getTimeNs() +
        "-" + overseerLeader + "-n_0000000000");
    try {
      cloudManager.getDistribStateManager().makePath(path, Utils.toJSON(id), CreateMode.EPHEMERAL, false);
    } catch (Exception e) {
      log.warn("Exception saving overseer leader id", e);
    }
  }

  // this method needs to be called under a lock
  private void setReplicaStates(String nodeId, Replica.State state, Set<String> changedCollections) {
    List<ReplicaInfo> replicas = nodeReplicaMap.get(nodeId);
    if (replicas != null) {
      replicas.forEach(r -> {
        r.getVariables().put(ZkStateReader.STATE_PROP, state.toString());
        if (state != Replica.State.ACTIVE) {
          r.getVariables().remove(ZkStateReader.LEADER_PROP);
        }
        changedCollections.add(r.getCollection());
      });
    }
  }

  // this method needs to be called under a lock
  private void createEphemeralLiveNode(String nodeId) throws Exception {
    DistribStateManager mgr = stateManager.withEphemeralId(nodeId);
    mgr.makePath(ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeId, null, CreateMode.EPHEMERAL, true);
    AutoScalingConfig cfg = stateManager.getAutoScalingConfig(null);
    if (cfg.hasTriggerForEvents(TriggerEventType.NODEADDED)) {
      mgr.makePath(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeId, null, CreateMode.EPHEMERAL, true);
    }
  }

  /**
   * Restore a previously removed node. This also simulates a short replica recovery state.
   * @param nodeId node id to restore
   * @return true when this operation restored any replicas, false otherwise (empty node).
   */
  public boolean simRestoreNode(String nodeId) throws Exception {
    liveNodes.add(nodeId);
    createEphemeralLiveNode(nodeId);
    Set<String> collections = new HashSet<>();
    lock.lockInterruptibly();
    try {
      setReplicaStates(nodeId, Replica.State.RECOVERING, collections);
    } finally {
      lock.unlock();
    }
    cloudManager.getTimeSource().sleep(1000);
    lock.lockInterruptibly();
    try {
      setReplicaStates(nodeId, Replica.State.ACTIVE, collections);
    } finally {
      lock.unlock();
    }
    if (!collections.isEmpty()) {
      collectionsStatesRef.set(null);
      simRunLeaderElection(collections, true);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Add a new replica. Note that if any details of a replica (node, coreNodeName, SolrCore name, etc)
   * are missing they will be filled in using the policy framework.
   * @param message replica details
   * @param results result of the operation
   */
  public void simAddReplica(ZkNodeProps message, NamedList results) throws Exception {
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    ClusterState clusterState = getClusterState();
    DocCollection coll = clusterState.getCollection(message.getStr(ZkStateReader.COLLECTION_PROP));
    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();

    Replica.Type replicaType = Replica.Type.valueOf(message.getStr(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name()).toUpperCase(Locale.ROOT));
    EnumMap<Replica.Type, Integer> replicaTypesVsCount = new EnumMap<>(Replica.Type.class);
    replicaTypesVsCount.put(Replica.Type.NRT, message.getInt(NRT_REPLICAS, replicaType == Replica.Type.NRT ? 1 : 0));
    replicaTypesVsCount.put(Replica.Type.TLOG, message.getInt(TLOG_REPLICAS, replicaType == Replica.Type.TLOG ? 1 : 0));
    replicaTypesVsCount.put(Replica.Type.PULL, message.getInt(PULL_REPLICAS, replicaType == Replica.Type.PULL ? 1 : 0));

    int totalReplicas = 0;
    for (Map.Entry<Replica.Type, Integer> entry : replicaTypesVsCount.entrySet()) {
      totalReplicas += entry.getValue();
    }
    if (totalReplicas > 1)  {
      if (message.getStr(CoreAdminParams.NAME) != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create " + totalReplicas + " replicas if 'name' parameter is specified");
      }
      if (message.getStr(CoreAdminParams.CORE_NODE_NAME) != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create " + totalReplicas + " replicas if 'coreNodeName' parameter is specified");
      }
    }

    List<ReplicaPosition> replicaPositions = AddReplicaCmd.buildReplicaPositions(cloudManager, clusterState, coll.getName(), message, replicaTypesVsCount, sessionWrapper);
    for (ReplicaPosition replicaPosition : replicaPositions) {
      AddReplicaCmd.CreateReplica createReplica = AddReplicaCmd.assignReplicaDetails(cloudManager, clusterState, message, replicaPosition);
      if (message.getStr(CoreAdminParams.CORE_NODE_NAME) == null) {
        createReplica.coreNodeName = Assign.assignCoreNodeName(stateManager, coll);
      }
      ReplicaInfo ri = new ReplicaInfo(
          createReplica.coreNodeName,
          createReplica.coreName,
          createReplica.collectionName,
          createReplica.sliceName,
          createReplica.replicaType,
          createReplica.node,
          message.getProperties()
      );
      simAddReplica(ri.getNode(), ri, true);
    }
    if (sessionWrapper.get() != null) {
      sessionWrapper.get().release();
    }
    results.add("success", "");
  }

  /**
   * Add a replica. Note that all details of the replica must be present here, including
   * node, coreNodeName and SolrCore name.
   * @param nodeId node id where the replica will be added
   * @param replicaInfo replica info
   * @param runLeaderElection if true then run a leader election after adding the replica.
   */
  public void simAddReplica(String nodeId, ReplicaInfo replicaInfo, boolean runLeaderElection) throws Exception {
    ensureNotClosed();
    // make sure SolrCore name is unique across cluster and coreNodeName within collection
    for (Map.Entry<String, List<ReplicaInfo>> e : nodeReplicaMap.entrySet()) {
      for (ReplicaInfo ri : e.getValue()) {
        if (ri.getCore().equals(replicaInfo.getCore())) {
          throw new Exception("Duplicate SolrCore name for existing=" + ri + " on node " + e.getKey() + " and new=" + replicaInfo);
        }
        if (ri.getName().equals(replicaInfo.getName()) && ri.getCollection().equals(replicaInfo.getCollection())) {
          throw new Exception("Duplicate coreNode name for existing=" + ri + " on node " + e.getKey() + " and new=" + replicaInfo);
        }
      }
    }
    if (!liveNodes.contains(nodeId)) {
      throw new Exception("Target node " + nodeId + " is not live: " + liveNodes);
    }
    // verify info
    if (replicaInfo.getCore() == null) {
      throw new Exception("Missing core: " + replicaInfo);
    }
    // XXX replica info is not supposed to have this as a variable
    replicaInfo.getVariables().remove(ZkStateReader.SHARD_ID_PROP);
    if (replicaInfo.getName() == null) {
      throw new Exception("Missing name: " + replicaInfo);
    }
    if (replicaInfo.getNode() == null) {
      throw new Exception("Missing node: " + replicaInfo);
    }
    if (!replicaInfo.getNode().equals(nodeId)) {
      throw new Exception("Wrong node (not " + nodeId + "): " + replicaInfo);
    }

    lock.lockInterruptibly();
    try {

      opDelay(replicaInfo.getCollection(), CollectionParams.CollectionAction.ADDREPLICA.name());

      List<ReplicaInfo> replicas = nodeReplicaMap.computeIfAbsent(nodeId, n -> new ArrayList<>());
      // mark replica as active
      replicaInfo.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      // add a property expected in Policy calculations, if missing
      if (replicaInfo.getVariable(Type.CORE_IDX.metricsAttribute) == null) {
        replicaInfo.getVariables().put(Type.CORE_IDX.metricsAttribute, new AtomicLong(SimCloudManager.DEFAULT_IDX_SIZE_BYTES));
        replicaInfo.getVariables().put(Variable.coreidxsize,
            new AtomicDouble((Double)Type.CORE_IDX.convertVal(SimCloudManager.DEFAULT_IDX_SIZE_BYTES)));
      }

      replicas.add(replicaInfo);
      colShardReplicaMap.computeIfAbsent(replicaInfo.getCollection(), c -> new ConcurrentHashMap<>())
          .computeIfAbsent(replicaInfo.getShard(), s -> new ArrayList<>())
          .add(replicaInfo);

      Map<String, Object> values = cloudManager.getSimNodeStateProvider().simGetAllNodeValues()
          .computeIfAbsent(nodeId, id -> new ConcurrentHashMap<>(SimCloudManager.createNodeValues(id)));
      // update the number of cores and freedisk in node values
      Integer cores = (Integer)values.get(ImplicitSnitch.CORES);
      if (cores == null) {
        cores = 0;
      }
      cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, ImplicitSnitch.CORES, cores + 1);
      Integer disk = (Integer)values.get(ImplicitSnitch.DISK);
      if (disk == null) {
        disk = SimCloudManager.DEFAULT_FREE_DISK;
      }
      cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, ImplicitSnitch.DISK, disk - 1);
      // fake metrics
      String registry = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, replicaInfo.getCollection(),
          replicaInfo.getShard(),
          Utils.parseMetricsReplicaName(replicaInfo.getCollection(), replicaInfo.getCore()));
      cloudManager.getMetricManager().registry(registry).counter("UPDATE./update.requests");
      cloudManager.getMetricManager().registry(registry).counter("QUERY./select.requests");
      cloudManager.getMetricManager().registerGauge(null, registry,
          () -> ((Number)replicaInfo.getVariable(Type.CORE_IDX.metricsAttribute)).longValue(),
          "", true, "INDEX.sizeInBytes");
      // at this point nuke our cached DocCollection state
      collectionsStatesRef.set(null);
      log.trace("-- simAddReplica {}", replicaInfo);
      if (runLeaderElection) {
        simRunLeaderElection(Collections.singleton(replicaInfo.getCollection()), true);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove replica.
   * @param nodeId node id
   * @param coreNodeName coreNodeName
   */
  public void simRemoveReplica(String nodeId, String coreNodeName) throws Exception {
    ensureNotClosed();
    lock.lockInterruptibly();
    List<ReplicaInfo> replicas = nodeReplicaMap.computeIfAbsent(nodeId, n -> new ArrayList<>());
    try {
      for (int i = 0; i < replicas.size(); i++) {
        if (coreNodeName.equals(replicas.get(i).getName())) {
          ReplicaInfo ri = replicas.remove(i);
          colShardReplicaMap.computeIfAbsent(ri.getCollection(), c -> new ConcurrentHashMap<>())
              .computeIfAbsent(ri.getShard(), s -> new ArrayList<>())
              .remove(ri);
          collectionsStatesRef.set(null);

          opDelay(ri.getCollection(), CollectionParams.CollectionAction.DELETEREPLICA.name());

          // update the number of cores in node values, if node is live
          if (liveNodes.contains(nodeId)) {
            Integer cores = (Integer)cloudManager.getSimNodeStateProvider().simGetNodeValue(nodeId, ImplicitSnitch.CORES);
            if (cores == null || cores == 0) {
              throw new Exception("Unexpected value of 'cores' (" + cores + ") on node: " + nodeId);
            }
            cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, ImplicitSnitch.CORES, cores - 1);
            Integer disk = (Integer)cloudManager.getSimNodeStateProvider().simGetNodeValue(nodeId, ImplicitSnitch.DISK);
            if (disk == null || disk == 0) {
              throw new Exception("Unexpected value of 'freedisk' (" + disk + ") on node: " + nodeId);
            }
            cloudManager.getSimNodeStateProvider().simSetNodeValue(nodeId, ImplicitSnitch.DISK, disk + 1);
          }
          log.trace("-- simRemoveReplica {}", ri);
          simRunLeaderElection(Collections.singleton(ri.getCollection()), true);
          return;
        }
      }
      throw new Exception("Replica " + coreNodeName + " not found on node " + nodeId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Save clusterstate.json to {@link DistribStateManager}.
   * @return saved state
   */
  private ClusterState saveClusterState(ClusterState state) throws IOException {
    ensureNotClosed();
    byte[] data = Utils.toJSON(state);
    try {
      VersionedData oldData = stateManager.getData(ZkStateReader.CLUSTER_STATE);
      int version = oldData != null ? oldData.getVersion() : -1;
      Assert.assertEquals(clusterStateVersion, version + 1);
      stateManager.setData(ZkStateReader.CLUSTER_STATE, data, version);
      log.debug("** saved cluster state version " + (version + 1));
      clusterStateVersion++;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return state;
  }

  /**
   * Delay an operation by a configured amount.
   * @param collection collection name
   * @param op operation name.
   */
  private void opDelay(String collection, String op) throws InterruptedException {
    Map<String, Long> delays = opDelays.get(collection);
    if (delays == null || delays.isEmpty() || !delays.containsKey(op)) {
      return;
    }
    cloudManager.getTimeSource().sleep(delays.get(op));
  }

  /**
   * Simulate running a shard leader election. This operation is a no-op if a leader already exists.
   * If a new leader is elected the cluster state is saved.
   * @param collections list of affected collections
   * @param saveClusterState if true then save cluster state regardless of changes.
   */
  private void simRunLeaderElection(Collection<String> collections, boolean saveClusterState) throws Exception {
    ensureNotClosed();
    if (saveClusterState) {
      collectionsStatesRef.set(null);
    }
    ClusterState state = getClusterState();
    state.forEachCollection(dc -> {
      if (!collections.contains(dc.getName())) {
        return;
      }
      dc.getSlices().forEach(s -> {
        if (s.getState() == Slice.State.INACTIVE) {
          log.trace("-- slice state is {}, skip leader election {} / {}", s.getState(), dc.getName(), s.getName());
          return;
        }
        if (s.getState() != Slice.State.ACTIVE) {
          log.trace("-- slice state is {}, but I will run leader election {} / {}", s.getState(), dc.getName(), s.getName());
        }
        if (s.getLeader() != null) {
          log.trace("-- already has leader {} / {}", dc.getName(), s.getName());
          return;
        }
        if (s.getReplicas().isEmpty()) {
          log.trace("-- no replicas in {} / {}", dc.getName(), s.getName());
          return;
        }
        log.trace("-- submit leader election for {} / {}", dc.getName(), s.getName());
        cloudManager.submit(() -> {
          simRunLeaderElection(dc.getName(), s, saveClusterState);
          return true;
        });
      });
    });
  }

  private void simRunLeaderElection(String collection, Slice s, boolean saveState) throws Exception {
    AtomicBoolean stateChanged = new AtomicBoolean(Boolean.FALSE);
    Replica leader = s.getLeader();
    if (leader == null || !liveNodes.contains(leader.getNodeName())) {
      log.trace("Running leader election for {} / {}", collection, s.getName());
      if (s.getReplicas().isEmpty()) { // no replicas - punt
        log.trace("-- no replicas in {} / {}", collection, s.getName());
        return;
      }
      ActionThrottle lt = getThrottle(collection, s.getName());
      synchronized (lt) {
        // collect all active and live
        List<ReplicaInfo> active = new ArrayList<>();
        AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);
        s.getReplicas().forEach(r -> {
          // find our ReplicaInfo for this replica
          ReplicaInfo ri = getReplicaInfo(r);
          if (ri == null) {
            throw new IllegalStateException("-- could not find ReplicaInfo for replica " + r);
          }
          synchronized (ri) {
            if (r.isActive(liveNodes.get())) {
              if (ri.getVariables().get(ZkStateReader.LEADER_PROP) != null) {
                log.trace("-- found existing leader {} / {}: {}, {}", collection, s.getName(), ri, r);
                alreadyHasLeader.set(true);
                return;
              } else {
                active.add(ri);
              }
            } else { // if it's on a node that is not live mark it down
              log.trace("-- replica not active on live nodes: {}, {}", liveNodes.get(), r);
              if (!liveNodes.contains(r.getNodeName())) {
                ri.getVariables().put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
                ri.getVariables().remove(ZkStateReader.LEADER_PROP);
                stateChanged.set(true);
              }
            }
          }
        });
        if (alreadyHasLeader.get()) {
          log.trace("-- already has leader {} / {}: {}", collection, s.getName(), s);
          return;
        }
        if (active.isEmpty()) {
          log.warn("Can't find any active replicas for {} / {}: {}", collection, s.getName(), s);
          log.debug("-- liveNodes: {}", liveNodes.get());
          return;
        }
        // pick first active one
        ReplicaInfo ri = null;
        for (ReplicaInfo a : active) {
          if (!a.getType().equals(Replica.Type.PULL)) {
            ri = a;
            break;
          }
        }
        if (ri == null) {
          log.warn("-- can't find any suitable replica type for {} / {}: {}", collection, s.getName(), s);
          return;
        }
        // now mark the leader election throttle
        lt.minimumWaitBetweenActions();
        lt.markAttemptingAction();
        synchronized (ri) {
          ri.getVariables().put(ZkStateReader.LEADER_PROP, "true");
        }
        log.debug("-- elected new leader for {} / {} (currentVersion={}): {}", collection,
            s.getName(), clusterStateVersion, ri);
        stateChanged.set(true);
      }
    } else {
      log.trace("-- already has leader for {} / {}", collection, s.getName());
    }
    if (stateChanged.get() || saveState) {
      collectionsStatesRef.set(null);
    }
  }

  /**
   * Create a new collection. This operation uses policy framework for node and replica assignments.
   * @param props collection details
   * @param results results of the operation.
   */
  public void simCreateCollection(ZkNodeProps props, NamedList results) throws Exception {
    ensureNotClosed();
    if (props.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, props.getStr(CommonAdminParams.ASYNC));
    }
    boolean waitForFinalState = props.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    final String collectionName = props.getStr(NAME);
    log.debug("-- simCreateCollection {}, currentVersion={}", collectionName, clusterStateVersion);

    String router = props.getStr("router.name", DocRouter.DEFAULT_NAME);
    String policy = props.getStr(Policy.POLICY);
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    boolean usePolicyFramework = !autoScalingConfig.getPolicy().getClusterPolicy().isEmpty() || policy != null;

    // fail fast if parameters are wrong or incomplete
    List<String> shardNames = CreateCollectionCmd.populateShardNames(props, router);
    CreateCollectionCmd.checkMaxShardsPerNode(props, usePolicyFramework);
    CreateCollectionCmd.checkReplicaTypes(props);

    // always force getting fresh state
    collectionsStatesRef.set(null);
    final ClusterState clusterState = getClusterState();

    String withCollection = props.getStr(CollectionAdminParams.WITH_COLLECTION);
    String wcShard = null;
    if (withCollection != null) {
      if (!clusterState.hasCollection(withCollection)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The 'withCollection' does not exist: " + withCollection);
      } else  {
        DocCollection collection = clusterState.getCollection(withCollection);
        if (collection.getActiveSlices().size() > 1)  {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + collection.getActiveSlices().size());
        }
        wcShard = collection.getActiveSlices().iterator().next().getName();
      }
    }
    final String withCollectionShard = wcShard;

    lock.lockInterruptibly();
    ZkWriteCommand cmd = ZkWriteCommand.noop();
    try {
      cmd = new ClusterStateMutator(cloudManager).createCollection(clusterState, props);
      if (cmd.noop) {
        log.warn("Collection {} already exists. exit", collectionName);
        log.debug("-- collection: {}, clusterState: {}", collectionName, clusterState);
        results.add("success", "no-op");
        return;
      }
      // add collection props
      DocCollection coll = cmd.collection;
      collProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>()).putAll(coll.getProperties());
      colShardReplicaMap.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>());
      // add slice props
      coll.getSlices().forEach(s -> {
        Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll.getName(), c -> new ConcurrentHashMap<>())
            .computeIfAbsent(s.getName(), slice -> new ConcurrentHashMap<>());
        s.getProperties().forEach((k, v) -> {
          if (k != null && v != null) {
            sliceProps.put(k, v);
          }
        });
        colShardReplicaMap.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
            .computeIfAbsent(s.getName(), sh -> new ArrayList<>());
      });

      // modify the `withCollection` and store this new collection's name with it
      if (withCollection != null) {
        ZkNodeProps message = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, MODIFYCOLLECTION.toString(),
            ZkStateReader.COLLECTION_PROP, withCollection,
            CollectionAdminParams.COLOCATED_WITH, collectionName);
        cmd = new CollectionMutator(cloudManager).modifyCollection(clusterState,message);
      }
      // force recreation of collection states
      collectionsStatesRef.set(null);

    } finally {
      lock.unlock();
    }
    opDelays.computeIfAbsent(collectionName, c -> new HashMap<>()).putAll(defaultOpDelays);

    opDelay(collectionName, CollectionParams.CollectionAction.CREATE.name());

    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();
    List<ReplicaPosition> replicaPositions = CreateCollectionCmd.buildReplicaPositions(cloudManager, getClusterState(), cmd.collection, props,
        shardNames, sessionWrapper);
    if (sessionWrapper.get() != null) {
      sessionWrapper.get().release();
    }
    // calculate expected number of positions
    int numTlogReplicas = props.getInt(TLOG_REPLICAS, 0);
    int numNrtReplicas = props.getInt(NRT_REPLICAS, props.getInt(REPLICATION_FACTOR, numTlogReplicas>0?0:1));
    int numPullReplicas = props.getInt(PULL_REPLICAS, 0);
    int totalReplicas = shardNames.size() * (numNrtReplicas + numPullReplicas + numTlogReplicas);
    Assert.assertEquals("unexpected number of replica positions", totalReplicas, replicaPositions.size());
    final CountDownLatch finalStateLatch = new CountDownLatch(replicaPositions.size());
    AtomicInteger replicaNum = new AtomicInteger(1);
    replicaPositions.forEach(pos -> {

      if (withCollection != null) {
        // check that we have a replica of `withCollection` on this node and if not, create one
        DocCollection collection = clusterState.getCollection(withCollection);
        List<Replica> replicas = collection.getReplicas(pos.node);
        if (replicas == null || replicas.isEmpty()) {
          Map<String, Object> replicaProps = new HashMap<>();
          replicaProps.put(ZkStateReader.NODE_NAME_PROP, pos.node);
          replicaProps.put(ZkStateReader.REPLICA_TYPE, pos.type.toString());
          String coreName = String.format(Locale.ROOT, "%s_%s_replica_%s%s", withCollection, withCollectionShard, pos.type.name().substring(0,1).toLowerCase(Locale.ROOT),
              collection.getReplicas().size() + 1);
          try {
            replicaProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
            replicaProps.put("SEARCHER.searcher.deletedDocs", new AtomicLong(0));
            replicaProps.put("SEARCHER.searcher.numDocs", new AtomicLong(0));
            replicaProps.put("SEARCHER.searcher.maxDoc", new AtomicLong(0));
            ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, withCollection, 0),
                coreName, withCollection, withCollectionShard, pos.type, pos.node, replicaProps);
            cloudManager.submit(() -> {
              simAddReplica(pos.node, ri, false);
              // do not count down the latch here
              return true;
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      Map<String, Object> replicaProps = new HashMap<>();
      replicaProps.put(ZkStateReader.NODE_NAME_PROP, pos.node);
      replicaProps.put(ZkStateReader.REPLICA_TYPE, pos.type.toString());
      String coreName = String.format(Locale.ROOT, "%s_%s_replica_%s%s", collectionName, pos.shard, pos.type.name().substring(0,1).toLowerCase(Locale.ROOT),
          replicaNum.getAndIncrement());
      try {
        replicaProps.put(ZkStateReader.CORE_NAME_PROP, coreName);
        replicaProps.put("SEARCHER.searcher.deletedDocs", new AtomicLong(0));
        replicaProps.put("SEARCHER.searcher.numDocs", new AtomicLong(0));
        replicaProps.put("SEARCHER.searcher.maxDoc", new AtomicLong(0));
        ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, collectionName, 0),
            coreName, collectionName, pos.shard, pos.type, pos.node, replicaProps);
        cloudManager.submit(() -> {
          simAddReplica(pos.node, ri, true);
          finalStateLatch.countDown();
          return true;
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // force recreation of collection states
    collectionsStatesRef.set(null);
    //simRunLeaderElection(Collections.singleton(collectionName), true);
    if (waitForFinalState) {
      boolean finished = finalStateLatch.await(cloudManager.getTimeSource().convertDelay(TimeUnit.SECONDS, 60, TimeUnit.MILLISECONDS),
          TimeUnit.MILLISECONDS);
      if (!finished) {
        results.add("failure", "Timeout waiting for all replicas to become active.");
        return;
      }
    }
    results.add("success", "");
    log.debug("-- finished createCollection {}, currentVersion={}", collectionName, clusterStateVersion);
  }

  /**
   * Delete a collection
   * @param collection collection name
   * @param async async id
   * @param results results of the operation
   */
  public void simDeleteCollection(String collection, String async, NamedList results) throws Exception {
    ensureNotClosed();
    if (async != null) {
      results.add(CoreAdminParams.REQUESTID, async);
    }
    lock.lockInterruptibly();
    try {
      collProperties.remove(collection);
      sliceProperties.remove(collection);
      leaderThrottles.remove(collection);
      colShardReplicaMap.remove(collection);

      opDelay(collection, CollectionParams.CollectionAction.DELETE.name());

      opDelays.remove(collection);
      nodeReplicaMap.forEach((n, replicas) -> {
        for (Iterator<ReplicaInfo> it = replicas.iterator(); it.hasNext(); ) {
          ReplicaInfo ri = it.next();
          if (ri.getCollection().equals(collection)) {
            it.remove();
            // update the number of cores in node values
            Integer cores = (Integer) cloudManager.getSimNodeStateProvider().simGetNodeValue(n, "cores");
            if (cores != null) { // node is still up
              if (cores == 0) {
                throw new RuntimeException("Unexpected value of 'cores' (" + cores + ") on node: " + n);
              }
              try {
                cloudManager.getSimNodeStateProvider().simSetNodeValue(n, "cores", cores - 1);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted");
              }
            }
          }
        }
      });
      collectionsStatesRef.set(null);
      results.add("success", "");
    } catch (Exception e) {
      log.warn("Exception", e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove all collections.
   */
  public void simDeleteAllCollections() throws Exception {
    lock.lockInterruptibly();
    collectionsStatesRef.set(null);
    try {
      collProperties.clear();
      sliceProperties.clear();
      leaderThrottles.clear();
      nodeReplicaMap.clear();
      colShardReplicaMap.clear();
      cloudManager.getSimNodeStateProvider().simGetAllNodeValues().forEach((n, values) -> {
        values.put(ImplicitSnitch.CORES, 0);
        values.put(ImplicitSnitch.DISK, SimCloudManager.DEFAULT_FREE_DISK);
        values.put(Variable.Type.TOTALDISK.tagName, SimCloudManager.DEFAULT_TOTAL_DISK);
        values.put(ImplicitSnitch.SYSLOADAVG, 1.0);
        values.put(ImplicitSnitch.HEAPUSAGE, 123450000);
      });
    } finally {
      lock.unlock();
    }
  }

  /**
   * Move replica. This uses a similar algorithm as {@link org.apache.solr.cloud.api.collections.MoveReplicaCmd#moveNormalReplica(ClusterState, NamedList, String, String, DocCollection, Replica, Slice, int, boolean)}.
   * @param message operation details
   * @param results operation results.
   */
  public void simMoveReplica(ZkNodeProps message, NamedList results) throws Exception {
    ensureNotClosed();
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collection = message.getStr(COLLECTION_PROP);
    String targetNode = message.getStr("targetNode");
    ClusterState clusterState = getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    String replicaName = message.getStr(REPLICA_PROP);
    Replica replica = coll.getReplica(replicaName);
    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Collection: " + collection + " replica: " + replicaName + " does not exist");
    }
    Slice slice = null;
    for (Slice s : coll.getSlices()) {
      if (s.getReplicas().contains(replica)) {
        slice = s;
      }
    }
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Replica has no 'slice' property! : " + replica);
    }

    opDelay(collection, CollectionParams.CollectionAction.MOVEREPLICA.name());

    // TODO: for now simulate moveNormalReplica sequence, where we first add new replica and then delete the old one

    String newSolrCoreName = Assign.buildSolrCoreName(stateManager, coll, slice.getName(), replica.getType());
    String coreNodeName = Assign.assignCoreNodeName(stateManager, coll);
    ReplicaInfo newReplica = new ReplicaInfo(coreNodeName, newSolrCoreName, collection, slice.getName(), replica.getType(), targetNode, null);
    log.debug("-- new replica: " + newReplica);
    // xxx should run leader election here already?
    simAddReplica(targetNode, newReplica, false);
    // this will trigger leader election
    simRemoveReplica(replica.getNodeName(), replica.getName());
    results.add("success", "");
  }

  /**
   * Create a new shard. This uses a similar algorithm as {@link CreateShardCmd}.
   * @param message operation details
   * @param results operation results
   */
  public void simCreateShard(ZkNodeProps message, NamedList results) throws Exception {
    ensureNotClosed();
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    ClusterState clusterState = getClusterState();
    lock.lockInterruptibly();
    try {
      ZkWriteCommand cmd = new CollectionMutator(cloudManager).createShard(clusterState, message);
      if (cmd.noop) {
        results.add("success", "no-op");
        return;
      }

      opDelay(collectionName, CollectionParams.CollectionAction.CREATESHARD.name());

      // copy shard properties -- our equivalent of creating an empty shard in cluster state
      DocCollection collection = cmd.collection;
      Slice slice = collection.getSlice(sliceName);
      Map<String, Object> props = sliceProperties.computeIfAbsent(collection.getName(), c -> new ConcurrentHashMap<>())
          .computeIfAbsent(sliceName, s -> new ConcurrentHashMap<>());
      props.clear();
      slice.getProperties().entrySet().stream()
          .filter(e -> !e.getKey().equals("range"))
          .filter(e -> !e.getKey().equals("replicas"))
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      // 2. create new replicas
      EnumMap<Replica.Type, Integer> replicaTypesVsCount = new EnumMap<>(Replica.Type.class);
      int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, collection.getInt(NRT_REPLICAS, collection.getInt(REPLICATION_FACTOR, 1))));
      int numTlogReplicas = message.getInt(TLOG_REPLICAS, message.getInt(TLOG_REPLICAS, collection.getInt(TLOG_REPLICAS, 0)));
      int numPullReplicas = message.getInt(PULL_REPLICAS, message.getInt(PULL_REPLICAS, collection.getInt(PULL_REPLICAS, 0)));
      replicaTypesVsCount.put(Replica.Type.NRT, numNrtReplicas);
      replicaTypesVsCount.put(Replica.Type.TLOG, numTlogReplicas);
      replicaTypesVsCount.put(Replica.Type.PULL, numPullReplicas);

      ZkNodeProps addReplicasProps = new ZkNodeProps(
          COLLECTION_PROP, collectionName,
          SHARD_ID_PROP, sliceName,
          ZkStateReader.NRT_REPLICAS, String.valueOf(replicaTypesVsCount.get(Replica.Type.NRT)),
          ZkStateReader.TLOG_REPLICAS, String.valueOf(replicaTypesVsCount.get(Replica.Type.TLOG)),
          ZkStateReader.PULL_REPLICAS, String.valueOf(replicaTypesVsCount.get(Replica.Type.PULL)),
          OverseerCollectionMessageHandler.CREATE_NODE_SET, message.getStr(OverseerCollectionMessageHandler.CREATE_NODE_SET)
          );

        try {
          simAddReplica(addReplicasProps, results);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

      collProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>());

      simRunLeaderElection(Collections.singleton(collectionName), true);
      results.add("success", "");
    } finally {
      lock.unlock();
    }
  }

  /**
   * Split a shard. This uses a similar algorithm as {@link SplitShardCmd}, including simulating its
   * quirks, and leaving the original parent slice in place.
   * @param message operation details
   * @param results operation results.
   */
  public void simSplitShard(ZkNodeProps message, NamedList results) throws Exception {
    ensureNotClosed();
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collectionName = message.getStr(COLLECTION_PROP);
    AtomicReference<String> sliceName = new AtomicReference<>();
    sliceName.set(message.getStr(SHARD_ID_PROP));
    String splitKey = message.getStr("split.key");

    ClusterState clusterState = getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice parentSlice = SplitShardCmd.getParentSlice(clusterState, collectionName, sliceName, splitKey);
    Replica leader = parentSlice.getLeader();
    // XXX leader election may not have happened yet - should we require it?
    if (leader == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Shard " + collectionName +
          " /  " + sliceName.get() + " has no leader and can't be split");
    }
    // start counting buffered updates
    Map<String, Object> props = sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
        .computeIfAbsent(sliceName.get(), ss -> new ConcurrentHashMap<>());
    if (props.containsKey(BUFFERED_UPDATES)) {
      log.debug("--- SOLR-12729: Overlapping splitShard commands for {} / {}", collectionName, sliceName.get());
      return;
    }
    props.put(BUFFERED_UPDATES, new AtomicLong());

    List<DocRouter.Range> subRanges = new ArrayList<>();
    List<String> subSlices = new ArrayList<>();
    List<String> subShardNames = new ArrayList<>();

    opDelay(collectionName, CollectionParams.CollectionAction.SPLITSHARD.name());

    SplitShardCmd.fillRanges(cloudManager, message, collection, parentSlice, subRanges, subSlices, subShardNames, true);
    // add replicas for new subShards
    int repFactor = parentSlice.getReplicas().size();
    Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
        .forCollection(collectionName)
        .forShard(subSlices)
        .assignNrtReplicas(repFactor)
        .assignTlogReplicas(0)
        .assignPullReplicas(0)
        .onNodes(new ArrayList<>(clusterState.getLiveNodes()))
        .build();
    Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(cloudManager);
    Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, collection);
    // reproduce the bug
    List<ReplicaPosition> replicaPositions = assignStrategy.assign(cloudManager, assignRequest);
    PolicyHelper.SessionWrapper sessionWrapper = PolicyHelper.getLastSessionWrapper(true);
    if (sessionWrapper != null) sessionWrapper.release();

    // adjust numDocs / deletedDocs / maxDoc
    String numDocsStr = String.valueOf(getReplicaInfo(leader).getVariable("SEARCHER.searcher.numDocs", "0"));
    long numDocs = Long.parseLong(numDocsStr);
    long newNumDocs = numDocs / subSlices.size();
    long remainderDocs = numDocs % subSlices.size();
    long newIndexSize = SimCloudManager.DEFAULT_IDX_SIZE_BYTES + newNumDocs * DEFAULT_DOC_SIZE_BYTES;
    long remainderIndexSize = SimCloudManager.DEFAULT_IDX_SIZE_BYTES + remainderDocs * DEFAULT_DOC_SIZE_BYTES;
    String remainderSlice = null;

    // add slice props
    for (int i = 0; i < subRanges.size(); i++) {
      String subSlice = subSlices.get(i);
      DocRouter.Range range = subRanges.get(i);
      Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
          .computeIfAbsent(subSlice, ss -> new ConcurrentHashMap<>());
      sliceProps.put(Slice.RANGE, range);
      sliceProps.put(Slice.PARENT, sliceName.get());
      sliceProps.put(ZkStateReader.STATE_PROP, Slice.State.CONSTRUCTION.toString());
      sliceProps.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
    }
    // add replicas
    for (ReplicaPosition replicaPosition : replicaPositions) {
      String subSliceName = replicaPosition.shard;
      String subShardNodeName = replicaPosition.node;
//      String solrCoreName = collectionName + "_" + subSliceName + "_replica_n" + (replicaPosition.index);
      String solrCoreName = Assign.buildSolrCoreName(collectionName, subSliceName, replicaPosition.type, Assign.incAndGetId(stateManager, collectionName, 0));
      Map<String, Object> replicaProps = new HashMap<>();
      replicaProps.put(ZkStateReader.SHARD_ID_PROP, replicaPosition.shard);
      replicaProps.put(ZkStateReader.NODE_NAME_PROP, replicaPosition.node);
      replicaProps.put(ZkStateReader.REPLICA_TYPE, replicaPosition.type.toString());
      replicaProps.put(ZkStateReader.BASE_URL_PROP, Utils.getBaseUrlForNodeName(subShardNodeName, "http"));

      long replicasNumDocs = newNumDocs;
      long replicasIndexSize = newIndexSize;
      if (remainderSlice == null) {
        remainderSlice = subSliceName;
      }
      if (remainderSlice.equals(subSliceName)) { // only add to one sub slice
        replicasNumDocs += remainderDocs;
        replicasIndexSize += remainderIndexSize;
      }
      replicaProps.put("SEARCHER.searcher.numDocs", new AtomicLong(replicasNumDocs));
      replicaProps.put("SEARCHER.searcher.maxDoc", new AtomicLong(replicasNumDocs));
      replicaProps.put("SEARCHER.searcher.deletedDocs", new AtomicLong(0));
      replicaProps.put(Type.CORE_IDX.metricsAttribute, new AtomicLong(replicasIndexSize));
      replicaProps.put(Variable.coreidxsize, new AtomicDouble((Double)Type.CORE_IDX.convertVal(replicasIndexSize)));

      ReplicaInfo ri = new ReplicaInfo("core_node" + Assign.incAndGetId(stateManager, collectionName, 0),
          solrCoreName, collectionName, replicaPosition.shard, replicaPosition.type, subShardNodeName, replicaProps);
      simAddReplica(replicaPosition.node, ri, false);
    }
    simRunLeaderElection(Collections.singleton(collectionName), true);

    // delay it once again to better simulate replica recoveries
    //opDelay(collectionName, CollectionParams.CollectionAction.SPLITSHARD.name());

    CloudTestUtils.waitForState(cloudManager, collectionName, 30, TimeUnit.SECONDS, (liveNodes, state) -> {
      for (String subSlice : subSlices) {
        Slice s = state.getSlice(subSlice);
        if (s.getLeader() == null) {
          log.debug("** no leader in {} / {}", collectionName, s);
          return false;
        }
        if (s.getReplicas().size() < repFactor) {
          log.debug("** expected {} repFactor but there are {} replicas", repFactor, s.getReplicas().size());
          return false;
        }
      }
      return true;
    });
    // mark the new slices as active and the old slice as inactive
    log.trace("-- switching slice states after split shard: collection={}, parent={}, subSlices={}", collectionName,
        sliceName.get(), subSlices);
    lock.lockInterruptibly();
    try {
      Map<String, Object> sProps = sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
          .computeIfAbsent(sliceName.get(), s -> new ConcurrentHashMap<>());
      sProps.put(ZkStateReader.STATE_PROP, Slice.State.INACTIVE.toString());
      sProps.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
      AtomicLong bufferedUpdates = (AtomicLong)sProps.remove(BUFFERED_UPDATES);
      if (bufferedUpdates.get() > 0) {
        // apply buffered updates
        long perShard = bufferedUpdates.get() / subSlices.size();
        long remainder = bufferedUpdates.get() % subSlices.size();
        log.debug("-- applying {} buffered docs from {} / {}, perShard={}, remainder={}", bufferedUpdates.get(),
            collectionName, parentSlice.getName(), perShard, remainder);
        for (int i = 0; i < subSlices.size(); i++) {
          String sub = subSlices.get(i);
          long numUpdates = perShard;
          if (i == 0) {
            numUpdates += remainder;
          }
          simSetShardValue(collectionName, sub, "SEARCHER.searcher.numDocs", numUpdates, true, false);
          simSetShardValue(collectionName, sub, "SEARCHER.searcher.maxDoc", numUpdates, true, false);
        }
      }
      // XXX also mark replicas as down? currently SplitShardCmd doesn't do this

      for (String s : subSlices) {
        Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>())
            .computeIfAbsent(s, ss -> new ConcurrentHashMap<>());
        sliceProps.put(ZkStateReader.STATE_PROP, Slice.State.ACTIVE.toString());
        sliceProps.put(ZkStateReader.STATE_TIMESTAMP_PROP, String.valueOf(cloudManager.getTimeSource().getEpochTimeNs()));
      }

      // invalidate cached state
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
    results.add("success", "");

  }

  /**
   * Delete a shard. This uses a similar algorithm as {@link org.apache.solr.cloud.api.collections.DeleteShardCmd}
   * @param message operation details
   * @param results operation results
   */
  public void simDeleteShard(ZkNodeProps message, NamedList results) throws Exception {
    ensureNotClosed();
    if (message.getStr(CommonAdminParams.ASYNC) != null) {
      results.add(CoreAdminParams.REQUESTID, message.getStr(CommonAdminParams.ASYNC));
    }
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    ClusterState clusterState = getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    if (collection == null) {
      throw new Exception("Collection " + collectionName + " doesn't exist");
    }
    Slice slice = collection.getSlice(sliceName);
    if (slice == null) {
      throw new Exception(" Collection " + collectionName + " slice " + sliceName + " doesn't exist.");
    }

    opDelay(collectionName, CollectionParams.CollectionAction.DELETESHARD.name());

    lock.lockInterruptibly();
    try {
      sliceProperties.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>()).remove(sliceName);
      colShardReplicaMap.computeIfAbsent(collectionName, c -> new ConcurrentHashMap<>()).remove(sliceName);
      nodeReplicaMap.forEach((n, replicas) -> {
        Iterator<ReplicaInfo> it = replicas.iterator();
        while (it.hasNext()) {
          ReplicaInfo ri = it.next();
          if (ri.getCollection().equals(collectionName) && ri.getShard().equals(sliceName)) {
            it.remove();
          }
        }
      });
      collectionsStatesRef.set(null);
      results.add("success", "");
    } catch (Exception e) {
      results.add("failure", e.toString());
    } finally {
      lock.unlock();
    }
  }

  public synchronized void createSystemCollection() throws IOException {
    try {
      if (colShardReplicaMap.containsKey(CollectionAdminParams.SYSTEM_COLL)) {
        return;
      }
      String repFactor = String.valueOf(Math.min(3, liveNodes.size()));
      ZkNodeProps props = new ZkNodeProps(
          NAME, CollectionAdminParams.SYSTEM_COLL,
          REPLICATION_FACTOR, repFactor,
          OverseerCollectionMessageHandler.NUM_SLICES, "1",
          CommonAdminParams.WAIT_FOR_FINAL_STATE, "true"
      );
      simCreateCollection(props, new NamedList());
      CloudTestUtils.waitForState(cloudManager, CollectionAdminParams.SYSTEM_COLL, 20, TimeUnit.SECONDS,
          CloudTestUtils.clusterShape(1, Integer.parseInt(repFactor), false, true));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Simulate an update by modifying replica metrics.
   * The following core metrics are updated:
   * <ul>
   *   <li><code>SEARCHER.searcher.numDocs</code> - increased by added docs, decreased by deleteById and deleteByQuery</li>
   *   <li><code>SEARCHER.searcher.deletedDocs</code> - decreased by deleteById and deleteByQuery by up to <code>numDocs</code></li>
   *   <li><code>SEARCHER.searcher.maxDoc</code> - always increased by the number of added docs.</li>
   * </ul>
   * <p>IMPORTANT limitations:</p>
   * <ul>
   *   <li>document replacements are always counted as new docs</li>
   *   <li>delete by ID always succeeds (unless numDocs == 0)</li>
   *   <li>deleteByQuery is not supported unless the query is <code>*:*</code></li>
   * </ul>
   * @param req update request. This request MUST have the <code>collection</code> param set.
   * @return {@link UpdateResponse}
   * @throws SolrException on errors, such as nonexistent collection or unsupported deleteByQuery
   */
  public UpdateResponse simUpdate(UpdateRequest req) throws SolrException, InterruptedException, IOException {
    ensureNotClosed();
    String collection = req.getCollection();
    if (collection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection not set");
    }
    if (!colShardReplicaMap.containsKey(collection)) {
      if (CollectionAdminParams.SYSTEM_COLL.equals(collection)) {
        // auto-create
        log.trace("-- auto-create .system when req=" + req);
        createSystemCollection();
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '" + collection + "' doesn't exist");
      }
    }

    DocCollection coll = getClusterState().getCollection(collection);
    DocRouter router = coll.getRouter();
    List<String> deletes = req.getDeleteById();
    if (deletes != null && !deletes.isEmpty()) {
      for (String id : deletes) {
        Slice s = router.getTargetSlice(id, null, null, req.getParams(), coll);
        Replica leader = s.getLeader();
        if (leader == null) {
          log.debug("-- no leader in " + s);
          continue;
        }
        cloudManager.getMetricManager().registry(createRegistryName(collection, s.getName(), leader)).counter("UPDATE./update.requests").inc();
        ReplicaInfo ri = getReplicaInfo(leader);
        Number numDocs = (Number)ri.getVariable("SEARCHER.searcher.numDocs");
        if (numDocs == null || numDocs.intValue() <= 0) {
          log.debug("-- attempting to delete nonexistent doc " + id + " from " + s.getLeader());
          continue;
        }
        AtomicLong bufferedUpdates = (AtomicLong)sliceProperties.get(collection).get(s.getName()).get(BUFFERED_UPDATES);
        if (bufferedUpdates != null) {
          if (bufferedUpdates.get() > 0) {
            bufferedUpdates.decrementAndGet();
          } else {
            log.debug("-- attempting to delete nonexistent buffered doc " + id + " from " + s.getLeader());
          }
          continue;
        }
        lock.lockInterruptibly();
        try {
          simSetShardValue(collection, s.getName(), "SEARCHER.searcher.deletedDocs", 1, true, false);
          simSetShardValue(collection, s.getName(), "SEARCHER.searcher.numDocs", -1, true, false);
          Number indexSize = (Number)ri.getVariable(Type.CORE_IDX.metricsAttribute);
          if (indexSize != null && indexSize.longValue() > SimCloudManager.DEFAULT_IDX_SIZE_BYTES) {
            indexSize = indexSize.longValue() - DEFAULT_DOC_SIZE_BYTES;
            simSetShardValue(collection, s.getName(), Type.CORE_IDX.metricsAttribute,
                new AtomicLong(indexSize.longValue()), false, false);
            simSetShardValue(collection, s.getName(), Variable.coreidxsize,
                new AtomicDouble((Double)Type.CORE_IDX.convertVal(indexSize)), false, false);
          } else {
            throw new Exception("unexpected indexSize ri=" + ri);
          }
        } catch (Exception e) {
          throw new IOException(e);
        } finally {
          lock.unlock();
        }
      }
    }
    deletes = req.getDeleteQuery();
    if (deletes != null && !deletes.isEmpty()) {
      for (String q : deletes) {
        if (!"*:*".equals(q)) {
          throw new UnsupportedOperationException("Only '*:*' query is supported in deleteByQuery");
        }
        for (Slice s : coll.getSlices()) {
          Replica leader = s.getLeader();
          if (leader == null) {
            log.debug("-- no leader in " + s);
            continue;
          }

          cloudManager.getMetricManager().registry(createRegistryName(collection, s.getName(), leader)).counter("UPDATE./update.requests").inc();
          ReplicaInfo ri = getReplicaInfo(leader);
          Number numDocs = (Number)ri.getVariable("SEARCHER.searcher.numDocs");
          if (numDocs == null || numDocs.intValue() == 0) {
            continue;
          }
          lock.lockInterruptibly();
          try {
            simSetShardValue(collection, s.getName(), "SEARCHER.searcher.deletedDocs", new AtomicLong(numDocs.longValue()), false, false);
            simSetShardValue(collection, s.getName(), "SEARCHER.searcher.numDocs", new AtomicLong(0), false, false);
            simSetShardValue(collection, s.getName(), Type.CORE_IDX.metricsAttribute,
                new AtomicLong(SimCloudManager.DEFAULT_IDX_SIZE_BYTES), false, false);
            simSetShardValue(collection, s.getName(), Variable.coreidxsize,
                new AtomicDouble((Double)Type.CORE_IDX.convertVal(SimCloudManager.DEFAULT_IDX_SIZE_BYTES)), false, false);
          } catch (Exception e) {
            throw new IOException(e);
          } finally {
            lock.unlock();
          }
        }
      }
    }
    List<SolrInputDocument> docs = req.getDocuments();
    int docCount = 0;
    Iterator<SolrInputDocument> it = null;
    if (docs != null) {
      docCount = docs.size();
    } else {
      it = req.getDocIterator();
      if (it != null) {
        while (it.hasNext()) {
          it.next();
          docCount++;
        }
      }
    }
    if (docCount > 0) {
      // this approach to updating counters and metrics drastically increases performance
      // of bulk updates, because simSetShardValue is relatively costly

      Map<String, AtomicLong> docUpdates = new HashMap<>();
      Map<String, Map<String, AtomicLong>> metricUpdates = new HashMap<>();

      // XXX don't add more than 2bln docs in one request
      boolean modified = false;
      lock.lockInterruptibly();
      try {
        coll = getClusterState().getCollection(collection);
        Slice[] slices = coll.getActiveSlicesArr();
        if (slices.length == 0) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection without slices");
        }
        int[] perSlice = new int[slices.length];

        if (it != null) {
          // BULK UPDATE: simulate random doc assignment without actually calling DocRouter,
          // which adds significant overhead

          int totalAdded = 0;
          for (int i = 0; i < slices.length; i++) {
            Slice s = slices[i];
            long count = (long) docCount * ((long) s.getRange().max - (long) s.getRange().min) / 0x100000000L;
            perSlice[i] = (int) count;
            totalAdded += perSlice[i];
          }
          // loss of precision due to integer math
          int diff = docCount - totalAdded;
          if (diff > 0) {
            // spread the remainder more or less equally
            int perRemain = diff / slices.length;
            int remainder = diff % slices.length;
            int remainderSlice = slices.length > 1 ? bulkUpdateRandom.nextInt(slices.length) : 0;
            for (int i = 0; i < slices.length; i++) {
              perSlice[i] += perRemain;
              if (i == remainderSlice) {
                perSlice[i] += remainder;
              }
            }
          }
          for (int i = 0; i < slices.length; i++) {
            Slice s = slices[i];
            Replica leader = s.getLeader();
            if (leader == null) {
              log.debug("-- no leader in " + s);
              continue;
            }
            metricUpdates.computeIfAbsent(s.getName(), sh -> new HashMap<>())
                .computeIfAbsent(leader.getCoreName(), cn -> new AtomicLong())
                .addAndGet(perSlice[i]);
            modified = true;
            AtomicLong bufferedUpdates = (AtomicLong)sliceProperties.get(collection).get(s.getName()).get(BUFFERED_UPDATES);
            if (bufferedUpdates != null) {
              bufferedUpdates.addAndGet(perSlice[i]);
              continue;
            }
            docUpdates.computeIfAbsent(s.getName(), sh -> new AtomicLong())
                .addAndGet(perSlice[i]);
          }
        } else {
          // SMALL UPDATE: use exact assignment via DocRouter
          for (SolrInputDocument doc : docs) {
            String id = (String) doc.getFieldValue("id");
            if (id == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Document without id: " + doc);
            }
            Slice s = coll.getRouter().getTargetSlice(id, doc, null, null, coll);
            Replica leader = s.getLeader();
            if (leader == null) {
              log.debug("-- no leader in " + s);
              continue;
            }
            metricUpdates.computeIfAbsent(s.getName(), sh -> new HashMap<>())
                .computeIfAbsent(leader.getCoreName(), cn -> new AtomicLong())
                .incrementAndGet();
            modified = true;
            AtomicLong bufferedUpdates = (AtomicLong)sliceProperties.get(collection).get(s.getName()).get(BUFFERED_UPDATES);
            if (bufferedUpdates != null) {
              bufferedUpdates.incrementAndGet();
              continue;
            }
            docUpdates.computeIfAbsent(s.getName(), sh -> new AtomicLong())
                .incrementAndGet();
          }
        }

        if (modified) {
          docUpdates.forEach((sh, count) -> {
            try {
              simSetShardValue(collection, sh, "SEARCHER.searcher.numDocs", count.get(), true, false);
              simSetShardValue(collection, sh, "SEARCHER.searcher.maxDoc", count.get(), true, false);
              // for each new document increase the size by DEFAULT_DOC_SIZE_BYTES
              simSetShardValue(collection, sh, Type.CORE_IDX.metricsAttribute,
                  DEFAULT_DOC_SIZE_BYTES * count.get(), true, false);
              simSetShardValue(collection, sh, Variable.coreidxsize,
                  Type.CORE_IDX.convertVal(DEFAULT_DOC_SIZE_BYTES * count.get()), true, false);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
          metricUpdates.forEach((sh, cores) -> {
            cores.forEach((core, count) -> {
              String registry = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, collection, sh,
                  Utils.parseMetricsReplicaName(collection, core));
              cloudManager.getMetricManager().registry(registry).counter("UPDATE./update.requests").inc(count.get());
            });
          });
        }
      } finally {
        lock.unlock();
      }
    }
    return new UpdateResponse();
  }

  public QueryResponse simQuery(QueryRequest req) throws SolrException, InterruptedException, IOException {
    ensureNotClosed();
    String collection = req.getCollection();
    if (collection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection not set");
    }
    if (!colShardReplicaMap.containsKey(collection)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection does not exist");
    }
    String query = req.getParams().get(CommonParams.Q);
    if (query == null || !query.equals("*:*")) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only '*:*' query is supported");
    }
    ClusterState clusterState = getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    AtomicLong count = new AtomicLong();
    for (Slice s : coll.getActiveSlicesArr()) {
      Replica r = s.getLeader();
      if (r == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, collection + "/" + s.getName() + " has no leader");
      }
      ReplicaInfo ri = getReplicaInfo(r);
      Number numDocs = (Number)ri.getVariable("SEARCHER.searcher.numDocs", 0L);
      count.addAndGet(numDocs.longValue());
      AtomicLong bufferedUpdates = (AtomicLong)sliceProperties.get(collection).get(s.getName()).get(BUFFERED_UPDATES);
      if (bufferedUpdates != null) {
        count.addAndGet(bufferedUpdates.get());
      }
    }
    QueryResponse rsp = new QueryResponse();
    NamedList<Object> values = new NamedList<>();
    values.add("responseHeader", new NamedList<>());
    SolrDocumentList docs = new SolrDocumentList();
    docs.setNumFound(count.get());
    values.add("response", docs);
    rsp.setResponse(values);
    return rsp;
  }

  private static String createRegistryName(String collection, String shard, Replica r) {
    return SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, collection, shard,
        Utils.parseMetricsReplicaName(collection, r.getCoreName()));
  }

  /**
   * Saves cluster properties to clusterprops.json.
   * @return current properties
   */
  private synchronized Map<String, Object> saveClusterProperties() throws Exception {
    if (lastSavedProperties != null && lastSavedProperties.equals(clusterProperties)) {
      return lastSavedProperties;
    }
    byte[] data = Utils.toJSON(clusterProperties);
    VersionedData oldData = stateManager.getData(ZkStateReader.CLUSTER_PROPS);
    int version = oldData != null ? oldData.getVersion() : -1;
    stateManager.setData(ZkStateReader.CLUSTER_PROPS, data, version);
    lastSavedProperties = (Map)Utils.fromJSON(data);
    return lastSavedProperties;
  }

  /**
   * Set all cluster properties. This also updates the clusterprops.json data in
   * {@link DistribStateManager}
   * @param properties properties to set
   */
  public void simSetClusterProperties(Map<String, Object> properties) throws Exception {
    lock.lockInterruptibly();
    try {
      clusterProperties.clear();
      if (properties != null) {
        this.clusterProperties.putAll(properties);
      }
      saveClusterProperties();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set a cluster property. This also updates the clusterprops.json data in
   * {@link DistribStateManager}
   * @param key property name
   * @param value property value
   */
  public void simSetClusterProperty(String key, Object value) throws Exception {
    lock.lockInterruptibly();
    try {
      if (value != null) {
        clusterProperties.put(key, value);
      } else {
        clusterProperties.remove(key);
      }
      saveClusterProperties();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set collection properties.
   * @param coll collection name
   * @param properties properties
   */
  public void simSetCollectionProperties(String coll, Map<String, Object> properties) throws Exception {
    lock.lockInterruptibly();
    try {
      if (properties == null) {
        collProperties.remove(coll);
      } else {
        Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
        props.clear();
        props.putAll(properties);
      }
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set collection property.
   * @param coll collection name
   * @param key property name
   * @param value property value
   */
  public void simSetCollectionProperty(String coll, String key, String value) throws Exception {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    lock.lockInterruptibly();
    try {
      if (value == null) {
        props.remove(key);
      } else {
        props.put(key, value);
      }
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set slice properties.
   * @param coll collection name
   * @param slice slice name
   * @param properties slice properties
   */
  public void simSetSliceProperties(String coll, String slice, Map<String, Object> properties) throws Exception {
    Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new HashMap<>()).computeIfAbsent(slice, s -> new HashMap<>());
    lock.lockInterruptibly();
    try {
      sliceProps.clear();
      if (properties != null) {
        sliceProps.putAll(properties);
      }
      collectionsStatesRef.set(null);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Set per-collection value (eg. a metric). This value will be applied to each replica.
   * @param collection collection name
   * @param key property name
   * @param value property value
   */
  public void simSetCollectionValue(String collection, String key, Object value) throws Exception {
    simSetCollectionValue(collection, key, value, false, false);
  }

  /**
   * Set per-collection value (eg. a metric). This value will be applied to each replica.
   * @param collection collection name
   * @param key property name
   * @param value property value
   * @param divide if the value is a {@link Number} and this param is true, then the value will be evenly
   *               divided by the number of replicas.
   */
  public void simSetCollectionValue(String collection, String key, Object value, boolean delta, boolean divide) throws Exception {
    simSetShardValue(collection, null, key, value, delta, divide);
  }

  /**
   * Set per-collection value (eg. a metric). This value will be applied to each replica in a selected shard.
   * @param collection collection name
   * @param shard shard name. If null then all shards will be affected.
   * @param key property name
   * @param value property value
   */
  public void simSetShardValue(String collection, String shard, String key, Object value) throws Exception {
    simSetShardValue(collection, shard, key, value, false, false);
  }

  /**
   * Set per-collection value (eg. a metric). This value will be applied to each replica in a selected shard.
   * @param collection collection name
   * @param shard shard name. If null then all shards will be affected.
   * @param key property name
   * @param value property value
   * @param delta if true then treat the numeric value as delta to add to the existing value
   *              (or set the value to delta if missing)
   * @param divide if the value is a {@link Number} and this is true, then the value will be evenly
   *               divided by the number of replicas.
   */
  public void simSetShardValue(String collection, String shard, String key, Object value, boolean delta, boolean divide) throws Exception {
    final List<ReplicaInfo> infos;
    if (shard == null) {
      infos = new ArrayList<>();
      colShardReplicaMap.computeIfAbsent(collection, c -> new ConcurrentHashMap<>())
        .forEach((sh, replicas) -> infos.addAll(replicas));
    } else {
      infos = colShardReplicaMap.computeIfAbsent(collection, c -> new ConcurrentHashMap<>())
          .computeIfAbsent(shard, s -> new ArrayList<>());
    }
    if (infos.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection " + collection + " doesn't exist (shard=" + shard + ").");
    }
    if (divide && value != null && (value instanceof Number)) {
      if ((value instanceof Long) || (value instanceof Integer)) {
        value = ((Number) value).longValue() / infos.size();
      } else {
        value = ((Number) value).doubleValue() / infos.size();
      }
    }
    for (ReplicaInfo r : infos) {
      synchronized (r) {
        if (value == null) {
          r.getVariables().remove(key);
        } else {
          if (delta) {
            Object prevValue = r.getVariables().get(key);
            if (prevValue != null) {
              if ((prevValue instanceof Number) && (value instanceof Number)) {
                if (((prevValue instanceof Long) || (prevValue instanceof Integer) ||
                    (prevValue instanceof AtomicLong) || (prevValue instanceof AtomicInteger)) &&
                    ((value instanceof Long) || (value instanceof Integer))) {
                  long newValue = ((Number)prevValue).longValue() + ((Number)value).longValue();
                  // minimize object allocations
                  if (prevValue instanceof AtomicLong) {
                    ((AtomicLong)prevValue).set(newValue);
                  } else if (prevValue instanceof AtomicInteger) {
                    ((AtomicInteger)prevValue).set(((Number)prevValue).intValue() + ((Number)value).intValue());
                  } else {
                    r.getVariables().put(key, newValue);
                  }
                } else {
                  double newValue = ((Number)prevValue).doubleValue() + ((Number)value).doubleValue();
                  if (prevValue instanceof AtomicDouble) {
                    ((AtomicDouble)prevValue).set(newValue);
                  } else {
                    r.getVariables().put(key, newValue);
                  }
                }
              } else {
                throw new UnsupportedOperationException("delta cannot be applied to non-numeric values: " + prevValue + " and " + value);
              }
            } else {
              if (value instanceof Integer) {
                r.getVariables().put(key, new AtomicInteger((Integer)value));
              } else if (value instanceof Long) {
                r.getVariables().put(key, new AtomicLong((Long)value));
              } else if (value instanceof Double) {
                r.getVariables().put(key, new AtomicDouble((Double)value));
              } else {
                r.getVariables().put(key, value);
              }
            }
          } else {
            if (value instanceof Integer) {
              r.getVariables().put(key, new AtomicInteger((Integer)value));
            } else if (value instanceof Long) {
              r.getVariables().put(key, new AtomicLong((Long)value));
            } else if (value instanceof Double) {
              r.getVariables().put(key, new AtomicDouble((Double)value));
            } else {
              r.getVariables().put(key, value);
            }
          }
        }
      }
    }
  }

  /**
   * Return all replica infos for a node.
   * @param node node id
   * @return copy of the list of replicas on that node, or empty list if none
   */
  public List<ReplicaInfo> simGetReplicaInfos(String node) {
    List<ReplicaInfo> replicas = nodeReplicaMap.get(node);
    if (replicas == null) {
      return Collections.emptyList();
    } else {
      // make a defensive copy to avoid ConcurrentModificationException
      return Arrays.asList(replicas.toArray(new ReplicaInfo[0]));
    }
  }

  public List<ReplicaInfo> simGetReplicaInfos(String collection, String shard) {
    List<ReplicaInfo> replicas = colShardReplicaMap.computeIfAbsent(collection, c -> new ConcurrentHashMap<>())
        .computeIfAbsent(shard, s -> new ArrayList<>());
    if (replicas == null) {
      return Collections.emptyList();
    } else {
      // make a defensive copy to avoid ConcurrentModificationException
      return Arrays.asList(replicas.toArray(new ReplicaInfo[replicas.size()]));
    }
  }

  /**
   * List collections.
   * @return list of existing collections.
   */
  public List<String> simListCollections() throws InterruptedException {
    return new ArrayList<>(colShardReplicaMap.keySet());
  }

  public Map<String, Map<String, Object>> simGetCollectionStats() throws IOException, InterruptedException {
    Map<String, Map<String, Object>> stats = new TreeMap<>();
    lock.lockInterruptibly();
    try {
      collectionsStatesRef.set(null);
      ClusterState state = getClusterState();
      state.forEachCollection(coll -> {
        Map<String, Object> perColl = new LinkedHashMap<>();
        stats.put(coll.getName(), perColl);
        perColl.put("shardsTotal", coll.getSlices().size());
        Map<String, AtomicInteger> shardState = new TreeMap<>();
        int noLeader = 0;

        SummaryStatistics docs = new SummaryStatistics();
        SummaryStatistics bytes = new SummaryStatistics();
        SummaryStatistics inactiveDocs = new SummaryStatistics();
        SummaryStatistics inactiveBytes = new SummaryStatistics();

        long deletedDocs = 0;
        long bufferedDocs = 0;
        int totalReplicas = 0;
        int activeReplicas = 0;

        for (Slice s : coll.getSlices()) {
          shardState.computeIfAbsent(s.getState().toString(), st -> new AtomicInteger())
              .incrementAndGet();
          totalReplicas += s.getReplicas().size();
          if (s.getState() != Slice.State.ACTIVE) {
            if (!s.getReplicas().isEmpty()) {
              ReplicaInfo ri = getReplicaInfo(s.getReplicas().iterator().next());
              if (ri != null) {
                Number numDocs = (Number)ri.getVariable("SEARCHER.searcher.numDocs");
                Number numBytes = (Number)ri.getVariable("INDEX.sizeInBytes");
                if (numDocs != null) {
                  inactiveDocs.addValue(numDocs.doubleValue());
                }
                if (numBytes != null) {
                  inactiveBytes.addValue(numBytes.doubleValue());
                }
              }
            }
            continue;
          }
          AtomicLong buffered = (AtomicLong)sliceProperties.get(coll.getName()).get(s.getName()).get(BUFFERED_UPDATES);
          if (buffered != null) {
            bufferedDocs += buffered.get();
          }
          activeReplicas += s.getReplicas().size();
          Replica leader = s.getLeader();
          if (leader == null) {
            noLeader++;
            if (!s.getReplicas().isEmpty()) {
              leader = s.getReplicas().iterator().next();
            }
          }
          ReplicaInfo ri = null;
          if (leader != null) {
            ri = getReplicaInfo(leader);
            if (ri == null) {
              log.warn("Unknown ReplicaInfo for {}", leader);
            }
          }
          if (ri != null) {
            Number numDocs = (Number)ri.getVariable("SEARCHER.searcher.numDocs");
            Number delDocs = (Number)ri.getVariable("SEARCHER.searcher.deleteDocs");
            Number numBytes = (Number)ri.getVariable("INDEX.sizeInBytes");
            if (numDocs != null) {
              docs.addValue(numDocs.doubleValue());
            }
            if (delDocs != null) {
              deletedDocs += delDocs.longValue();
            }
            if (numBytes != null) {
              bytes.addValue(numBytes.doubleValue());
            }
          }
        }
        perColl.put("shardsState", shardState);
        perColl.put("  shardsWithoutLeader", noLeader);
        perColl.put("totalReplicas", totalReplicas);
        perColl.put("  activeReplicas", activeReplicas);
        perColl.put("  inactiveReplicas", totalReplicas - activeReplicas);
        long totalDocs = (long)docs.getSum() + bufferedDocs;
        perColl.put("totalActiveDocs", String.format(Locale.ROOT, "%,d", totalDocs));
        perColl.put("  bufferedDocs", String.format(Locale.ROOT, "%,d", bufferedDocs));
        perColl.put("  maxActiveSliceDocs", String.format(Locale.ROOT, "%,d", (long)docs.getMax()));
        perColl.put("  minActiveSliceDocs", String.format(Locale.ROOT, "%,d", (long)docs.getMin()));
        perColl.put("  avgActiveSliceDocs", String.format(Locale.ROOT, "%,.0f", docs.getMean()));
        perColl.put("totalInactiveDocs", String.format(Locale.ROOT, "%,d", (long)inactiveDocs.getSum()));
        perColl.put("  maxInactiveSliceDocs", String.format(Locale.ROOT, "%,d", (long)inactiveDocs.getMax()));
        perColl.put("  minInactiveSliceDocs", String.format(Locale.ROOT, "%,d", (long)inactiveDocs.getMin()));
        perColl.put("  avgInactiveSliceDocs", String.format(Locale.ROOT, "%,.0f", inactiveDocs.getMean()));
        perColl.put("totalActiveBytes", String.format(Locale.ROOT, "%,d", (long)bytes.getSum()));
        perColl.put("  maxActiveSliceBytes", String.format(Locale.ROOT, "%,d", (long)bytes.getMax()));
        perColl.put("  minActiveSliceBytes", String.format(Locale.ROOT, "%,d", (long)bytes.getMin()));
        perColl.put("  avgActiveSliceBytes", String.format(Locale.ROOT, "%,.0f", bytes.getMean()));
        perColl.put("totalInactiveBytes", String.format(Locale.ROOT, "%,d", (long)inactiveBytes.getSum()));
        perColl.put("  maxInactiveSliceBytes", String.format(Locale.ROOT, "%,d", (long)inactiveBytes.getMax()));
        perColl.put("  minInactiveSliceBytes", String.format(Locale.ROOT, "%,d", (long)inactiveBytes.getMin()));
        perColl.put("  avgInactiveSliceBytes", String.format(Locale.ROOT, "%,.0f", inactiveBytes.getMean()));
        perColl.put("totalActiveDeletedDocs", String.format(Locale.ROOT, "%,d", deletedDocs));
      });
      return stats;
    } finally {
      lock.unlock();
    }
  }

  // interface methods

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    try {
      return getClusterState().getCollectionRef(collection);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public Set<String> getLiveNodes() {
    return liveNodes.get();
  }

  @Override
  public List<String> resolveAlias(String alias) {
    throw new UnsupportedOperationException("resolveAlias not implemented");
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    ensureNotClosed();
    Map<String, DocCollection> states = getCollectionStates();
    ClusterState state = new ClusterState(clusterStateVersion, liveNodes.get(), states);
    return state;
  }

  // this method uses a simple cache in collectionsStatesRef. Operations that modify
  // cluster state should always reset this cache so that the changes become visible
  private Map<String, DocCollection> getCollectionStates() throws IOException {
    Map<String, DocCollection> collectionStates = collectionsStatesRef.get();
    if (collectionStates != null) {
      return collectionStates;
    }
    lock.lock();
    collectionsStatesRef.set(null);
    log.debug("** creating new collection states, currentVersion={}", clusterStateVersion);
    try {
      Map<String, Map<String, Map<String, Replica>>> collMap = new HashMap<>();
      nodeReplicaMap.forEach((n, replicas) -> {
        replicas.forEach(ri -> {
          Map<String, Object> props;
          synchronized (ri) {
            props = new HashMap<>(ri.getVariables());
          }
          props.put(ZkStateReader.NODE_NAME_PROP, n);
          props.put(ZkStateReader.CORE_NAME_PROP, ri.getCore());
          props.put(ZkStateReader.REPLICA_TYPE, ri.getType().toString());
          props.put(ZkStateReader.STATE_PROP, ri.getState().toString());
          Replica r = new Replica(ri.getName(), props);
          collMap.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
              .computeIfAbsent(ri.getShard(), s -> new HashMap<>())
              .put(ri.getName(), r);
        });
      });

      // add empty slices
      sliceProperties.forEach((c, perSliceProps) -> {
        perSliceProps.forEach((slice, props) -> {
          collMap.computeIfAbsent(c, co -> new ConcurrentHashMap<>()).computeIfAbsent(slice, s -> new ConcurrentHashMap<>());
        });
      });
      // add empty collections
      collProperties.keySet().forEach(c -> {
        collMap.computeIfAbsent(c, co -> new ConcurrentHashMap<>());
      });

      Map<String, DocCollection> res = new HashMap<>();
      collMap.forEach((coll, shards) -> {
        Map<String, Slice> slices = new HashMap<>();
        shards.forEach((s, replicas) -> {
          Map<String, Object> sliceProps = sliceProperties.computeIfAbsent(coll, c -> new ConcurrentHashMap<>()).computeIfAbsent(s, sl -> new ConcurrentHashMap<>());
          Slice slice = new Slice(s, replicas, sliceProps);
          slices.put(s, slice);
        });
        Map<String, Object> collProps = collProperties.computeIfAbsent(coll, c -> new ConcurrentHashMap<>());
        Map<String, Object> routerProp = (Map<String, Object>) collProps.getOrDefault(DocCollection.DOC_ROUTER, Collections.singletonMap("name", DocRouter.DEFAULT_NAME));
        DocRouter router = DocRouter.getDocRouter((String)routerProp.getOrDefault("name", DocRouter.DEFAULT_NAME));
        DocCollection dc = new DocCollection(coll, slices, collProps, router, clusterStateVersion, ZkStateReader.CLUSTER_STATE);
        res.put(coll, dc);
      });
      saveClusterState(new ClusterState(clusterStateVersion, liveNodes.get(), res));
      collectionsStatesRef.set(res);
      return res;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return clusterProperties;
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    Map<String, Object> props = collProperties.computeIfAbsent(coll, c -> new HashMap<>());
    return (String)props.get("policy");
  }

  @Override
  public void connect() {

  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private void ensureNotClosed() throws IOException {
    if (closed) {
      throw new IOException("already closed");
    }
  }
}
