package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.cloud.NodeStateWatcher.NodeStateChangeListener;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible node assignments, cluster state file?
 */
public class Overseer implements NodeStateChangeListener {
  
  public static final String ASSIGNMENTS_NODE = "/node_assignments";
  public static final String STATES_NODE = "/node_states";
  private static Logger log = LoggerFactory.getLogger(Overseer.class);
  
  private final SolrZkClient zkClient;
  private final ZkStateReader reader;
  
  // node stateWatches
  private HashMap<String,NodeStateWatcher> nodeStateWatches = new HashMap<String,NodeStateWatcher>();
  
  public Overseer(final SolrZkClient zkClient, final ZkStateReader reader) throws KeeperException, InterruptedException {
    log.info("Constructing new Overseer");
    this.zkClient = zkClient;
    this.reader = reader;
    createWatches();
  }
  
  public synchronized void createWatches()
      throws KeeperException, InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes
    
    if (!zkClient.exists(ZkStateReader.CLUSTER_STATE)) {
      try {
        zkClient.create(ZkStateReader.CLUSTER_STATE, null,
            CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        // if someone beats us to creating this ignore it
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
      }
    }
    
    addLiveNodesWatch();
  }

  private void addLiveNodesWatch() throws KeeperException,
      InterruptedException {
    List<String> liveNodes = zkClient.getChildren(
        ZkStateReader.LIVE_NODES_ZKNODE, new Watcher() {
          
          @Override
          public void process(WatchedEvent event) {
            try {
                List<String> liveNodes = zkClient.getChildren(
                    ZkStateReader.LIVE_NODES_ZKNODE, this);
                Set<String> liveNodesSet = new HashSet<String>();
                liveNodesSet.addAll(liveNodes);
                processLiveNodesChanged(nodeStateWatches.keySet(), liveNodes);
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                return;
              }
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
          }
        });
    
    processLiveNodesChanged(Collections.<String>emptySet(), liveNodes);
  }
  
  private void processLiveNodesChanged(Collection<String> oldLiveNodes,
      Collection<String> liveNodes) throws InterruptedException, KeeperException {
    
    Set<String> upNodes = complement(liveNodes, oldLiveNodes);
    if (upNodes.size() > 0) {
      addNodeStateWatches(upNodes);
    }
    
    Set<String> downNodes = complement(oldLiveNodes, liveNodes);
    for(String node: downNodes) {
      NodeStateWatcher watcher = nodeStateWatches.remove(node);
      if(watcher!=null) {
        watcher.close();
      }
    }
  }
  
  private void addNodeStateWatches(Set<String> nodeNames) throws InterruptedException, KeeperException {
    
    for (String nodeName : nodeNames) {
      final String path = STATES_NODE + "/" + nodeName;
      synchronized (nodeStateWatches) {
        if (!nodeStateWatches.containsKey(nodeName)) {
          try {
            if (!zkClient.exists(path)) {
              zkClient.makePath(path);
            }
          } catch (NodeExistsException e) {
            // thats okay...
          }

          NodeStateWatcher nsw = new NodeStateWatcher(zkClient, nodeName, path, this);
          nodeStateWatches.put(nodeName, nsw);
          byte[] state = zkClient.getData(path, nsw, null);
          
          nsw.processStateChange(state);

        } else {
          log.debug("watch already added");
        }
      }
    }
  }
  
  /**
   * Try to assign core to the cluster
   * @throws KeeperException 
   * @throws InterruptedException 
   */
  private void updateState(String nodeName, CoreState coreState) throws KeeperException, InterruptedException {
    String collection = coreState.getCollectionName();
    String coreName = coreState.getCoreName();
    
    synchronized (reader.getUpdateLock()) {
      reader.updateCloudState(true); //get fresh copy of the state
      String shardId;
      CloudState state = reader.getCloudState();
      if (coreState.getProperties().get(ZkStateReader.SHARD_ID_PROP) == null) {
        shardId = AssignShard.assignShard(collection, state);
      } else {
        shardId = coreState.getProperties().get(ZkStateReader.SHARD_ID_PROP);
      }
      
      Map<String,String> props = new HashMap<String,String>();
      for (Entry<String,String> entry : coreState.getProperties().entrySet()) {
        props.put(entry.getKey(), entry.getValue());
      }
      ZkNodeProps zkProps = new ZkNodeProps(props);
      Slice slice = state.getSlice(collection, shardId);
      Map<String,ZkNodeProps> shardProps;
      if (slice == null) {
        shardProps = new HashMap<String,ZkNodeProps>();
      } else {
        shardProps = state.getSlice(collection, shardId).getShardsCopy();
      }
      shardProps.put(coreName, zkProps);

      slice = new Slice(shardId, shardProps);
      CloudState newCloudState = new CloudState(state.getLiveNodes(),
          state.getCollectionStates());
      newCloudState.addSlice(collection, slice);
      try {
        zkClient.setData(ZkStateReader.CLUSTER_STATE,
            ZkStateReader.toJSON(newCloudState));
        publishNodeAssignments(nodeName, newCloudState, nodeStateWatches.get(nodeName).getCurrentState());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Interrupted while publishing new state", e);
      }
    }

  }
  
  @Override
  public void coreCreated(String nodeName, Set<CoreState> states) throws KeeperException, InterruptedException {
    log.debug("Cores created: " + nodeName + " states:" +states);
    for (CoreState state : states) {
      updateState(nodeName, state);
    }
  }
  
  /**
   * Publish assignments for node
   * 
   * @param node
   * @param cloudState
   * @param states
   * @throws KeeperException
   */
  private void publishNodeAssignments(String node, CloudState cloudState,
      Set<CoreState> states) throws KeeperException, InterruptedException {
    ArrayList<CoreAssignment> assignments = new ArrayList<CoreAssignment>();
    for(CoreState coreState: states) {
      final String coreName = coreState.getCoreName();
      HashMap<String, String> coreProperties = new HashMap<String, String>();
      Map<String, Slice> slices = cloudState.getSlices(coreState.getCollectionName());
      for(Entry<String, Slice> entry: slices.entrySet()) {
        ZkNodeProps myProps = entry.getValue().getShards().get(coreName);
        if(myProps!=null) {
          coreProperties.put(ZkStateReader.SHARD_ID_PROP, entry.getKey());
        }
      }
      CoreAssignment assignment = new CoreAssignment(coreName, coreProperties);
      assignments.add(assignment);
    }
    
    //serialize
    byte[] content = ZkStateReader.toJSON(assignments);
    final String nodeName = ASSIGNMENTS_NODE + "/" + node;
    zkClient.setData(nodeName, content);
  }
  
  private Set<String> complement(Collection<String> next,
      Collection<String> prev) {
    Set<String> downCollections = new HashSet<String>();
    downCollections.addAll(next);
    downCollections.removeAll(prev);
    return downCollections;
  }

  @Override
  public void coreChanged(String nodeName, Set<CoreState> states) throws KeeperException, InterruptedException  {
    log.debug("Cores changed: " + nodeName + " states:" + states);
    for (CoreState state : states) {
      updateState(nodeName, state);
    }
  }
  
  public static void createClientNodes(SolrZkClient zkClient, String nodeName) throws KeeperException, InterruptedException {
    createZkNode(zkClient, STATES_NODE + "/" + nodeName);
    createZkNode(zkClient, ASSIGNMENTS_NODE + "/" + nodeName);
  }
  
  private static void createZkNode(SolrZkClient zkClient, String nodeName) throws KeeperException, InterruptedException {
    
    if (log.isInfoEnabled()) {
      log.info("creating node:" + nodeName);
    }
    
    try {
      if (!zkClient.exists(nodeName)) {
        zkClient.makePath(nodeName, CreateMode.PERSISTENT, null);
      }
      
    } catch (NodeExistsException e) {
      // it's ok
    }
  }
}