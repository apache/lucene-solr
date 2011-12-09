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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible node assignments, cluster state file?
 */
public class Overseer implements NodeStateChangeListener {
  
  private static Logger log = LoggerFactory.getLogger(Overseer.class);
  
  private final SolrZkClient zkClient;
  private final ZkStateReader reader;
  
  // node stateWatches
  private HashMap<String,NodeStateWatcher> nodeStateWatches = new HashMap<String,NodeStateWatcher>();
  
  public Overseer(final SolrZkClient zkClient, final ZkStateReader reader) throws KeeperException, InterruptedException {
    log.info("Constructing new Overseer");
    this.zkClient = zkClient;
    this.reader = reader;
    createZkNodes(zkClient);
    createWatches();
  }
  
  public static void createZkNodes(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    //create assignments node if it does not exist
    if (!zkClient.exists("/node_assignments")) {
      try {
        zkClient.makePath("/node_assignments", CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
      }
    }
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
    
    processLiveNodesChanged(Collections.EMPTY_SET, liveNodes);
  }
  
  private void processLiveNodesChanged(Collection<String> oldLiveNodes,
      Collection<String> liveNodes) {
    
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
  
  private void addNodeStateWatches(Set<String> nodeNames) {
    
    for (String nodeName : nodeNames) {
      String path = "/node_states/" + nodeName;
      synchronized (nodeStateWatches) {
        if (!nodeStateWatches.containsKey(nodeName)) {
          try {
            if (!zkClient.exists(path)) {
              zkClient.makePath(path);
            }
          } catch (KeeperException e1) {
            if (e1.code() != Code.NODEEXISTS) {
              throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR, "Could not create node for watch. Connection lost?", e1);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Could not create node for watch. Connection lost?", e);
          }

          NodeStateWatcher nsw = new NodeStateWatcher(zkClient, nodeName, path, this);
          nodeStateWatches.put(nodeName, nsw);
          try {
            byte[] state = zkClient.getData(path, nsw, null);
            nsw.processStateChange(state);
          } catch (KeeperException e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Could not read initial node state. Connection lost?", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR, "Could not read initial node state. Connection lost?", e);
          }
        } else {
          log.debug("watch already added");
        }
      }
    }
  }
  
  /**
   * Try to assign core to the cluster
   */
  private void updateState(String nodeName, CoreState coreState) {
    String collection = coreState.getCollectionName();
    String coreName = coreState.getCoreName();
    
    synchronized (reader.getUpdateLock()) {
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
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not publish new state. Connection lost?", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not publish new state. Connection lost?", e);
      }
    }

  }
  
  @Override
  public void coreCreated(String nodeName, Set<CoreState> states) throws KeeperException {
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
      final String collection = coreState.getCollectionName();
      HashMap<String, String> coreProperties = new HashMap<String, String>();
      Map<String, Slice> slices = cloudState.getSlices(coreState.getCollectionName());
      for(Entry<String, Slice> entry: slices.entrySet()) {
        ZkNodeProps myProps = entry.getValue().getShards().get(coreName);
        if(myProps!=null) {
          coreProperties.put(ZkStateReader.SHARD_ID_PROP, entry.getKey());
        }
      }
      CoreAssignment assignment = new CoreAssignment(coreName, collection, coreProperties);
      assignments.add(assignment);
    }
    
    //serialize
    byte[] content = ZkStateReader.toJSON(assignments);
    final String nodeName = "/node_assignments/" + node;
    if (!zkClient.exists(nodeName)) {
      try {
        zkClient.makePath(nodeName);
      } catch (KeeperException ke) {
        if (ke.code() != Code.NODEEXISTS) {
          throw ke;
        }
      }
    }
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
  public void coreChanged(String nodeName, Set<CoreState> states) throws KeeperException  {
    log.debug("Cores changed: " + nodeName + " states:" + states);
    for (CoreState state : states) {
      updateState(nodeName, state);
    }
  }
}