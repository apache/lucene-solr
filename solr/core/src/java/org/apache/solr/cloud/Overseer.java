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

import java.io.IOException;
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
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.CloudStateUtility;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
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
  
  private SolrZkClient zkClient;
  
  private volatile CloudState cloudState = new CloudState();
  
  // desired collection configuration
  private HashMap<String,ZkNodeProps> collections = new HashMap<String, ZkNodeProps>();
  
//  // live nodes
//  private HashMap<String,ZkNodeProps> liveNodes = new HashMap<String, ZkNodeProps>();
  
  // node stateWatches
  private HashMap<String,NodeStateWatcher> nodeStateWatches = new HashMap<String,NodeStateWatcher>();
  
  public Overseer(final SolrZkClient zkClient) throws KeeperException {
    log.info("Constructing new Overseer");
    this.zkClient = zkClient;
   
    try {
      createZkNodes(zkClient);
      createClusterStateWatchersAndUpdate();
    } catch (InterruptedException e) {
      // nocommit
      e.printStackTrace();
    } catch (KeeperException e) {
      // nocommit
      e.printStackTrace();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"",e);
    }
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
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public synchronized void createClusterStateWatchersAndUpdate()
      throws KeeperException, InterruptedException, IOException {
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
    
    log.info("Getting the existing cluster state from ZooKeeper... ");
    cloudState = CloudStateUtility.get(zkClient, null);
    
    //watch for live nodes
    addLiveNodesWatch();
    
    // watch collections
    addCollectionsWatch();
  }

  private void addCollectionsWatch() throws KeeperException,
      InterruptedException {
    final List<String> collections = zkClient.getChildren(
        ZkStateReader.COLLECTIONS_ZKNODE, new Watcher() {
          
          @Override
          public void process(WatchedEvent event) {
            log.info("Updating collections");
            try {
              List<String> collections = zkClient.getChildren(
                  ZkStateReader.COLLECTIONS_ZKNODE, this);
              processCollectionChange(cloudState.getCollections(), collections);
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
    
    processCollectionChange(cloudState.getCollections(), collections);
  }

  private void addLiveNodesWatch() throws KeeperException,
      InterruptedException {
    List<String> liveNodes = zkClient.getChildren(
        ZkStateReader.LIVE_NODES_ZKNODE, new Watcher() {
          
          @Override
          public void process(WatchedEvent event) {
            try {
              synchronized (cloudState) {
                List<String> liveNodes = zkClient.getChildren(
                    ZkStateReader.LIVE_NODES_ZKNODE, this);
                Set<String> liveNodesSet = new HashSet<String>();
                liveNodesSet.addAll(liveNodes);
                processLiveNodesChanged(cloudState.getLiveNodes(), liveNodes);
                
                cloudState = new CloudState(liveNodesSet, cloudState
                    .getCollectionStates());
                // TODO: why are we syncing on cloudState and not the update
                // lock?
              }
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
    

    Set<String> liveNodesSet = new HashSet<String>();
    liveNodesSet.addAll(liveNodes);
    processLiveNodesChanged(cloudState.getLiveNodes(), liveNodes);
    synchronized (cloudState) {
      cloudState = new CloudState(liveNodesSet,
          this.cloudState.getCollectionStates());
      // TODO: why are we syncing on cloudState and not the update
      // lock?
    }
    processLiveNodesChanged(Collections.EMPTY_SET, liveNodes);
  }
  
  private void processCollectionChange(Set<String> oldCollections,
      List<String> newCollections) {
    Set<String> downCollections = complement(oldCollections, newCollections);
    if (downCollections.size() > 0) {
      collectionsDown(downCollections);
    }
    Set<String> upCollections = complement(newCollections, oldCollections);
    if (upCollections.size() > 0) {
      collectionsUp(upCollections);
    }
  }
  
  private void processLiveNodesChanged(Collection<String> oldLiveNodes,
      Collection<String> liveNodes) {
    
    System.out.println("Nodes changed:" + oldLiveNodes + " -> " + liveNodes);
    
    Set<String> downNodes = complement(oldLiveNodes, liveNodes);
    if (downNodes.size() > 0) {
      nodesDown(downNodes);
    }
    Set<String> upNodes = complement(liveNodes, oldLiveNodes);
    if (upNodes.size() > 0) {
      nodesUp(upNodes);
      addNodeStateWatches(upNodes);
    }
  }
  
  private void addNodeStateWatches(Set<String> nodeNames) {
    
    for (String nodeName : nodeNames) {
      String path = "/node_states/" + nodeName;
      synchronized (nodeStateWatches) {
        if (!nodeStateWatches.containsKey(nodeName)) {

          try {
            zkClient.makePath(path);
          } catch (KeeperException e1) {
            if(e1.code()!=Code.NODEEXISTS) {
              e1.printStackTrace();
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

          NodeStateWatcher nsw = new NodeStateWatcher(zkClient, nodeName, path, this);
          nodeStateWatches.put(nodeName, nsw);
          try {
            byte[] state = zkClient.getData(path, nsw, null);
            nsw.processStateChange(state);
          } catch (KeeperException e) {
            //nocommit
            log.error("", e);
          } catch (InterruptedException e) {
          //nocommit
          }
          
        } else {
          log.info("watch already added");
        }
      }
    }
  }
  
  /**
   * Try to assign core to the cluster
   */
  private void updateState(CoreState coreState) {
    
    //System.out.println("updateState called for core:" + coreState);
    
    String collection = coreState.getCollectionName();
    String coreName = coreState.getCoreName();
    
    synchronized (cloudState) {
      String shardId;
      if (coreState.getProperties().get("shard_id") == null) {
        shardId = AssignShard.assignShard(collection,
            collections.get(collection), cloudState);
      } else {
        shardId = coreState.getProperties().get("shard_id");
      }
      
      Map<String,String> props = new HashMap<String,String>();
      for (Entry<String,String> entry : coreState.getProperties().entrySet()) {
        props.put(entry.getKey(), entry.getValue());
      }
      ZkNodeProps zkProps = new ZkNodeProps(props);
      Slice slice = cloudState.getSlice(collection, shardId);
      Map<String,ZkNodeProps> shardProps;
      if (slice == null) {
        shardProps = new HashMap<String,ZkNodeProps>();
      } else {
        shardProps = cloudState.getSlice(collection, shardId).getShardsCopy();
      }
      shardProps.put(coreName, zkProps);

      slice = new Slice(shardId, shardProps);
      CloudState state = new CloudState(cloudState.getLiveNodes(),
          cloudState.getCollectionStates());
      state.addSlice(collection, slice);
      cloudState = state;
      publishCloudState();
    }
  }
  
  /*
   * Let others know about state change.
   */
  private void publishCloudState() {
    try {
      System.out.println("publish:" + cloudState.getCollections());
      CloudStateUtility.update(zkClient, cloudState, null);
    } catch (KeeperException e) {
      log.error("Could not publish cloud state.", e);
    } catch (InterruptedException e) {
      log.error("Could not publish cloud state.", e);
    } catch (IOException e) {
      log.error("Could not publish cloud state.", e);
    }
  }
  
  protected void nodesUp(Set<String> nodes) {
    log.debug("nodes appeared: " + nodes);
    synchronized (cloudState) {
      HashSet<String> currentNodes = new HashSet<String>();
      currentNodes.addAll(cloudState.getLiveNodes());
      currentNodes.addAll(nodes);
      CloudState state = new CloudState(currentNodes,
          cloudState.getCollectionStates());
      cloudState = state;
    }
  }
  
  protected void nodesDown(Set<String> nodes) {
    log.debug("Nodes down: " + nodes);
  }
  
  private void collectionsUp(Set<String> upCollections) {
    log.debug("Collections up: " + upCollections);
    for (String collection : upCollections) {
      try {
        byte[] data = zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/"
            + collection, null, null);
        if (data != null) {
          ZkNodeProps props = new ZkNodeProps();
          try {
            props = ZkNodeProps.load(data);
          } catch (IOException e) {
            log.error("Could not load ZkNodeProps", e);
          }
          collections.put(collection, props);
          log.info("Registered collection " + collection + " with following properties: "
              + props);
        } else {
          log.info("Collection " + collection + " had no properties.");
        }
        
      } catch (KeeperException e) {
        log.error("Could not read collection settings.", e);
      } catch (InterruptedException e) {
        log.error("Could not read collection settings.", e);
      }
    }
  }
  
  private void collectionsDown(Set<String> downCollections) {
    log.info("Collections deleted: " + downCollections);
  }
  
  @Override
  public void coreCreated(String nodeName, Set<CoreState> states) throws IOException, KeeperException {
    log.info("Cores created: " + nodeName + " states:" +states);
    for (CoreState state : states) {
      updateState(state);
    }
    
    publishNodeAssignments(nodeName, cloudState, nodeStateWatches.get(nodeName).getCurrentState());
  }
  
  @Override
  public void coreDeleted(String nodeName, Set<CoreState> states) {
    log.info("Cores " + states + " deleted from node:" + nodeName);
  }
  
  /**
   * Publish assignments for node
   * @param node
   * @throws IOException 
   */
  private void publishNodeAssignments(String node, CloudState cloudState,
      Set<CoreState> states) throws IOException, KeeperException {
    ArrayList<CoreAssignment> assignments = new ArrayList<CoreAssignment>();
    for(CoreState coreState: states) {
      final String coreName = coreState.getCoreName();
      final String collection = coreState.getCollectionName();
      HashMap<String, String> coreProperties = new HashMap<String, String>();
      Map<String, Slice> slices = cloudState.getSlices(coreState.getCollectionName());
      for(Entry<String, Slice> entry: slices.entrySet()) {
        ZkNodeProps myProps = entry.getValue().getShards().get(coreName);
        if(myProps!=null) {
          coreProperties.put("shard_name", entry.getKey());
        }
      }
      
      CoreAssignment assignment = new CoreAssignment(coreName, collection, coreProperties);
      assignments.add(assignment);
    }
    
    //serialize
    byte[] content = CoreAssignment.tobytes(assignments.toArray(new CoreAssignment[assignments.size()]));
    try {
      zkClient.setData("/node_assignments/" + node, content);
    } catch (InterruptedException e) {
      // nocommit
      e.printStackTrace();
    }
  }
  
  private Set<String> complement(Collection<String> next,
      Collection<String> prev) {
    Set<String> downCollections = new HashSet<String>();
    downCollections.addAll(next);
    downCollections.removeAll(prev);
    return downCollections;
  }

  @Override
  public void coreChanged(String nodeName, Set<CoreState> states) throws IOException, KeeperException  {
    log.info("Cores changed: " + nodeName + " states:" + states);
    for (CoreState state : states) {
      updateState(state);
    }
    
    publishNodeAssignments(nodeName, cloudState, nodeStateWatches.get(nodeName).getCurrentState());
  }

}
