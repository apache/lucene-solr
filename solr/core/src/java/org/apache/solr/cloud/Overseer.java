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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Set;

import org.apache.solr.cloud.NodeStateWatcher.NodeStateChangeListener;
import org.apache.solr.cloud.ShardLeaderWatcher.ShardLeaderListener;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkOperation;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible node assignments, cluster state file?
 */
public class Overseer implements NodeStateChangeListener, ShardLeaderListener {

  private static final int STATE_UPDATE_DELAY = 500;  // delay between cloud state updates

  static enum Op {
    LeaderChange, StateChange; 
  }

  private final class CloudStateUpdateRequest {
    
    final Op operation;
    final Object[] args;
    
     CloudStateUpdateRequest(final Op operation, final Object... args) {
       this.operation = operation;
       this.args = args;
    }
  }
  
  public static final String ASSIGNMENTS_NODE = "/node_assignments";
  public static final String STATES_NODE = "/node_states";
  private static Logger log = LoggerFactory.getLogger(Overseer.class);
  
  private final SolrZkClient zkClient;
  
  // pooled updates
  private final LinkedBlockingQueue<CloudStateUpdateRequest> fifo = new LinkedBlockingQueue<CloudStateUpdateRequest>();
  
  // node stateWatches
  private HashMap<String,NodeStateWatcher> nodeStateWatches = new HashMap<String,NodeStateWatcher>();

  // shard leader watchers  (collection->slice->watcher)
  private HashMap<String, HashMap<String,ShardLeaderWatcher>> shardLeaderWatches = new HashMap<String,HashMap<String,ShardLeaderWatcher>>();
  private ZkCmdExecutor zkCmdExecutor;

  private static class CloudStateUpdater implements Runnable {
    
    private final LinkedBlockingQueue<CloudStateUpdateRequest> fifo;
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    
    public CloudStateUpdater(final LinkedBlockingQueue<CloudStateUpdateRequest> fifo, final ZkStateReader reader, final SolrZkClient zkClient, final String myId) {
      this.fifo = fifo;
      this.myId = myId;
      this.reader = reader;
      this.zkClient = zkClient;
    }
      @Override
      public void run() {
        while (amILeader()) {
          
          
          LinkedList<CloudStateUpdateRequest> requests = new LinkedList<Overseer.CloudStateUpdateRequest>();
          while (!fifo.isEmpty()) {
            // collect all queued requests
            CloudStateUpdateRequest req;
            req = fifo.poll();
            if (req == null) {
              break;
            }
            requests.add(req);
          }

          if (requests.size() > 0) {
            // process updates
            synchronized (reader.getUpdateLock()) {
              try {
                reader.updateCloudState(true);
                CloudState cloudState = reader.getCloudState();
                for (CloudStateUpdateRequest request : requests) {

                  switch (request.operation) {
                  case LeaderChange:
                    cloudState = setShardLeader(cloudState,
                        (String) request.args[0], (String) request.args[1],
                        (String) request.args[2]);

                    break;
                  case StateChange:
                    cloudState = updateState(cloudState,
                        (String) request.args[0], (CoreState) request.args[1]);
                    break;

                  }
                }

                log.info("Announcing new cluster state");
                zkClient.setData(ZkStateReader.CLUSTER_STATE,
                    ZkStateReader.toJSON(cloudState), true);

              } catch (KeeperException e) {
                // XXX stop processing, exit
                return;
              } catch (InterruptedException e) {
                // XXX stop processing, exit
                return;
              }
            }
          }

          try {
            Thread.sleep(STATE_UPDATE_DELAY);
          } catch (InterruptedException e) {
            //
          }
        }
      }
      
      private boolean amILeader() {
        try {
          ZkNodeProps props = ZkNodeProps.load(zkClient.getData("/overseer_elect/leader", null, null, false));
          if(myId.equals(props.get("id"))) {
            return true;
          }
        } catch (KeeperException e) {
          // assume we're dead
        } catch (InterruptedException e) {
          // assume we're dead
        }
        log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
        return false;
      }
      /**
       * Try to assign core to the cluster
       * @throws KeeperException 
       * @throws InterruptedException 
       */
      private CloudState updateState(CloudState state, String nodeName, CoreState coreState) throws KeeperException, InterruptedException {
        String collection = coreState.getCollectionName();
        String zkCoreNodeName = coreState.getCoreNodeName();
        
          String shardId;
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
          shardProps.put(zkCoreNodeName, zkProps);

          slice = new Slice(shardId, shardProps);
          CloudState newCloudState = updateSlice(state, collection, slice);
          return newCloudState;
      }
      
      private CloudState updateSlice(CloudState state, String collection, Slice slice) {
        
        final Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        newStates.putAll(state.getCollectionStates());
        
        if (!newStates.containsKey(collection)) {
          newStates.put(collection, new LinkedHashMap<String,Slice>());
        }
        
        final Map<String, Slice> slices = newStates.get(collection);
        if (!slices.containsKey(slice.getName())) {
          slices.put(slice.getName(), slice);
        } else {
          final Map<String,ZkNodeProps> shards = new LinkedHashMap<String,ZkNodeProps>();
          final Slice existingSlice = slices.get(slice.getName());
          shards.putAll(existingSlice.getShards());
          //XXX preserve existing leader
          for(Entry<String, ZkNodeProps> edit: slice.getShards().entrySet()) {
            if(existingSlice.getShards().get(edit.getKey())!=null && existingSlice.getShards().get(edit.getKey()).containsKey(ZkStateReader.LEADER_PROP)) {
              HashMap<String, String> newProps = new HashMap<String,String>();
              newProps.putAll(edit.getValue().getProperties());
              newProps.put(ZkStateReader.LEADER_PROP, existingSlice.getShards().get(edit.getKey()).get(ZkStateReader.LEADER_PROP));
              shards.put(edit.getKey(), new ZkNodeProps(newProps));
            } else {
              shards.put(edit.getKey(), edit.getValue());
            }
          }
          final Slice updatedSlice = new Slice(slice.getName(), shards);
          slices.put(slice.getName(), updatedSlice);
        }
        return new CloudState(state.getLiveNodes(), newStates);
      }
      
      private CloudState setShardLeader(CloudState state, String collection, String sliceName, String leaderUrl) {
        
        boolean updated = false;
        final Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        newStates.putAll(state.getCollectionStates());
        
        final Map<String, Slice> slices = newStates.get(collection);

        if(slices==null) {
          log.error("Could not mark shard leader for non existing collection.");
          return state;
        }
        
        if (!slices.containsKey(sliceName)) {
          log.error("Could not mark leader for non existing slice.");
          return state;
        } else {
          final Map<String,ZkNodeProps> newShards = new LinkedHashMap<String,ZkNodeProps>();
          for(Entry<String, ZkNodeProps> shard: slices.get(sliceName).getShards().entrySet()) {
            Map<String, String> newShardProps = new LinkedHashMap<String,String>();
            newShardProps.putAll(shard.getValue().getProperties());
            
            String wasLeader = newShardProps.remove(ZkStateReader.LEADER_PROP);  //clean any previously existed flag

            ZkCoreNodeProps zkCoreNodeProps = new ZkCoreNodeProps(new ZkNodeProps(newShardProps));
            if(leaderUrl!=null && leaderUrl.equals(zkCoreNodeProps.getCoreUrl())) {
              newShardProps.put(ZkStateReader.LEADER_PROP,"true");
              if (wasLeader == null) {
                updated = true;
              }
            } else {
              if (wasLeader != null) {
                updated = true;
              }
            }
            newShards.put(shard.getKey(), new ZkNodeProps(newShardProps));
          }
          Slice slice = new Slice(sliceName, newShards);
          slices.put(sliceName, slice);
        }
        if (updated) {
          return new CloudState(state.getLiveNodes(), newStates);
        } else {
          return state;
        }
      }

    }
  
  public Overseer(final SolrZkClient zkClient, final ZkStateReader reader, String id) throws KeeperException, InterruptedException {
    log.info("Constructing new Overseer id=" + id);
    this.zkClient = zkClient;
    this.zkCmdExecutor = new ZkCmdExecutor();
    createWatches();
    
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer delayed state updater");
    Thread updaterThread = new Thread(tg, new CloudStateUpdater(fifo, reader, zkClient, id));
    updaterThread.setDaemon(true);
    updaterThread.start();
  }
  
  public synchronized void createWatches()
      throws KeeperException, InterruptedException {
    addCollectionsWatch();
    addLiveNodesWatch();
  }

  /* 
   * Watch for collections so we can add watches for its shard leaders.
   */
  private void addCollectionsWatch() throws KeeperException,
      InterruptedException {
    
    zkCmdExecutor.ensureExists(ZkStateReader.COLLECTIONS_ZKNODE, zkClient);
    
    List<String> collections = zkClient.getChildren(ZkStateReader.COLLECTIONS_ZKNODE, new Watcher(){
      @Override
      public void process(WatchedEvent event) {
        try {
          List<String> collections = zkClient.getChildren(ZkStateReader.COLLECTIONS_ZKNODE, this, true);
          collectionsChanged(collections);
        } catch (KeeperException e) {
            if (e.code() == Code.CONNECTIONLOSS || e.code() == Code.SESSIONEXPIRED) {
            log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
            return;
          }
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          log.warn("", e);
        }
      }
    }, true);
    
    collectionsChanged(collections);
  }
  
  private void collectionsChanged(Collection<String> collections) throws KeeperException, InterruptedException {
    synchronized (shardLeaderWatches) {
      for(String collection: collections) {
        if(!shardLeaderWatches.containsKey(collection)) {
          shardLeaderWatches.put(collection, new HashMap<String,ShardLeaderWatcher>());
          addShardLeadersWatch(collection);
        }
      }
      //XXX not handling delete collections..
    }
  }

  /**
   * Add a watch for node containing shard leaders for a collection
   * @param collection
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void addShardLeadersWatch(final String collection) throws KeeperException,
      InterruptedException {
    
    zkCmdExecutor.ensureExists(ZkStateReader.getShardLeadersPath(collection, null), zkClient);
    
    final List<String> leaderNodes = zkClient.getChildren(
        ZkStateReader.getShardLeadersPath(collection, null), new Watcher() {
          
          @Override
          public void process(WatchedEvent event) {
            try {
              List<String> leaderNodes = zkClient.getChildren(
                  ZkStateReader.getShardLeadersPath(collection, null), this, true);
              
              processLeaderNodesChanged(collection, leaderNodes);
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                return;
              }
              SolrException.log(log, "", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
            }
          }
        }, true);
    
    processLeaderNodesChanged(collection, leaderNodes);
  }

  /**
   * Process change in shard leaders. Make sure we have watches for each leader.
   */
  private void processLeaderNodesChanged(final String collection, final Collection<String> shardIds) {
    if(log.isInfoEnabled()) {
      log.info("Leader nodes changed for collection: " + collection + " nodes now:" + shardIds);
    }
    
    Map<String, ShardLeaderWatcher> watches = shardLeaderWatches.get(collection);
    Set<String> currentWatches = new HashSet<String>();
    currentWatches.addAll(watches.keySet());
    
    Set<String> newLeaders = complement(shardIds, currentWatches);

    Set<String> lostLeaders = complement(currentWatches, shardIds);
    //remove watches for lost shards
    for (String shardId : lostLeaders) {
      ShardLeaderWatcher watcher = watches.remove(shardId);
      if (watcher != null) {
        watcher.close();
        announceLeader(collection, shardId, new ZkCoreNodeProps(new ZkNodeProps()));  //removes loeader for shard
      }
    }
    
    //add watches for the new shards
    for(String shardId: newLeaders) {
      try {
        ShardLeaderWatcher watcher = new ShardLeaderWatcher(shardId, collection, zkClient, this);
        watches.put(shardId, watcher);
      } catch (KeeperException e) {
        log.error("Failed to create watcher for shard leader col:" + collection + " shard:" + shardId + ", exception: " + e.getClass());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Failed to create watcher for shard leader col:" + collection + " shard:" + shardId + ", exception: " + e.getClass());
      }
    }
  }

  private void addLiveNodesWatch() throws KeeperException,
      InterruptedException {
    List<String> liveNodes = zkCmdExecutor.retryOperation(new ZkOperation() {
      
      @Override
      public Object execute() throws KeeperException, InterruptedException {
        return zkClient.getChildren(
            ZkStateReader.LIVE_NODES_ZKNODE, new Watcher() {
              
              @Override
              public void process(WatchedEvent event) {
                try {
                    List<String> liveNodes = zkClient.getChildren(
                        ZkStateReader.LIVE_NODES_ZKNODE, this, true);
                    Set<String> liveNodesSet = new HashSet<String>();
                    liveNodesSet.addAll(liveNodes);
                    processLiveNodesChanged(nodeStateWatches.keySet(), liveNodes);
                } catch (KeeperException e) {
                  if (e.code() == KeeperException.Code.SESSIONEXPIRED
                      || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                    log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                    return;
                  }
                  SolrException.log(log, "", e);
                  throw new ZooKeeperException(
                      SolrException.ErrorCode.SERVER_ERROR, "", e);
                } catch (InterruptedException e) {
                  // Restore the interrupted status
                  Thread.currentThread().interrupt();
                }
              }
            }, true);
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
    }
  }
  
  private void addNodeStateWatches(Set<String> nodeNames) throws InterruptedException, KeeperException {
    
    for (String nodeName : nodeNames) {
      final String path = STATES_NODE + "/" + nodeName;
      synchronized (nodeStateWatches) {
        if (!nodeStateWatches.containsKey(nodeName)) {
          zkCmdExecutor.ensureExists(path, zkClient);
          nodeStateWatches.put(nodeName, new NodeStateWatcher(zkClient, nodeName, path, this));
        } else {
          log.debug("watch already added");
        }
      }
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
  public void coreChanged(final String nodeName, final Set<CoreState> states)
      throws KeeperException, InterruptedException {
    log.info("Core change pooled: " + nodeName + " states:" + states);
    for (CoreState state : states) {
      fifo.add(new CloudStateUpdateRequest(Op.StateChange, nodeName, state));
    }
  }
  
  public static void createClientNodes(SolrZkClient zkClient, String nodeName) throws KeeperException, InterruptedException {
    final String node = STATES_NODE + "/" + nodeName;
    if (log.isInfoEnabled()) {
      log.info("creating node:" + node);
    }
    
    ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor();
    zkCmdExecutor.ensureExists(node, zkClient);
  }

  @Override
  public void announceLeader(String collection, String shardId, ZkCoreNodeProps props) {
    log.info("Leader change pooled.");
    fifo.add(new CloudStateUpdateRequest(Op.LeaderChange, collection, shardId, props.getCoreUrl()));
  }
  
}