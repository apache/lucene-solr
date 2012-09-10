package org.apache.solr.cloud;

/*
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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ClosableThread;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible node assignments, cluster state file?
 */
public class Overseer {
  public static final String QUEUE_OPERATION = "operation";

  private static final int STATE_UPDATE_DELAY = 500;  // delay between cloud state updates

  private static Logger log = LoggerFactory.getLogger(Overseer.class);
  
  private class ClusterStateUpdater implements Runnable, ClosableThread {
    
    private static final String DELETECORE = "deletecore";
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    private final DistributedQueue stateUpdateQueue; 
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue 
    private final DistributedQueue workQueue;
    private volatile boolean isClosed;
    
    public ClusterStateUpdater(final ZkStateReader reader, final String myId) {
      this.zkClient = reader.getZkClient();
      this.stateUpdateQueue = getInQueue(zkClient);
      this.workQueue = getInternalQueue(zkClient);
      this.myId = myId;
      this.reader = reader;
    }
    
    @Override
    public void run() {
        
      if(!this.isClosed && amILeader()) {
        // see if there's something left from the previous Overseer and re
        // process all events that were not persisted into cloud state
          synchronized (reader.getUpdateLock()) { //XXX this only protects against edits inside single node
            try {
              byte[] head = workQueue.peek();
              
              if (head != null) {
                reader.updateClusterState(true);
                ClusterState clusterState = reader.getClusterState();
                log.info("Replaying operations from work queue.");
                
                while (head != null && amILeader()) {
                  final ZkNodeProps message = ZkNodeProps.load(head);
                  final String operation = message
                      .getStr(QUEUE_OPERATION);
                  clusterState = processMessage(clusterState, message, operation);
                  zkClient.setData(ZkStateReader.CLUSTER_STATE,
                      ZkStateReader.toJSON(clusterState), true);
                  workQueue.remove();
                  head = workQueue.peek();
                }
              }
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("Solr cannot talk to ZK");
                return;
              }
              SolrException.log(log, "", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      
      log.info("Starting to work on the main queue");
      while (!this.isClosed && amILeader()) {
        synchronized (reader.getUpdateLock()) {
          try {
            byte[] head = stateUpdateQueue.peek();
            
            if (head != null) {
              reader.updateClusterState(true);
              ClusterState clusterState = reader.getClusterState();
              
              while (head != null) {
                final ZkNodeProps message = ZkNodeProps.load(head);
                final String operation = message.getStr(QUEUE_OPERATION);
                
                clusterState = processMessage(clusterState, message, operation);
                workQueue.offer(head);
                stateUpdateQueue.remove();
                head = stateUpdateQueue.peek();
              }
              zkClient.setData(ZkStateReader.CLUSTER_STATE,
                  ZkStateReader.toJSON(clusterState), true);
            }
            // clean work queue
            while (workQueue.poll() != null);
            
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED
                || e.code() == KeeperException.Code.CONNECTIONLOSS) {
              log.warn("Overseer cannot talk to ZK");
              return;
            }
            SolrException.log(log, "", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        
        try {
          Thread.sleep(STATE_UPDATE_DELAY);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private ClusterState processMessage(ClusterState clusterState,
        final ZkNodeProps message, final String operation) {
      if ("state".equals(operation)) {
        clusterState = updateState(clusterState, message);
      } else if (DELETECORE.equals(operation)) {
        clusterState = removeCore(clusterState, message);
      } else if (ZkStateReader.LEADER_PROP.equals(operation)) {

        StringBuilder sb = new StringBuilder();
        String baseUrl = message.getStr(ZkStateReader.BASE_URL_PROP);
        String coreName = message.getStr(ZkStateReader.CORE_NAME_PROP);
        sb.append(baseUrl);
        if (baseUrl != null && !baseUrl.endsWith("/")) sb.append("/");
        sb.append(coreName == null ? "" : coreName);
        if (!(sb.substring(sb.length() - 1).equals("/"))) sb.append("/");
        clusterState = setShardLeader(clusterState,
            message.getStr(ZkStateReader.COLLECTION_PROP),
            message.getStr(ZkStateReader.SHARD_ID_PROP),
            sb.length() > 0 ? sb.toString() : null);

      } else {
        throw new RuntimeException("unknown operation:" + operation
            + " contents:" + message.getProperties());
      }
      return clusterState;
    }
      
      private boolean amILeader() {
        try {
          ZkNodeProps props = ZkNodeProps.load(zkClient.getData("/overseer_elect/leader", null, null, true));
          if(myId.equals(props.getStr("id"))) {
            return true;
          }
        } catch (KeeperException e) {
          log.warn("", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
        return false;
      }
      /**
       * Try to assign core to the cluster. 
       */
      private ClusterState updateState(ClusterState state, final ZkNodeProps message) {
        final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
        final String zkCoreNodeName = message.getStr(ZkStateReader.NODE_NAME_PROP) + "_" + message.getStr(ZkStateReader.CORE_NAME_PROP);
        final Integer numShards = message.getStr(ZkStateReader.NUM_SHARDS_PROP)!=null?Integer.parseInt(message.getStr(ZkStateReader.NUM_SHARDS_PROP)):null;
        
        //collection does not yet exist, create placeholders if num shards is specified
        if (!state.getCollections().contains(collection)
            && numShards!=null) {
          state = createCollection(state, collection, numShards);
        }
        
        // use the provided non null shardId
        String shardId = message.getStr(ZkStateReader.SHARD_ID_PROP);
        if (shardId == null) {
          String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);
          //get shardId from ClusterState
          shardId = getAssignedId(state, nodeName, message);
        }
        if(shardId == null) {
          //request new shardId 
          shardId = AssignShard.assignShard(collection, state, numShards);
        }
          
          Map<String,Object> props = new HashMap<String,Object>();
          Map<String,Object> coreProps = new HashMap<String,Object>(message.getProperties().size());
          coreProps.putAll(message.getProperties());
          // we don't put num_shards in the clusterstate
          coreProps.remove(ZkStateReader.NUM_SHARDS_PROP);
          coreProps.remove(QUEUE_OPERATION);
          for (Entry<String,Object> entry : coreProps.entrySet()) {
            props.put(entry.getKey(), entry.getValue());
          }
          Replica zkProps = new Replica(zkCoreNodeName, props);
          Slice slice = state.getSlice(collection, shardId);
          Map<String,Replica> shardProps;
          if (slice == null) {
            shardProps = new HashMap<String,Replica>();
          } else {
            shardProps = state.getSlice(collection, shardId).getReplicasCopy();
          }
          shardProps.put(zkCoreNodeName, zkProps);

          slice = new Slice(shardId, shardProps);
          ClusterState newClusterState = updateSlice(state, collection, slice);
          return newClusterState;
      }

      private ClusterState createCollection(ClusterState state, String collectionName, int numShards) {
        Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String, Slice>>();
        Map<String, Slice> newSlices = new LinkedHashMap<String,Slice>();
        newStates.putAll(state.getCollectionStates());
        for (int i = 0; i < numShards; i++) {
          final String sliceName = "shard" + (i+1);
          newSlices.put(sliceName, new Slice(sliceName, Collections.EMPTY_MAP));
        }
        newStates.put(collectionName, newSlices);
        ClusterState newClusterState = new ClusterState(state.getLiveNodes(), newStates);
        return newClusterState;
      }
      
      /*
       * Return an already assigned id or null if not assigned
       */
      private String getAssignedId(final ClusterState state, final String nodeName,
          final ZkNodeProps coreState) {
        final String key = coreState.getStr(ZkStateReader.NODE_NAME_PROP) + "_" +  coreState.getStr(ZkStateReader.CORE_NAME_PROP);
        Map<String, Slice> slices = state.getSlices(coreState.getStr(ZkStateReader.COLLECTION_PROP));
        if (slices != null) {
          for (Slice slice : slices.values()) {
            if (slice.getReplicasMap().get(key) != null) {
              return slice.getName();
            }
          }
        }
        return null;
      }
      
      private ClusterState updateSlice(ClusterState state, String collection, Slice slice) {
        
        final Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        newStates.putAll(state.getCollectionStates());
        
        if (!newStates.containsKey(collection)) {
          newStates.put(collection, new LinkedHashMap<String,Slice>());
        }
        
        final Map<String, Slice> slices = newStates.get(collection);
        if (!slices.containsKey(slice.getName())) {
          slices.put(slice.getName(), slice);
        } else {
          final Map<String,Replica> shards = new LinkedHashMap<String,Replica>();
          final Slice existingSlice = slices.get(slice.getName());
          shards.putAll(existingSlice.getReplicasMap());
          //XXX preserve existing leader
          for(Entry<String, Replica> edit: slice.getReplicasMap().entrySet()) {
            if(existingSlice.getReplicasMap().get(edit.getKey())!=null && existingSlice.getReplicasMap().get(edit.getKey()).containsKey(ZkStateReader.LEADER_PROP)) {
              HashMap<String, Object> newProps = new HashMap<String,Object>();
              newProps.putAll(edit.getValue().getProperties());
              newProps.put(ZkStateReader.LEADER_PROP, existingSlice.getReplicasMap().get(edit.getKey()).getStr(ZkStateReader.LEADER_PROP));
              shards.put(edit.getKey(), new Replica(edit.getKey(), newProps));
            } else {
              shards.put(edit.getKey(), edit.getValue());
            }
          }
          final Slice updatedSlice = new Slice(slice.getName(), shards);
          slices.put(slice.getName(), updatedSlice);
        }
        return new ClusterState(state.getLiveNodes(), newStates);
      }
      
      private ClusterState setShardLeader(ClusterState state, String collection, String sliceName, String leaderUrl) {
        
        final Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        newStates.putAll(state.getCollectionStates());
        
        final Map<String, Slice> slices = newStates.get(collection);

        if(slices==null) {
          log.error("Could not mark shard leader for non existing collection:" + collection);
          return state;
        }
        
        if (!slices.containsKey(sliceName)) {
          log.error("Could not mark leader for non existing slice:" + sliceName);
          return state;
        } else {
          final Map<String,Replica> newShards = new LinkedHashMap<String,Replica>();
          for(Entry<String, Replica> shard: slices.get(sliceName).getReplicasMap().entrySet()) {
            Map<String, Object> newShardProps = new LinkedHashMap<String,Object>();
            newShardProps.putAll(shard.getValue().getProperties());
            
            newShardProps.remove(ZkStateReader.LEADER_PROP);  //clean any previously existed flag
            
            ZkCoreNodeProps zkCoreNodeProps = new ZkCoreNodeProps(new ZkNodeProps(newShardProps));
            if(leaderUrl!=null && leaderUrl.equals(zkCoreNodeProps.getCoreUrl())) {
              newShardProps.put(ZkStateReader.LEADER_PROP,"true");
            }
            newShards.put(shard.getKey(), new Replica(shard.getKey(), newShardProps));
          }
          Slice slice = new Slice(sliceName, newShards);
          slices.put(sliceName, slice);
        }
        return new ClusterState(state.getLiveNodes(), newStates);
      }
      
      /*
       * Remove core from cloudstate
       */
      private ClusterState removeCore(final ClusterState clusterState, ZkNodeProps message) {
        
        final String coreNodeName = message.getStr(ZkStateReader.NODE_NAME_PROP) + "_" + message.getStr(ZkStateReader.CORE_NAME_PROP);
        final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

        final LinkedHashMap<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        for(String collectionName: clusterState.getCollections()) {
          if(collection.equals(collectionName)) {
            Map<String, Slice> slices = clusterState.getSlices(collection);
            LinkedHashMap<String, Slice> newSlices = new LinkedHashMap<String, Slice>();
            for(Slice slice: slices.values()) {
              if(slice.getReplicasMap().containsKey(coreNodeName)) {
                LinkedHashMap<String, Replica> newShards = new LinkedHashMap<String, Replica>();
                newShards.putAll(slice.getReplicasMap());
                newShards.remove(coreNodeName);
                
                Slice newSlice = new Slice(slice.getName(), newShards);
                newSlices.put(slice.getName(), newSlice);

              } else {
                newSlices.put(slice.getName(), slice);
              }
            }
            int cnt = 0;
            for (Slice slice : newSlices.values()) {
              cnt+=slice.getReplicasMap().size();
            }
            // TODO: if no nodes are left after this unload
            // remove from zk - do we have a race where Overseer
            // see's registered nodes and publishes though?
            if (cnt > 0) {
              newStates.put(collectionName, newSlices);
            } else {
              // TODO: it might be better logically to have this in ZkController
              // but for tests (it's easier) it seems better for the moment to leave CoreContainer and/or
              // ZkController out of the Overseer.
              try {
                zkClient.clean("/collections/" + collectionName);
              } catch (InterruptedException e) {
                SolrException.log(log, "Cleaning up collection in zk was interrupted:" + collectionName, e);
                Thread.currentThread().interrupt();
              } catch (KeeperException e) {
                SolrException.log(log, "Problem cleaning up collection in zk:" + collectionName, e);
              }
            }
          } else {
            newStates.put(collectionName, clusterState.getSlices(collectionName));
          }
        }
        ClusterState newState = new ClusterState(clusterState.getLiveNodes(), newStates);
        return newState;
     }

      @Override
      public void close() {
        this.isClosed = true;
      }

      @Override
      public boolean isClosed() {
        return this.isClosed;
      }
    
  }

  class OverseerThread extends Thread implements ClosableThread {

    private volatile boolean isClosed;

    public OverseerThread(ThreadGroup tg,
        ClusterStateUpdater clusterStateUpdater) {
      super(tg, clusterStateUpdater);
    }

    public OverseerThread(ThreadGroup ccTg,
        OverseerCollectionProcessor overseerCollectionProcessor, String string) {
      super(ccTg, overseerCollectionProcessor, string);
    }

    @Override
    public void close() {
      this.isClosed = true;
    }

    @Override
    public boolean isClosed() {
      return this.isClosed;
    }
    
  }
  
  private OverseerThread ccThread;

  private OverseerThread updaterThread;

  private volatile boolean isClosed;

  private ZkStateReader reader;

  private ShardHandler shardHandler;

  private String adminPath;
  
  public Overseer(ShardHandler shardHandler, String adminPath, final ZkStateReader reader) throws KeeperException, InterruptedException {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.adminPath = adminPath;
  }
  
  public void start(String id) {
    log.info("Overseer (id=" + id + ") starting");
    createOverseerNode(reader.getZkClient());
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id));
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");
    ccThread = new OverseerThread(ccTg, new OverseerCollectionProcessor(reader, id, shardHandler, adminPath), 
        "Overseer-" + id);
    ccThread.setDaemon(true);
    
    updaterThread.start();
    ccThread.start();
  }
  
  public void close() {
    isClosed = true;
    if (updaterThread != null) {
      updaterThread.close();
      updaterThread.interrupt();
    }
    if (ccThread != null) {
      ccThread.close();
      ccThread.interrupt();
    }
  }

  /**
   * Get queue that can be used to send messages to Overseer.
   */
  public static DistributedQueue getInQueue(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue", null);
  }

  /* Internal queue, not to be used outside of Overseer */
  static DistributedQueue getInternalQueue(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue-work", null);
  }
  
  /* Collection creation queue */
  static DistributedQueue getCollectionQueue(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/collection-queue-work", null);
  }
  
  private static void createOverseerNode(final SolrZkClient zkClient) {
    try {
      zkClient.create("/overseer", new byte[0], CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
      //ok
    } catch (InterruptedException e) {
      log.error("Could not create Overseer node: " + e.getClass() + ":" + e.getMessage());
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("Could not create Overseer node: " + e.getClass() + ":" + e.getMessage());
      throw new RuntimeException(e);
    }
  }

}
