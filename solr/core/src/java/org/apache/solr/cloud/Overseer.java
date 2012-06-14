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
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
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
  
  private static class CloudStateUpdater implements Runnable {
    
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    private final DistributedQueue stateUpdateQueue; 
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue 
    private final DistributedQueue workQueue;
    
    public CloudStateUpdater(final ZkStateReader reader, final String myId) {
      this.zkClient = reader.getZkClient();
      this.stateUpdateQueue = getInQueue(zkClient);
      this.workQueue = getInternalQueue(zkClient);
      this.myId = myId;
      this.reader = reader;
    }
    
    @Override
    public void run() {
        
      if(amILeader()) {
        // see if there's something left from the previous Overseer and re
        // process all events that were not persisted into cloud state
          synchronized (reader.getUpdateLock()) { //XXX this only protects against edits inside single node
            try {
              byte[] head = workQueue.peek();
              
              if (head != null) {
                reader.updateCloudState(true);
                CloudState cloudState = reader.getCloudState();
                log.info("Replaying operations from work queue.");
                
                while (head != null && amILeader()) {
                  final ZkNodeProps message = ZkNodeProps.load(head);
                  final String operation = message
                      .get(QUEUE_OPERATION);
                  cloudState = processMessage(cloudState, message, operation);
                  zkClient.setData(ZkStateReader.CLUSTER_STATE,
                      ZkStateReader.toJSON(cloudState), true);
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
      while (amILeader()) {
        synchronized (reader.getUpdateLock()) {
          try {
            byte[] head = stateUpdateQueue.peek();
            
            if (head != null) {
              reader.updateCloudState(true);
              CloudState cloudState = reader.getCloudState();
              
              while (head != null) {
                final ZkNodeProps message = ZkNodeProps.load(head);
                final String operation = message.get(QUEUE_OPERATION);
                
                cloudState = processMessage(cloudState, message, operation);
                byte[] processed = stateUpdateQueue.remove();
                workQueue.offer(processed);
                head = stateUpdateQueue.peek();
              }
              zkClient.setData(ZkStateReader.CLUSTER_STATE,
                  ZkStateReader.toJSON(cloudState), true);
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

    private CloudState processMessage(CloudState cloudState,
        final ZkNodeProps message, final String operation)
        throws KeeperException, InterruptedException {
      if ("state".equals(operation)) {
        cloudState = updateState(cloudState, message);
      } else if ("deletecore".equals(operation)) {
        cloudState = removeCore(cloudState, message);
      } else if (ZkStateReader.LEADER_PROP.equals(operation)) {
        StringBuilder sb = new StringBuilder();
        String baseUrl = message.get(ZkStateReader.BASE_URL_PROP);
        String coreName = message.get(ZkStateReader.CORE_NAME_PROP);
        sb.append(baseUrl);
        if (!baseUrl.endsWith("/")) sb.append("/");
        sb.append(coreName == null ? "" : coreName);
        if (!(sb.substring(sb.length() - 1).equals("/"))) sb
            .append("/");
        cloudState = setShardLeader(cloudState,
            message.get(ZkStateReader.COLLECTION_PROP),
            message.get(ZkStateReader.SHARD_ID_PROP), sb.toString());
      } else {
        throw new RuntimeException("unknown operation:" + operation
            + " contents:" + message.getProperties());
      }
      return cloudState;
    }
      
      private boolean amILeader() {
        try {
          ZkNodeProps props = ZkNodeProps.load(zkClient.getData("/overseer_elect/leader", null, null, true));
          if(myId.equals(props.get("id"))) {
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
       * @throws KeeperException 
       * @throws InterruptedException 
       */
      private CloudState updateState(CloudState state, final ZkNodeProps message) throws KeeperException, InterruptedException {
        final String collection = message.get(ZkStateReader.COLLECTION_PROP);
        final String zkCoreNodeName = message.get(ZkStateReader.NODE_NAME_PROP) + "_" + message.get(ZkStateReader.CORE_NAME_PROP);
        final Integer numShards = message.get(ZkStateReader.NUM_SHARDS_PROP)!=null?Integer.parseInt(message.get(ZkStateReader.NUM_SHARDS_PROP)):null;
        
        //collection does not yet exist, create placeholders if num shards is specified
        if (!state.getCollections().contains(collection)
            && numShards!=null) {
          state = createCollection(state, collection, numShards);
        }
        
        // use the provided non null shardId
        String shardId = message.get(ZkStateReader.SHARD_ID_PROP);
        if (shardId == null) {
          String nodeName = message.get(ZkStateReader.NODE_NAME_PROP);
          //get shardId from CloudState
          shardId = getAssignedId(state, nodeName, message);
        }
        if(shardId == null) {
          //request new shardId 
          shardId = AssignShard.assignShard(collection, state, numShards);
        }
          
          Map<String,String> props = new HashMap<String,String>();
          Map<String,String> coreProps = new HashMap<String,String>(message.getProperties().size());
          coreProps.putAll(message.getProperties());
          // we don't put num_shards in the clusterstate
          coreProps.remove(ZkStateReader.NUM_SHARDS_PROP);
          coreProps.remove(QUEUE_OPERATION);
          for (Entry<String,String> entry : coreProps.entrySet()) {
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

      private CloudState createCollection(CloudState state, String collectionName, int numShards) {
        Map<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String, Slice>>();
        Map<String, Slice> newSlices = new LinkedHashMap<String,Slice>();
        newStates.putAll(state.getCollectionStates());
        for (int i = 0; i < numShards; i++) {
          final String sliceName = "shard" + (i+1);
          newSlices.put(sliceName, new Slice(sliceName, Collections.EMPTY_MAP));
        }
        newStates.put(collectionName, newSlices);
        CloudState newCloudState = new CloudState(state.getLiveNodes(), newStates);
        return newCloudState;
      }
      
      /*
       * Return an already assigned id or null if not assigned
       */
      private String getAssignedId(final CloudState state, final String nodeName,
          final ZkNodeProps coreState) {
        final String key = coreState.get(ZkStateReader.NODE_NAME_PROP) + "_" +  coreState.get(ZkStateReader.CORE_NAME_PROP);
        Map<String, Slice> slices = state.getSlices(coreState.get(ZkStateReader.COLLECTION_PROP));
        if (slices != null) {
          for (Slice slice : slices.values()) {
            if (slice.getShards().get(key) != null) {
              return slice.getName();
            }
          }
        }
        return null;
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
          final Map<String,ZkNodeProps> newShards = new LinkedHashMap<String,ZkNodeProps>();
          for(Entry<String, ZkNodeProps> shard: slices.get(sliceName).getShards().entrySet()) {
            Map<String, String> newShardProps = new LinkedHashMap<String,String>();
            newShardProps.putAll(shard.getValue().getProperties());
            
            newShardProps.remove(ZkStateReader.LEADER_PROP);  //clean any previously existed flag
            
            ZkCoreNodeProps zkCoreNodeProps = new ZkCoreNodeProps(new ZkNodeProps(newShardProps));
            if(leaderUrl!=null && leaderUrl.equals(zkCoreNodeProps.getCoreUrl())) {
              newShardProps.put(ZkStateReader.LEADER_PROP,"true");
            }
            newShards.put(shard.getKey(), new ZkNodeProps(newShardProps));
          }
          Slice slice = new Slice(sliceName, newShards);
          slices.put(sliceName, slice);
        }
        return new CloudState(state.getLiveNodes(), newStates);
      }
      
      /*
       * Remove core from cloudstate
       */
      private CloudState removeCore(final CloudState cloudState, ZkNodeProps message) {
        
        final String coreNodeName = message.get(ZkStateReader.NODE_NAME_PROP) + "_" + message.get(ZkStateReader.CORE_NAME_PROP);
        final String collection = message.get(ZkStateReader.COLLECTION_PROP);

        final LinkedHashMap<String, Map<String, Slice>> newStates = new LinkedHashMap<String,Map<String,Slice>>();
        for(String collectionName: cloudState.getCollections()) {
          if(collection.equals(collectionName)) {
            Map<String, Slice> slices = cloudState.getSlices(collection);
            LinkedHashMap<String, Slice> newSlices = new LinkedHashMap<String, Slice>();
            for(Slice slice: slices.values()) {
              if(slice.getShards().containsKey(coreNodeName)) {
                LinkedHashMap<String, ZkNodeProps> newShards = new LinkedHashMap<String, ZkNodeProps>();
                newShards.putAll(slice.getShards());
                newShards.remove(coreNodeName);
                Slice newSlice = new Slice(slice.getName(), newShards);
                newSlices.put(slice.getName(), newSlice);
              } else {
                newSlices.put(slice.getName(), slice);
              }
            }
            newStates.put(collectionName, newSlices);
          } else {
            newStates.put(collectionName, cloudState.getSlices(collectionName));
          }
        }
        CloudState newState = new CloudState(cloudState.getLiveNodes(), newStates);
        return newState;
     }
  }
  
  public Overseer(final ZkStateReader reader, final String id) throws KeeperException, InterruptedException {
    log.info("Overseer (id=" + id + ") starting");
    createOverseerNode(reader.getZkClient());
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    Thread updaterThread = new Thread(tg, new CloudStateUpdater(reader, id));
    updaterThread.setDaemon(true);
    updaterThread.start();
  }

  /**
   * Get queue that can be used to send messages to Overseer.
   */
  public static DistributedQueue getInQueue(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient.getSolrZooKeeper(), "/overseer/queue", null);
  }

  /* Internal queue, not to be used outside of Overseer */
  static DistributedQueue getInternalQueue(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient.getSolrZooKeeper(), "/overseer/queue-work", null);
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
