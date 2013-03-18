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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClosableThread;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
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

  private static final int STATE_UPDATE_DELAY = 1500;  // delay between cloud state updates

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
        
      if (!this.isClosed && amILeader()) {
        // see if there's something left from the previous Overseer and re
        // process all events that were not persisted into cloud state
        synchronized (reader.getUpdateLock()) { // XXX this only protects
                                                // against edits inside single
                                                // node
          try {
            byte[] head = workQueue.peek();
            
            if (head != null) {
              reader.updateClusterState(true);
              ClusterState clusterState = reader.getClusterState();
              log.info("Replaying operations from work queue.");
              
              while (head != null && amILeader()) {
                final ZkNodeProps message = ZkNodeProps.load(head);
                final String operation = message.getStr(QUEUE_OPERATION);
                clusterState = processMessage(clusterState, message, operation);
                zkClient.setData(ZkStateReader.CLUSTER_STATE,
                    ZkStateReader.toJSON(clusterState), true);
                
                workQueue.poll();
                
                head = workQueue.peek();
              }
            }
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            }
            log.error("Exception in Overseer work queue loop", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
            
          } catch (Exception e) {
            log.error("Exception in Overseer work queue loop", e);
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
                
                stateUpdateQueue.poll();
                head = stateUpdateQueue.peek();
              }
              zkClient.setData(ZkStateReader.CLUSTER_STATE,
                  ZkStateReader.toJSON(clusterState), true);
            }
            // clean work queue
            while (workQueue.poll() != null);
            
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
              return;
            }
            log.error("Exception in Overseer main queue loop", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
            
          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
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
        String coreNodeName = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
        if (coreNodeName == null) {
          // it must be the default then
          coreNodeName = message.getStr(ZkStateReader.NODE_NAME_PROP) + "_" + message.getStr(ZkStateReader.CORE_NAME_PROP);
        }
        Integer numShards = message.getStr(ZkStateReader.NUM_SHARDS_PROP)!=null?Integer.parseInt(message.getStr(ZkStateReader.NUM_SHARDS_PROP)):null;
        log.info("Update state numShards={} message={}", numShards, message);
        //collection does not yet exist, create placeholders if num shards is specified
        boolean collectionExists = state.getCollections().contains(collection);
        if (!collectionExists && numShards!=null) {
          state = createCollection(state, collection, numShards);
        }
        
        // use the provided non null shardId
        String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);
        if (sliceName == null) {
          //String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);
          //get shardId from ClusterState
          sliceName = getAssignedId(state, coreNodeName, message);
          if (sliceName != null) {
            log.info("shard=" + sliceName + " is already registered");
          }
        }
        if(sliceName == null) {
          //request new shardId 
          if (collectionExists) {
            // use existing numShards
            numShards = state.getCollectionStates().get(collection).getSlices().size();
            log.info("Collection already exists with " + ZkStateReader.NUM_SHARDS_PROP + "=" + numShards);
          }
          sliceName = AssignShard.assignShard(collection, state, numShards);
          log.info("Assigning new node to shard shard=" + sliceName);
        }

        Slice slice = state.getSlice(collection, sliceName);
        Map<String,Object> replicaProps = new LinkedHashMap<String,Object>();

        replicaProps.putAll(message.getProperties());
        // System.out.println("########## UPDATE MESSAGE: " + JSONUtil.toJSON(message));
        if (slice != null) {
          Replica oldReplica = slice.getReplicasMap().get(coreNodeName);
          if (oldReplica != null && oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
            replicaProps.put(ZkStateReader.LEADER_PROP, oldReplica.get(ZkStateReader.LEADER_PROP));
          }
        }

        // we don't put num_shards in the clusterstate
          replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
          replicaProps.remove(QUEUE_OPERATION);
          
          // remove any props with null values
          Set<Entry<String,Object>> entrySet = replicaProps.entrySet();
          List<String> removeKeys = new ArrayList<String>();
          for (Entry<String,Object> entry : entrySet) {
            if (entry.getValue() == null) {
              removeKeys.add(entry.getKey());
            }
          }
          for (String removeKey : removeKeys) {
            replicaProps.remove(removeKey);
          }
          replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);


          Replica replica = new Replica(coreNodeName, replicaProps);

         // TODO: where do we get slice properties in this message?  or should there be a separate create-slice message if we want that?

          Map<String,Object> sliceProps = null;
          Map<String,Replica> replicas;

          if (slice != null) {
            sliceProps = slice.getProperties();
            replicas = slice.getReplicasCopy();
          } else {
            replicas = new HashMap<String, Replica>(1);
          }

          replicas.put(replica.getName(), replica);
          slice = new Slice(sliceName, replicas, sliceProps);

          ClusterState newClusterState = updateSlice(state, collection, slice);
          return newClusterState;
      }

    private  Map<String,Object> defaultCollectionProps() {
      HashMap<String,Object> props = new HashMap<String, Object>(2);
      props.put(DocCollection.DOC_ROUTER, DocRouter.DEFAULT_NAME);
      return props;
    }

      private ClusterState createCollection(ClusterState state, String collectionName, int numShards) {
        log.info("Create collection {} with numShards {}", collectionName, numShards);

        DocRouter router = DocRouter.DEFAULT;
        List<DocRouter.Range> ranges = router.partitionRange(numShards, router.fullRange());

        Map<String, DocCollection> newCollections = new LinkedHashMap<String,DocCollection>();


        Map<String, Slice> newSlices = new LinkedHashMap<String,Slice>();
        newCollections.putAll(state.getCollectionStates());
        for (int i = 0; i < numShards; i++) {
          final String sliceName = "shard" + (i+1);

          Map<String,Object> sliceProps = new LinkedHashMap<String,Object>(1);
          sliceProps.put(Slice.RANGE, ranges.get(i));

          newSlices.put(sliceName, new Slice(sliceName, null, sliceProps));
        }

        // TODO: fill in with collection properties read from the /collections/<collectionName> node
        Map<String,Object> collectionProps = defaultCollectionProps();

        DocCollection newCollection = new DocCollection(collectionName, newSlices, collectionProps, router);

        newCollections.put(collectionName, newCollection);
        ClusterState newClusterState = new ClusterState(state.getLiveNodes(), newCollections);
        return newClusterState;
      }

      /*
       * Return an already assigned id or null if not assigned
       */
      private String getAssignedId(final ClusterState state, final String nodeName,
          final ZkNodeProps coreState) {
        Collection<Slice> slices = state.getSlices(coreState.getStr(ZkStateReader.COLLECTION_PROP));
        if (slices != null) {
          for (Slice slice : slices) {
            if (slice.getReplicasMap().get(nodeName) != null) {
              return slice.getName();
            }
          }
        }
        return null;
      }
      
      private ClusterState updateSlice(ClusterState state, String collectionName, Slice slice) {
        // System.out.println("###!!!### OLD CLUSTERSTATE: " + JSONUtil.toJSON(state.getCollectionStates()));
        // System.out.println("Updating slice:" + slice);

        Map<String, DocCollection> newCollections = new LinkedHashMap<String,DocCollection>(state.getCollectionStates());  // make a shallow copy
        DocCollection coll = newCollections.get(collectionName);
        Map<String,Slice> slices;
        Map<String,Object> props;
        DocRouter router;

        if (coll == null) {
          //  when updateSlice is called on a collection that doesn't exist, it's currently when a core is publishing itself
          // without explicitly creating a collection.  In this current case, we assume custom sharding with an "implicit" router.
          slices = new HashMap<String, Slice>(1);
          props = new HashMap<String,Object>(1);
          props.put(DocCollection.DOC_ROUTER, ImplicitDocRouter.NAME);
          router = new ImplicitDocRouter();
        } else {
          props = coll.getProperties();
          router = coll.getRouter();
          slices = new LinkedHashMap<String, Slice>(coll.getSlicesMap()); // make a shallow copy
        }
        slices.put(slice.getName(), slice);
        DocCollection newCollection = new DocCollection(collectionName, slices, props, router);
        newCollections.put(collectionName, newCollection);

        // System.out.println("###!!!### NEW CLUSTERSTATE: " + JSONUtil.toJSON(newCollections));

        return new ClusterState(state.getLiveNodes(), newCollections);
      }
      
      private ClusterState setShardLeader(ClusterState state, String collectionName, String sliceName, String leaderUrl) {

        final Map<String, DocCollection> newCollections = new LinkedHashMap<String,DocCollection>(state.getCollectionStates());
        DocCollection coll = newCollections.get(collectionName);
        if(coll == null) {
          log.error("Could not mark shard leader for non existing collection:" + collectionName);
          return state;
        }

        Map<String, Slice> slices = coll.getSlicesMap();
        // make a shallow copy and add it to the new collection
        slices = new LinkedHashMap<String,Slice>(slices);

        Slice slice = slices.get(sliceName);
        if (slice == null) {
          log.error("Could not mark leader for non existing slice:" + sliceName);
          return state;
        } else {
          // TODO: consider just putting the leader property on the shard, not on individual replicas

          Replica oldLeader = slice.getLeader();

          final Map<String,Replica> newReplicas = new LinkedHashMap<String,Replica>();

          for (Replica replica : slice.getReplicas()) {

            // TODO: this should only be calculated once and cached somewhere?
            String coreURL = ZkCoreNodeProps.getCoreUrl(replica.getStr(ZkStateReader.BASE_URL_PROP), replica.getStr(ZkStateReader.CORE_NAME_PROP));

            if (replica == oldLeader && !coreURL.equals(leaderUrl)) {
              Map<String,Object> replicaProps = new LinkedHashMap<String,Object>(replica.getProperties());
              replicaProps.remove(Slice.LEADER);
              replica = new Replica(replica.getName(), replicaProps);
            } else if (coreURL.equals(leaderUrl)) {
              Map<String,Object> replicaProps = new LinkedHashMap<String,Object>(replica.getProperties());
              replicaProps.put(Slice.LEADER, "true");  // TODO: allow booleans instead of strings
              replica = new Replica(replica.getName(), replicaProps);
            }

            newReplicas.put(replica.getName(), replica);
          }

          Map<String,Object> newSliceProps = slice.shallowCopy();
          newSliceProps.put(Slice.REPLICAS, newReplicas);
          Slice newSlice = new Slice(slice.getName(), newReplicas, slice.getProperties());
          slices.put(newSlice.getName(), newSlice);
        }


        DocCollection newCollection = new DocCollection(coll.getName(), slices, coll.getProperties(), coll.getRouter());
        newCollections.put(collectionName, newCollection);
        return new ClusterState(state.getLiveNodes(), newCollections);
      }

      /*
       * Remove core from cloudstate
       */
      private ClusterState removeCore(final ClusterState clusterState, ZkNodeProps message) {
        
        String cnn = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
        if (cnn == null) {
          // it must be the default then
          cnn = message.getStr(ZkStateReader.NODE_NAME_PROP) + "_" + message.getStr(ZkStateReader.CORE_NAME_PROP);
        }

        final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

        final Map<String, DocCollection> newCollections = new LinkedHashMap<String,DocCollection>(clusterState.getCollectionStates()); // shallow copy
        DocCollection coll = newCollections.get(collection);
        if (coll == null) {
          // TODO: log/error that we didn't find it?
          return clusterState;
        }

        Map<String, Slice> newSlices = new LinkedHashMap<String, Slice>();
        for (Slice slice : coll.getSlices()) {
          Replica replica = slice.getReplica(cnn);
          if (replica != null) {
            Map<String, Replica> newReplicas = slice.getReplicasCopy();
            newReplicas.remove(cnn);
            // TODO TODO TODO!!! if there are no replicas left for the slice, and the slice has no hash range, remove it
            // if (newReplicas.size() == 0 && slice.getRange() == null) {
            // if there are no replicas left for the slice remove it
            if (newReplicas.size() == 0) {
              slice = null;
            } else {
              slice = new Slice(slice.getName(), newReplicas, slice.getProperties());
            }
          }

          if (slice != null) {
            newSlices.put(slice.getName(), slice);
          }
        }

        // if there are no slices left in the collection, remove it?
        if (newSlices.size() == 0) {
          newCollections.remove(coll.getName());

          // TODO: it might be better logically to have this in ZkController
          // but for tests (it's easier) it seems better for the moment to leave CoreContainer and/or
          // ZkController out of the Overseer.
          try {
            zkClient.clean("/collections/" + collection);
          } catch (InterruptedException e) {
            SolrException.log(log, "Cleaning up collection in zk was interrupted:" + collection, e);
            Thread.currentThread().interrupt();
          } catch (KeeperException e) {
            SolrException.log(log, "Problem cleaning up collection in zk:" + collection, e);
          }


        } else {
          DocCollection newCollection = new DocCollection(coll.getName(), newSlices, coll.getProperties(), coll.getRouter());
          newCollections.put(newCollection.getName(), newCollection);
        }

        ClusterState newState = new ClusterState(clusterState.getLiveNodes(), newCollections);
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
      try {
        updaterThread.close();
        updaterThread.interrupt();
      } catch (Throwable t) {
        log.error("Error closing updaterThread", t);
      }
    }
    if (ccThread != null) {
      try {
        ccThread.close();
        ccThread.interrupt();
      } catch (Throwable t) {
        log.error("Error closing ccThread", t);
      }
    }
    
    try {
      reader.close();
    } catch (Throwable t) {
      log.error("Error closing zkStateReader", t);
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
      log.error("Could not create Overseer node", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("Could not create Overseer node", e);
      throw new RuntimeException(e);
    }
  }

}
