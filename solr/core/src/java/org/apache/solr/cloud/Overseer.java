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

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.IOUtils;
import org.apache.solr.util.stats.Clock;
import org.apache.solr.util.stats.Timer;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible node assignments, cluster state file?
 */
public class Overseer implements Closeable {
  public static final String QUEUE_OPERATION = "operation";

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionParams.CollectionAction#DELETE}
   */
  @Deprecated
  public static final String REMOVECOLLECTION = "removecollection";

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionParams.CollectionAction#DELETESHARD}
   */
  @Deprecated
  public static final String REMOVESHARD = "removeshard";

  /**
   * Enum of actions supported by the overseer only.
   *
   * There are other actions supported which are public and defined
   * in {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   */
  public static enum OverseerAction {
    LEADER,
    DELETECORE,
    ADDROUTINGRULE,
    REMOVEROUTINGRULE,
    UPDATESHARDSTATE,
    STATE,
    QUIT;

    public static OverseerAction get(String p) {
      if (p != null) {
        try {
          return OverseerAction.valueOf(p.toUpperCase(Locale.ROOT));
        } catch (Exception ex) {
        }
      }
      return null;
    }

    public boolean isEqual(String s) {
      return s != null && toString().equals(s.toUpperCase(Locale.ROOT));
    }

    public String toLower() {
      return toString().toLowerCase(Locale.ROOT);
    }
  }


  public static final int STATE_UPDATE_DELAY = 1500;  // delay between cloud state updates

  private static Logger log = LoggerFactory.getLogger(Overseer.class);

  static enum LeaderStatus { DONT_KNOW, NO, YES };

  private long lastUpdatedTime = 0;

  private class ClusterStateUpdater implements Runnable, Closeable {
    
    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    //queue where everybody can throw tasks
    private final DistributedQueue stateUpdateQueue; 
    //Internal queue where overseer stores events that have not yet been published into cloudstate
    //If Overseer dies while extracting the main queue a new overseer will start from this queue 
    private final DistributedQueue workQueue;
    // Internal map which holds the information about running tasks.
    private final DistributedMap runningMap;
    // Internal map which holds the information about successfully completed tasks.
    private final DistributedMap completedMap;
    // Internal map which holds the information about failed tasks.
    private final DistributedMap failureMap;

    private final Stats zkStats;

    private Map clusterProps;
    private boolean isClosed = false;

    private final Map<String, Object> updateNodes = new LinkedHashMap<>();
    private boolean isClusterStateModified = false;


    public ClusterStateUpdater(final ZkStateReader reader, final String myId, Stats zkStats) {
      this.zkClient = reader.getZkClient();
      this.zkStats = zkStats;
      this.stateUpdateQueue = getInQueue(zkClient, zkStats);
      this.workQueue = getInternalQueue(zkClient, zkStats);
      this.failureMap = getFailureMap(zkClient);
      this.runningMap = getRunningMap(zkClient);
      this.completedMap = getCompletedMap(zkClient);
      this.myId = myId;
      this.reader = reader;
      clusterProps = reader.getClusterProps();
    }

    public Stats getStateUpdateQueueStats() {
      return stateUpdateQueue.getStats();
    }

    public Stats getWorkQueueStats()  {
      return workQueue.getStats();
    }
    
    @Override
    public void run() {

      LeaderStatus isLeader = amILeader();
      while (isLeader == LeaderStatus.DONT_KNOW) {
        log.debug("am_i_leader unclear {}", isLeader);
        isLeader = amILeader();  // not a no, not a yes, try ask again
      }
      if (!this.isClosed && LeaderStatus.YES == isLeader) {
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
              
              while (head != null) {
                isLeader = amILeader();
                if (LeaderStatus.NO == isLeader) {
                  break;
                }
                else if (LeaderStatus.YES == isLeader) {
                  final ZkNodeProps message = ZkNodeProps.load(head);
                  final String operation = message.getStr(QUEUE_OPERATION);
                  final TimerContext timerContext = stats.time(operation);
                  try {
                    clusterState = processMessage(clusterState, message, operation);
                    stats.success(operation);
                  } catch (Exception e) {
                    // generally there is nothing we can do - in most cases, we have
                    // an issue that will fail again on retry or we cannot communicate with     a
                    // ZooKeeper in which case another Overseer should take over
                    // TODO: if ordering for the message is not important, we could
                    // track retries and put it back on the end of the queue
                    log.error("Overseer could not process the current clusterstate state update message, skipping the message.", e);
                    stats.error(operation);
                  } finally {
                    timerContext.stop();
                  }
                  updateZkStates(clusterState);
                  
                  workQueue.poll(); // poll-ing removes the element we got by peek-ing
                }
                else {
                  log.info("am_i_leader unclear {}", isLeader);                  
                  // re-peek below in case our 'head' value is out-of-date by now
                }
                
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
      try {
        while (!this.isClosed) {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          }
          else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try ask again
          }
          DistributedQueue.QueueEvent head = null;
          try {
            head = stateUpdateQueue.peek(true);
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn(
                  "Solr cannot talk to ZK, exiting Overseer main queue loop", e);
              return;
            }
            log.error("Exception in Overseer main queue loop", e);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;

          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
          }
          synchronized (reader.getUpdateLock()) {
            try {
              reader.updateClusterState(true);
              ClusterState clusterState = reader.getClusterState();

              while (head != null) {
                final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
                final String operation = message.getStr(QUEUE_OPERATION);
                final TimerContext timerContext = stats.time(operation);
                try {
                  clusterState = processMessage(clusterState, message, operation);
                  stats.success(operation);
                } catch (Exception e) {
                  // generally there is nothing we can do - in most cases, we have
                  // an issue that will fail again on retry or we cannot communicate with
                  // ZooKeeper in which case another Overseer should take over
                  // TODO: if ordering for the message is not important, we could
                  // track retries and put it back on the end of the queue
                  log.error("Overseer could not process the current clusterstate state update message, skipping the message.", e);
                  stats.error(operation);
                } finally {
                  timerContext.stop();
                }
                workQueue.offer(head.getBytes());

                stateUpdateQueue.poll();

                if (isClosed || System.nanoTime() - lastUpdatedTime > TimeUnit.NANOSECONDS.convert(STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS)) break;
                if(!updateNodes.isEmpty()) break;
                // if an event comes in the next 100ms batch it together
                head = stateUpdateQueue.peek(100);
              }
              updateZkStates(clusterState);
              // clean work queue
              while (workQueue.poll() != null) ;

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

        }
      } finally {
        log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));
        new Thread("OverseerExitThread"){
          //do this in a separate thread because any wait is interrupted in this main thread
          @Override
          public void run() {
            checkIfIamStillLeader();
          }
        }.start();
      }
    }

    private void updateZkStates(ClusterState clusterState) throws KeeperException, InterruptedException {
      TimerContext timerContext = stats.time("update_state");
      boolean success = false;
      try {
        if (!updateNodes.isEmpty()) {
          for (Entry<String,Object> e : updateNodes.entrySet()) {
            if (e.getValue() == null) {
              if (zkClient.exists(e.getKey(), true)) zkClient.delete(e.getKey(), 0, true);
            } else {
              byte[] data = ZkStateReader.toJSON(e.getValue());
              if (zkClient.exists(e.getKey(), true)) {
                log.info("going to update_collection {}", e.getKey());
                zkClient.setData(e.getKey(), data, true);
              } else {
                log.info("going to create_collection {}", e.getKey());
                zkClient.create(e.getKey(), data, CreateMode.PERSISTENT, true);
              }
            }
          }
          updateNodes.clear();
        }
        
        if (isClusterStateModified) {
          lastUpdatedTime = System.nanoTime();
          zkClient.setData(ZkStateReader.CLUSTER_STATE,
              ZkStateReader.toJSON(clusterState), true);
          isClusterStateModified = false;
        }
        success = true;
      } finally {
        timerContext.stop();
        if (success)  {
          stats.success("update_state");
        } else  {
          stats.error("update_state");
        }
      }
    }

    private void checkIfIamStillLeader() {
      if (zkController != null && zkController.getCoreContainer().isShutDown()) return;//shutting down no need to go further
      org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
      String path = "/overseer_elect/leader";
      byte[] data = null;
      try {
        data = zkClient.getData(path, null, stat, true);
      } catch (Exception e) {
        log.error("could not read the data" ,e);
        return;
      }
      try {
        Map m = (Map) ZkStateReader.fromJSON(data);
        String id = (String) m.get("id");
        if(overseerCollectionProcessor.getId().equals(id)){
          try {
            log.info("I'm exiting , but I'm still the leader");
            zkClient.delete(path,stat.getVersion(),true);
          } catch (KeeperException.BadVersionException e) {
            //no problem ignore it some other Overseer has already taken over
          } catch (Exception e) {
            log.error("Could not delete my leader node ", e);
          }

        } else{
          log.info("somebody else has already taken up the overseer position");
        }
      } finally {
        //if I am not shutting down, Then I need to rejoin election
        try {
          if (zkController != null && !zkController.getCoreContainer().isShutDown()) {
            zkController.rejoinOverseerElection(null, false);
          }
        } catch (Exception e) {
          log.warn("Unable to rejoinElection ",e);
        }
      }
    }

    private ClusterState processMessage(ClusterState clusterState,
        final ZkNodeProps message, final String operation) {

      CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(operation);
      if (collectionAction != null) {
        switch (collectionAction) {
          case CREATE:
            clusterState = buildCollection(clusterState, message);
            break;
          case DELETE:
            clusterState = removeCollection(clusterState, message);
            break;
          case CREATESHARD:
            clusterState = createShard(clusterState, message);
            break;
          case DELETESHARD:
            clusterState = removeShard(clusterState, message);
            break;
          case ADDREPLICA:
            clusterState = createReplica(clusterState, message);
            break;
          case CLUSTERPROP:
            handleProp(message);
            break;
          default:
            throw new RuntimeException("unknown operation:" + operation
                + " contents:" + message.getProperties());
        }
      } else {
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction != null) {
          switch (overseerAction) {
            case STATE:
              if (isLegacy(clusterProps)) {
                clusterState = updateState(clusterState, message);
              } else {
                clusterState = updateStateNew(clusterState, message);
              }
              break;
            case LEADER:
              clusterState = setShardLeader(clusterState, message);
              break;
            case DELETECORE:
              clusterState = removeCore(clusterState, message);
              break;
            case ADDROUTINGRULE:
              clusterState = addRoutingRule(clusterState, message);
              break;
            case REMOVEROUTINGRULE:
              clusterState = removeRoutingRule(clusterState, message);
              break;
            case UPDATESHARDSTATE:
              clusterState = updateShardState(clusterState, message);
              break;
            case QUIT:
              if (myId.equals(message.get("id"))) {
                log.info("Quit command received {}", LeaderElector.getNodeName(myId));
                overseerCollectionProcessor.close();
                close();
              } else {
                log.warn("Overseer received wrong QUIT message {}", message);
              }
              break;
            default:
              throw new RuntimeException("unknown operation:" + operation
                  + " contents:" + message.getProperties());
          }
        } else  {
          // merely for back-compat where overseer action names were different from the ones
          // specified in CollectionAction. See SOLR-6115. Remove this in 5.0
          switch (operation) {
            case OverseerCollectionProcessor.CREATECOLLECTION:
              clusterState = buildCollection(clusterState, message);
              break;
            case REMOVECOLLECTION:
              clusterState = removeCollection(clusterState, message);
              break;
            case REMOVESHARD:
              clusterState = removeShard(clusterState, message);
              break;
            default:
              throw new RuntimeException("unknown operation:" + operation
                  + " contents:" + message.getProperties());
          }
        }
      }

      return clusterState;
    }

    private ClusterState setShardLeader(ClusterState clusterState, ZkNodeProps message) {
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
      return clusterState;
    }

    private void handleProp(ZkNodeProps message)  {
      String name = message.getStr("name");
      String val = message.getStr("val");
      Map m =  reader.getClusterProps();
      if(val ==null) m.remove(name);
      else m.put(name,val);

      try {
        if(reader.getZkClient().exists(ZkStateReader.CLUSTER_PROPS,true))
          reader.getZkClient().setData(ZkStateReader.CLUSTER_PROPS,ZkStateReader.toJSON(m),true);
        else
          reader.getZkClient().create(ZkStateReader.CLUSTER_PROPS, ZkStateReader.toJSON(m),CreateMode.PERSISTENT, true);
        clusterProps = reader.getClusterProps();
      } catch (Exception e) {
        log.error("Unable to set cluster property", e);

      }
    }

    private ClusterState createReplica(ClusterState clusterState, ZkNodeProps message) {
      log.info("createReplica() {} ", message);
      String coll = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);
      DocCollection collection = clusterState.getCollection(coll);
      Slice sl = collection.getSlice(slice);
      if(sl == null){
        log.error("Invalid Collection/Slice {}/{} ",coll,slice);
        return clusterState;
      }

      String coreNodeName = Assign.assignNode(coll, clusterState);
      Replica replica = new Replica(coreNodeName,
          makeMap(
          ZkStateReader.CORE_NAME_PROP, message.getStr(ZkStateReader.CORE_NAME_PROP),
          ZkStateReader.BASE_URL_PROP,message.getStr(ZkStateReader.BASE_URL_PROP),
          ZkStateReader.STATE_PROP,message.getStr(ZkStateReader.STATE_PROP)));
      sl.getReplicasMap().put(coreNodeName, replica);
      return newState(clusterState, singletonMap(coll, collection));
    }

    private ClusterState buildCollection(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr("name");
      log.info("building a new collection: " + collection);
      if(clusterState.hasCollection(collection) ){
        log.warn("Collection {} already exists. exit" ,collection);
        return clusterState;
      }

      ArrayList<String> shardNames = new ArrayList<>();

      if(ImplicitDocRouter.NAME.equals( message.getStr("router.name",DocRouter.DEFAULT_NAME))){
        getShardNames(shardNames,message.getStr("shards",DocRouter.DEFAULT_NAME));
      } else {
        int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, -1);
        if(numShards<1) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,"numShards is a required parameter for 'compositeId' router");
        getShardNames(numShards, shardNames);
      }

      return createCollection(clusterState, collection, shardNames, message);
    }

    private ClusterState updateShardState(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      log.info("Update shard state invoked for collection: " + collection + " with message: " + message);
      for (String key : message.keySet()) {
        if (ZkStateReader.COLLECTION_PROP.equals(key)) continue;
        if (QUEUE_OPERATION.equals(key)) continue;

        Slice slice = clusterState.getSlice(collection, key);
        if (slice == null)  {
          throw new RuntimeException("Overseer.updateShardState unknown collection: " + collection + " slice: " + key);
        }
        log.info("Update shard state " + key + " to " + message.getStr(key));
        Map<String, Object> props = slice.shallowCopy();
        if (Slice.RECOVERY.equals(props.get(Slice.STATE)) && Slice.ACTIVE.equals(message.getStr(key))) {
          props.remove(Slice.PARENT);
        }
        props.put(Slice.STATE, message.getStr(key));
        Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props);
        clusterState = updateSlice(clusterState, collection, newSlice);
      }

      return clusterState;
    }

    private ClusterState addRoutingRule(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
      String routeKey = message.getStr("routeKey");
      String range = message.getStr("range");
      String targetCollection = message.getStr("targetCollection");
      String targetShard = message.getStr("targetShard");
      String expireAt = message.getStr("expireAt");

      Slice slice = clusterState.getSlice(collection, shard);
      if (slice == null)  {
        throw new RuntimeException("Overseer.addRoutingRule unknown collection: " + collection + " slice:" + shard);
      }

      Map<String, RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules == null)
        routingRules = new HashMap<>();
      RoutingRule r = routingRules.get(routeKey);
      if (r == null) {
        Map<String, Object> map = new HashMap<>();
        map.put("routeRanges", range);
        map.put("targetCollection", targetCollection);
        map.put("expireAt", expireAt);
        RoutingRule rule = new RoutingRule(routeKey, map);
        routingRules.put(routeKey, rule);
      } else  {
        // add this range
        Map<String, Object> map = r.shallowCopy();
        map.put("routeRanges", map.get("routeRanges") + "," + range);
        map.put("expireAt", expireAt);
        routingRules.put(routeKey, new RoutingRule(routeKey, map));
      }

      Map<String, Object> props = slice.shallowCopy();
      props.put("routingRules", routingRules);

      Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props);
      clusterState = updateSlice(clusterState, collection, newSlice);
      return clusterState;
    }

    private boolean checkCollectionKeyExistence(ZkNodeProps message) {
      return checkKeyExistence(message, ZkStateReader.COLLECTION_PROP);
    }
    
    private boolean checkKeyExistence(ZkNodeProps message, String key) {
      String value = message.getStr(key);
      if (value == null || value.trim().length() == 0) {
        log.error("Skipping invalid Overseer message because it has no " + key + " specified: " + message);
        return false;
      }
      return true;
    }

    private ClusterState removeRoutingRule(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
      String routeKeyStr = message.getStr("routeKey");

      log.info("Overseer.removeRoutingRule invoked for collection: " + collection
          + " shard: " + shard + " routeKey: " + routeKeyStr);

      Slice slice = clusterState.getSlice(collection, shard);
      if (slice == null)  {
        log.warn("Unknown collection: " + collection + " shard: " + shard);
        return clusterState;
      }
      Map<String, RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules != null) {
        routingRules.remove(routeKeyStr); // no rules left
        Map<String, Object> props = slice.shallowCopy();
        props.put("routingRules", routingRules);
        Slice newSlice = new Slice(slice.getName(), slice.getReplicasCopy(), props);
        clusterState = updateSlice(clusterState, collection, newSlice);
      }

      return clusterState;
    }

    private ClusterState createShard(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      String shardId = message.getStr(ZkStateReader.SHARD_ID_PROP);
      Slice slice = clusterState.getSlice(collection, shardId);
      if (slice == null)  {
        Map<String, Replica> replicas = Collections.EMPTY_MAP;
        Map<String, Object> sliceProps = new HashMap<>();
        String shardRange = message.getStr(ZkStateReader.SHARD_RANGE_PROP);
        String shardState = message.getStr(ZkStateReader.SHARD_STATE_PROP);
        String shardParent = message.getStr(ZkStateReader.SHARD_PARENT_PROP);
        sliceProps.put(Slice.RANGE, shardRange);
        sliceProps.put(Slice.STATE, shardState);
        if (shardParent != null)  {
          sliceProps.put(Slice.PARENT, shardParent);
        }
        slice = new Slice(shardId, replicas, sliceProps);
        clusterState = updateSlice(clusterState, collection, slice);
      } else  {
        log.error("Unable to create Shard: " + shardId + " because it already exists in collection: " + collection);
      }
      return clusterState;
    }

    private LeaderStatus amILeader() {
      TimerContext timerContext = stats.time("am_i_leader");
      boolean success = true;
      try {
        ZkNodeProps props = ZkNodeProps.load(zkClient.getData(
            "/overseer_elect/leader", null, null, true));
        if (myId.equals(props.getStr("id"))) {
          return LeaderStatus.YES;
        }
      } catch (KeeperException e) {
        success = false;
        if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.error("", e);
          return LeaderStatus.DONT_KNOW;
        } else if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
          log.info("", e);
        } else {
          log.warn("", e);
        }
      } catch (InterruptedException e) {
        success = false;
        Thread.currentThread().interrupt();
      } finally {
        timerContext.stop();
        if (success)  {
          stats.success("am_i_leader");
        } else  {
          stats.error("am_i_leader");
        }
      }
      log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
      return LeaderStatus.NO;
    }

    private ClusterState updateStateNew(ClusterState clusterState, ZkNodeProps message) {
      String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;
      String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

      if(collection==null || sliceName == null){
        log.error("Invalid collection and slice {}", message);
        return clusterState;
      }
      Slice slice = clusterState.getSlice(collection, sliceName);
      if(slice == null){
        log.error("No such slice exists {}", message);
        return clusterState;
      }

      return updateState(clusterState, message);
    }
    
      /**
       * Try to assign core to the cluster. 
       */
      private ClusterState updateState(ClusterState clusterState, final ZkNodeProps message) {
        final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
        if (!checkCollectionKeyExistence(message)) return clusterState;
        Integer numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, null);
        log.info("Update state numShards={} message={}", numShards, message);

        List<String> shardNames  = new ArrayList<>();

        //collection does not yet exist, create placeholders if num shards is specified
        boolean collectionExists = clusterState.hasCollection(collection);
        if (!collectionExists && numShards!=null) {
          getShardNames(numShards, shardNames);
          clusterState = createCollection(clusterState, collection, shardNames, message);
        }
        String sliceName = message.getStr(ZkStateReader.SHARD_ID_PROP);

        String coreNodeName = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
        if (coreNodeName == null) {
          coreNodeName = getAssignedCoreNodeName(clusterState, message);
          if (coreNodeName != null) {
            log.info("node=" + coreNodeName + " is already registered");
          } else {
            // if coreNodeName is null, auto assign one
            coreNodeName = Assign.assignNode(collection, clusterState);
          }
          message.getProperties().put(ZkStateReader.CORE_NODE_NAME_PROP,
              coreNodeName);
        }

        // use the provided non null shardId
        if (sliceName == null) {
          //get shardId from ClusterState
          sliceName = getAssignedId(clusterState, coreNodeName, message);
          if (sliceName != null) {
            log.info("shard=" + sliceName + " is already registered");
          }
        }
        if(sliceName == null) {
          //request new shardId 
          if (collectionExists) {
            // use existing numShards
            numShards = clusterState.getCollection(collection).getSlices().size();
            log.info("Collection already exists with " + ZkStateReader.NUM_SHARDS_PROP + "=" + numShards);
          }
          sliceName = Assign.assignShard(collection, clusterState, numShards);
          log.info("Assigning new node to shard shard=" + sliceName);
        }

        Slice slice = clusterState.getSlice(collection, sliceName);

        Map<String,Object> replicaProps = new LinkedHashMap<>();

        replicaProps.putAll(message.getProperties());
        // System.out.println("########## UPDATE MESSAGE: " + JSONUtil.toJSON(message));
        if (slice != null) {
          Replica oldReplica = slice.getReplicasMap().get(coreNodeName);
          if (oldReplica != null && oldReplica.containsKey(ZkStateReader.LEADER_PROP)) {
            replicaProps.put(ZkStateReader.LEADER_PROP, oldReplica.get(ZkStateReader.LEADER_PROP));
          }
        }

        // we don't put these in the clusterstate
          replicaProps.remove(ZkStateReader.NUM_SHARDS_PROP);
          replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
          replicaProps.remove(ZkStateReader.SHARD_ID_PROP);
          replicaProps.remove(ZkStateReader.COLLECTION_PROP);
          replicaProps.remove(QUEUE_OPERATION);

          // remove any props with null values
          Set<Entry<String,Object>> entrySet = replicaProps.entrySet();
          List<String> removeKeys = new ArrayList<>();
          for (Entry<String,Object> entry : entrySet) {
            if (entry.getValue() == null) {
              removeKeys.add(entry.getKey());
            }
          }
          for (String removeKey : removeKeys) {
            replicaProps.remove(removeKey);
          }
          replicaProps.remove(ZkStateReader.CORE_NODE_NAME_PROP);
          // remove shard specific properties
          String shardRange = (String) replicaProps.remove(ZkStateReader.SHARD_RANGE_PROP);
          String shardState = (String) replicaProps.remove(ZkStateReader.SHARD_STATE_PROP);
          String shardParent = (String) replicaProps.remove(ZkStateReader.SHARD_PARENT_PROP);


          Replica replica = new Replica(coreNodeName, replicaProps);

         // TODO: where do we get slice properties in this message?  or should there be a separate create-slice message if we want that?

          Map<String,Object> sliceProps = null;
          Map<String,Replica> replicas;

          if (slice != null) {
            clusterState = checkAndCompleteShardSplit(clusterState, collection, coreNodeName, sliceName, replicaProps);
            // get the current slice again because it may have been updated due to checkAndCompleteShardSplit method
            slice = clusterState.getSlice(collection, sliceName);
            sliceProps = slice.getProperties();
            replicas = slice.getReplicasCopy();
          } else {
            replicas = new HashMap<>(1);
            sliceProps = new HashMap<>();
            sliceProps.put(Slice.RANGE, shardRange);
            sliceProps.put(Slice.STATE, shardState);
            sliceProps.put(Slice.PARENT, shardParent);
          }

          replicas.put(replica.getName(), replica);
          slice = new Slice(sliceName, replicas, sliceProps);

          ClusterState newClusterState = updateSlice(clusterState, collection, slice);
          return newClusterState;
      }



    private ClusterState checkAndCompleteShardSplit(ClusterState state, String collection, String coreNodeName, String sliceName, Map<String,Object> replicaProps) {
      Slice slice = state.getSlice(collection, sliceName);
      Map<String, Object> sliceProps = slice.getProperties();
      String sliceState = slice.getState();
      if (Slice.RECOVERY.equals(sliceState))  {
        log.info("Shard: {} is in recovery state", sliceName);
        // is this replica active?
        if (ZkStateReader.ACTIVE.equals(replicaProps.get(ZkStateReader.STATE_PROP))) {
          log.info("Shard: {} is in recovery state and coreNodeName: {} is active", sliceName, coreNodeName);
          // are all other replicas also active?
          boolean allActive = true;
          for (Entry<String, Replica> entry : slice.getReplicasMap().entrySet()) {
            if (coreNodeName.equals(entry.getKey()))  continue;
            if (!Slice.ACTIVE.equals(entry.getValue().getStr(Slice.STATE))) {
              allActive = false;
              break;
            }
          }
          if (allActive)  {
            log.info("Shard: {} - all replicas are active. Finding status of fellow sub-shards", sliceName);
            // find out about other sub shards
            Map<String, Slice> allSlicesCopy = new HashMap<>(state.getSlicesMap(collection));
            List<Slice> subShardSlices = new ArrayList<>();
            outer:
            for (Entry<String, Slice> entry : allSlicesCopy.entrySet()) {
              if (sliceName.equals(entry.getKey()))
                continue;
              Slice otherSlice = entry.getValue();
              if (Slice.RECOVERY.equals(otherSlice.getState())) {
                if (slice.getParent() != null && slice.getParent().equals(otherSlice.getParent()))  {
                  log.info("Shard: {} - Fellow sub-shard: {} found", sliceName, otherSlice.getName());
                  // this is a fellow sub shard so check if all replicas are active
                  for (Entry<String, Replica> sliceEntry : otherSlice.getReplicasMap().entrySet()) {
                    if (!ZkStateReader.ACTIVE.equals(sliceEntry.getValue().getStr(ZkStateReader.STATE_PROP)))  {
                      allActive = false;
                      break outer;
                    }
                  }
                  log.info("Shard: {} - Fellow sub-shard: {} has all replicas active", sliceName, otherSlice.getName());
                  subShardSlices.add(otherSlice);
                }
              }
            }
            if (allActive)  {
              // hurray, all sub shard replicas are active
              log.info("Shard: {} - All replicas across all fellow sub-shards are now ACTIVE. Preparing to switch shard states.", sliceName);
              String parentSliceName = (String) sliceProps.remove(Slice.PARENT);

              Map<String, Object> propMap = new HashMap<>();
              propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
              propMap.put(parentSliceName, Slice.INACTIVE);
              propMap.put(sliceName, Slice.ACTIVE);
              for (Slice subShardSlice : subShardSlices) {
                propMap.put(subShardSlice.getName(), Slice.ACTIVE);
              }
              propMap.put(ZkStateReader.COLLECTION_PROP, collection);
              ZkNodeProps m = new ZkNodeProps(propMap);
              state = updateShardState(state, m);
            }
          }
        }
      }
      return state;
    }

    private ClusterState createCollection(ClusterState state, String collectionName, List<String> shards , ZkNodeProps message) {
        log.info("Create collection {} with shards {}", collectionName, shards);

        Map<String, Object> routerSpec = DocRouter.getRouterSpec(message);
        String routerName = routerSpec.get("name") == null ? DocRouter.DEFAULT_NAME : (String) routerSpec.get("name");
        DocRouter router = DocRouter.getDocRouter(routerName);

        List<DocRouter.Range> ranges = router.partitionRange(shards.size(), router.fullRange());



        Map<String, Slice> newSlices = new LinkedHashMap<>();

        for (int i = 0; i < shards.size(); i++) {
          String sliceName = shards.get(i);

          Map<String, Object> sliceProps = new LinkedHashMap<>(1);
          sliceProps.put(Slice.RANGE, ranges == null? null: ranges.get(i));

          newSlices.put(sliceName, new Slice(sliceName, null, sliceProps));
        }

        // TODO: fill in with collection properties read from the /collections/<collectionName> node
        Map<String,Object> collectionProps = new HashMap<>();

        for (Entry<String, Object> e : OverseerCollectionProcessor.COLL_PROPS.entrySet()) {
          Object val = message.get(e.getKey());
          if(val == null){
            val = OverseerCollectionProcessor.COLL_PROPS.get(e.getKey());
          }
          if(val != null) collectionProps.put(e.getKey(),val);
        }
        collectionProps.put(DocCollection.DOC_ROUTER, routerSpec);

      if (message.getStr("fromApi") == null) {
        collectionProps.put("autoCreated", "true");
      }
      
      String znode = message.getInt(DocCollection.STATE_FORMAT, 1) == 1 ? null
          : ZkStateReader.getCollectionPath(collectionName);
      
      DocCollection newCollection = new DocCollection(collectionName,
          newSlices, collectionProps, router, -1, znode);
      
      isClusterStateModified = true;
      
      log.info("state version {} {}", collectionName, newCollection.getStateFormat());
      
      return newState(state, singletonMap(newCollection.getName(), newCollection));
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
      
      private String getAssignedCoreNodeName(ClusterState state, ZkNodeProps message) {
        Collection<Slice> slices = state.getSlices(message.getStr(ZkStateReader.COLLECTION_PROP));
        if (slices != null) {
          for (Slice slice : slices) {
            for (Replica replica : slice.getReplicas()) {
              String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
              String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
              
              String msgNodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);
              String msgCore = message.getStr(ZkStateReader.CORE_NAME_PROP);
              
              if (nodeName.equals(msgNodeName) && core.equals(msgCore)) {
                return replica.getName();
              }
            }
          }
        }
        return null;
      }

      private ClusterState updateSlice(ClusterState state, String collectionName, Slice slice) {
        // System.out.println("###!!!### OLD CLUSTERSTATE: " + JSONUtil.toJSON(state.getCollectionStates()));
        // System.out.println("Updating slice:" + slice);
        DocCollection newCollection = null;
        DocCollection coll = state.getCollectionOrNull(collectionName) ;
        Map<String,Slice> slices;
        
        if (coll == null) {
          //  when updateSlice is called on a collection that doesn't exist, it's currently when a core is publishing itself
          // without explicitly creating a collection.  In this current case, we assume custom sharding with an "implicit" router.
          slices = new LinkedHashMap<>(1);
          slices.put(slice.getName(), slice);
          Map<String,Object> props = new HashMap<>(1);
          props.put(DocCollection.DOC_ROUTER, ZkNodeProps.makeMap("name",ImplicitDocRouter.NAME));
          newCollection = new DocCollection(collectionName, slices, props, new ImplicitDocRouter());
        } else {
          slices = new LinkedHashMap<>(coll.getSlicesMap()); // make a shallow copy
          slices.put(slice.getName(), slice);
          newCollection = coll.copyWithSlices(slices);
        }


        // System.out.println("###!!!### NEW CLUSTERSTATE: " + JSONUtil.toJSON(newCollections));

        return newState(state, singletonMap(collectionName, newCollection));
      }
      
      private ClusterState setShardLeader(ClusterState state, String collectionName, String sliceName, String leaderUrl) {
        DocCollection coll = state.getCollectionOrNull(collectionName);

        if(coll == null) {
          log.error("Could not mark shard leader for non existing collection:" + collectionName);
          return state;
        }

        Map<String, Slice> slices = coll.getSlicesMap();
        // make a shallow copy and add it to the new collection
        slices = new LinkedHashMap<>(slices);

        Slice slice = slices.get(sliceName);
        if (slice == null) {
          slice = coll.getSlice(sliceName);
        }

        if (slice == null) {
          log.error("Could not mark leader for non existing/active slice:" + sliceName);
          return state;
        } else {
          // TODO: consider just putting the leader property on the shard, not on individual replicas

          Replica oldLeader = slice.getLeader();

          final Map<String,Replica> newReplicas = new LinkedHashMap<>();

          for (Replica replica : slice.getReplicas()) {

            // TODO: this should only be calculated once and cached somewhere?
            String coreURL = ZkCoreNodeProps.getCoreUrl(replica.getStr(ZkStateReader.BASE_URL_PROP), replica.getStr(ZkStateReader.CORE_NAME_PROP));

            if (replica == oldLeader && !coreURL.equals(leaderUrl)) {
              Map<String,Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
              replicaProps.remove(Slice.LEADER);
              replica = new Replica(replica.getName(), replicaProps);
            } else if (coreURL.equals(leaderUrl)) {
              Map<String,Object> replicaProps = new LinkedHashMap<>(replica.getProperties());
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


        DocCollection newCollection = coll.copyWithSlices(slices);
        return newState(state, singletonMap(collectionName, newCollection));
      }

    private ClusterState newState(ClusterState state, Map<String, DocCollection> colls) {
      for (Entry<String, DocCollection> e : colls.entrySet()) {
        DocCollection c = e.getValue();
        if (c == null) {
          isClusterStateModified = true;
          state = state.copyWith(singletonMap(e.getKey(), (DocCollection) null));
          updateNodes.put(ZkStateReader.getCollectionPath(e.getKey()) ,null);
          continue;
        }

        if (c.getStateFormat() > 1) {
          updateNodes.put(ZkStateReader.getCollectionPath(c.getName()),
              new ClusterState(-1, Collections.<String>emptySet(), singletonMap(c.getName(), c)));
        } else {
          isClusterStateModified = true;
        }
        state = state.copyWith(singletonMap(e.getKey(), c));

      }
      return state;
    }

    /*
     * Remove collection from cloudstate
     */
    private ClusterState removeCollection(final ClusterState clusterState, ZkNodeProps message) {
      final String collection = message.getStr("name");
      if (!checkKeyExistence(message, "name")) return clusterState;
      DocCollection coll = clusterState.getCollectionOrNull(collection);
      if(coll == null) return  clusterState;

      isClusterStateModified = true;
      if (coll.getStateFormat() > 1) {
        try {
          log.info("Deleting state for collection : {}", collection);
          zkClient.delete(ZkStateReader.getCollectionPath(collection), -1, true);
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unable to remove collection state :" + collection);
        }
      }
      return newState(clusterState, singletonMap(coll.getName(),(DocCollection) null));
    }
    /*
     * Remove collection slice from cloudstate
     */
    private ClusterState removeShard(final ClusterState clusterState, ZkNodeProps message) {
      final String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
      final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
      if (!checkCollectionKeyExistence(message)) return clusterState;

      log.info("Removing collection: " + collection + " shard: " + sliceId + " from clusterstate");

      DocCollection coll = clusterState.getCollection(collection);

      Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlicesMap());
      newSlices.remove(sliceId);

      DocCollection newCollection = coll.copyWithSlices(newSlices);
      return newState(clusterState, singletonMap(collection,newCollection));
    }

    /*
       * Remove core from cloudstate
       */
      private ClusterState removeCore(final ClusterState clusterState, ZkNodeProps message) {
        final String cnn = message.getStr(ZkStateReader.CORE_NODE_NAME_PROP);
        final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
        if (!checkCollectionKeyExistence(message)) return clusterState;

        DocCollection coll = clusterState.getCollectionOrNull(collection) ;
        if (coll == null) {
          // TODO: log/error that we didn't find it?
          // just in case, remove the zk collection node
          try {
            zkClient.clean("/collections/" + collection);
          } catch (InterruptedException e) {
            SolrException.log(log, "Cleaning up collection in zk was interrupted:" + collection, e);
            Thread.currentThread().interrupt();
          } catch (KeeperException e) {
            SolrException.log(log, "Problem cleaning up collection in zk:" + collection, e);
          }
          return clusterState;
        }

        Map<String, Slice> newSlices = new LinkedHashMap<>();
        boolean lastSlice = false;
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
              lastSlice = true;
            } else {
              slice = new Slice(slice.getName(), newReplicas, slice.getProperties());
            }
          }

          if (slice != null) {
            newSlices.put(slice.getName(), slice);
          }
        }

        if (lastSlice) {
          // remove all empty pre allocated slices
          for (Slice slice : coll.getSlices()) {
            if (slice.getReplicas().size() == 0) {
              newSlices.remove(slice.getName());
            }
          }
        }

        // if there are no slices left in the collection, remove it?
        if (newSlices.size() == 0) {

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
          return newState(clusterState,singletonMap(collection, (DocCollection) null));

        } else {
          DocCollection newCollection = coll.copyWithSlices(newSlices);
          return newState(clusterState,singletonMap(collection,newCollection));
        }

     }

      @Override
      public void close() {
        this.isClosed = true;
      }

  }

  static void getShardNames(Integer numShards, List<String> shardNames) {
    if(numShards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "numShards" + " is a required param");
    for (int i = 0; i < numShards; i++) {
      final String sliceName = "shard" + (i + 1);
      shardNames.add(sliceName);
    }

  }

  static void getShardNames(List<String> shardNames, String shards) {
    if(shards ==null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
    for (String s : shards.split(",")) {
      if(s ==null || s.trim().isEmpty()) continue;
      shardNames.add(s.trim());
    }
    if(shardNames.isEmpty())
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");

  }

  class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private Closeable thread;

    public OverseerThread(ThreadGroup tg, Closeable thread) {
      super(tg, (Runnable) thread);
      this.thread = thread;
    }

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void close() throws IOException {
      thread.close();
      this.isClosed = true;
    }

    public boolean isClosed() {
      return this.isClosed;
    }
    
  }
  
  private OverseerThread ccThread;

  private OverseerThread updaterThread;
  
  private OverseerThread arfoThread;

  private final ZkStateReader reader;

  private final ShardHandler shardHandler;
  
  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private OverseerCollectionProcessor overseerCollectionProcessor;

  private ZkController zkController;

  private Stats stats;
  private String id;
  private boolean closed;
  private ConfigSolr config;

  // overseer not responsible for closing reader
  public Overseer(ShardHandler shardHandler,
      UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, ConfigSolr config)
      throws KeeperException, InterruptedException {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;
  }
  
  public synchronized void start(String id) {
    this.id = id;
    closed = false;
    doClose();
    log.info("Overseer (id=" + id + ") starting");
    createOverseerNode(reader.getZkClient());
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id, stats), "OverseerStateUpdate-" + id);
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

    overseerCollectionProcessor = new OverseerCollectionProcessor(reader, id, shardHandler, adminPath, stats);
    ccThread = new OverseerThread(ccTg, overseerCollectionProcessor, "OverseerCollectionProcessor-" + id);
    ccThread.setDaemon(true);
    
    ThreadGroup ohcfTg = new ThreadGroup("Overseer Hdfs SolrCore Failover Thread.");

    OverseerAutoReplicaFailoverThread autoReplicaFailoverThread = new OverseerAutoReplicaFailoverThread(config, reader, updateShardHandler);
    arfoThread = new OverseerThread(ohcfTg, autoReplicaFailoverThread, "OverseerHdfsCoreFailoverThread-" + id);
    arfoThread.setDaemon(true);
    
    updaterThread.start();
    ccThread.start();
    arfoThread.start();
  }
  
  
  /**
   * For tests.
   * 
   * @lucene.internal
   * @return state updater thread
   */
  public synchronized OverseerThread getUpdaterThread() {
    return updaterThread;
  }
  
  public synchronized void close() {
    if (closed) return;
    log.info("Overseer (id=" + id + ") closing");
    
    doClose();
    this.closed = true;
  }

  private void doClose() {
    
    if (updaterThread != null) {
      IOUtils.closeQuietly(updaterThread);
      updaterThread.interrupt();
    }
    if (ccThread != null) {
      IOUtils.closeQuietly(ccThread);
      ccThread.interrupt();
    }
    if (arfoThread != null) {
      IOUtils.closeQuietly(arfoThread);
      arfoThread.interrupt();
    }
    
    updaterThread = null;
    ccThread = null;
    arfoThread = null;
  }

  /**
   * Get queue that can be used to send messages to Overseer.
   */
  public static DistributedQueue getInQueue(final SolrZkClient zkClient) {
    return getInQueue(zkClient, new Stats());
  }

  static DistributedQueue getInQueue(final SolrZkClient zkClient, Stats zkStats)  {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue", zkStats);
  }

  /* Internal queue, not to be used outside of Overseer */
  static DistributedQueue getInternalQueue(final SolrZkClient zkClient, Stats zkStats) {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/queue-work", zkStats);
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/collection-map-running", null);
  }

  /* Internal map for successfully completed tasks, not to be used outside of the Overseer */
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/collection-map-completed", null);
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new DistributedMap(zkClient, "/overseer/collection-map-failure", null);
  }
  
  /* Collection creation queue */
  static DistributedQueue getCollectionQueue(final SolrZkClient zkClient) {
    return getCollectionQueue(zkClient, new Stats());
  }

  static DistributedQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats)  {
    createOverseerNode(zkClient);
    return new DistributedQueue(zkClient, "/overseer/collection-queue-work", zkStats);
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
  public static boolean isLegacy(Map clusterProps) {
    return !"false".equals(clusterProps.get(ZkStateReader.LEGACY_CLOUD));
  }

  public ZkStateReader getZkStateReader() {
    return reader;
  }

  /**
   * Used to hold statistics about overseer operations. It will be exposed
   * to the OverseerCollectionProcessor to return statistics.
   *
   * This is experimental API and subject to change.
   */
  public static class Stats {
    static final int MAX_STORED_FAILURES = 10;

    final Map<String, Stat> stats = Collections.synchronizedMap(new HashMap<String, Stat>());

    public Map<String, Stat> getStats() {
      return stats;
    }

    public int getSuccessCount(String operation) {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      return stat == null ? 0 : stat.success.get();
    }

    public int getErrorCount(String operation)  {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      return stat == null ? 0 : stat.errors.get();
    }

    public void success(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      synchronized (stats) {
        Stat stat = stats.get(op);
        if (stat == null) {
          stat = new Stat();
          stats.put(op, stat);
        }
        stat.success.incrementAndGet();
      }
    }

    public void error(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      synchronized (stats) {
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      stat.errors.incrementAndGet();
    }
    }

    public TimerContext time(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat;
      synchronized (stats) {
        stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      }
      return stat.requestTime.time();
    }

    public void storeFailureDetails(String operation, ZkNodeProps request, SolrResponse resp) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat ;
      synchronized (stats) {
        stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      LinkedList<FailedOp> failedOps = stat.failureDetails;
      synchronized (failedOps)  {
        if (failedOps.size() >= MAX_STORED_FAILURES)  {
          failedOps.removeFirst();
        }
        failedOps.addLast(new FailedOp(request, resp));
      }
    }
    }

    public List<FailedOp> getFailureDetails(String operation) {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      if (stat == null || stat.failureDetails.isEmpty()) return null;
      LinkedList<FailedOp> failedOps = stat.failureDetails;
      synchronized (failedOps)  {
        ArrayList<FailedOp> ret = new ArrayList<>(failedOps);
        return ret;
      }
    }
  }

  public static class Stat  {
    public final AtomicInteger success;
    public final AtomicInteger errors;
    public final Timer requestTime;
    public final LinkedList<FailedOp> failureDetails;

    public Stat() {
      this.success = new AtomicInteger();
      this.errors = new AtomicInteger();
      this.requestTime = new Timer(TimeUnit.MILLISECONDS, TimeUnit.MINUTES, Clock.defaultClock());
      this.failureDetails = new LinkedList<>();
    }
  }

  public static class FailedOp  {
    public final ZkNodeProps req;
    public final SolrResponse resp;

    public FailedOp(ZkNodeProps req, SolrResponse resp) {
      this.req = req;
      this.resp = resp;
    }
  }
}
