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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.stats.Clock;
import org.apache.solr.util.stats.Timer;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionProcessor.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARD_UNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;

/**
 * Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.
 */
public class Overseer implements Closeable {
  public static final String QUEUE_OPERATION = "operation";

  public static final int STATE_UPDATE_DELAY = 1500;  // delay between cloud state updates

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  
  private static Logger log = LoggerFactory.getLogger(Overseer.class);

  enum LeaderStatus {DONT_KNOW, NO, YES}

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

    private boolean isClosed = false;

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

      log.info("Starting to work on the main queue");
      try {
        ZkStateWriter zkStateWriter = new ZkStateWriter(reader, stats);
        ClusterState clusterState = null;
        boolean refreshClusterState = true; // let's refresh in the first iteration
        while (!this.isClosed) {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          }
          else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try ask again
          }

          if (refreshClusterState) {
            try {
              reader.updateClusterState(true);
              clusterState = reader.getClusterState();
              refreshClusterState = false;

              // if there were any errors while processing
              // the state queue, items would have been left in the
              // work queue so let's process those first
              byte[] data = workQueue.peek();
              boolean hadWorkItems = data != null;
              while (data != null)  {
                final ZkNodeProps message = ZkNodeProps.load(data);
                log.info("processMessage: workQueueSize: {}, message = {}", workQueue.getStats().getQueueLength(), message);
                // force flush to ZK after each message because there is no fallback if workQueue items
                // are removed from workQueue but fail to be written to ZK
                clusterState = processQueueItem(message, clusterState, zkStateWriter, false, null);
                workQueue.poll(); // poll-ing removes the element we got by peek-ing
                data = workQueue.peek();
              }
              // force flush at the end of the loop
              if (hadWorkItems) {
                clusterState = zkStateWriter.writePendingUpdates();
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
          try {
            while (head != null) {
              final byte[] data = head.getBytes();
              final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
              log.info("processMessage: queueSize: {}, message = {} current state version: {}", stateUpdateQueue.getStats().getQueueLength(), message, clusterState.getZkClusterStateVersion());
              // we can batch here because workQueue is our fallback in case a ZK write failed
              clusterState = processQueueItem(message, clusterState, zkStateWriter, true, new ZkStateWriter.ZkWriteCallback() {
                @Override
                public void onEnqueue() throws Exception {
                  workQueue.offer(data);
                }

                @Override
                public void onWrite() throws Exception {
                  // remove everything from workQueue
                  while (workQueue.poll() != null);
                }
              });

              // it is safer to keep this poll here because an invalid message might never be queued
              // and therefore we can't rely on the ZkWriteCallback to remove the item
              stateUpdateQueue.poll();

              if (isClosed) break;
              // if an event comes in the next 100ms batch it together
              head = stateUpdateQueue.peek(100);
            }
            // we should force write all pending updates because the next iteration might sleep until there
            // are more items in the main queue
            clusterState = zkStateWriter.writePendingUpdates();
            // clean work queue
            while (workQueue.poll() != null);

          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
              log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
              return;
            }
            log.error("Exception in Overseer main queue loop", e);
            refreshClusterState = true; // it might have been a bad version error
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
            refreshClusterState = true; // it might have been a bad version error
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

    private ClusterState processQueueItem(ZkNodeProps message, ClusterState clusterState, ZkStateWriter zkStateWriter, boolean enableBatching, ZkStateWriter.ZkWriteCallback callback) throws Exception {
      final String operation = message.getStr(QUEUE_OPERATION);
      ZkWriteCommand zkWriteCommand = null;
      final TimerContext timerContext = stats.time(operation);
      try {
        zkWriteCommand = processMessage(clusterState, message, operation);
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
      if (zkWriteCommand != null) {
        clusterState = zkStateWriter.enqueueUpdate(clusterState, zkWriteCommand, callback);
        if (!enableBatching)  {
          clusterState = zkStateWriter.writePendingUpdates();
        }
      }
      return clusterState;
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
        Map m = (Map) Utils.fromJSON(data);
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

    private ZkWriteCommand processMessage(ClusterState clusterState,
        final ZkNodeProps message, final String operation) {
      CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(operation);
      if (collectionAction != null) {
        switch (collectionAction) {
          case CREATE:
            return new ClusterStateMutator(getZkStateReader()).createCollection(clusterState, message);
          case DELETE:
            return new ClusterStateMutator(getZkStateReader()).deleteCollection(clusterState, message);
          case CREATESHARD:
            return new CollectionMutator(getZkStateReader()).createShard(clusterState, message);
          case DELETESHARD:
            return new CollectionMutator(getZkStateReader()).deleteShard(clusterState, message);
          case ADDREPLICA:
            return new SliceMutator(getZkStateReader()).addReplica(clusterState, message);
          case ADDREPLICAPROP:
            return new ReplicaMutator(getZkStateReader()).addReplicaProperty(clusterState, message);
          case DELETEREPLICAPROP:
            return new ReplicaMutator(getZkStateReader()).deleteReplicaProperty(clusterState, message);
          case BALANCESHARDUNIQUE:
            ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
            if (dProp.balanceProperty()) {
              String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
              return new ZkWriteCommand(collName, dProp.getDocCollection());
            }
            break;
          case MODIFYCOLLECTION:
            CollectionsHandler.verifyRuleParams(zkController.getCoreContainer() ,message.getProperties());
            return new CollectionMutator(reader).modifyCollection(clusterState,message);
          default:
            throw new RuntimeException("unknown operation:" + operation
                + " contents:" + message.getProperties());
        }
      } else {
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction == null) {
          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
        switch (overseerAction) {
          case STATE:
            return new ReplicaMutator(getZkStateReader()).setState(clusterState, message);
          case LEADER:
            return new SliceMutator(getZkStateReader()).setShardLeader(clusterState, message);
          case DELETECORE:
            return new SliceMutator(getZkStateReader()).removeReplica(clusterState, message);
          case ADDROUTINGRULE:
            return new SliceMutator(getZkStateReader()).addRoutingRule(clusterState, message);
          case REMOVEROUTINGRULE:
            return new SliceMutator(getZkStateReader()).removeRoutingRule(clusterState, message);
          case UPDATESHARDSTATE:
            return new SliceMutator(getZkStateReader()).updateShardState(clusterState, message);
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
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
      }

      return ZkStateWriter.NO_OP;
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

    @Override
      public void close() {
        this.isClosed = true;
      }

  }
  // Class to encapsulate processing replica properties that have at most one replica hosting a property per slice.
  private class ExclusiveSliceProperty {
    private ClusterState clusterState;
    private final boolean onlyActiveNodes;
    private final String property;
    private final DocCollection collection;
    private final String collectionName;

    // Key structure. For each node, list all replicas on it regardless of whether they have the property or not.
    private final Map<String, List<SliceReplica>> nodesHostingReplicas = new HashMap<>();
    // Key structure. For each node, a list of the replicas _currently_ hosting the property.
    private final Map<String, List<SliceReplica>> nodesHostingProp = new HashMap<>();
    Set<String> shardsNeedingHosts = new HashSet<String>();
    Map<String, Slice> changedSlices = new HashMap<>(); // Work on copies rather than the underlying cluster state.

    private int origMaxPropPerNode = 0;
    private int origModulo = 0;
    private int tmpMaxPropPerNode = 0;
    private int tmpModulo = 0;
    Random rand = new Random();

    private int assigned = 0;

    ExclusiveSliceProperty(ClusterState clusterState, ZkNodeProps message) {
      this.clusterState = clusterState;
      String tmp = message.getStr(ZkStateReader.PROPERTY_PROP);
      if (StringUtils.startsWith(tmp, OverseerCollectionProcessor.COLL_PROP_PREFIX) == false) {
        tmp = OverseerCollectionProcessor.COLL_PROP_PREFIX + tmp;
      }
      this.property = tmp.toLowerCase(Locale.ROOT);
      collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);

      if (StringUtils.isBlank(collectionName) || StringUtils.isBlank(property)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Overseer '" + message.getStr(QUEUE_OPERATION) + "'  requires both the '" + ZkStateReader.COLLECTION_PROP + "' and '" +
                ZkStateReader.PROPERTY_PROP + "' parameters. No action taken ");
      }

      Boolean shardUnique = Boolean.parseBoolean(message.getStr(SHARD_UNIQUE));
      if (shardUnique == false &&
          SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(this.property) == false) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Balancing properties amongst replicas in a slice requires that"
            + " the property be a pre-defined property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true' " +
            " Property: " + this.property + " shardUnique: " + Boolean.toString(shardUnique));
      }

      collection = clusterState.getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Could not find collection ' " + collectionName + "' for overseer operation '" +
                message.getStr(QUEUE_OPERATION) + "'. No action taken.");
      }
      onlyActiveNodes = Boolean.parseBoolean(message.getStr(ONLY_ACTIVE_NODES, "true"));
    }


    private DocCollection getDocCollection() {
      return collection;
    }

    private boolean isActive(Replica replica) {
      return replica.getState() == Replica.State.ACTIVE;
    }

    // Collect a list of all the nodes that _can_ host the indicated property. Along the way, also collect any of
    // the replicas on that node that _already_ host the property as well as any slices that do _not_ have the
    // property hosted.
    //
    // Return true if anything node needs it's property reassigned. False if the property is already balanced for
    // the collection.

    private boolean collectCurrentPropStats() {
      int maxAssigned = 0;
      // Get a list of potential replicas that can host the property _and_ their counts
      // Move any obvious entries to a list of replicas to change the property on
      Set<String> allHosts = new HashSet<>();
      for (Slice slice : collection.getSlices()) {
        boolean sliceHasProp = false;
        for (Replica replica : slice.getReplicas()) {
          if (onlyActiveNodes && isActive(replica) == false) {
            if (StringUtils.isNotBlank(replica.getStr(property))) {
              removeProp(slice, replica.getName()); // Note, we won't be committing this to ZK until later.
            }
            continue;
          }
          allHosts.add(replica.getNodeName());
          String nodeName = replica.getNodeName();
          if (StringUtils.isNotBlank(replica.getStr(property))) {
            if (sliceHasProp) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "'" + BALANCESHARDUNIQUE + "' should only be called for properties that have at most one member " +
                      "in any slice with the property set. No action taken.");
            }
            if (nodesHostingProp.containsKey(nodeName) == false) {
              nodesHostingProp.put(nodeName, new ArrayList<SliceReplica>());
            }
            nodesHostingProp.get(nodeName).add(new SliceReplica(slice, replica));
            ++assigned;
            maxAssigned = Math.max(maxAssigned, nodesHostingProp.get(nodeName).size());
            sliceHasProp = true;
          }
          if (nodesHostingReplicas.containsKey(nodeName) == false) {
            nodesHostingReplicas.put(nodeName, new ArrayList<SliceReplica>());
          }
          nodesHostingReplicas.get(nodeName).add(new SliceReplica(slice, replica));
        }
      }

      // If the total number of already-hosted properties assigned to nodes
      // that have potential to host leaders is equal to the slice count _AND_ none of the current nodes has more than
      // the max number of properties, there's nothing to do.
      origMaxPropPerNode = collection.getSlices().size() / allHosts.size();

      // Some nodes can have one more of the proeprty if the numbers aren't exactly even.
      origModulo = collection.getSlices().size() % allHosts.size();
      if (origModulo > 0) {
        origMaxPropPerNode++;  // have to have some nodes with 1 more property.
      }

      // We can say for sure that we need to rebalance if we don't have as many assigned properties as slices.
      if (assigned != collection.getSlices().size()) {
        return true;
      }

      // Make sure there are no more slices at the limit than the "leftovers"
      // Let's say there's 7 slices and 3 nodes. We need to distribute the property as 3 on node1, 2 on node2 and 2 on node3
      // (3, 2, 2) We need to be careful to not distribute them as 3, 3, 1. that's what this check is all about.
      int counter = origModulo;
      for (List<SliceReplica> list : nodesHostingProp.values()) {
        if (list.size() == origMaxPropPerNode) --counter;
      }
      if (counter == 0) return false; // nodes with 1 extra leader are exactly the needed number

      return true;
    }

    private void removeSliceAlreadyHostedFromPossibles(String sliceName) {
      for (Map.Entry<String, List<SliceReplica>> entReplica : nodesHostingReplicas.entrySet()) {

        ListIterator<SliceReplica> iter = entReplica.getValue().listIterator();
        while (iter.hasNext()) {
          SliceReplica sr = iter.next();
          if (sr.slice.getName().equals(sliceName))
            iter.remove();
        }
      }
    }

    private void balanceUnassignedReplicas() {
      tmpMaxPropPerNode = origMaxPropPerNode; // A bit clumsy, but don't want to duplicate code.
      tmpModulo = origModulo;

      // Get the nodeName and shardName for the node that has the least room for this

      while (shardsNeedingHosts.size() > 0) {
        String nodeName = "";
        int minSize = Integer.MAX_VALUE;
        SliceReplica srToChange = null;
        for (String slice : shardsNeedingHosts) {
          for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingReplicas.entrySet()) {
            // A little tricky. If we don't set this to something below, then it means all possible places to
            // put this property are full up, so just put it somewhere.
            if (srToChange == null && ent.getValue().size() > 0) {
              srToChange = ent.getValue().get(0);
            }
            ListIterator<SliceReplica> iter = ent.getValue().listIterator();
            while (iter.hasNext()) {
              SliceReplica sr = iter.next();
              if (StringUtils.equals(slice, sr.slice.getName()) == false) {
                continue;
              }
              if (nodesHostingProp.containsKey(ent.getKey()) == false) {
                nodesHostingProp.put(ent.getKey(), new ArrayList<SliceReplica>());
              }
              if (minSize > nodesHostingReplicas.get(ent.getKey()).size() && nodesHostingProp.get(ent.getKey()).size() < tmpMaxPropPerNode) {
                minSize = nodesHostingReplicas.get(ent.getKey()).size();
                srToChange = sr;
                nodeName = ent.getKey();
              }
            }
          }
        }
        // Now, you have a slice and node to put it on
        shardsNeedingHosts.remove(srToChange.slice.getName());
        if (nodesHostingProp.containsKey(nodeName) == false) {
          nodesHostingProp.put(nodeName, new ArrayList<SliceReplica>());
        }
        nodesHostingProp.get(nodeName).add(srToChange);
        adjustLimits(nodesHostingProp.get(nodeName));
        removeSliceAlreadyHostedFromPossibles(srToChange.slice.getName());
        addProp(srToChange.slice, srToChange.replica.getName());
      }
    }

    // Adjust the min/max counts per allowed per node. Special handling here for dealing with the fact
    // that no node should have more than 1 more replica with this property than any other.
    private void adjustLimits(List<SliceReplica> changeList) {
      if (changeList.size() == tmpMaxPropPerNode) {
        if (tmpModulo < 0) return;

        --tmpModulo;
        if (tmpModulo == 0) {
          --tmpMaxPropPerNode;
          --tmpModulo;  // Prevent dropping tmpMaxPropPerNode again.
        }
      }
    }

    // Go through the list of presently-hosted proeprties and remove any that have too many replicas that host the property
    private void removeOverallocatedReplicas() {
      tmpMaxPropPerNode = origMaxPropPerNode; // A bit clumsy, but don't want to duplicate code.
      tmpModulo = origModulo;

      for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingProp.entrySet()) {
        while (ent.getValue().size() > tmpMaxPropPerNode) { // remove delta nodes
          ent.getValue().remove(rand.nextInt(ent.getValue().size()));
        }
        adjustLimits(ent.getValue());
      }
    }

    private void removeProp(Slice origSlice, String replicaName) {
      getReplicaFromChanged(origSlice, replicaName).getProperties().remove(property);
    }

    private void addProp(Slice origSlice, String replicaName) {
      getReplicaFromChanged(origSlice, replicaName).getProperties().put(property, "true");
    }

    // Just a place to encapsulate the fact that we need to have new slices (copy) to update before we
    // put this all in the cluster state.
    private Replica getReplicaFromChanged(Slice origSlice, String replicaName) {
      Slice newSlice = changedSlices.get(origSlice.getName());
      Replica replica;
      if (newSlice != null) {
        replica = newSlice.getReplica(replicaName);
      } else {
        newSlice = new Slice(origSlice.getName(), origSlice.getReplicasCopy(), origSlice.shallowCopy());
        changedSlices.put(origSlice.getName(), newSlice);
        replica = newSlice.getReplica(replicaName);
      }
      if (replica == null) {
        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Should have been able to find replica '" +
            replicaName + "' in slice '" + origSlice.getName() + "'. No action taken");
      }
      return replica;

    }
    // Main entry point for carrying out the action. Returns "true" if we have actually moved properties around.

    private boolean balanceProperty() {
      if (collectCurrentPropStats() == false) {
        return false;
      }

      // we have two lists based on nodeName
      // 1> all the nodes that _could_ host a property for the slice
      // 2> all the nodes that _currently_ host a property for the slice.

      // So, remove a replica from the nodes that have too many
      removeOverallocatedReplicas();

      // prune replicas belonging to a slice that have the property currently assigned from the list of replicas
      // that could host the property.
      for (Map.Entry<String, List<SliceReplica>> entProp : nodesHostingProp.entrySet()) {
        for (SliceReplica srHosting : entProp.getValue()) {
          removeSliceAlreadyHostedFromPossibles(srHosting.slice.getName());
        }
      }

      // Assemble the list of slices that do not have any replica hosting the property:
      for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingReplicas.entrySet()) {
        ListIterator<SliceReplica> iter = ent.getValue().listIterator();
        while (iter.hasNext()) {
          SliceReplica sr = iter.next();
          shardsNeedingHosts.add(sr.slice.getName());
        }
      }

      // At this point, nodesHostingProp should contain _only_ lists of replicas that belong to slices that do _not_
      // have any replica hosting the property. So let's assign them.

      balanceUnassignedReplicas();
      for (Slice newSlice : changedSlices.values()) {
        DocCollection docCollection = CollectionMutator.updateSlice(collectionName, clusterState.getCollection(collectionName), newSlice);
        clusterState = ClusterStateMutator.newState(clusterState, collectionName, docCollection);
      }
      return true;
    }
  }

  private class SliceReplica {
    private Slice slice;
    private Replica replica;

    SliceReplica(Slice slice, Replica replica) {
      this.slice = slice;
      this.replica = replica;
    }
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
  private CloudConfig config;

  // overseer not responsible for closing reader
  public Overseer(ShardHandler shardHandler,
      UpdateShardHandler updateShardHandler, String adminPath,
      final ZkStateReader reader, ZkController zkController, CloudConfig config)
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
    stats = new Stats();
    log.info("Overseer (id=" + id + ") starting");
    createOverseerNode(reader.getZkClient());
    //launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    updaterThread = new OverseerThread(tg, new ClusterStateUpdater(reader, id, stats), "OverseerStateUpdate-" + id);
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

    overseerCollectionProcessor = new OverseerCollectionProcessor(reader, id, shardHandler, adminPath, stats, Overseer.this);
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

  public Stats getStats() {
    return stats;
  }

  ZkController getZkController(){
    return zkController;
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

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-completed", null, NUM_RESPONSES_TO_STORE);
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    createOverseerNode(zkClient);
    return new SizeLimitedDistributedMap(zkClient, "/overseer/collection-map-failure", null, NUM_RESPONSES_TO_STORE);
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

    final Map<String, Stat> stats = new ConcurrentHashMap<>();
    private volatile int queueLength;

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
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      stat.success.incrementAndGet();
    }

    public void error(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      stat.errors.incrementAndGet();
    }

    public TimerContext time(String operation) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
      if (stat == null) {
        stat = new Stat();
        stats.put(op, stat);
      }
      return stat.requestTime.time();
    }

    public void storeFailureDetails(String operation, ZkNodeProps request, SolrResponse resp) {
      String op = operation.toLowerCase(Locale.ROOT);
      Stat stat = stats.get(op);
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

    public List<FailedOp> getFailureDetails(String operation) {
      Stat stat = stats.get(operation.toLowerCase(Locale.ROOT));
      if (stat == null || stat.failureDetails.isEmpty()) return null;
      LinkedList<FailedOp> failedOps = stat.failureDetails;
      synchronized (failedOps)  {
        ArrayList<FailedOp> ret = new ArrayList<>(failedOps);
        return ret;
      }
    }

    public int getQueueLength() {
      return queueLength;
    }

    public void setQueueLength(int queueLength) {
      this.queueLength = queueLength;
    }

    public void clear() {
      stats.clear();
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
