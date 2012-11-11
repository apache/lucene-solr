package org.apache.solr.cloud;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverseerCollectionProcessor implements Runnable {
  public static final String REPLICATION_FACTOR = "replicationFactor";

  public static final String DELETECOLLECTION = "deletecollection";

  public static final String CREATECOLLECTION = "createcollection";

  public static final String RELOADCOLLECTION = "reloadcollection";
  
  // TODO: use from Overseer?
  private static final String QUEUE_OPERATION = "operation";


  
  private static Logger log = LoggerFactory
      .getLogger(OverseerCollectionProcessor.class);
  
  private DistributedQueue workQueue;
  
  private String myId;

  private ShardHandler shardHandler;

  private String adminPath;

  private ZkStateReader zkStateReader;

  private boolean isClosed;
  
  public OverseerCollectionProcessor(ZkStateReader zkStateReader, String myId, ShardHandler shardHandler, String adminPath) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.shardHandler = shardHandler;
    this.adminPath = adminPath;
    workQueue = Overseer.getCollectionQueue(zkStateReader.getZkClient());
  }
  
  @Override
  public void run() {
    log.info("Process current queue of collection messages");
    while (amILeader() && !isClosed) {
      try {
        byte[] head = workQueue.peek(true);
        
        //if (head != null) {    // should not happen since we block above
          final ZkNodeProps message = ZkNodeProps.load(head);
          final String operation = message.getStr(QUEUE_OPERATION);
        try {
          boolean success = processMessage(message, operation);
          if (!success) {
            // TODO: what to do on failure / partial failure
            // if we fail, do we clean up then ?
            SolrException.log(log,
                "Collection " + operation + " of " + message.getStr("name")
                    + " failed");
          }
        } catch(Throwable t) {
          SolrException.log(log,
              "Collection " + operation + " of " + message.getStr("name")
                  + " failed", t);
        }
        //}
        workQueue.remove();
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.SESSIONEXPIRED
            || e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.warn("Overseer cannot talk to ZK");
          return;
        }
        SolrException.log(log, "", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
  
  public void close() {
    isClosed = true;
  }
  
  private boolean amILeader() {
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
          "/overseer_elect/leader", null, null, true));
      if (myId.equals(props.getStr("id"))) {
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
  
  private boolean processMessage(ZkNodeProps message, String operation) {
    if (CREATECOLLECTION.equals(operation)) {
      return createCollection(zkStateReader.getClusterState(), message);
    } else if (DELETECOLLECTION.equals(operation)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      return collectionCmd(zkStateReader.getClusterState(), message, params);
    } else if (RELOADCOLLECTION.equals(operation)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());
      return collectionCmd(zkStateReader.getClusterState(), message, params);
    }
    // unknown command, toss it from our queue
    return true;
  }

  private boolean createCollection(ClusterState clusterState, ZkNodeProps message) {
    
    // look at the replication factor and see if it matches reality
    // if it does not, find best nodes to create more cores
    
    String numReplicasString = message.getStr(REPLICATION_FACTOR);
    int numReplicas;
    try {
      numReplicas = numReplicasString == null ? 0 : Integer.parseInt(numReplicasString);
    } catch (Exception ex) {
      SolrException.log(log, "Could not parse " + REPLICATION_FACTOR, ex);
      return false;
    }
    String numShardsString = message.getStr("numShards");
    int numShards;
    try {
      numShards = numShardsString == null ? 0 : Integer.parseInt(numShardsString);
    } catch (Exception ex) {
      SolrException.log(log, "Could not parse numShards", ex);
      return false;
    }
    
    String name = message.getStr("name");
    String configName = message.getStr("collection.configName");
    
    // we need to look at every node and see how many cores it serves
    // add our new cores to existing nodes serving the least number of cores
    // but (for now) require that each core goes on a distinct node.
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
    
    
    // TODO: add smarter options that look at the current number of cores per node?
    // for now we just go random
    Set<String> nodes = clusterState.getLiveNodes();
    List<String> nodeList = new ArrayList<String>(nodes.size());
    nodeList.addAll(nodes);
    Collections.shuffle(nodeList);
    
    int numNodes = numShards * (numReplicas + 1);
    List<String> createOnNodes = nodeList.subList(0, Math.min(nodeList.size(), numNodes));
    
    log.info("Create collection " + name + " on " + createOnNodes);
    
    for (String replica : createOnNodes) {
      // TODO: this does not work if original url had _ in it
      // We should have a master list
      replica = replica.replaceAll("_", "/");
      params.set(CoreAdminParams.NAME, name);
      params.set("collection.configName", configName);
      params.set("numShards", numShards);
      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      // TODO: this sucks
      if (replica.startsWith("http://")) replica = replica.substring(7);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;
      
      shardHandler.submit(sreq, replica, sreq.params);
    }
    
    int failed = 0;
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        Throwable e = srsp.getException();
        if (e != null) {
          // should we retry?
          // TODO: we should return errors to the client
          // TODO: what if one fails and others succeed?
          failed++;
          log.error("Error talking to shard: " + srsp.getShard(), e);
        }
      }
    } while (srsp != null);

    
    // if all calls succeeded, return true
    if (failed > 0) {
      return false;
    }
    return true;
  }
  
  private boolean collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params) {
    log.info("Executing Collection Cmd : " + params);
    String name = message.getStr("name");
    
    Map<String,Slice> slices = clusterState.getCollectionStates().get(name);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + name);
    }
    
    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,Replica> shards = slice.getReplicasMap();
      Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String,Replica> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP))) {
          // For thread safety, only simple clone the ModifiableSolrParams
          ModifiableSolrParams cloneParams = new ModifiableSolrParams();
          cloneParams.add(params);
          cloneParams.set(CoreAdminParams.CORE,
              node.getStr(ZkStateReader.CORE_NAME_PROP));
          
          String replica = node.getStr(ZkStateReader.BASE_URL_PROP);
          ShardRequest sreq = new ShardRequest();
          
          // yes, they must use same admin handler path everywhere...
          cloneParams.set("qt", adminPath);
          sreq.purpose = 1;
          // TODO: this sucks
          if (replica.startsWith("http://")) replica = replica.substring(7);
          sreq.shards = new String[] {replica};
          sreq.actualShards = sreq.shards;
          sreq.params = cloneParams;
          log.info("Collection Admin sending CoreAdmin cmd to " + replica
              + " params:" + sreq.params);
          shardHandler.submit(sreq, replica, sreq.params);
        }
      }
    }
    
    int failed = 0;
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        Throwable e = srsp.getException();
        if (e != null) {
          // should we retry?
          // TODO: we should return errors to the client
          // TODO: what if one fails and others succeed?
          failed++;
          log.error("Error talking to shard: " + srsp.getShard(), e);
        }
      }
    } while (srsp != null);

    
    // if all calls succeeded, return true
    if (failed > 0) {
      return false;
    }
    return true;
  }
}
