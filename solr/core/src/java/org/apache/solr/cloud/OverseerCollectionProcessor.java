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
import org.apache.solr.common.cloud.CloudState;
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
  public static final String DELETECOLLECTION = "deletecollection";

  public static final String CREATECOLLECTION = "createcollection";

  // TODO: use from Overseer?
  private static final String QUEUE_OPERATION = "operation";
  
  private static Logger log = LoggerFactory
      .getLogger(OverseerCollectionProcessor.class);
  
  private DistributedQueue workQueue;
  
  private String myId;

  private ShardHandler shardHandler;

  private String adminPath;

  private ZkStateReader zkStateReader;
  
  public OverseerCollectionProcessor(ZkStateReader zkStateReader, String myId, ShardHandler shardHandler, String adminPath) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.shardHandler = shardHandler;
    this.adminPath = adminPath;
    workQueue = Overseer.getCollectionQueue(zkStateReader.getZkClient());
  }
  
  @Override
  public void run() {
    log.info("Process current queue of collection creations");
    while (amILeader()) {
      try {
        byte[] head = workQueue.peek(true);
        
        //if (head != null) {    // should not happen since we block above
          final ZkNodeProps message = ZkNodeProps.load(head);
          final String operation = message.get(QUEUE_OPERATION);
          
          boolean success = processMessage(message, operation);
          if (!success) {
            // TODO: what to do on failure / partial failure
            // if we fail, do we clean up then ?
            SolrException.log(log, "Collection creation of " + message.get("name") + " failed");
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
  
  private boolean amILeader() {
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
          "/overseer_elect/leader", null, null, true));
      if (myId.equals(props.get("id"))) {
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
      return createCollection(zkStateReader.getCloudState(), message);
    } else if (DELETECOLLECTION.equals(operation)) {
      return deleteCollection(zkStateReader.getCloudState(), message);
    }
    // unknown command, toss it from our queue
    return true;
  }
  
  private boolean deleteCollection(CloudState cloudState, ZkNodeProps message) {
    
    String name = message.get("name");
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
    
    Map<String,Slice> slices = cloudState.getCollectionStates().get(name);
    
    if (slices == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not find collection:" + name);
    }
    
    for (Map.Entry<String,Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String,ZkNodeProps> shards = slice.getShards();
      Set<Map.Entry<String,ZkNodeProps>> shardEntries = shards.entrySet();
      for (Map.Entry<String,ZkNodeProps> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        if (cloudState.liveNodesContain(node.get(ZkStateReader.NODE_NAME_PROP))) {
          params.set(CoreAdminParams.CORE, name);
          params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);

          String replica = node.get(ZkStateReader.BASE_URL_PROP);
          ShardRequest sreq = new ShardRequest();
          // yes, they must use same admin handler path everywhere...
          params.set("qt", adminPath);

          sreq.purpose = 1;
          // TODO: this sucks
          if (replica.startsWith("http://")) replica = replica.substring(7);
          sreq.shards = new String[] {replica};
          sreq.actualShards = sreq.shards;
          sreq.params = params;
          
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

  // TODO: bad name conflict with another method
  private boolean createCollection(CloudState cloudState, ZkNodeProps message) {
    
    // look at the replication factor and see if it matches reality
    // if it does not, find best nodes to create more cores
    
    String numReplicasString = message.get("numReplicas");
    int numReplicas;
    try {
      numReplicas = numReplicasString == null ? 0 : Integer.parseInt(numReplicasString);
    } catch (Exception ex) {
      SolrException.log(log, "Could not parse numReplicas", ex);
      return false;
    }
    String numShardsString = message.get("numShards");
    int numShards;
    try {
      numShards = numShardsString == null ? 0 : Integer.parseInt(numShardsString);
    } catch (Exception ex) {
      SolrException.log(log, "Could not parse numShards", ex);
      return false;
    }
    
    String name = message.get("name");
    String configName = message.get("collection.configName");
    
    // we need to look at every node and see how many cores it serves
    // add our new cores to existing nodes serving the least number of cores
    // but (for now) require that each core goes on a distinct node.
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
    
    
    // TODO: add smarter options that look at the current number of cores per node?
    // for now we just go random
    Set<String> nodes = cloudState.getLiveNodes();
    List<String> nodeList = new ArrayList<String>(nodes.size());
    nodeList.addAll(nodes);
    Collections.shuffle(nodeList);
    
    int numNodes = numShards * (numReplicas + 1);
    List<String> createOnNodes = nodeList.subList(0, Math.min(nodeList.size() -1, numNodes - 1));
    
    for (String replica : createOnNodes) {
      // TODO: this does not work if original url had _ in it
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
}
