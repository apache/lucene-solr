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

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClosableThread;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.cloud.Assign.Node;
import static org.apache.solr.cloud.Assign.getNodesForNewShard;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;


public class OverseerCollectionProcessor implements Runnable, ClosableThread {
  
  public static final String NUM_SLICES = "numShards";
  
  public static final String REPLICATION_FACTOR = "replicationFactor";
  
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  
  public static final String CREATE_NODE_SET = "createNodeSet";
  
  public static final String DELETECOLLECTION = "deletecollection";

  public static final String CREATECOLLECTION = "createcollection";

  public static final String RELOADCOLLECTION = "reloadcollection";
  
  public static final String CREATEALIAS = "createalias";
  
  public static final String DELETEALIAS = "deletealias";
  
  public static final String SPLITSHARD = "splitshard";

  public static final String DELETESHARD = "deleteshard";

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String CREATESHARD = "createshard";

  public static final String COLL_CONF = "collection.configName";


  public static final Map<String,Object> COLL_PROPS = ZkNodeProps.makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      REPLICATION_FACTOR, "1",
      MAX_SHARDS_PER_NODE, "1");


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
    this(zkStateReader, myId, shardHandler, adminPath, Overseer.getCollectionQueue(zkStateReader.getZkClient()));
  }

  protected OverseerCollectionProcessor(ZkStateReader zkStateReader, String myId, ShardHandler shardHandler, String adminPath, DistributedQueue workQueue) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.shardHandler = shardHandler;
    this.adminPath = adminPath;
    this.workQueue = workQueue;
  }
  
  @Override
  public void run() {
       log.info("Process current queue of collection creations");
       while (amILeader() && !isClosed) {
         try {
           QueueEvent head = workQueue.peek(true);
           final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
           log.info("Overseer Collection Processor: Get the message id:" + head.getId() + " message:" + message.toString());
           final String operation = message.getStr(QUEUE_OPERATION);
           SolrResponse response = processMessage(message, operation);
           head.setBytes(SolrResponse.serializable(response));
           workQueue.remove(head);
          log.info("Overseer Collection Processor: Message id:" + head.getId() + " complete, response:"+ response.getResponse().toString());
        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.SESSIONEXPIRED
              || e.code() == KeeperException.Code.CONNECTIONLOSS) {
             log.warn("Overseer cannot talk to ZK");
             return;
           }
           SolrException.log(log, "", e);
           throw new ZooKeeperException(
               SolrException.ErrorCode.SERVER_ERROR, "", e);
         } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
           return;
         } catch (Throwable e) {
           SolrException.log(log, "", e);
         }
       }
  }
  
  public void close() {
    isClosed = true;
  }
  
  protected boolean amILeader() {
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
  
  
  protected SolrResponse processMessage(ZkNodeProps message, String operation) {
    log.warn("OverseerCollectionProcessor.processMessage : "+ operation + " , "+ message.toString());

    NamedList results = new NamedList();
    try {
      if (CREATECOLLECTION.equals(operation)) {
        createCollection(zkStateReader.getClusterState(), message, results);
      } else if (DELETECOLLECTION.equals(operation)) {
        deleteCollection(message, results);
      } else if (RELOADCOLLECTION.equals(operation)) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());
        collectionCmd(zkStateReader.getClusterState(), message, params, results, ZkStateReader.ACTIVE);
      } else if (CREATEALIAS.equals(operation)) {
        createAlias(zkStateReader.getAliases(), message);
      } else if (DELETEALIAS.equals(operation)) {
        deleteAlias(zkStateReader.getAliases(), message);
      } else if (SPLITSHARD.equals(operation))  {
        splitShard(zkStateReader.getClusterState(), message, results);
      } else if (CREATESHARD.equals(operation))  {
        createShard(zkStateReader.getClusterState(), message, results);
      } else if (DELETESHARD.equals(operation)) {
        deleteShard(zkStateReader.getClusterState(), message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }

    } catch (Throwable t) {
      SolrException.log(log, "Collection " + operation + " of " + operation
          + " failed", t);
      results.add("Operation " + operation + " caused exception:", t);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", t.getMessage());
      nl.add("rspCode", t instanceof SolrException ? ((SolrException)t).code() : -1);
      results.add("exception", nl);
    } 
    
    return new OverseerSolrResponse(results);
  }

  private void deleteCollection(ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collection = message.getStr("name");
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);
      collectionCmd(zkStateReader.getClusterState(), message, params, results,
          null);
      
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          Overseer.REMOVECOLLECTION, "name", collection);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(
          ZkStateReader.toJSON(m));
      
      // wait for a while until we don't see the collection
      long now = System.currentTimeMillis();
      long timeout = now + 30000;
      boolean removed = false;
      while (System.currentTimeMillis() < timeout) {
        Thread.sleep(100);
        removed = !zkStateReader.getClusterState().getCollections()
            .contains(message.getStr("name"));
        if (removed) {
          Thread.sleep(100); // just a bit of time so it's more likely other
                             // readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + message.getStr("name"));
      }
      
    } finally {
      
      try {
        if (zkStateReader.getZkClient().exists(
            ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
          zkStateReader.getZkClient().clean(
              ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection);
        }
      } catch (InterruptedException e) {
        SolrException.log(log, "Cleaning up collection in zk was interrupted:"
            + collection, e);
        Thread.currentThread().interrupt();
      } catch (KeeperException e) {
        SolrException.log(log, "Problem cleaning up collection in zk:"
            + collection, e);
      }
    }
  }

  private void createAlias(Aliases aliases, ZkNodeProps message) {
    String aliasName = message.getStr("name");
    String collections = message.getStr("collections");
    
    Map<String,Map<String,String>> newAliasesMap = new HashMap<String,Map<String,String>>();
    Map<String,String> newCollectionAliasesMap = new HashMap<String,String>();
    Map<String,String> prevColAliases = aliases.getCollectionAliasMap();
    if (prevColAliases != null) {
      newCollectionAliasesMap.putAll(prevColAliases);
    }
    newCollectionAliasesMap.put(aliasName, collections);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes  = ZkStateReader.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES,
          jsonBytes, true);
      
      checkForAlias(aliasName, collections);
      // some fudge for other nodes
      Thread.sleep(100);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      log.warn("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }
  
  private void checkForAlias(String name, String value) {

    long now = System.currentTimeMillis();
    long timeout = now + 30000;
    boolean success = false;
    Aliases aliases = null;
    while (System.currentTimeMillis() < timeout) {
      aliases = zkStateReader.getAliases();
      String collections = aliases.getCollectionAlias(name);
      if (collections != null && collections.equals(value)) {
        success = true;
        break;
      }
    }
    if (!success) {
      log.warn("Timeout waiting to be notified of Alias change...");
    }
  }
  
  private void checkForAliasAbsence(String name) {

    long now = System.currentTimeMillis();
    long timeout = now + 30000;
    boolean success = false;
    Aliases aliases = null;
    while (System.currentTimeMillis() < timeout) {
      aliases = zkStateReader.getAliases();
      String collections = aliases.getCollectionAlias(name);
      if (collections == null) {
        success = true;
        break;
      }
    }
    if (!success) {
      log.warn("Timeout waiting to be notified of Alias change...");
    }
  }

  private void deleteAlias(Aliases aliases, ZkNodeProps message) {
    String aliasName = message.getStr("name");

    Map<String,Map<String,String>> newAliasesMap = new HashMap<String,Map<String,String>>();
    Map<String,String> newCollectionAliasesMap = new HashMap<String,String>();
    newCollectionAliasesMap.putAll(aliases.getCollectionAliasMap());
    newCollectionAliasesMap.remove(aliasName);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes  = ZkStateReader.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES,
          jsonBytes, true);
      checkForAliasAbsence(aliasName);
      // some fudge for other nodes
      Thread.sleep(100);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      log.warn("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    
  }

  private boolean createShard(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    log.info("Create shard invoked: {}", message);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    if(collectionName == null || shard ==null)
      throw new SolrException(ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters" );
    int numSlices = 1;

    DocCollection collection = clusterState.getCollection(collectionName);
    int maxShardsPerNode = collection.getInt(MAX_SHARDS_PER_NODE, 1);
    int repFactor = message.getInt(REPLICATION_FACTOR, collection.getInt(REPLICATION_FACTOR, 1));
    String createNodeSetStr = message.getStr(CREATE_NODE_SET);

    ArrayList<Node> sortedNodeList = getNodesForNewShard(clusterState, collectionName, numSlices, maxShardsPerNode, repFactor, createNodeSetStr);

    Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));
    // wait for a while until we see the shard
    long waitUntil = System.currentTimeMillis() + 30000;
    boolean created = false;
    while (System.currentTimeMillis() < waitUntil) {
      Thread.sleep(100);
      created = zkStateReader.getClusterState().getCollection(collectionName).getSlice(shard) != null;
      if (created) break;
    }
    if (!created)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create shard: " + message.getStr("name"));


    String configName = message.getStr(COLL_CONF);
    String sliceName = shard;
    for (int j = 1; j <= repFactor; j++) {
      String nodeName = sortedNodeList.get(((j - 1)) % sortedNodeList.size()).nodeName;
      String shardName = collectionName + "_" + sliceName + "_replica" + j;
      log.info("Creating shard " + shardName + " as part of slice "
          + sliceName + " of collection " + collectionName + " on "
          + nodeName);

      // Need to create new params for each request
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

      params.set(CoreAdminParams.NAME, shardName);
      params.set(COLL_CONF, configName);
      params.set(CoreAdminParams.COLLECTION, collectionName);
      params.set(CoreAdminParams.SHARD, sliceName);
      params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getZkClient()
          .getBaseUrlForNodeName(nodeName);
      if (replica.startsWith("http://")) replica = replica.substring(7);
      sreq.shards = new String[]{replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

    }

    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp);
      }
    } while (srsp != null);

    log.info("Finished create command on all shards for collection: "
        + collectionName);

    return true;
  }


  private boolean splitShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    log.info("Split shard invoked");
    String collectionName = message.getStr("collection");
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);
    Slice parentSlice = clusterState.getSlice(collectionName, slice);
    
    if (parentSlice == null) {
      if(clusterState.getCollections().contains(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collectionName);
      }      
    }
    
    // find the leader for the shard
    Replica parentShardLeader = clusterState.getLeader(collectionName, slice);
    DocCollection collection = clusterState.getCollection(collectionName);
    DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
    DocRouter.Range range = parentSlice.getRange();
    if (range == null) {
      range = new PlainIdRouter().fullRange();
    }

    // todo: fixed to two partitions?
    // todo: accept the range as a param to api?
    List<DocRouter.Range> subRanges = router.partitionRange(2, range);
    try {
      List<String> subSlices = new ArrayList<String>(subRanges.size());
      List<String> subShardNames = new ArrayList<String>(subRanges.size());
      String nodeName = parentShardLeader.getNodeName();
      for (int i = 0; i < subRanges.size(); i++) {
        String subSlice = slice + "_" + i;
        subSlices.add(subSlice);
        String subShardName = collectionName + "_" + subSlice + "_replica1";
        subShardNames.add(subShardName);

        Slice oSlice = clusterState.getSlice(collectionName, subSlice);
        if (oSlice != null) {
          if (Slice.ACTIVE.equals(oSlice.getState())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Sub-shard: " + subSlice + " exists in active state. Aborting split shard.");
          } else if (Slice.CONSTRUCTION.equals(oSlice.getState()))  {
            for (Replica replica : oSlice.getReplicas()) {
              if (clusterState.liveNodesContain(replica.getNodeName())) {
                String core = replica.getStr("core");
                log.info("Unloading core: " + core + " from node: " + replica.getNodeName());
                ModifiableSolrParams params = new ModifiableSolrParams();
                params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
                params.set(CoreAdminParams.CORE, core);
                params.set(CoreAdminParams.DELETE_INDEX, "true");
                sendShardRequest(replica.getNodeName(), params);
              } else  {
                log.warn("Replica {} exists in shard {} but is not live and cannot be unloaded", replica, oSlice);
              }
            }
          }
        }
      }

      // do not abort splitshard if the unloading fails
      // this can happen because the replicas created previously may be down
      // the only side effect of this is that the sub shard may end up having more replicas than we want
      collectShardResponses(results, false, null);

      for (int i=0; i<subRanges.size(); i++)  {
        String subSlice = subSlices.get(i);
        String subShardName = subShardNames.get(i);
        DocRouter.Range subRange = subRanges.get(i);

        log.info("Creating shard " + subShardName + " as part of slice "
            + subSlice + " of collection " + collectionName + " on "
            + nodeName);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, subShardName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, subSlice);
        params.set(CoreAdminParams.SHARD_RANGE, subRange.toString());
        params.set(CoreAdminParams.SHARD_STATE, Slice.CONSTRUCTION);
        //params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices); todo: is it necessary, we're not creating collections?

        sendShardRequest(nodeName, params);
      }

      collectShardResponses(results, true,
          "SPLTSHARD failed to create subshard leaders");

      for (String subShardName : subShardNames) {
        // wait for parent leader to acknowledge the sub-shard core
        log.info("Asking parent leader to wait for: " + subShardName + " to be alive on: " + nodeName);
        String coreNodeName = waitForCoreNodeName(collection, zkStateReader.getZkClient().getBaseUrlForNodeName(nodeName), subShardName);
        CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
        cmd.setCoreName(subShardName);
        cmd.setNodeName(nodeName);
        cmd.setCoreNodeName(coreNodeName);
        cmd.setState(ZkStateReader.ACTIVE);
        cmd.setCheckLive(true);
        cmd.setOnlyIfLeader(true);
        sendShardRequest(nodeName, new ModifiableSolrParams(cmd.getParams()));
      }

      collectShardResponses(results, true,
          "SPLTSHARD timed out waiting for subshard leaders to come up");
      
      log.info("Successfully created all sub-shards for collection "
          + collectionName + " parent shard: " + slice + " on: " + parentShardLeader);

      log.info("Splitting shard " + parentShardLeader.getName() + " as part of slice "
          + slice + " of collection " + collectionName + " on "
          + parentShardLeader);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
      params.set(CoreAdminParams.CORE, parentShardLeader.getStr("core"));
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);
        params.add(CoreAdminParams.TARGET_CORE, subShardName);
      }

      sendShardRequest(parentShardLeader.getNodeName(), params);
      collectShardResponses(results, true, "SPLITSHARD failed to invoke SPLIT core admin command");

      log.info("Index on shard: " + nodeName + " split into two successfully");

      // apply buffered updates on sub-shards
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);

        log.info("Applying buffered updates on : " + subShardName);

        params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
        params.set(CoreAdminParams.NAME, subShardName);

        sendShardRequest(nodeName, params);
      }

      collectShardResponses(results, true,
          "SPLITSHARD failed while asking sub shard leaders to apply buffered updates");

      log.info("Successfully applied buffered updates on : " + subShardNames);

      // Replica creation for the new Slices

      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores

      // TODO: Have replication factor decided in some other way instead of numShards for the parent

      int repFactor = clusterState.getSlice(collectionName, slice).getReplicas().size();

      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.

      // TODO: add smarter options that look at the current number of cores per
      // node?
      // for now we just go random
      Set<String> nodes = clusterState.getLiveNodes();
      List<String> nodeList = new ArrayList<String>(nodes.size());
      nodeList.addAll(nodes);
      
      Collections.shuffle(nodeList);

      // TODO: Have maxShardsPerNode param for this operation?

      // Remove the node that hosts the parent shard for replica creation.
      nodeList.remove(nodeName);
      
      // TODO: change this to handle sharding a slice into > 2 sub-shards.

      for (int i = 1; i <= subSlices.size(); i++) {
        Collections.shuffle(nodeList);
        String sliceName = subSlices.get(i - 1);
        for (int j = 2; j <= repFactor; j++) {
          String subShardNodeName = nodeList.get((repFactor * (i - 1) + (j - 2)) % nodeList.size());
          String shardName = collectionName + "_" + sliceName + "_replica" + (j);

          log.info("Creating replica shard " + shardName + " as part of slice "
              + sliceName + " of collection " + collectionName + " on "
              + subShardNodeName);

          // Need to create new params for each request
          params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

          params.set(CoreAdminParams.NAME, shardName);
          params.set(CoreAdminParams.COLLECTION, collectionName);
          params.set(CoreAdminParams.SHARD, sliceName);
          // TODO:  Figure the config used by the parent shard and use it.
          //params.set("collection.configName", configName);
          
          //Not using this property. Do we really need to use it?
          //params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

          sendShardRequest(subShardNodeName, params);

          String coreNodeName = waitForCoreNodeName(collection, zkStateReader.getZkClient().getBaseUrlForNodeName(subShardNodeName), shardName);
          // wait for the replicas to be seen as active on sub shard leader
          log.info("Asking sub shard leader to wait for: " + shardName + " to be alive on: " + subShardNodeName);
          CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
          cmd.setCoreName(subShardNames.get(i-1));
          cmd.setNodeName(subShardNodeName);
          cmd.setCoreNodeName(coreNodeName);
          cmd.setState(ZkStateReader.ACTIVE);
          cmd.setCheckLive(true);
          cmd.setOnlyIfLeader(true);
          sendShardRequest(nodeName, new ModifiableSolrParams(cmd.getParams()));
        }
      }

      collectShardResponses(results, true,
          "SPLTSHARD failed to create subshard replicas or timed out waiting for them to come up");

      log.info("Calling soft commit to make sub shard updates visible");
      String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
      // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
      // and we force open a searcher so that we have documents to show upon switching states
      UpdateResponse updateResponse = null;
      try {
        updateResponse = softCommit(coreUrl);
        processResponse(results, null, coreUrl, updateResponse, slice);
      } catch (Exception e) {
        processResponse(results, e, coreUrl, updateResponse, slice);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
      }

      log.info("Successfully created all replica shards for all sub-slices "
          + subSlices);

      log.info("Requesting update shard state");
      DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
      Map<String, Object> propMap = new HashMap<String, Object>();
      propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
      propMap.put(slice, Slice.INACTIVE);
      for (String subSlice : subSlices) {
        propMap.put(subSlice, Slice.ACTIVE);
      }
      propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
      ZkNodeProps m = new ZkNodeProps(propMap);
      inQueue.offer(ZkStateReader.toJSON(m));

      return true;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Error executing split operation for collection: " + collectionName + " parent shard: " + slice, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, null, e);
    }
  }

  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {
    HttpSolrServer server = null;
    try {
      server = new HttpSolrServer(url);
      server.setConnectionTimeout(30000);
      server.setSoTimeout(120000);
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      return ureq.process(server);
    } finally {
      if (server != null) {
        server.shutdown();
      }
    }
  }
  
  private String waitForCoreNodeName(DocCollection collection, String msgBaseUrl, String msgCore) {
    int retryCount = 320;
    while (retryCount-- > 0) {
      Map<String,Slice> slicesMap = zkStateReader.getClusterState()
          .getSlicesMap(collection.getName());
      if (slicesMap != null) {
        
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this
            
            String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
            
            if (baseUrl.equals(msgBaseUrl) && core.equals(msgCore)) {
              return replica.getName();
            }
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not find coreNodeName");
  }

  private void collectShardResponses(NamedList results, boolean abortOnError, String msgOnError) {
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp);
        Throwable exception = srsp.getException();
        if (abortOnError && exception != null)  {
          // drain pending requests
          while (srsp != null)  {
            srsp = shardHandler.takeCompletedOrError();
          }
          throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError, exception);
        }
      }
    } while (srsp != null);
  }

  
  private void deleteShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    log.info("Delete shard invoked");
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    Slice slice = clusterState.getSlice(collection, sliceId);
    
    if (slice == null) {
      if(clusterState.getCollections().contains(collection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No shard with the specified name exists: " + slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No collection with the specified name exists: " + collection);
      }      
    }
    // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
    // TODO: Add check for range gaps on Slice deletion
    if (!(slice.getRange() == null || slice.getState().equals(Slice.INACTIVE))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The slice: " + slice.getName() + " is currently "
          + slice.getState() + ". Only INACTIVE (or custom-hashed) slices can be deleted.");
    }

    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INDEX, "true");
      sliceCmd(clusterState, params, null, slice);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp);
        }
      } while (srsp != null);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          Overseer.REMOVESHARD, ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.SHARD_ID_PROP, sliceId);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));

      // wait for a while until we don't see the shard
      long now = System.currentTimeMillis();
      long timeout = now + 30000;
      boolean removed = false;
      while (System.currentTimeMillis() < timeout) {
        Thread.sleep(100);
        removed = zkStateReader.getClusterState().getSlice(collection, sliceId) == null;
        if (removed) {
          Thread.sleep(100); // just a bit of time so it's more likely other readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + collection + " shard: " + sliceId);
      }

      log.info("Successfully deleted collection: " + collection + ", shard: " + sliceId);

    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error executing delete operation for collection: " + collection + " shard: " + sliceId, e);
    }
  }

  private void sendShardRequest(String nodeName, ModifiableSolrParams params) {
    ShardRequest sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getZkClient().getBaseUrlForNodeName(nodeName);
    if (replica.startsWith("http://")) replica = replica.substring(7);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;

    shardHandler.submit(sreq, replica, sreq.params);
  }

  private void createCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String collectionName = message.getStr("name");
    if (clusterState.getCollections().contains(collectionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }
    
    try {
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores
      
      int repFactor = message.getInt( REPLICATION_FACTOR, 1);
      Integer numSlices = message.getInt(NUM_SLICES, null);
      String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);
      List<String> shardNames = new ArrayList<String>();
      if(ImplicitDocRouter.NAME.equals(router)){
        Overseer.getShardNames(shardNames, message.getStr("shards",null));
        numSlices = shardNames.size();
      } else {
        Overseer.getShardNames(numSlices,shardNames);
      }

      if (numSlices == null ) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " is a required param");
      }

      int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, 1);
      String createNodeSetStr; 
      List<String> createNodeList = ((createNodeSetStr = message.getStr(CREATE_NODE_SET)) == null)?null:StrUtils.splitSmart(createNodeSetStr, ",", true);
      
      if (repFactor <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, REPLICATION_FACTOR + " must be greater than or equal to 0");
      }
      
      if (numSlices <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " must be > 0");
      }
      
      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.
      
      // TODO: add smarter options that look at the current number of cores per
      // node?
      // for now we just go random
      Set<String> nodes = clusterState.getLiveNodes();
      List<String> nodeList = new ArrayList<String>(nodes.size());
      nodeList.addAll(nodes);
      if (createNodeList != null) nodeList.retainAll(createNodeList);
      Collections.shuffle(nodeList);
      
      if (nodeList.size() <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName
            + ". No live Solr-instances" + ((createNodeList != null)?" among Solr-instances specified in " + CREATE_NODE_SET + ":" + createNodeSetStr:""));
      }
      
      if (repFactor > nodeList.size()) {
        log.warn("Specified "
            + REPLICATION_FACTOR
            + " of "
            + repFactor
            + " on collection "
            + collectionName
            + " is higher than or equal to the number of Solr instances currently live or part of your " + CREATE_NODE_SET + "("
            + nodeList.size()
            + "). Its unusual to run two replica of the same slice on the same Solr-instance.");
      }
      
      int maxShardsAllowedToCreate = maxShardsPerNode * nodeList.size();
      int requestedShardsToCreate = numSlices * repFactor;
      if (maxShardsAllowedToCreate < requestedShardsToCreate) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName + ". Value of "
            + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
            + ", and the number of live nodes is " + nodeList.size()
            + ". This allows a maximum of " + maxShardsAllowedToCreate
            + " to be created. Value of " + NUM_SLICES + " is " + numSlices
            + " and value of " + REPLICATION_FACTOR + " is " + repFactor
            + ". This requires " + requestedShardsToCreate
            + " shards to be created (higher than the allowed number)");
      }

      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));

      // wait for a while until we don't see the collection
      long waitUntil = System.currentTimeMillis() + 30000;
      boolean created = false;
      while (System.currentTimeMillis() < waitUntil) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().getCollections().contains(message.getStr("name"));
        if(created) break;
      }
      if (!created)
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully createcollection: " + message.getStr("name"));


      String configName = message.getStr(COLL_CONF);
      log.info("going to create cores replicas shardNames {} , repFactor : {}", shardNames, repFactor);
      for (int i = 1; i <= shardNames.size(); i++) {
        String sliceName = shardNames.get(i-1);
        for (int j = 1; j <= repFactor; j++) {
          String nodeName = nodeList.get((repFactor * (i - 1) + (j - 1)) % nodeList.size());
          String shardName = collectionName + "_" + sliceName + "_replica" + j;
          log.info("Creating shard " + shardName + " as part of slice "
              + sliceName + " of collection " + collectionName + " on "
              + nodeName);

          // Need to create new params for each request
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

          params.set(CoreAdminParams.NAME, shardName);
          params.set(COLL_CONF, configName);
          params.set(CoreAdminParams.COLLECTION, collectionName);
          params.set(CoreAdminParams.SHARD, sliceName);
          params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

          ShardRequest sreq = new ShardRequest();
          params.set("qt", adminPath);
          sreq.purpose = 1;
          String replica = zkStateReader.getZkClient()
            .getBaseUrlForNodeName(nodeName);
          if (replica.startsWith("http://")) replica = replica.substring(7);
          sreq.shards = new String[] {replica};
          sreq.actualShards = sreq.shards;
          sreq.params = params;

          shardHandler.submit(sreq, replica, sreq.params);

        }
      }

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp);
        }
      } while (srsp != null);

      log.info("Finished create command on all shards for collection: "
          + collectionName);

    } catch (SolrException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR, null, ex);
    }
  }

  private void collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params, NamedList results, String stateMatcher) {
    log.info("Executing Collection Cmd : " + params);
    String collectionName = message.getStr("name");
    
    DocCollection coll = clusterState.getCollection(collectionName);
    
    for (Map.Entry<String,Slice> entry : coll.getSlicesMap().entrySet()) {
      Slice slice = entry.getValue();
      sliceCmd(clusterState, params, stateMatcher, slice);
    }
    
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp);
      }
    } while (srsp != null);

  }

  private void sliceCmd(ClusterState clusterState, ModifiableSolrParams params, String stateMatcher, Slice slice) {
    Map<String,Replica> shards = slice.getReplicasMap();
    Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
    for (Map.Entry<String,Replica> shardEntry : shardEntries) {
      final ZkNodeProps node = shardEntry.getValue();
      if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP)) && (stateMatcher != null ? node.getStr(ZkStateReader.STATE_PROP).equals(stateMatcher) : true)) {
        // For thread safety, only simple clone the ModifiableSolrParams
        ModifiableSolrParams cloneParams = new ModifiableSolrParams();
        cloneParams.add(params);
        cloneParams.set(CoreAdminParams.CORE,
            node.getStr(ZkStateReader.CORE_NAME_PROP));

        String replica = node.getStr(ZkStateReader.BASE_URL_PROP);
        ShardRequest sreq = new ShardRequest();
        sreq.nodeName = node.getStr(ZkStateReader.NODE_NAME_PROP);
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

  private void processResponse(NamedList results, ShardResponse srsp) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard);
  }

  private void processResponse(NamedList results, Throwable e, String nodeName, SolrResponse solrResponse, String shard) {
    if (e != null) {
      log.error("Error from shard: " + shard, e);

      SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
      if (failure == null) {
        failure = new SimpleOrderedMap();
        results.add("failure", failure);
      }

      failure.add(nodeName, e.getClass().getName() + ":" + e.getMessage());

    } else {

      SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
      if (success == null) {
        success = new SimpleOrderedMap();
        results.add("success", success);
      }

      success.add(nodeName, solrResponse.getResponse());
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

}
