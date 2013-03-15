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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClosableThread;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    NamedList results = new NamedList();
    try {
      if (CREATECOLLECTION.equals(operation)) {
        createCollection(zkStateReader.getClusterState(), message, results);
      } else if (DELETECOLLECTION.equals(operation)) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
        params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
        collectionCmd(zkStateReader.getClusterState(), message, params, results);
      } else if (RELOADCOLLECTION.equals(operation)) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());
        collectionCmd(zkStateReader.getClusterState(), message, params, results);
      } else if (CREATEALIAS.equals(operation)) {
        createAlias(zkStateReader.getAliases(), message);
      } else if (DELETEALIAS.equals(operation)) {
        deleteAlias(zkStateReader.getAliases(), message);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }
      int failed = 0;
      ShardResponse srsp;
      
      do {
        srsp = shardHandler.takeCompletedIncludingErrors();
        if (srsp != null) {
          Throwable e = srsp.getException();
          if (e != null) {
            failed++;
            log.error("Error talking to shard: " + srsp.getShard(), e);
            results.add(srsp.getShard(), e);
          } else {
            results.add(srsp.getShard(), srsp.getSolrResponse().getResponse());
          }
        }
      } while (srsp != null);
    } catch (Exception ex) {
      SolrException.log(log, "Collection " + operation + " of " + operation
          + " failed", ex);
      results.add("Operation " + operation + " caused exception:", ex);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", ex.getMessage());
      nl.add("rspCode", ex instanceof SolrException ? ((SolrException)ex).code() : -1);
      results.add("exception", nl);
    } finally {
      return new OverseerSolrResponse(results);
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
  
  private void createCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    String collectionName = message.getStr("name");
    if (clusterState.getCollections().contains(collectionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }
    
    try {
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores
      
      int repFactor = msgStrToInt(message, REPLICATION_FACTOR, 1);
      Integer numSlices = msgStrToInt(message, NUM_SLICES, null);
      
      if (numSlices == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
      }
      
      int maxShardsPerNode = msgStrToInt(message, MAX_SHARDS_PER_NODE, 1);
      String createNodeSetStr; 
      List<String> createNodeList = ((createNodeSetStr = message.getStr(CREATE_NODE_SET)) == null)?null:StrUtils.splitSmart(createNodeSetStr, ",", true);
      
      if (repFactor <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " is a required paramater");
      }
      
      if (numSlices <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " must be > 0");
      }
      
      String configName = message.getStr("collection.configName");
      
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
            + " is higher than or equal to the number of Solr instances currently live ("
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
      
      for (int i = 1; i <= numSlices; i++) {
        for (int j = 1; j <= repFactor; j++) {
          String nodeName = nodeList.get((repFactor * (i - 1) + (j - 1)) % nodeList.size());
          String sliceName = "shard" + i;
          String shardName = collectionName + "_" + sliceName + "_replica" + j;
          log.info("Creating shard " + shardName + " as part of slice "
              + sliceName + " of collection " + collectionName + " on "
              + nodeName);
          
          // Need to create new params for each request
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
          
          params.set(CoreAdminParams.NAME, shardName);
          params.set("collection.configName", configName);
          params.set(CoreAdminParams.COLLECTION, collectionName);
          params.set(CoreAdminParams.SHARD, sliceName);
          params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);
          
          ShardRequest sreq = new ShardRequest();
          sreq.nodeName = nodeName;
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
  
  private boolean collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params, NamedList results) {
    log.info("Executing Collection Cmd : " + params);
    String collectionName = message.getStr("name");
    
    DocCollection coll = clusterState.getCollection(collectionName);
    
    if (coll == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Could not find collection:" + collectionName);
    }
    
    for (Map.Entry<String,Slice> entry : coll.getSlicesMap().entrySet()) {
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
    
    int failed = 0;
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp);
      }
    } while (srsp != null);

    
    // if all calls succeeded, return true
    if (failed > 0) {
      return false;
    }
    return true;
  }

  private void processResponse(NamedList results, ShardResponse srsp) {
    Throwable e = srsp.getException();
    if (e != null) {
      log.error("Error from shard: " + srsp.getShard(), e);
      
      SimpleOrderedMap failure = (SimpleOrderedMap) results.get("failure");
      if (failure == null) {
        failure = new SimpleOrderedMap();
        results.add("failure", failure);
      }

      failure.add(srsp.getNodeName(), e.getClass().getName() + ":" + e.getMessage());
      
    } else {
      
      SimpleOrderedMap success = (SimpleOrderedMap) results.get("success");
      if (success == null) {
        success = new SimpleOrderedMap();
        results.add("success", success);
      }
      
      success.add(srsp.getNodeName(), srsp.getSolrResponse().getResponse());
    }
  }
  
  private int msgStrToInt(ZkNodeProps message, String key, Integer def)
      throws Exception {
    String str = message.getStr(key);
    try {
      return str == null ? def : Integer.parseInt(str);
    } catch (Exception ex) {
      SolrException.log(log, "Could not parse " + key, ex);
      throw ex;
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
