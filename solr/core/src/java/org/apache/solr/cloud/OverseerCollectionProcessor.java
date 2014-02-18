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

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClosableThread;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.PlainIdRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.solr.cloud.Assign.Node;
import static org.apache.solr.cloud.Assign.getNodesForNewShard;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;


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

  public static final String DELETEREPLICA = "deletereplica";

  public static final String MIGRATE = "migrate";

  public static final String COLL_CONF = "collection.configName";

  public static final String COLL_PROP_PREFIX = "property.";

  public static final Set<String> KNOWN_CLUSTER_PROPS = ImmutableSet.of("legacyCloud");

  public static final Map<String,Object> COLL_PROPS = ZkNodeProps.makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      REPLICATION_FACTOR, "1",
      MAX_SHARDS_PER_NODE, "1",
      "external",null );


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
       LeaderStatus isLeader = amILeader();
       while (isLeader == LeaderStatus.DONT_KNOW) {
         log.debug("am_i_leader unclear {}", isLeader);
         isLeader = amILeader();  // not a no, not a yes, try ask again
       }

    try {
      prioritizeOverseerNodes();
    } catch (Exception e) {
      log.error("Unable to prioritize overseer ", e);

    }
    while (!this.isClosed) {
         try {
           isLeader = amILeader();
           if (LeaderStatus.NO == isLeader) {
             break;
           }
           else if (LeaderStatus.YES != isLeader) {
             log.debug("am_i_leader unclear {}", isLeader);                  
             continue; // not a no, not a yes, try asking again
           }
           
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
         } catch (Exception e) {
           SolrException.log(log, "", e);
         }
       }
  }
  
  public void close() {
    isClosed = true;
  }

  private void prioritizeOverseerNodes() throws KeeperException, InterruptedException {
    log.info("prioritizing overseer nodes");
    SolrZkClient zk = zkStateReader.getZkClient();
    if(!zk.exists(ZkStateReader.ROLES,true))return;
    Map m = (Map) ZkStateReader.fromJSON(zk.getData(ZkStateReader.ROLES, null, new Stat(), true));

    List overseerDesignates = (List) m.get("overseer");
    if(overseerDesignates==null || overseerDesignates.isEmpty()) return;
    if(overseerDesignates.size() == 1 && overseerDesignates.contains(getLeaderNode(zk))) return;
    log.info("overseer designates {}", overseerDesignates);

    List<String> nodeNames = getSortedNodeNames(zk);
    if(nodeNames.size()<2) return;

//
    ArrayList<String> nodesTobePushedBack =  new ArrayList<String>();
    //ensure that the node right behind the leader , i.r at position 1 is a Overseer
    List<String> availableDesignates = new ArrayList<String>();

    log.info("sorted nodes {}", nodeNames);//TODO to be removed
    for (int i = 0; i < nodeNames.size(); i++) {
      String s = nodeNames.get(i);

      if (overseerDesignates.contains(s)) {
        availableDesignates.add(s);

        for(int j=0;j<i;j++){
          if(!overseerDesignates.contains(nodeNames.get(j))) {
            if(!nodesTobePushedBack.contains(nodeNames.get(j))) nodesTobePushedBack.add(nodeNames.get(j));
          }
        }

      }
      if(availableDesignates.size()>1) break;
    }

    if(!availableDesignates.isEmpty()){
      for (int i = nodesTobePushedBack.size() - 1; i >= 0; i--) {
         String s = nodesTobePushedBack.get(i);
        log.info("pushing back {} ", s);
        invokeOverseerOp(s, "rejoin");
      }

      //wait for a while to ensure the designate has indeed come in front
      boolean prioritizationComplete = false;
      long timeout = System.currentTimeMillis() + 2500;

      for(;System.currentTimeMillis()< timeout ;){
        List<String> currentNodeNames = getSortedNodeNames(zk);

        int totalLeaders = 0;

        for(int i=0;i<availableDesignates.size();i++) {
          if(overseerDesignates.contains(currentNodeNames.get(i))) totalLeaders++;
        }
        if(totalLeaders == availableDesignates.size()){
          prioritizationComplete = true;
          break;
        }
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          log.warn("Thread interrupted",e);
          break;

        }
      }

      if(!prioritizationComplete) {
        log.warn("available designates and current state {} {} ", availableDesignates, getSortedNodeNames(zk));
      }

    } else {
      log.warn("No overseer designates are available, overseerDesignates: {}, live nodes : {}",overseerDesignates,nodeNames);
      return;
    }

    String leaderNode = getLeaderNode(zkStateReader.getZkClient());
    if(leaderNode ==null) return;
    if(!overseerDesignates.contains(leaderNode) && !availableDesignates.isEmpty()){
      //this means there are designated Overseer nodes and I am not one of them , kill myself
      String newLeader = availableDesignates.get(0);
      log.info("I am not an overseerdesignate , forcing a new leader {} ", newLeader);
      invokeOverseerOp(newLeader, "leader");
    }

  }
  public static List<String> getSortedNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE, null, true);
    } catch (Exception e) {
      log.warn("error ", e);
      return new ArrayList<String>();
    }
    LeaderElector.sortSeqs(children);
    ArrayList<String> nodeNames = new ArrayList<String>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));
    return nodeNames;
  }

  public static String getLeaderNode(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    byte[] data = new byte[0];
    try {
      data = zkClient.getData("/overseer_elect/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map m = (Map) ZkStateReader.fromJSON(data);
    String s = (String) m.get("id");
//    log.info("leader-id {}",s);
    String nodeName = LeaderElector.getNodeName(s);
//    log.info("Leader {}", nodeName);
    return nodeName;
  }

  private void invokeOverseerOp(String nodeName, String op) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.OVERSEEROP.toString());
    params.set("op", op);
    params.set("qt", adminPath);
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = 1;
    String replica = nodeName.replaceFirst("_", "/");
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    shardHandler.submit(sreq, replica, sreq.params);
  }


  protected LeaderStatus amILeader() {
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
          "/overseer_elect/leader", null, null, true));
      if (myId.equals(props.getStr("id"))) {
        return LeaderStatus.YES;
      }
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
        log.error("", e);
        return LeaderStatus.DONT_KNOW;
      } else if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
        log.info("", e);
      } else {
        log.warn("", e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
    return LeaderStatus.NO;
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
      } else if (DELETEREPLICA.equals(operation)) {
        deleteReplica(zkStateReader.getClusterState(), message, results);
      } else if (MIGRATE.equals(operation)) {
        migrate(zkStateReader.getClusterState(), message, results);
      } else if(CollectionParams.CollectionAction.CLUSTERPROP.toString().equalsIgnoreCase(operation)){
        handleProp(message,results);
      } else if(REMOVEROLE.toString().toLowerCase(Locale.ROOT).equals(operation) || ADDROLE.toString().toLowerCase(Locale.ROOT).equals(operation) ){
        processRoleCommand(message, operation);
      }

      else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }

    } catch (Exception e) {
      SolrException.log(log, "Collection " + operation + " of " + operation
          + " failed", e);
      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    } 
    
    return new OverseerSolrResponse(results);
  }

  private void handleProp(ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String name = message.getStr("name");
    String val = message.getStr("val");
    Map m = zkStateReader.getClusterProps();
    if(val ==null) m.remove(name);
    else m.put(name,val);
    if(zkStateReader.getZkClient().exists(ZkStateReader.CLUSTER_PROPS,true))
      zkStateReader.getZkClient().setData(ZkStateReader.CLUSTER_PROPS,ZkStateReader.toJSON(m),true);
    else
      zkStateReader.getZkClient().create(ZkStateReader.CLUSTER_PROPS, ZkStateReader.toJSON(m),CreateMode.PERSISTENT, true);



  }
  private void processRoleCommand(ZkNodeProps message, String operation) throws KeeperException, InterruptedException {
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map roles = null;
    String node = message.getStr("node");

    String roleName = message.getStr("role");
    boolean nodeExists = false;
    if(nodeExists = zkClient.exists(ZkStateReader.ROLES, true)){
      roles = (Map) ZkStateReader.fromJSON(zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true));
    } else {
      roles = new LinkedHashMap(1);
    }

    List nodeList= (List) roles.get(roleName);
    if(nodeList == null) roles.put(roleName, nodeList = new ArrayList());
    if(ADDROLE.toString().toLowerCase(Locale.ROOT).equals(operation) ){
      log.info("Overseer role added to {}", node);
      if(!nodeList.contains(node)) nodeList.add(node);
    } else if(REMOVEROLE.toString().toLowerCase(Locale.ROOT).equals(operation)) {
      log.info("Overseer role removed from {}", node);
      nodeList.remove(node);
    }

    if(nodeExists){
      zkClient.setData(ZkStateReader.ROLES, ZkStateReader.toJSON(roles),true);
    } else {
      zkClient.create(ZkStateReader.ROLES, ZkStateReader.toJSON(roles), CreateMode.PERSISTENT,true);
    }
    //if there are too many nodes this command may time out. And most likely dedicated
    // overseers are created when there are too many nodes  . So , do this operation in a separate thread
    new Thread(){
      @Override
      public void run() {
        try {
          prioritizeOverseerNodes();
        } catch (Exception e) {
          log.error("Error in prioritizing Overseer",e);
        }

      }
    }.start();
  }

  private void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP,REPLICA_PROP);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);
    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = coll.getSlice(shard);
    if(slice==null){
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid shard name : "+shard+" in collection : "+ collectionName);
    }
    Replica replica = slice.getReplica(replicaName);
    if(replica == null){
      ArrayList<String> l = new ArrayList<String>();
      for (Replica r : slice.getReplicas()) l.add(r.getName());
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid replica : " + replicaName + " in shard/collection : "
          + shard + "/"+ collectionName + " available replicas are "+ StrUtils.join(l,','));
    }

    String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
    String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    
    // assume the core exists and try to unload it
    Map m = ZkNodeProps.makeMap("qt", adminPath, CoreAdminParams.ACTION,
        CoreAdminAction.UNLOAD.toString(), CoreAdminParams.CORE, core);
    
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = 1;
    sreq.shards = new String[] {baseUrl};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams(new MapSolrParams(m));
    try {
      shardHandler.submit(sreq, baseUrl, sreq.params);
    } catch (Exception e) {
      log.warn("Exception trying to unload core " + sreq, e);
    }
    
    collectShardResponses(!Slice.ACTIVE.equals(replica.getStr(Slice.STATE)) ? new NamedList() : results, false, null);
    
    if (waitForCoreNodeGone(collectionName, shard, replicaName, 5000)) return;//check if the core unload removed the corenode zk enry
    deleteCoreNode(collectionName, replicaName, replica, core); // try and ensure core info is removed from clusterstate
    if(waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return;

    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not  remove replica : " + collectionName + "/" + shard+"/" + replicaName);
  }

  private boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    long waitUntil = System.currentTimeMillis() + timeoutms;
    boolean deleted = false;
    while (System.currentTimeMillis() < waitUntil) {
      Thread.sleep(100);
      deleted = zkStateReader.getClusterState().getCollection(collectionName).getSlice(shard).getReplica(replicaName) == null;
      if (deleted) break;
    }
    return deleted;
  }

  private void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws KeeperException, InterruptedException {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, Overseer.DELETECORE,
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));
  }

  private void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

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
        removed = !zkStateReader.getClusterState().hasCollection(message.getStr(collection));
        if (removed) {
          Thread.sleep(300); // just a bit of time so it's more likely other
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
      addPropertyParams(message, params);

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
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
    String splitKey = message.getStr("split.key");

    DocCollection collection = clusterState.getCollection(collectionName);
    DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;

    Slice parentSlice = null;

    if (slice == null)  {
      if (router instanceof CompositeIdRouter) {
        Collection<Slice> searchSlices = router.getSearchSlicesSingle(splitKey, new ModifiableSolrParams(), collection);
        if (searchSlices.isEmpty()) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to find an active shard for split.key: " + splitKey);
        }
        if (searchSlices.size() > 1)  {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Splitting a split.key: " + splitKey + " which spans multiple shards is not supported");
        }
        parentSlice = searchSlices.iterator().next();
        slice = parentSlice.getName();
        log.info("Split by route.key: {}, parent shard is: {} ", splitKey, slice);
      } else  {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Split by route key can only be used with CompositeIdRouter or subclass. Found router: " + router.getClass().getName());
      }
    } else  {
      parentSlice = clusterState.getSlice(collectionName, slice);
    }

    if (parentSlice == null) {
      if(clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collectionName);
      }      
    }
    
    // find the leader for the shard
    Replica parentShardLeader = null;
    try {
      parentShardLeader = zkStateReader.getLeaderRetry(collectionName, slice, 10000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    DocRouter.Range range = parentSlice.getRange();
    if (range == null) {
      range = new PlainIdRouter().fullRange();
    }

    List<DocRouter.Range> subRanges = null;
    String rangesStr = message.getStr(CoreAdminParams.RANGES);
    if (rangesStr != null)  {
      String[] ranges = rangesStr.split(",");
      if (ranges.length == 0 || ranges.length == 1) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "There must be at least two ranges specified to split a shard");
      } else  {
        subRanges = new ArrayList<DocRouter.Range>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
          String r = ranges[i];
          try {
            subRanges.add(DocRouter.DEFAULT.fromString(r));
          } catch (Exception e) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Exception in parsing hexadecimal hash range: " + r, e);
          }
          if (!subRanges.get(i).isSubsetOf(range)) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "Specified hash range: " + r + " is not a subset of parent shard's range: " + range.toString());
          }
        }
        List<DocRouter.Range> temp = new ArrayList<DocRouter.Range>(subRanges); // copy to preserve original order
        Collections.sort(temp);
        if (!range.equals(new DocRouter.Range(temp.get(0).min, temp.get(temp.size() - 1).max)))  {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Specified hash ranges: " + rangesStr + " do not cover the entire range of parent shard: " + range);
        }
        for (int i = 1; i < temp.size(); i++) {
          if (temp.get(i - 1).max + 1 != temp.get(i).min) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "Specified hash ranges: " + rangesStr + " either overlap with each other or " +
                    "do not cover the entire range of parent shard: " + range);
          }
        }
      }
    } else if (splitKey != null)  {
      if (router instanceof CompositeIdRouter) {
        CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
        subRanges = compositeIdRouter.partitionRangeByKey(splitKey, range);
        if (subRanges.size() == 1)  {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "The split.key: " + splitKey + " has a hash range that is exactly equal to hash range of shard: " + slice);
        }
        for (DocRouter.Range subRange : subRanges) {
          if (subRange.min == subRange.max) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "The split.key: " + splitKey + " must be a compositeId");
          }
        }
        log.info("Partitioning parent shard " + slice + " range: " + parentSlice.getRange() + " yields: " + subRanges);
        rangesStr = "";
        for (int i = 0; i < subRanges.size(); i++) {
          DocRouter.Range subRange = subRanges.get(i);
          rangesStr += subRange.toString();
          if (i < subRanges.size() - 1)
            rangesStr += ',';
        }
      }
    } else  {
      // todo: fixed to two partitions?
      subRanges = router.partitionRange(2, range);
    }

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
          } else if (Slice.CONSTRUCTION.equals(oSlice.getState()) || Slice.RECOVERY.equals(oSlice.getState()))  {
            // delete the shards
            for (String sub : subSlices) {
              log.info("Sub-shard: {} already exists therefore requesting its deletion", sub);
              Map<String, Object> propMap = new HashMap<String, Object>();
              propMap.put(Overseer.QUEUE_OPERATION, "deleteshard");
              propMap.put(COLLECTION_PROP, collectionName);
              propMap.put(SHARD_ID_PROP, sub);
              ZkNodeProps m = new ZkNodeProps(propMap);
              try {
                deleteShard(clusterState, m, new NamedList());
              } catch (Exception e) {
                throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to delete already existing sub shard: " + sub, e);
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

        Map<String, Object> propMap = new HashMap<String, Object>();
        propMap.put(Overseer.QUEUE_OPERATION, "createshard");
        propMap.put(ZkStateReader.SHARD_ID_PROP, subSlice);
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        propMap.put(ZkStateReader.SHARD_RANGE_PROP, subRange.toString());
        propMap.put(ZkStateReader.SHARD_STATE_PROP, Slice.CONSTRUCTION);
        propMap.put(ZkStateReader.SHARD_PARENT_PROP, parentSlice.getName());
        ZkNodeProps m = new ZkNodeProps(propMap);
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        inQueue.offer(ZkStateReader.toJSON(m));

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, subShardName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, subSlice);
        addPropertyParams(message, params);
        sendShardRequest(nodeName, params);
      }

      collectShardResponses(results, true,
          "SPLTSHARD failed to create subshard leaders");

      for (String subShardName : subShardNames) {
        // wait for parent leader to acknowledge the sub-shard core
        log.info("Asking parent leader to wait for: " + subShardName + " to be alive on: " + nodeName);
        String coreNodeName = waitForCoreNodeName(collection, zkStateReader.getBaseUrlForNodeName(nodeName), subShardName);
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
      params.set(CoreAdminParams.RANGES, rangesStr);

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
          addPropertyParams(message, params);
          // TODO:  Figure the config used by the parent shard and use it.
          //params.set("collection.configName", configName);
          
          //Not using this property. Do we really need to use it?
          //params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

          sendShardRequest(subShardNodeName, params);

          String coreNodeName = waitForCoreNodeName(collection, zkStateReader.getBaseUrlForNodeName(subShardNodeName), shardName);
          // wait for the replicas to be seen as active on sub shard leader
          log.info("Asking sub shard leader to wait for: " + shardName + " to be alive on: " + subShardNodeName);
          CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
          cmd.setCoreName(subShardNames.get(i-1));
          cmd.setNodeName(subShardNodeName);
          cmd.setCoreNodeName(coreNodeName);
          cmd.setState(ZkStateReader.RECOVERING);
          cmd.setCheckLive(true);
          cmd.setOnlyIfLeader(true);
          sendShardRequest(nodeName, new ModifiableSolrParams(cmd.getParams()));
        }
      }

      collectShardResponses(results, true,
          "SPLTSHARD failed to create subshard replicas or timed out waiting for them to come up");

      log.info("Successfully created all replica shards for all sub-slices " + subSlices);

      commit(results, slice, parentShardLeader);

      if (repFactor == 1) {
        // switch sub shard states to 'active'
        log.info("Replication factor is 1 so switching shard states");
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
      } else  {
        log.info("Requesting shard state be set to 'recovery'");
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        Map<String, Object> propMap = new HashMap<String, Object>();
        propMap.put(Overseer.QUEUE_OPERATION, "updateshardstate");
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.RECOVERY);
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(ZkStateReader.toJSON(m));
      }

      return true;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Error executing split operation for collection: " + collectionName + " parent shard: " + slice, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, null, e);
    }
  }

  private void commit(NamedList results, String slice, Replica parentShardLeader) {
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
      if(clusterState.hasCollection(collection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No shard with the specified name exists: " + slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No collection with the specified name exists: " + collection);
      }      
    }
    // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
    // TODO: Add check for range gaps on Slice deletion
    if (!(slice.getRange() == null || slice.getState().equals(Slice.INACTIVE) || slice.getState().equals(Slice.RECOVERY))) {
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

  private void migrate(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String sourceCollectionName = message.getStr("collection");
    String splitKey = message.getStr("split.key");
    String targetCollectionName = message.getStr("target.collection");
    int timeout = message.getInt("forward.timeout", 10 * 60) * 1000;

    DocCollection sourceCollection = clusterState.getCollection(sourceCollectionName);
    if (sourceCollection == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown source collection: " + sourceCollectionName);
    }
    DocCollection targetCollection = clusterState.getCollection(targetCollectionName);
    if (targetCollection == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown target collection: " + sourceCollectionName);
    }
    if (!(sourceCollection.getRouter() instanceof CompositeIdRouter))  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Source collection must use a compositeId router");
    }
    if (!(targetCollection.getRouter() instanceof CompositeIdRouter))  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Target collection must use a compositeId router");
    }
    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    CompositeIdRouter targetRouter = (CompositeIdRouter) targetCollection.getRouter();
    Collection<Slice> sourceSlices = sourceRouter.getSearchSlicesSingle(splitKey, null, sourceCollection);
    if (sourceSlices.isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No active slices available in source collection: " + sourceCollection + "for given split.key: " + splitKey);
    }
    Collection<Slice> targetSlices = targetRouter.getSearchSlicesSingle(splitKey, null, targetCollection);
    if (targetSlices.isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No active slices available in target collection: " + targetCollection + "for given split.key: " + splitKey);
    }

    for (Slice sourceSlice : sourceSlices) {
      for (Slice targetSlice : targetSlices) {
        log.info("Migrating source shard: {} to target shard: {} for split.key = " + splitKey, sourceSlice, targetSlice);
        migrateKey(clusterState, sourceCollection, sourceSlice, targetCollection, targetSlice, splitKey, timeout, results);
      }
    }
  }

  private void migrateKey(ClusterState clusterState, DocCollection sourceCollection, Slice sourceSlice, DocCollection targetCollection, Slice targetSlice, String splitKey, int timeout, NamedList results) throws KeeperException, InterruptedException {
    String tempSourceCollectionName = "split_" + sourceSlice.getName() + "_temp_" + targetSlice.getName();
    if (clusterState.hasCollection(tempSourceCollectionName)) {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      Map<String, Object> props = ZkNodeProps.makeMap(
          QUEUE_OPERATION, DELETECOLLECTION,
          "name", tempSourceCollectionName);
      try {
        deleteCollection(new ZkNodeProps(props), results);
      } catch (Exception e) {
        log.warn("Unable to clean up existing temporary collection: " + tempSourceCollectionName, e);
      }
    }

    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    DocRouter.Range keyHashRange = sourceRouter.keyHashRange(splitKey);

    log.info("Hash range for split.key: {} is: {}", splitKey, keyHashRange);
    // intersect source range, keyHashRange and target range
    // this is the range that has to be split from source and transferred to target
    DocRouter.Range splitRange = intersect(targetSlice.getRange(), intersect(sourceSlice.getRange(), keyHashRange));
    if (splitRange == null) {
      log.info("No common hashes between source shard: {} and target shard: {}", sourceSlice.getName(), targetSlice.getName());
      return;
    }
    log.info("Common hash range between source shard: {} and target shard: {} = " + splitRange, sourceSlice.getName(), targetSlice.getName());

    Replica targetLeader = zkStateReader.getLeaderRetry(targetCollection.getName(), targetSlice.getName(), 10000);

    log.info("Asking target leader node: " + targetLeader.getNodeName() + " core: "
        + targetLeader.getStr("core") + " to buffer updates");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTBUFFERUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));
    sendShardRequest(targetLeader.getNodeName(), params);
    collectShardResponses(results, true, "MIGRATE failed to request node to buffer updates");

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, Overseer.ADD_ROUTING_RULE,
        COLLECTION_PROP, sourceCollection.getName(),
        SHARD_ID_PROP, sourceSlice.getName(),
        "routeKey", SolrIndexSplitter.getRouteKey(splitKey) + "!",
        "range", splitRange.toString(),
        "targetCollection", targetCollection.getName(),
        "expireAt", String.valueOf(System.currentTimeMillis() + timeout));
    log.info("Adding routing rule: " + m);
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(
        ZkStateReader.toJSON(m));

    // wait for a while until we see the new rule
    log.info("Waiting to see routing rule updated in clusterstate");
    long waitUntil = System.currentTimeMillis() + 60000;
    boolean added = false;
    while (System.currentTimeMillis() < waitUntil) {
      Thread.sleep(100);
      Map<String, RoutingRule> rules = zkStateReader.getClusterState().getSlice(sourceCollection.getName(), sourceSlice.getName()).getRoutingRules();
      if (rules != null) {
        RoutingRule rule = rules.get(SolrIndexSplitter.getRouteKey(splitKey) + "!");
        if (rule != null && rule.getRouteRanges().contains(splitRange)) {
          added = true;
          break;
        }
      }
    }
    if (!added) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not add routing rule: " + m);
    }

    log.info("Routing rule added successfully");

    // Create temp core on source shard
    Replica sourceLeader = zkStateReader.getLeaderRetry(sourceCollection.getName(), sourceSlice.getName(), 10000);

    // create a temporary collection with just one node on the shard leader
    String configName = zkStateReader.readConfigName(sourceCollection.getName());
    Map<String, Object> props = ZkNodeProps.makeMap(
        QUEUE_OPERATION, CREATECOLLECTION,
        "name", tempSourceCollectionName,
        REPLICATION_FACTOR, 1,
        NUM_SLICES, 1,
        COLL_CONF, configName,
        CREATE_NODE_SET, sourceLeader.getNodeName());
    log.info("Creating temporary collection: " + props);
    createCollection(clusterState, new ZkNodeProps(props), results);
    // refresh cluster state
    clusterState = zkStateReader.getClusterState();
    Slice tempSourceSlice = clusterState.getCollection(tempSourceCollectionName).getSlices().iterator().next();
    Replica tempSourceLeader = zkStateReader.getLeaderRetry(tempSourceCollectionName, tempSourceSlice.getName(), 120000);

    String tempCollectionReplica1 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica1";
    String coreNodeName = waitForCoreNodeName(clusterState.getCollection(tempSourceCollectionName),
        zkStateReader.getBaseUrlForNodeName(sourceLeader.getNodeName()), tempCollectionReplica1);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking source leader to wait for: " + tempCollectionReplica1 + " to be alive on: " + sourceLeader.getNodeName());
    CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempCollectionReplica1);
    cmd.setNodeName(sourceLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(ZkStateReader.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    sendShardRequest(tempSourceLeader.getNodeName(), new ModifiableSolrParams(cmd.getParams()));

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection leader or timed out waiting for it to come up");

    log.info("Asking source leader to split index");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
    params.set(CoreAdminParams.CORE, sourceLeader.getStr("core"));
    params.add(CoreAdminParams.TARGET_CORE, tempSourceLeader.getStr("core"));
    params.set(CoreAdminParams.RANGES, splitRange.toString());
    params.set("split.key", splitKey);

    sendShardRequest(sourceLeader.getNodeName(), params);
    collectShardResponses(results, true, "MIGRATE failed to invoke SPLIT core admin command");

    log.info("Creating a replica of temporary collection: {} on the target leader node: {}",
        tempSourceCollectionName, targetLeader.getNodeName());
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
    String tempCollectionReplica2 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica2";
    params.set(CoreAdminParams.NAME, tempCollectionReplica2);
    params.set(CoreAdminParams.COLLECTION, tempSourceCollectionName);
    params.set(CoreAdminParams.SHARD, tempSourceSlice.getName());
    sendShardRequest(targetLeader.getNodeName(), params);

    coreNodeName = waitForCoreNodeName(clusterState.getCollection(tempSourceCollectionName),
        zkStateReader.getBaseUrlForNodeName(targetLeader.getNodeName()), tempCollectionReplica2);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking temp source leader to wait for: " + tempCollectionReplica2 + " to be alive on: " + targetLeader.getNodeName());
    cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempSourceLeader.getStr("core"));
    cmd.setNodeName(targetLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(ZkStateReader.ACTIVE); // todo introduce asynchronous actions
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    sendShardRequest(tempSourceLeader.getNodeName(), new ModifiableSolrParams(cmd.getParams()));

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection replica or timed out waiting for them to come up");

    log.info("Successfully created replica of temp source collection on target leader node");

    log.info("Requesting merge of temp source collection replica to target leader");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.MERGEINDEXES.toString());
    params.set(CoreAdminParams.CORE, targetLeader.getStr("core"));
    params.set(CoreAdminParams.SRC_CORE, tempCollectionReplica2);
    sendShardRequest(targetLeader.getNodeName(), params);
    collectShardResponses(results, true,
        "MIGRATE failed to merge " + tempCollectionReplica2 + " to " + targetLeader.getStr("core") + " on node: " + targetLeader.getNodeName());

    log.info("Asking target leader to apply buffered updates");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));
    sendShardRequest(targetLeader.getNodeName(), params);
    collectShardResponses(results, true,
        "MIGRATE failed to request node to apply buffered updates");

    try {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      props = ZkNodeProps.makeMap(
          QUEUE_OPERATION, DELETECOLLECTION,
          "name", tempSourceCollectionName);
      deleteCollection(new ZkNodeProps(props), results);
    } catch (Exception e) {
      log.error("Unable to delete temporary collection: " + tempSourceCollectionName
          + ". Please remove it manually", e);
    }
  }

  private DocRouter.Range intersect(DocRouter.Range a, DocRouter.Range b) {
    if (a == null || b == null || !a.overlaps(b)) {
      return null;
    } else if (a.isSubsetOf(b))
      return a;
    else if (b.isSubsetOf(a))
      return b;
    else if (b.includes(a.max)) {
      return new DocRouter.Range(b.min, a.max);
    } else  {
      return new DocRouter.Range(a.min, b.max);
    }
  }

  private void sendShardRequest(String nodeName, ModifiableSolrParams params) {
    ShardRequest sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;

    shardHandler.submit(sreq, replica, sreq.params);
  }

  private void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }
  private void createCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String collectionName = message.getStr("name");
    if (clusterState.hasCollection(collectionName)) {
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
        throw new SolrException(ErrorCode.BAD_REQUEST, REPLICATION_FACTOR + " must be greater than 0");
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
      String configName = createConfNode(collectionName, message);

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
          addPropertyParams(message, params);

          ShardRequest sreq = new ShardRequest();
          params.set("qt", adminPath);
          sreq.purpose = 1;
          String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
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

  private String createConfNode(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    String configName = message.getStr(OverseerCollectionProcessor.COLL_CONF);
    if(configName == null){
      // if there is only one conf, use that
      List<String> configNames=null;
      try {
        configNames = zkStateReader.getZkClient().getChildren(ZkController.CONFIGS_ZKNODE, null, true);
        if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          log.info("Only one config set found in zk - using it:" + configName);
        }
      } catch (KeeperException.NoNodeException e) {

      }

    }

    if(configName!= null){
      log.info("creating collections conf node {} ",ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll);
      zkStateReader.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll,
          ZkStateReader.toJSON(ZkNodeProps.makeMap(ZkController.CONFIGNAME_PROP,configName)),true );

    } else {
      String msg = "Could not obtain config name";
      log.warn(msg);
    }
    return configName;

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
