package org.apache.solr.handler.admin;

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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ASYNC;
import static org.apache.solr.cloud.OverseerCollectionProcessor.COLL_CONF;
import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SLICE_UNIQUE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ONLY_IF_DOWN;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REPLICATION_FACTOR;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REQUESTID;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ROUTER;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SPLITSHARD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestSyncShard;
import org.apache.solr.cloud.DistributedQueue;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class CollectionsHandler extends RequestHandlerBase {
  protected static Logger log = LoggerFactory.getLogger(CollectionsHandler.class);
  protected final CoreContainer coreContainer;

  public CollectionsHandler() {
    super();
    // Unlike most request handlers, CoreContainer initialization 
    // should happen in the constructor...  
    this.coreContainer = null;
  }


  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CollectionsHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @Override
  final public void init(NamedList args) {

  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance that created this
   * handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Core container instance missing");
    }

    // Make sure that the core is ZKAware
    if(!cores.isZooKeeperAware()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Pick the action
    SolrParams params = req.getParams();
    CollectionAction action = null;
    String a = params.get(CoreAdminParams.ACTION);
    if (a != null) {
      action = CollectionAction.get(a);
    }
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown action: " + a);
    }

    switch (action) {
      case CREATE: {
        this.handleCreateAction(req, rsp);
        break;
      }
      case DELETE: {
        this.handleDeleteAction(req, rsp);
        break;
      }
      case RELOAD: {
        this.handleReloadAction(req, rsp);
        break;
      }
      case SYNCSHARD: {
        this.handleSyncShardAction(req, rsp);
        break;
      }
      case CREATEALIAS: {
        this.handleCreateAliasAction(req, rsp);
        break;
      }
      case DELETEALIAS: {
        this.handleDeleteAliasAction(req, rsp);
        break;
      }
      case SPLITSHARD:  {
        this.handleSplitShardAction(req, rsp);
        break;
      }
      case DELETESHARD: {
        this.handleDeleteShardAction(req, rsp);
        break;
      }
      case CREATESHARD: {
        this.handleCreateShard(req, rsp);
        break;
      }
      case DELETEREPLICA: {
        this.handleRemoveReplica(req, rsp);
        break;
      }
      case MIGRATE: {
        this.handleMigrate(req, rsp);
        break;
      }
      case ADDROLE: {
        handleRole(ADDROLE, req, rsp);
        break;
      }
      case REMOVEROLE: {
        handleRole(REMOVEROLE, req, rsp);
        break;
      }
      case CLUSTERPROP: {
        this.handleProp(req, rsp);
        break;
      }
      case ADDREPLICA:  {
        this.handleAddReplica(req, rsp);
        break;
      }
      case REQUESTSTATUS: {
        this.handleRequestStatus(req, rsp);
        break;
      }
      case OVERSEERSTATUS:  {
        this.handleOverseerStatus(req, rsp);
        break;
      }
      case LIST: {
        this.handleListAction(req, rsp);
        break;
      }
      case CLUSTERSTATUS:  {
        this.handleClusterStatus(req, rsp);
        break;
      }
      case ADDREPLICAPROP: {
        this.handleAddReplicaProp(req, rsp);
        break;
      }
      case DELETEREPLICAPROP: {
        this.handleDeleteReplicaProp(req, rsp);
        break;
      }

      default: {
          throw new RuntimeException("Unknown action: " + action);
      }
    }

    rsp.setHttpCaching(false);
  }

  private void handleAddReplicaProp(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP, PROPERTY_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_VALUE_PROP);


    Map<String, Object> map = ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    copyIfNotNull(req.getParams(), map, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP,
        SLICE_UNIQUE, PROPERTY_VALUE_PROP);

    String property = (String) map.get(PROPERTY_PROP);
    if (property.startsWith(OverseerCollectionProcessor.COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionProcessor.COLL_PROP_PREFIX + property;
    }

    boolean uniquePerSlice = Boolean.parseBoolean((String) map.get(SLICE_UNIQUE));

    // Check if we're trying to set a property with parameters that allow us to set the property on multiple replicas
    // in a slice on properties that are known to only be one-per-slice and error out if so.
    if (StringUtils.isNotBlank((String)map.get(SLICE_UNIQUE)) &&
        Overseer.sliceUniqueBooleanProperties.contains(property.toLowerCase(Locale.ROOT)) &&
        uniquePerSlice == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer replica property command received for property " + property +
              " with the " + SLICE_UNIQUE +
              " parameter set to something other than 'true'. No action taken.");
    }
    handleResponse(ADDREPLICAPROP.toLower(), new ZkNodeProps(map), rsp);
  }

  private void handleDeleteReplicaProp(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP, PROPERTY_PROP, SHARD_ID_PROP, REPLICA_PROP);

    Map<String, Object> map = ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    copyIfNotNull(req.getParams(), map, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);

    handleResponse(DELETEREPLICAPROP.toLower(), new ZkNodeProps(map), rsp);
  }



  private void handleOverseerStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    Map<String, Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION, OVERSEERSTATUS.toLower());
    handleResponse(OVERSEERSTATUS.toLower(), new ZkNodeProps(props), rsp);
  }

  private void handleProp(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check("name");
    String name = req.getParams().get("name");
    if(!OverseerCollectionProcessor.KNOWN_CLUSTER_PROPS.contains(name)){
      throw new SolrException(ErrorCode.BAD_REQUEST, "Not a known cluster property "+ name);
    }

    Map<String,Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION, CLUSTERPROP.toLower() );
    copyIfNotNull(req.getParams(),props,
        "name",
        "val");

    Overseer.getInQueue(coreContainer.getZkController().getZkClient()).offer(ZkStateReader.toJSON(props)) ;
  }

  static Set<String> KNOWN_ROLES = ImmutableSet.of("overseer");

  private void handleRole(CollectionAction action, SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check("role", "node");
    Map<String, Object> map = ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION, action.toLower());
    copyIfNotNull(req.getParams(), map,"role", "node");
    ZkNodeProps m = new ZkNodeProps(map);
    if(!KNOWN_ROLES.contains(m.getStr("role"))) throw new SolrException(ErrorCode.BAD_REQUEST,"Unknown role. Supported roles are ,"+ KNOWN_ROLES);
    handleResponse(action.toString().toLowerCase(Locale.ROOT), m, rsp);
  }

  public static long DEFAULT_ZK_TIMEOUT = 180*1000;

  private void handleRequestStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.debug("REQUESTSTATUS action invoked: " + req.getParamString());
    req.getParams().required().check(REQUESTID);

    String requestId = req.getParams().get(REQUESTID);

    if (requestId.equals("-1")) {
      // Special taskId (-1), clears up the request state maps.
      if(requestId.equals("-1")) {
        coreContainer.getZkController().getOverseerCompletedMap().clear();
        coreContainer.getZkController().getOverseerFailureMap().clear();
        return;
      }
    } else {
      NamedList<Object> results = new NamedList<>();
      if (coreContainer.getZkController().getOverseerCompletedMap().contains(requestId)) {
        SimpleOrderedMap success = new SimpleOrderedMap();
        success.add("state", "completed");
        success.add("msg", "found " + requestId + " in completed tasks");
        results.add("status", success);
      } else if (coreContainer.getZkController().getOverseerFailureMap().contains(requestId)) {
        SimpleOrderedMap success = new SimpleOrderedMap();
        success.add("state", "failed");
        success.add("msg", "found " + requestId + " in failed tasks");
        results.add("status", success);
      } else if (coreContainer.getZkController().getOverseerRunningMap().contains(requestId)) {
        SimpleOrderedMap success = new SimpleOrderedMap();
        success.add("state", "running");
        success.add("msg", "found " + requestId + " in running tasks");
        results.add("status", success);
      } else if(overseerCollectionQueueContains(requestId)){
        SimpleOrderedMap success = new SimpleOrderedMap();
        success.add("state", "submitted");
        success.add("msg", "found " + requestId + " in submitted tasks");
        results.add("status", success);
      } else {
        SimpleOrderedMap failure = new SimpleOrderedMap();
        failure.add("state", "notfound");
        failure.add("msg", "Did not find taskid [" + requestId + "] in any tasks queue");
        results.add("status", failure);
      }
      SolrResponse response = new OverseerSolrResponse(results);

      rsp.getValues().addAll(response.getResponse());
    }
  }

  private boolean overseerCollectionQueueContains(String asyncId) throws KeeperException, InterruptedException {
    DistributedQueue collectionQueue = coreContainer.getZkController().getOverseerCollectionQueue();
    return collectionQueue.containsTaskWithRequestId(asyncId);
  }

  private void handleResponse(String operation, ZkNodeProps m,
                              SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    handleResponse(operation, m, rsp, DEFAULT_ZK_TIMEOUT);
  }
  
  private void handleResponse(String operation, ZkNodeProps m,
      SolrQueryResponse rsp, long timeout) throws KeeperException, InterruptedException {
    long time = System.nanoTime();

     if(m.containsKey(ASYNC) && m.get(ASYNC) != null) {
 
       String asyncId = m.getStr(ASYNC);
 
       if(asyncId.equals("-1")) {
         throw new SolrException(ErrorCode.BAD_REQUEST, "requestid can not be -1. It is reserved for cleanup purposes.");
       }
 
       NamedList<String> r = new NamedList<>();
 
       if (coreContainer.getZkController().getOverseerCompletedMap().contains(asyncId) ||
           coreContainer.getZkController().getOverseerFailureMap().contains(asyncId) ||
           coreContainer.getZkController().getOverseerRunningMap().contains(asyncId) ||
           overseerCollectionQueueContains(asyncId)) {
         r.add("error", "Task with the same requestid already exists.");
 
       } else {
         coreContainer.getZkController().getOverseerCollectionQueue()
             .offer(ZkStateReader.toJSON(m));
       }
       r.add(CoreAdminParams.REQUESTID, (String) m.get(ASYNC));
       SolrResponse response = new OverseerSolrResponse(r);
 
       rsp.getValues().addAll(response.getResponse());
 
       return;
     }

    QueueEvent event = coreContainer.getZkController()
        .getOverseerCollectionQueue()
        .offer(ZkStateReader.toJSON(m), timeout);
    if (event.getBytes() != null) {
      SolrResponse response = SolrResponse.deserialize(event.getBytes());
      rsp.getValues().addAll(response.getResponse());
      SimpleOrderedMap exp = (SimpleOrderedMap) response.getResponse().get("exception");
      if (exp != null) {
        Integer code = (Integer) exp.get("rspCode");
        rsp.setException(new SolrException(code != null && code != -1 ? ErrorCode.getErrorCode(code) : ErrorCode.SERVER_ERROR, (String)exp.get("msg")));
      }
    } else {
      if (System.nanoTime() - time >= TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection time out:" + timeout / 1000 + "s");
      } else if (event.getWatchedEvent() != null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection error [Watcher fired on path: "
            + event.getWatchedEvent().getPath() + " state: "
            + event.getWatchedEvent().getState() + " type "
            + event.getWatchedEvent().getType() + "]");
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection unkown case");
      }
    }
  }
  
  private void handleReloadAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Reloading Collection : " + req.getParamString());
    String name = req.getParams().required().get("name");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        RELOAD.toLower(), "name", name);

    handleResponse(RELOAD.toLower(), m, rsp);
  }
  
  private void handleSyncShardAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException, SolrServerException, IOException {
    log.info("Syncing shard : " + req.getParamString());
    String collection = req.getParams().required().get("collection");
    String shard = req.getParams().required().get("shard");
    
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    
    ZkNodeProps leaderProps = clusterState.getLeader(collection, shard);
    ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
    
    HttpSolrServer server = new HttpSolrServer(nodeProps.getBaseUrl());
    try {
      server.setConnectionTimeout(15000);
      server.setSoTimeout(60000);
      RequestSyncShard reqSyncShard = new CoreAdminRequest.RequestSyncShard();
      reqSyncShard.setCollection(collection);
      reqSyncShard.setShard(shard);
      reqSyncShard.setCoreName(nodeProps.getCoreName());
      server.request(reqSyncShard);
    } finally {
      server.shutdown();
    }
  }
  
  private void handleCreateAliasAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws Exception {
    log.info("Create alias action : " + req.getParamString());
    String name = req.getParams().required().get("name");
    String collections = req.getParams().required().get("collections");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        CREATEALIAS.toLower(), "name", name, "collections",
        collections);
    
    handleResponse(CREATEALIAS.toLower(), m, rsp);
  }
  
  private void handleDeleteAliasAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws Exception {
    log.info("Delete alias action : " + req.getParamString());
    String name = req.getParams().required().get("name");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        DELETEALIAS.toLower(), "name", name);
    
    handleResponse(DELETEALIAS.toLower(), m, rsp);
  }

  private void handleDeleteAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Deleting Collection : " + req.getParamString());
    
    String name = req.getParams().required().get("name");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        DELETE.toLower(), "name", name);

    handleResponse(DELETE.toLower(), m, rsp);
  }

  // very simple currently, you can pass a template collection, and the new collection is created on
  // every node the template collection is on
  // there is a lot more to add - you should also be able to create with an explicit server list
  // we might also want to think about error handling (add the request to a zk queue and involve overseer?)
  // as well as specific replicas= options
  private void handleCreateAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws InterruptedException, KeeperException {
    log.info("Creating Collection : " + req.getParamString());
    String name = req.getParams().required().get("name");
    if (name == null) {
      log.error("Collection name is required to create a new collection");
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Collection name is required to create a new collection");
    }
    
    Map<String,Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION,
        CREATE.toLower(),
        "fromApi","true");
    copyIfNotNull(req.getParams(),props,
        "name",
        REPLICATION_FACTOR,
         COLL_CONF,
         NUM_SLICES,
         MAX_SHARDS_PER_NODE,
         CREATE_NODE_SET,
         SHARDS_PROP,
         ASYNC,
         DocCollection.STATE_FORMAT,
         AUTO_ADD_REPLICAS,
        "router.");

    copyPropertiesIfNotNull(req.getParams(), props);

    ZkNodeProps m = new ZkNodeProps(props);
    handleResponse(CREATE.toLower(), m, rsp);
  }

  private void handleRemoveReplica(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Remove replica: " + req.getParamString());
    req.getParams().required().check(COLLECTION_PROP, SHARD_ID_PROP, "replica");
    Map<String, Object> map = makeMap(QUEUE_OPERATION, DELETEREPLICA.toLower());
    copyIfNotNull(req.getParams(),map,COLLECTION_PROP,SHARD_ID_PROP,"replica", ASYNC, ONLY_IF_DOWN);
    ZkNodeProps m = new ZkNodeProps(map);
    handleResponse(DELETEREPLICA.toLower(), m, rsp);
  }


  private void handleCreateShard(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Create shard: " + req.getParamString());
    req.getParams().required().check(COLLECTION_PROP, SHARD_ID_PROP);
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    if(!ImplicitDocRouter.NAME.equals( ((Map) clusterState.getCollection(req.getParams().get(COLLECTION_PROP)).get(ROUTER)).get("name") )  )
      throw new SolrException(ErrorCode.BAD_REQUEST, "shards can be added only to 'implicit' collections" );

    Map<String, Object> map = makeMap(QUEUE_OPERATION, CREATESHARD.toLower());
    copyIfNotNull(req.getParams(),map,COLLECTION_PROP, SHARD_ID_PROP, ZkStateReader.REPLICATION_FACTOR, CREATE_NODE_SET, ASYNC);
    copyPropertiesIfNotNull(req.getParams(), map);
    ZkNodeProps m = new ZkNodeProps(map);
    handleResponse(CREATESHARD.toLower(), m, rsp);
  }

  private static void copyIfNotNull(SolrParams params, Map<String, Object> props, String... keys) {
    ArrayList<String> prefixes = new ArrayList<>(1);
    if(keys !=null){
      for (String key : keys) {
        if(key.endsWith(".")) {
          prefixes.add(key);
          continue;
        }
        String v = params.get(key);
        if(v != null) props.put(key,v);
      }
    }
    if(prefixes.isEmpty()) return;
    Iterator<String> it = params.getParameterNamesIterator();
    String prefix = null;
    for(;it.hasNext();){
      String name = it.next();
      for (int i = 0; i < prefixes.size(); i++) {
        if(name.startsWith(prefixes.get(i))){
          String val = params.get(name);
          if(val !=null) props.put(name,val);
        }
      }
    }

  }

  private void copyPropertiesIfNotNull(SolrParams params, Map<String, Object> props) {
    Iterator<String> iter =  params.getParameterNamesIterator();
    while (iter.hasNext()) {
      String param = iter.next();
      if (param.startsWith(OverseerCollectionProcessor.COLL_PROP_PREFIX)) {
        props.put(param, params.get(param));
      }
    }
  }


  private void handleDeleteShardAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws InterruptedException, KeeperException {
    log.info("Deleting Shard : " + req.getParamString());
    String name = req.getParams().required().get(ZkStateReader.COLLECTION_PROP);
    String shard = req.getParams().required().get(ZkStateReader.SHARD_ID_PROP);
    
    Map<String,Object> props = new HashMap<>();
    props.put(ZkStateReader.COLLECTION_PROP, name);
    props.put(Overseer.QUEUE_OPERATION, DELETESHARD.toLower());
    props.put(ZkStateReader.SHARD_ID_PROP, shard);

    ZkNodeProps m = new ZkNodeProps(props);
    handleResponse(DELETESHARD.toLower(), m, rsp);
  }

  private void handleSplitShardAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Splitting shard : " + req.getParamString());
    String name = req.getParams().required().get("collection");
    // TODO : add support for multiple shards
    String shard = req.getParams().get("shard");
    String rangesStr = req.getParams().get(CoreAdminParams.RANGES);
    String splitKey = req.getParams().get("split.key");

    if (splitKey == null && shard == null) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: shard");
    }
    if (splitKey != null && shard != null)  {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Only one of 'shard' or 'split.key' should be specified");
    }
    if (splitKey != null && rangesStr != null)  {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Only one of 'ranges' or 'split.key' should be specified");
    }

    Map<String,Object> props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, SPLITSHARD.toLower());
    props.put("collection", name);
    if (shard != null)  {
      props.put(ZkStateReader.SHARD_ID_PROP, shard);
    }
    if (splitKey != null) {
      props.put("split.key", splitKey);
    }
    if (rangesStr != null)  {
      props.put(CoreAdminParams.RANGES, rangesStr);
    }

    if (req.getParams().get(ASYNC) != null)
      props.put(ASYNC, req.getParams().get(ASYNC));

    copyPropertiesIfNotNull(req.getParams(), props);

    ZkNodeProps m = new ZkNodeProps(props);

    handleResponse(SPLITSHARD.toLower(), m, rsp, DEFAULT_ZK_TIMEOUT * 5);
  }

  private void handleMigrate(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Migrate action invoked: " + req.getParamString());
    req.getParams().required().check("collection", "split.key", "target.collection");
    Map<String,Object> props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, MIGRATE.toLower());
    copyIfNotNull(req.getParams(), props, "collection", "split.key", "target.collection", "forward.timeout", ASYNC);
    ZkNodeProps m = new ZkNodeProps(props);
    handleResponse(MIGRATE.toLower(), m, rsp, DEFAULT_ZK_TIMEOUT * 20);
  }

  private void handleAddReplica(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException  {
    log.info("Add replica action invoked: " + req.getParamString());
    Map<String,Object> props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, CollectionAction.ADDREPLICA.toString());
    copyIfNotNull(req.getParams(), props, COLLECTION_PROP, "node", SHARD_ID_PROP, ShardParams._ROUTE_,
        CoreAdminParams.NAME, CoreAdminParams.INSTANCE_DIR, CoreAdminParams.DATA_DIR, ASYNC);
    ZkNodeProps m = new ZkNodeProps(props);
    handleResponse(CollectionAction.ADDREPLICA.toString(), m, rsp);
  }

  /**
   * Handle cluster status request.
   * Can return status per specific collection/shard or per all collections.
   *
   * @param req solr request
   * @param rsp solr response
   */
  private void handleClusterStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    Map<String,Object> props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, CollectionAction.CLUSTERSTATUS.toLower());
    copyIfNotNull(req.getParams(), props, COLLECTION_PROP, SHARD_ID_PROP, ShardParams._ROUTE_);
    handleResponse(CollectionAction.CLUSTERSTATUS.toString(), new ZkNodeProps(props), rsp);
  }

  /**
   * Handled list collection request.
   * Do list collection request to zk host
   *
   * @param req solr request
   * @param rsp solr response
   * @throws KeeperException      zk connection failed
   * @throws InterruptedException connection interrupted
   */
  private void handleListAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    Map<String, Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION, CollectionAction.LIST.toString().toLowerCase(Locale.ROOT));
    handleResponse(CollectionAction.LIST.toString(), new ZkNodeProps(props), rsp);
  }


  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage SolrCloud Collections";
  }

}
