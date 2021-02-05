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
package org.apache.solr.cloud.api.collections;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.LockTree;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerMessageHandler;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;
import static org.apache.solr.client.solrj.response.RequestStatusState.FAILED;
import static org.apache.solr.client.solrj.response.RequestStatusState.NOT_FOUND;
import static org.apache.solr.client.solrj.response.RequestStatusState.RUNNING;
import static org.apache.solr.client.solrj.response.RequestStatusState.SUBMITTED;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ALIASPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MAINTAINROUTEDALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_COLL_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_REPLICA_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_SHARD_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REBALANCELEADERS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REINDEXCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RENAME;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REPLACENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RESTORE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SPLITSHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related
 * overseer messages.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler, SolrCloseable {

  public static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String REQUESTID = "requestid";

  public static final String COLL_PROP_PREFIX = "property.";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  public static final String SHARD_UNIQUE = "shardUnique";

  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  public static final Map<String, Object> COLLECTION_PROPS_AND_DEFAULTS = Collections.unmodifiableMap(makeMap(
          ROUTER, DocRouter.DEFAULT_NAME,
          ZkStateReader.REPLICATION_FACTOR, "1",
          ZkStateReader.NRT_REPLICAS, "1",
          ZkStateReader.TLOG_REPLICAS, "0",
          ZkStateReader.PULL_REPLICAS, "0",
          ZkStateReader.MAX_SHARDS_PER_NODE, "1",
          WITH_COLLECTION, null,
          COLOCATED_WITH, null));

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FAILURE_FIELD = "failure";
  public static final String SUCCESS_FIELD = "success";
  final LBHttp2SolrClient overseerLbClient;

  Overseer overseer;
  HttpShardHandlerFactory shardHandlerFactory;
  String adminPath;
  ZkStateReader zkStateReader;
  SolrCloudManager cloudManager;
  String myId;
  Stats stats;
  TimeSource timeSource;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  final private LockTree lockTree = new LockTree();

//  ExecutorService tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS,
//      new SynchronousQueue<>(),
//      new SolrNamedThreadFactory("OverseerCollectionMessageHandlerThreadFactory"));

  public static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  final Map<CollectionAction, Cmd> commandMap;

  private volatile boolean isClosed;

  public OverseerCollectionMessageHandler(CoreContainer cc, String myId,
                                          LBHttp2SolrClient overseerLbClient,
                                          String adminPath,
                                          Stats stats,
                                          Overseer overseer) {
    // TODO: can leak single instance of this oddly in AddReplicaTest
    // assert ObjectReleaseTracker.track(this);
    this.zkStateReader = cc.getZkController().getZkStateReader();
    this.shardHandlerFactory = (HttpShardHandlerFactory) cc.getShardHandlerFactory();
    this.overseerLbClient = overseerLbClient;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    this.cloudManager = overseer.getSolrCloudManager();
    this.timeSource = cloudManager.getTimeSource();
    this.isClosed = false;
    commandMap = new ImmutableMap.Builder<CollectionAction, Cmd>()
            .put(REPLACENODE, new ReplaceNodeCmd(this))
            .put(DELETENODE, new DeleteNodeCmd(this))
            .put(BACKUP, new BackupCmd(this))
            .put(RESTORE, new RestoreCmd(this))
            .put(CREATESNAPSHOT, new CreateSnapshotCmd(this))
            .put(DELETESNAPSHOT, new DeleteSnapshotCmd(this))
            .put(SPLITSHARD, new SplitShardCmd(this))
            .put(MOCK_COLL_TASK, this::mockOperation)
            .put(MOCK_SHARD_TASK, this::mockOperation)
            .put(MOCK_REPLICA_TASK, this::mockOperation)
            .put(CREATESHARD, new CreateShardCmd(this))
            .put(MIGRATE, new MigrateCmd(this))
            .put(CREATE, new CreateCollectionCmd(this, overseer.getCoreContainer(), cloudManager))
            .put(MODIFYCOLLECTION, this::modifyCollection)
            .put(ADDREPLICAPROP, this::processReplicaAddPropertyCommand)
            .put(DELETEREPLICAPROP, this::processReplicaDeletePropertyCommand)
            .put(BALANCESHARDUNIQUE, this::balanceProperty)
            .put(REBALANCELEADERS, this::processRebalanceLeaders)
            .put(RELOAD, this::reloadCollection)
            .put(DELETE, new DeleteCollectionCmd(this))
            .put(CREATEALIAS, new CreateAliasCmd(this))
            .put(DELETEALIAS, new DeleteAliasCmd(this))
            .put(ALIASPROP, new SetAliasPropCmd(this))
            .put(MAINTAINROUTEDALIAS, new MaintainRoutedAliasCmd(this))
            .put(OVERSEERSTATUS, new OverseerStatusCmd(this))
            .put(DELETESHARD, new DeleteShardCmd(this))
            .put(DELETEREPLICA, new DeleteReplicaCmd(this))
            .put(ADDREPLICA, new AddReplicaCmd(this))
            .put(MOVEREPLICA, new MoveReplicaCmd(this))
            .put(REINDEXCOLLECTION, new ReindexCollectionCmd(this))
            .put(RENAME, new RenameCmd(this))
            .build()
    ;
  }

  @Override
  @SuppressWarnings("unchecked")
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation, ZkStateWriter zkWriter) throws InterruptedException {
    MDCLoggingContext.setCollection(message.getStr(COLLECTION));
    MDCLoggingContext.setCoreName(message.getStr(REPLICA_PROP));
    if (log.isDebugEnabled()) log.debug("OverseerCollectionMessageHandler.processMessage : {} , {}", operation, message);

    ClusterState clusterState = zkWriter.getClusterstate(false);
    @SuppressWarnings({"rawtypes"}) NamedList results = new NamedList();
    try {
      String collection = message.getStr("collection");
      if (collection == null) {
        collection = message.getStr("name");
      }

      if (operation.equals("cleanup")) {
        log.info("Found item that needs cleanup {}", message);
        String op = message.getStr(Overseer.QUEUE_OPERATION);
        CollectionAction action = getCollectionAction(op);
        Cmd command = commandMap.get(action);
        boolean drop = command.cleanup(message);
        if (drop) {
          return null;
        }
        return new OverseerSolrResponse(null);
      }

      CollectionAction action = getCollectionAction(operation);
      Cmd command = commandMap.get(action);
      if (command != null) {
        AddReplicaCmd.Response responce = command.call(clusterState, message, results);
        if (responce == null) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "CMD did not return a response:" + operation);
        }

        if (log.isDebugEnabled()) log.debug("Command returned clusterstate={} results={}", responce.clusterState, results);

        if (responce.clusterState != null) {
          DocCollection docColl = responce.clusterState.getCollectionOrNull(collection);
          Map<String,DocCollection> collectionStates = null;
          if (docColl != null) {
            log.info("create new single collection state for collection {}", docColl.getName());
            collectionStates = new HashMap<>();
            collectionStates.put(docColl.getName(), docColl);
          } else {
            log.info("collection not found in returned state {} {}", collection, responce.clusterState);
            if (collection != null) {
              zkWriter.removeCollection(collection);
            }
          }
          if (collectionStates != null) {
            ClusterState cs = ClusterState.getRefCS(collectionStates, -2);
            zkWriter.enqueueUpdate(cs, null, false);
          }

          overseer.writePendingUpdates();
        }

        // nocommit consider
        if (responce != null && responce.asyncFinalRunner != null) {
          AddReplicaCmd.Response resp = responce.asyncFinalRunner.call();
          if (log.isDebugEnabled()) log.debug("Finalize after Command returned clusterstate={}", resp.clusterState);
          if (resp.clusterState != null) {
            DocCollection docColl = resp.clusterState.getCollectionOrNull(collection);
            Map<String,DocCollection> collectionStates;
            if (docColl != null) {
              collectionStates = new HashMap<>();
              collectionStates.put(docColl.getName(), docColl);
            } else {
              collectionStates = new HashMap<>();
            }
            ClusterState cs = ClusterState.getRefCS(collectionStates, -2);

            zkWriter.enqueueUpdate(cs, null, false);
            overseer.writePendingUpdates();
          }
        }

        if (collection != null && responce.clusterState != null) {
          Integer version = zkWriter.lastWrittenVersion(collection);
          if (version != null && !action.equals(DELETE)) {
            results.add("csver", version);
          } else {
            //deleted
          }
        }

      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
      if (results.get("success") == null) results.add("success", new NamedList<>());

      if (results.get("failure") != null) {
        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
        nl.add("msg", "Operation failed " + operation + " " + results.get("failure"));
        nl.add("rspCode", 500);
        results.add("exception", nl);
      }

    } catch (Exception e) {
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr(NAME);

      if (collName == null) {
        if (log.isDebugEnabled()) log.debug("Operation " + operation + " failed", e);
      } else {
        if (log.isDebugEnabled()) log.debug("Collection: " + collName + " operation: " + operation + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
      results.add("exception", nl);
    }

    return new OverseerSolrResponse(results);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
  @SuppressWarnings({"unchecked"})
  private AddReplicaCmd.Response mockOperation(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws InterruptedException {
    //only for test purposes
    Thread.sleep(message.getInt("sleep", 1));
    if (log.isInfoEnabled()) {
      log.info("MOCK_TASK_EXECUTED time {} data {}", System.currentTimeMillis(), Utils.toJSONString(message));
    }
    results.add("MOCK_FINISHED", System.currentTimeMillis());
    return null;
  }

  private CollectionAction getCollectionAction(String operation) {
    CollectionAction action = CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }
    return action;
  }

  @SuppressWarnings({"unchecked"})
  private AddReplicaCmd.Response reloadCollection(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws KeeperException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());

    String asyncId = message.getStr(ASYNC);
    collectionCmd(message, params, results, Replica.State.ACTIVE, asyncId);

    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    response.results = results;
    // nocommit - we don't change this for this cmd, we should be able to indicate that to caller
    response.clusterState = null;
    return response;
  }

  @SuppressWarnings("unchecked")
  private AddReplicaCmd.Response processRebalanceLeaders(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {
    checkRequired(message, COLLECTION_PROP, NODE_NAME_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP, REJOIN_AT_HEAD_PROP);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
    params.set(NODE_NAME_PROP, message.getStr(NODE_NAME_PROP));
    params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
    params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REJOINLEADERELECTION.toString());
    params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
    params.set(ELECTION_NODE_PROP, message.getStr(ELECTION_NODE_PROP));

    String baseUrl = zkStateReader.getBaseUrlForNodeName(message.getStr(message.getStr(NODE_NAME_PROP)));
    ShardRequest sreq = new ShardRequest();
    sreq.nodeName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    // yes, they must use same admin handler path everywhere...
    params.set("qt", adminPath);
    sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
    sreq.shards = new String[] {baseUrl};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler(overseerLbClient);
    shardHandler.submit(sreq, baseUrl, sreq.params);
    return null;
  }

  @SuppressWarnings("unchecked")
  private AddReplicaCmd.Response processReplicaAddPropertyCommand(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, ZkStateReader.NUM_SHARDS_PROP, "shards", REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
    Map<String, Object> propMap = new HashMap<>(message.getProperties().size() + 1);
    propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    overseer.offerStateUpdate(Utils.toJSON(m));
    return null;
  }

  private AddReplicaCmd.Response processReplicaDeletePropertyCommand(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
    Map<String, Object> propMap = new HashMap<>(message.getProperties().size() + 1);
    propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    overseer.offerStateUpdate(Utils.toJSON(m));
    return null;
  }

  private AddReplicaCmd.Response balanceProperty(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
              "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
                      "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
    }
    Map<String, Object> m = new HashMap<>(message.getProperties().size() + 1);
    m.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
    m.putAll(message.getProperties());
    overseer.offerStateUpdate(Utils.toJSON(m));
    return null;
  }

  /**
   * Get collection status from cluster state.
   * Can return collection status by given shard name.
   *
   *
   * @param collection collection map parsed from JSON-serialized {@link ClusterState}
   * @param name  collection name
   * @param requestedShards a set of shards to be returned in the status.
   *                        An empty or null values indicates <b>all</b> shards.
   * @return map of collection properties
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getCollectionStatus(Map<String, Object> collection, String name, Set<String> requestedShards) {
    if (collection == null)  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
    }
    if (requestedShards == null || requestedShards.isEmpty()) {
      return collection;
    } else {
      Map<String, Object> shards = (Map<String, Object>) collection.get("shards");
      Map<String, Object>  selected = new HashMap<>(1);
      for (String selectedShard : requestedShards) {
        if (!shards.containsKey(selectedShard)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " shard: " + selectedShard + " not found");
        }
        selected.put(selectedShard, shards.get(selectedShard));
        collection.put("shards", selected);
      }
      return collection;
    }
  }

  @SuppressWarnings("unchecked")
  AddReplicaCmd.Response deleteReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {
    return ((DeleteReplicaCmd) commandMap.get(DELETEREPLICA)).call(clusterState, message, results);
  }

  void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws Exception {
    ZkNodeProps m = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
            ZkStateReader.CORE_NAME_PROP, core,
            ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
            ZkStateReader.COLLECTION_PROP, collectionName);
    overseer.offerStateUpdate(Utils.toJSON(m));
  }

  void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

  }

  void checkResults(String label, NamedList<Object> results, boolean failureIsFatal) throws SolrException {
    Object failure = results.get("failure");
    if (failure == null) {
      failure = results.get("error");
    }
    if (failure != null) {
      String msg = "Error: " + label + ": " + Utils.toJSONString(results);
      if (failureIsFatal) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      } else {
        log.error(msg);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  void commit(@SuppressWarnings({"rawtypes"})NamedList results, String slice, Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    String coreUrl = parentShardLeader.getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl, overseer.getCoreContainer().getUpdateShardHandler().getTheSharedHttpClient());
      processResponse(results, null, coreUrl, updateResponse, slice, Collections.emptySet());
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      processResponse(results, e, coreUrl, updateResponse, slice, Collections.emptySet());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }


  static UpdateResponse softCommit(String url, Http2SolrClient httpClient) throws SolrServerException, IOException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setBasePath(url);
    ureq.setParams(new ModifiableSolrParams());
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
    return ureq.process(httpClient);
  }

  void waitForNewShard(String collectionName, String sliceName) {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    try {
      zkStateReader.waitForState(collectionName, 30, TimeUnit.SECONDS, (n, c) -> {
        if (c == null)
          return false;
        Slice slice = c.getSlice(sliceName);
        if (slice != null) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException e) {
      String error = "Timeout waiting for new shard.";
      throw new ZkController.NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted", e);
    }
  }

  DocRouter.Range intersect(DocRouter.Range a, DocRouter.Range b) {
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

  void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  void addPropertyParams(ZkNodeProps message, Map<String, Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }


  private AddReplicaCmd.Response modifyCollection(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {

    final String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    //the rest of the processing is based on writing cluster state properties
    //remove the property here to avoid any errors down the pipeline due to this property appearing
    String configName = (String) message.getProperties().remove(CollectionAdminParams.COLL_CONF);

    if (configName != null) {
      validateConfigOrThrowSolrException(configName);

      createConfNode(cloudManager.getDistribStateManager(), configName, collectionName);
      reloadCollection(null, new ZkNodeProps(NAME, collectionName), results);
    }

    clusterState = new CollectionMutator(cloudManager).modifyCollection(clusterState, message);

    // if switching to/from read-only mode reload the collection
    if (message.keySet().contains(ZkStateReader.READ_ONLY)) {
      reloadCollection(null, new ZkNodeProps(NAME, collectionName), results);
    }
    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    response.clusterState = clusterState;
    return response;
  }

  AddReplicaCmd.Response cleanupCollection(String collectionName, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    log.error("Cleaning up collection [{}].", collectionName);
    Map<String, Object> props = makeMap(
            Overseer.QUEUE_OPERATION, DELETE.toLower(),
            NAME, collectionName);
    AddReplicaCmd.Response response = commandMap.get(DELETE).call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);
    return response;
  }

  Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreUrls, boolean requireActive) {
    log.info("wait to see {} in clusterstate {}", coreUrls, zkStateReader.getClusterState().getCollection(collectionName));
    assert coreUrls.size() > 0;

    AtomicReference<Map<String, Replica>> result = new AtomicReference<>();
    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      zkStateReader.waitForState(collectionName, 60, TimeUnit.SECONDS, (n, c) -> { // TODO config timeout up for prod, down for non nightly tests
        if (c == null)
          return false;
        Map<String, Replica> r = new HashMap<>();
        for (String coreUrl : coreUrls) {
          if (r.containsKey(coreUrl)) continue;
          Collection<Slice> slices = c.getSlices();
          if (slices != null) {
            for (Slice slice : slices) {
              for (Replica replica : slice.getReplicas()) {
                if (coreUrl.equals(replica.getCoreUrl()) && ((requireActive ? replica.getState().equals(Replica.State.ACTIVE) : true)
                        && zkStateReader.isNodeLive(replica.getNodeName()))) {
                  r.put(coreUrl, replica);
                  break;
                }
              }
            }
          }
        }

        if (r.size() == coreUrls.size()) {
          result.set(r);
          return true;
        } else {
          errorMessage.set("Timed out waiting to see all replicas: " + coreUrls + " in cluster state. Last state: " + c);
          return false;
        }

      });
    } catch (TimeoutException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state.";
      throw new SolrException(ErrorCode.SERVER_ERROR, error);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted", e);
    }
    return result.get();
  }

  AddReplicaCmd.Response addReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
          throws Exception {

    AddReplicaCmd.Response response = commandMap.get(ADDREPLICA).call(clusterState, message, results);

    return response;
  }

  AddReplicaCmd.Response addReplicaWithResp(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
      throws Exception {

    AddReplicaCmd.Response response = ((AddReplicaCmd) commandMap.get(ADDREPLICA)).call(clusterState, message, results);
    return response;
  }

  void validateConfigOrThrowSolrException(String configName) throws IOException, KeeperException, InterruptedException {
    boolean isValid = cloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if(!isValid) {
      //overseer.getZkStateReader().getZkClient().printLayout();
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  public static void createConfNode(DistribStateManager stateManager, String configName, String coll) throws IOException, AlreadyExistsException, BadVersionException, KeeperException, InterruptedException {

    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.debug("creating collections conf node {} ", collDir);
      byte[] data = Utils.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (stateManager.hasData(collDir)) {
        stateManager.setData(collDir, data, -1);
      } else {
        stateManager.makePath(collDir, data, CreateMode.PERSISTENT, false);
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST,"Unable to get config name");
    }
  }

  private List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                                      NamedList<Object> results, Replica.State stateMatcher, String asyncId) throws KeeperException, InterruptedException {
    return collectionCmd( message, params, results, stateMatcher, asyncId, Collections.emptySet());
  }

  List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
      NamedList<Object> results, Replica.State stateMatcher,
      String asyncId, Set<String> okayExceptions) throws KeeperException, InterruptedException {
    return collectionCmd(message, params, results, stateMatcher, asyncId, okayExceptions, null, null);
  }

  /**
   * Send request to all replicas of a collection
   * @return List of replicas which is not live for receiving the request
   */
  List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                              NamedList<Object> results, Replica.State stateMatcher,
      String asyncId, Set<String> okayExceptions, ShardHandler shardHandler,
      ShardRequestTracker shardRequestTracker) throws KeeperException, InterruptedException {
    log.info("Executing Collection Cmd={}, asyncId={}", params, asyncId);
    String collectionName = message.getStr(NAME);
    boolean processResponses = false;

    if (shardHandler == null) {
      shardHandler = shardHandlerFactory.getShardHandler(overseerLbClient);
      processResponses = true;
    }

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollectionOrNull(collectionName);
    if (coll == null) return null;
    List<Replica> notLivesReplicas = new ArrayList<>();
    if (shardRequestTracker == null) {
      shardRequestTracker = new ShardRequestTracker(asyncId, message.getStr(Overseer.QUEUE_OPERATION), adminPath, zkStateReader, shardHandlerFactory, overseer);
    }
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(shardRequestTracker.sliceCmd(params, stateMatcher, slice, shardHandler));
    }
    if (processResponses) {
      shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);
    }
    return notLivesReplicas;
  }

  private static void processResponse(NamedList<Object> results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  @SuppressWarnings("deprecation")
  private static void processResponse(NamedList<Object> results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    String rootThrowable = null;
    if (e instanceof BaseHttpSolrClient.RemoteSolrException) {
      rootThrowable = ((BaseHttpSolrClient.RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: {}", shard, e);
      addFailure(results, nodeName, e.getClass().getName() + ":" + e.getMessage());
    } else {
      addSuccess(results, nodeName, solrResponse.getResponse());
    }
  }

  @SuppressWarnings("unchecked")
  private static void addFailure(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> failure = (SimpleOrderedMap<Object>) results.get("failure");
    if (failure == null) {
      failure = new SimpleOrderedMap<>();
      results.add("failure", failure);
    }
    failure.add(key, value);
  }

  @SuppressWarnings("unchecked")
  private static void addSuccess(NamedList<Object> results, String key, Object value) {
    SimpleOrderedMap<Object> success = (SimpleOrderedMap<Object>) results.get("success");
    if (success == null) {
      success = new SimpleOrderedMap<>();
      results.add("success", success);
    }
    success.add(key, value);
  }

  private static Set<CountDownLatch> latches = ConcurrentHashMap.newKeySet();

  private static NamedList<Object> waitForCoreAdminAsyncCallToComplete(String nodeName, String requestId, String adminPath, ZkStateReader zkStateReader, HttpShardHandlerFactory shardHandlerFactory,
                                                                       Overseer overseer) throws KeeperException, InterruptedException {
    log.info("waitForCoreAdminAsyncCallToComplete {}", requestId);
    ZkController zkController = overseer.getCoreContainer().getZkController();
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler(overseer.overseerLbClient);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;

    sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
    sreq.shards = new String[] {replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    CountDownLatch latch = new CountDownLatch(1);
    latches.add(latch);
    // mn- from DistributedMap
    final String successPath = "/overseer/collection-map-completed" + "/mn-" + requestId;
    final String failAsyncPathToWaitOn = "/overseer/collection-map-failure" + "/mn-" + requestId;
    final String runningAsyncPathToWaitOn = "/overseer/collection-map-running" + "/mn-" + requestId;

    if (zkController.getOverseerRunningMap().contains(requestId)) {
      WatchForResponseNode waitForResponse = new WatchForResponseNode(latch, zkStateReader.getZkClient(), successPath);
      try {
        Stat rstats1 = zkStateReader.getZkClient().exists(successPath, waitForResponse);
        if (log.isDebugEnabled()) log.debug("created watch for async response, stat={}", rstats1);
        if (rstats1 != null) {
          latch.countDown();
        }

        Stat rstats2 = zkStateReader.getZkClient().exists(failAsyncPathToWaitOn, waitForResponse);
        if (log.isDebugEnabled()) log.debug("created watch for async response, stat={}", rstats2);
        if (rstats2 != null) {
          latch.countDown();
        }

        if (log.isDebugEnabled()) log.debug("created watch for response {}", requestId);
        boolean success = false;
        for (int i = 0; i < 5; i++) {
          success = latch.await(3, TimeUnit.SECONDS); // nocommit - still need a central timeout strat
          if (success) {
            if (log.isDebugEnabled()) log.debug("latch was triggered {}", requestId);
            break;
          } else {
            if (log.isDebugEnabled()) log.debug("no latch, await again {}", requestId);
          }
        }

        if (!success) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout waiting to see async zk node " + successPath);
        }

      } finally {
        IOUtils.closeQuietly(waitForResponse);
        latch.countDown();
        latches.remove(latch);
      }

    }

    if (log.isDebugEnabled()) log.debug("prepare response {}", requestId);

    SolrQueryResponse srsp = new SolrQueryResponse();
    final NamedList<Object> results = srsp.getValues();
    if (zkController.getOverseerCompletedMap().contains(requestId)) {
      NamedList<String> status = new NamedList<>();
      status.add("state", COMPLETED.toString());
      status.add("msg", "found [" + requestId + "] in completed tasks");
      results.add("STATUS", status);
    } else if (zkController.getOverseerFailureMap().contains(requestId)) {
      NamedList<String> status = new NamedList<>();
      status.add("state", FAILED.toString());
      status.add("msg", "found [" + requestId + "] in failed tasks");
      results.add("STATUS", status);
    } else if (zkController.getOverseerRunningMap().contains(requestId)) {
      NamedList<String> status = new NamedList<>();
      status.add("state", RUNNING.toString());
      status.add("msg", "found [" + requestId + "] in running tasks");
      results.add("STATUS", status);
    } else if (zkController.getOverseerCollectionQueue().containsTaskWithRequestId(ASYNC, requestId)) {
      NamedList<String> status = new NamedList<>();
      status.add("state", SUBMITTED.toString());
      status.add("msg", "found [" + requestId + "] in submitted tasks");
      results.add("STATUS", status);
    } else {
      NamedList<String> status = new NamedList<>();
      status.add("state", NOT_FOUND.toString());
      status.add("msg", "Did not find [" + requestId + "] in any tasks queue");
      results.add("STATUS", status);
    }

    String r = ((NamedList<String>) srsp.getValues().get("STATUS")).get("state").toLowerCase(Locale.ROOT);
    if (r.equals("running")) {
      if (log.isDebugEnabled()) log.debug("The task is still RUNNING, continuing to wait.");
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Task is still running even after reporting complete requestId: " + requestId + "" + srsp.getValues().get("STATUS") + "retried " + counter + "times");
    } else if (r.equals("completed")) {
      // we're done with this entry in the DistributeMap
      overseer.getCoreContainer().getZkController().clearAsyncId(requestId);
      if (log.isDebugEnabled()) log.debug("The task is COMPLETED, returning");
      return srsp.getValues();
    } else if (r.equals("failed")) {
      // TODO: Improve this. Get more information.
      if (log.isDebugEnabled()) log.debug("The task is FAILED, returning");

    } else if (r.equals("not_found")) {
      if (log.isDebugEnabled()) log.debug("The task is notfound, retry");
      return srsp.getValues();
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getValues().get("STATUS"));
    }

    throw new SolrException(ErrorCode.SERVER_ERROR, "No response on request for async status url=" + replica + " params=" + sreq.params);
  }

  @Override
  public String getName() {
    return "Overseer Collection Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "collection_" + operation;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP) ?
            message.getStr(COLLECTION_PROP) : message.getStr(NAME);
  }


  private long sessionId = -1;
  private LockTree.Session lockSession;

  @Override
  public Lock lockTask(ZkNodeProps message, OverseerTaskProcessor.TaskBatch taskBatch) {
    if (lockSession == null || sessionId != taskBatch.getId()) {
      //this is always called in the same thread.
      //Each batch is supposed to have a new taskBatch
      //So if taskBatch changes we must create a new Session
      // also check if the running tasks are empty. If yes, clear lockTree
      // this will ensure that locks are not 'leaked'
      if(taskBatch.getRunningTasks() == 0) lockTree.clear();
      lockSession = lockTree.getSession();
    }
    return lockSession.lock(getCollectionAction(message.getStr(Overseer.QUEUE_OPERATION)),
            Arrays.asList(
                    getTaskKey(message),
                    message.getStr(ZkStateReader.SHARD_ID_PROP),
                    message.getStr(ZkStateReader.REPLICA_PROP))

    );
  }


  @Override
  public void close() throws IOException {
    this.isClosed = true;
    latches.forEach(countDownLatch -> countDownLatch.countDown());
    latches.clear();

    // assert ObjectReleaseTracker.release(this);
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  protected interface Cmd {
    AddReplicaCmd.Response call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception;

    default boolean cleanup(ZkNodeProps message) {
      return false;
    }
  }

  protected interface Finalize {
    AddReplicaCmd.Response call() throws Exception;
  }

  /*
   * backward compatibility reasons, add the response with the async ID as top level.
   * This can be removed in Solr 9
   */
  @Deprecated
  static boolean INCLUDE_TOP_LEVEL_RESPONSE = true;

  public ShardRequestTracker syncRequestTracker() {
    return new ShardRequestTracker(null,null, adminPath, zkStateReader, shardHandlerFactory, overseer);
  }

  public ShardRequestTracker asyncRequestTracker(String asyncId, String operation) {
    return new ShardRequestTracker(asyncId, operation, adminPath, zkStateReader, shardHandlerFactory, overseer);
  }

  public static class ShardRequestTracker{
    private final String asyncId;
    private final Map<String,String> shardAsyncIdByNode = new ConcurrentHashMap<>();
    private final String adminPath;
    private final ZkStateReader zkStateReader;
    private final HttpShardHandlerFactory shardHandlerFactory;
    private final Overseer overseer;
    private final String operation;

    ShardRequestTracker(String asyncId, String operation, String adminPath, ZkStateReader reader, HttpShardHandlerFactory shardHandlerFactory, Overseer overseer) {
      this.asyncId = asyncId;
      this.adminPath = adminPath;
      this.operation = operation;
      this.zkStateReader = reader;
      this.shardHandlerFactory = shardHandlerFactory;
      this.overseer = overseer;
    }

    /**
     * Send request to all replicas of a slice
     * @return List of replicas which is not live for receiving the request
     */
    public List<Replica> sliceCmd(ModifiableSolrParams params, Replica.State stateMatcher,
                                  Slice slice, ShardHandler shardHandler) {
      List<Replica> notLiveReplicas = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        if ((stateMatcher == null || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
          if (zkStateReader.isNodeLive(replica.getNodeName())) {
            // For thread safety, only simple clone the ModifiableSolrParams
            ModifiableSolrParams cloneParams = new ModifiableSolrParams();
            cloneParams.add(params);
            cloneParams.set(CoreAdminParams.CORE, replica.getName());

            sendShardRequest(replica.getNodeName(), cloneParams, shardHandler);
          } else {
            notLiveReplicas.add(replica);
          }
        }
      }
      return notLiveReplicas;
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params,
                                 ShardHandler shardHandler) {
      sendShardRequest(nodeName, params, shardHandler, adminPath, zkStateReader);
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler,
                                 String adminPath, ZkStateReader zkStateReader) {
      if (asyncId != null) {
        String coreAdminAsyncId = asyncId + "-" + operation + "-" + Math.abs(System.nanoTime());
        params.set(ASYNC, coreAdminAsyncId);
        track(nodeName, coreAdminAsyncId);
      }

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.nodeName = nodeName;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError) throws KeeperException, InterruptedException {
      processResponses(results, shardHandler, abortOnError, msgOnError, Collections.emptySet());
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
                          Set<String> okayExceptions) throws KeeperException, InterruptedException {
      // Processes all shard responses
      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp, okayExceptions);
          Throwable exception = srsp.getException();
          if (abortOnError && exception != null) {
            // drain pending requests
            while (srsp != null) {
              srsp = shardHandler.takeCompletedOrError();
            }
            throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError, exception);
          }
        }
      } while (srsp != null);

      // If request is async wait for the core admin to complete before returning
      if (asyncId != null) {
        waitForAsyncCallsToComplete(results); // TODO: Shouldn't we abort with msgOnError exception when failure?
        shardAsyncIdByNode.clear();
      }
    }

    private void waitForAsyncCallsToComplete(NamedList<Object> results) {
      shardAsyncIdByNode.forEach((node, shardAsyncId) -> {
        log.debug("I am Waiting for :{}/{}", node, shardAsyncId);
        NamedList<Object> reqResult = null;
        try {
          reqResult = waitForCoreAdminAsyncCallToComplete(node, shardAsyncId, adminPath, zkStateReader, shardHandlerFactory, overseer);
        } catch (KeeperException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
        if (INCLUDE_TOP_LEVEL_RESPONSE) {
          results.add(shardAsyncId, reqResult);
        }
        if ("failed".equalsIgnoreCase(((NamedList<String>)reqResult.get("STATUS")).get("state"))) {
          log.error("Error from shard {}: {}", node,  reqResult);
          addFailure(results, node, reqResult);
        } else {
          addSuccess(results, node, reqResult);
        }

      });
    }

    /** @deprecated consider to make it private after {@link CreateCollectionCmd} refactoring*/
    @Deprecated void track(String nodeName, String coreAdminAsyncId) {
      shardAsyncIdByNode.put(nodeName, coreAdminAsyncId);
    }
  }

  private static class WatchForResponseNode implements Watcher, Closeable {
    private final CountDownLatch latch;
    private final SolrZkClient zkClient;
    private final String watchPath;
    private boolean closed;

    public WatchForResponseNode(CountDownLatch latch, SolrZkClient zkClient, String watchPath) {
      this.zkClient = zkClient;
      this.latch = latch;
      this.watchPath = watchPath;
    }

    @Override
    public void process(WatchedEvent event) {
      if (log.isDebugEnabled()) log.debug("waitForAsyncId {}", event);
      if (Event.EventType.None.equals(event.getType()) || closed) {
        return;
      }
      if (event.getType().equals(Event.EventType.NodeCreated)) {
        if (log.isDebugEnabled()) log.debug("Overseer request response zk node created");
        latch.countDown();
        return;
      } else if (event.getType().equals(Event.EventType.NodeDeleted)) {
        if (log.isDebugEnabled()) log.debug("Overseer request response zk node deleted");
        latch.countDown();
        return;
      } else if (event.getType().equals(Event.EventType.NodeDataChanged)) {
        if (log.isDebugEnabled()) log.debug("Overseer request response zk node data changed");
        latch.countDown();
        return;
      }
    }

    @Override
    public void close() throws IOException {
      this.closed = true;
      SolrZooKeeper zk = zkClient.getSolrZooKeeper();
      try {
        zk.removeWatches(watchPath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }
}
