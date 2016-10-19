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
package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.ReplicaAssigner.Position;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related
 * overseer messages.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler , Closeable {

  public static final String NUM_SLICES = "numShards";

  static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = "createNodeSet.shuffle";
  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = "createNodeSet";

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String REQUESTID = "requestid";

  public static final String COLL_CONF = "collection.configName";

  public static final String COLL_PROP_PREFIX = "property.";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  public static final String SHARD_UNIQUE = "shardUnique";

  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  public static final Map<String, Object> COLL_PROPS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.MAX_SHARDS_PER_NODE, "1",
      ZkStateReader.AUTO_ADD_REPLICAS, "false",
      DocCollection.RULE, null,
      SNITCH, null));

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Overseer overseer;
  ShardHandlerFactory shardHandlerFactory;
  String adminPath;
  ZkStateReader zkStateReader;
  String myId;
  Overseer.Stats stats;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  final private LockTree lockTree = new LockTree();
  ExecutorService tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS,
      new SynchronousQueue<>(),
      new DefaultSolrThreadFactory("OverseerCollectionMessageHandlerThreadFactory"));

  static final Random RANDOM;
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

  public OverseerCollectionMessageHandler(ZkStateReader zkStateReader, String myId,
                                        final ShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Overseer.Stats stats,
                                        Overseer overseer,
                                        OverseerNodePrioritizer overseerPrioritizer) {
    this.zkStateReader = zkStateReader;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    commandMap = new ImmutableMap.Builder<CollectionAction, Cmd>()
        .put(REPLACENODE, new ReplaceNodeCmd(this))
        .put(DELETENODE, new DeleteNodeCmd(this))
        .put(BACKUP, new BackupCmd(this))
        .put(RESTORE, new RestoreCmd(this))
        .put(CREATESNAPSHOT, new CreateSnapshotCmd(this))
        .put(DELETESNAPSHOT, new DeleteSnapshotCmd(this))
        .put(SPLITSHARD, new SplitShardCmd(this))
        .put(ADDROLE, new OverseerRoleCmd(this, ADDROLE, overseerPrioritizer))
        .put(REMOVEROLE, new OverseerRoleCmd(this, REMOVEROLE, overseerPrioritizer))
        .put(MOCK_COLL_TASK, this::mockOperation)
        .put(MOCK_SHARD_TASK, this::mockOperation)
        .put(MOCK_REPLICA_TASK, this::mockOperation)
        .put(MIGRATESTATEFORMAT, this::migrateStateFormat)
        .put(CREATESHARD, new CreateShardCmd(this))
        .put(MIGRATE, new MigrateCmd(this))
        .put(CREATE, new CreateCollectionCmd(this))
        .put(MODIFYCOLLECTION, this::modifyCollection)
        .put(ADDREPLICAPROP, this::processReplicaAddPropertyCommand)
        .put(DELETEREPLICAPROP, this::processReplicaDeletePropertyCommand)
        .put(BALANCESHARDUNIQUE, this::balanceProperty)
        .put(REBALANCELEADERS, this::processRebalanceLeaders)
        .put(RELOAD, this::reloadCollection)
        .put(DELETE, new DeleteCollectionCmd(this))
        .put(CREATEALIAS, new CreateAliasCmd(this))
        .put(DELETEALIAS, new DeleteAliasCmd(this))
        .put(OVERSEERSTATUS, new OverseerStatusCmd(this))
        .put(DELETESHARD, new DeleteShardCmd(this))
        .put(DELETEREPLICA, new DeleteReplicaCmd(this))
        .put(ADDREPLICA, new AddReplicaCmd(this))
        .build()
    ;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SolrResponse processMessage(ZkNodeProps message, String operation) {
    log.debug("OverseerCollectionMessageHandler.processMessage : "+ operation + " , "+ message.toString());

    NamedList results = new NamedList();
    try {
      CollectionAction action = getCollectionAction(operation);
      Cmd command = commandMap.get(action);
      if (command != null) {
        command.call(zkStateReader.getClusterState(), message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }
    } catch (Exception e) {
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr(NAME);

      if (collName == null) {
        SolrException.log(log, "Operation " + operation + " failed", e);
      } else  {
        SolrException.log(log, "Collection: " + collName + " operation: " + operation
            + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
  private void mockOperation(ClusterState state, ZkNodeProps message, NamedList results) throws InterruptedException {
    //only for test purposes
    Thread.sleep(message.getInt("sleep", 1));
    log.info("MOCK_TASK_EXECUTED time {} data {}", System.currentTimeMillis(), Utils.toJSONString(message));
    results.add("MOCK_FINISHED", System.currentTimeMillis());
  }

  private CollectionAction getCollectionAction(String operation) {
    CollectionAction action = CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }
    return action;
  }

  private void reloadCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());

    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = null;
    if (asyncId != null) {
      requestMap = new HashMap<>();
    }
    collectionCmd(message, params, results, Replica.State.ACTIVE, asyncId, requestMap);
  }

  @SuppressWarnings("unchecked")
  private void processRebalanceLeaders(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP,
        CORE_NODE_NAME_PROP, BASE_URL_PROP, REJOIN_AT_HEAD_PROP);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
    params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
    params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REJOINLEADERELECTION.toString());
    params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
    params.set(CORE_NODE_NAME_PROP, message.getStr(CORE_NODE_NAME_PROP));
    params.set(ELECTION_NODE_PROP, message.getStr(ELECTION_NODE_PROP));
    params.set(BASE_URL_PROP, message.getStr(BASE_URL_PROP));

    String baseUrl = message.getStr(BASE_URL_PROP);
    ShardRequest sreq = new ShardRequest();
    sreq.nodeName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    // yes, they must use same admin handler path everywhere...
    params.set("qt", adminPath);
    sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
    sreq.shards = new String[] {baseUrl};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    shardHandler.submit(sreq, baseUrl, sreq.params);
  }

  @SuppressWarnings("unchecked")
  private void processReplicaAddPropertyCommand(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));
  }

  private void processReplicaDeletePropertyCommand(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));
  }

  private void balanceProperty(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
              "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
    }
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
    propMap.putAll(message.getProperties());
    inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
  }

  /**
   * Walks the tree of collection status to verify that any replicas not reporting a "down" status is
   * on a live node, if any replicas reporting their status as "active" but the node is not live is
   * marked as "down"; used by CLUSTERSTATUS.
   * @param liveNodes List of currently live node names.
   * @param collectionProps Map of collection status information pulled directly from ZooKeeper.
   */

  @SuppressWarnings("unchecked")
  protected void crossCheckReplicaStateWithLiveNodes(List<String> liveNodes, NamedList<Object> collectionProps) {
    Iterator<Map.Entry<String,Object>> colls = collectionProps.iterator();
    while (colls.hasNext()) {
      Map.Entry<String,Object> next = colls.next();
      Map<String,Object> collMap = (Map<String,Object>)next.getValue();
      Map<String,Object> shards = (Map<String,Object>)collMap.get("shards");
      for (Object nextShard : shards.values()) {
        Map<String,Object> shardMap = (Map<String,Object>)nextShard;
        Map<String,Object> replicas = (Map<String,Object>)shardMap.get("replicas");
        for (Object nextReplica : replicas.values()) {
          Map<String,Object> replicaMap = (Map<String,Object>)nextReplica;
          if (Replica.State.getState((String) replicaMap.get(ZkStateReader.STATE_PROP)) != Replica.State.DOWN) {
            // not down, so verify the node is live
            String node_name = (String)replicaMap.get(ZkStateReader.NODE_NAME_PROP);
            if (!liveNodes.contains(node_name)) {
              // node is not live, so this replica is actually down
              replicaMap.put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
            }
          }
        }
      }
    }
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
      Map<String, Object>  selected = new HashMap<>();
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
  void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws Exception {
    ((DeleteReplicaCmd) commandMap.get(DELETEREPLICA)).deleteReplica(clusterState, message, results, onComplete);

  }

  boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS);
    boolean deleted = false;
    while (! timeout.hasTimedOut()) {
      Thread.sleep(100);
      DocCollection docCollection = zkStateReader.getClusterState().getCollection(collectionName);
      if(docCollection != null) {
        Slice slice = docCollection.getSlice(shard);
        if(slice == null || slice.getReplica(replicaName) == null) {
          deleted =  true;
        }
      }
      // Return true if either someone already deleted the collection/slice/replica.
      if (docCollection == null || deleted) break;
    }
    return deleted;
  }

  void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws KeeperException, InterruptedException {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));
  }

  void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

  }

  //TODO should we not remove in the next release ?
  private void migrateStateFormat(ClusterState state, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    final String collectionName = message.getStr(COLLECTION_PROP);

    boolean firstLoop = true;
    // wait for a while until the state format changes
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    while (! timeout.hasTimedOut()) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + collectionName + " not found");
      }
      if (collection.getStateFormat() == 2) {
        // Done.
        results.add("success", new SimpleOrderedMap<>());
        return;
      }

      if (firstLoop) {
        // Actually queue the migration command.
        firstLoop = false;
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, MIGRATESTATEFORMAT.toLower(), COLLECTION_PROP, collectionName);
        Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));
      }
      Thread.sleep(100);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not migrate state format for collection: " + collectionName);
  }

  void commit(NamedList results, String slice, Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl);
      processResponse(results, null, coreUrl, updateResponse, slice, Collections.emptySet());
    } catch (Exception e) {
      processResponse(results, e, coreUrl, updateResponse, slice, Collections.emptySet());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }


  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {

    try (HttpSolrClient client = new HttpSolrClient.Builder(url).build()) {
      client.setConnectionTimeout(30000);
      client.setSoTimeout(120000);
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      return ureq.process(client);
    }
  }

  String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore) {
    int retryCount = 320;
    while (retryCount-- > 0) {
      Map<String,Slice> slicesMap = zkStateReader.getClusterState()
          .getSlicesMap(collectionName);
      if (slicesMap != null) {

        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            if (nodeName.equals(msgNodeName) && core.equals(msgCore)) {
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

  void waitForNewShard(String collectionName, String sliceName) throws KeeperException, InterruptedException {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    RTimer timer = new RTimer();
    int retryCount = 320;
    while (retryCount-- > 0) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Unable to find collection: " + collectionName + " in clusterstate");
      }
      Slice slice = collection.getSlice(sliceName);
      if (slice != null) {
        log.debug("Waited for {}ms for slice {} of collection {} to be available",
            timer.getTime(), sliceName, collectionName);
        return;
      }
      Thread.sleep(1000);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not find new slice " + sliceName + " in collection " + collectionName
            + " even after waiting for " + timer.getTime() + "ms"
    );
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

  void sendShardRequest(String nodeName, ModifiableSolrParams params,
                        ShardHandler shardHandler, String asyncId,
                        Map<String, String> requestMap) {
    sendShardRequest(nodeName, params, shardHandler, asyncId, requestMap, adminPath, zkStateReader);

  }

  public static void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler,
                                      String asyncId, Map<String, String> requestMap, String adminPath,
                                      ZkStateReader zkStateReader) {
    if (asyncId != null) {
      String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
      params.set(ASYNC, coreAdminAsyncId);
      requestMap.put(nodeName, coreAdminAsyncId);
    }

    ShardRequest sreq = new ShardRequest();
    params.set("qt", adminPath);
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.nodeName = nodeName;
    sreq.params = params;

    shardHandler.submit(sreq, replica, sreq.params);
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

  static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random) {
    // TODO: add smarter options that look at the current number of cores per
    // node?
    // for now we just go random (except when createNodeSet and createNodeSet.shuffle=false are passed in)

    List<String> nodeList;

    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null)?null:StrUtils.splitSmart((CREATE_NODE_SET_EMPTY.equals(createNodeSetStr)?"":createNodeSetStr), ",", true);

    if (createNodeList != null) {
      nodeList = new ArrayList<>(createNodeList);
      nodeList.retainAll(liveNodes);
      if (message.getBool(CREATE_NODE_SET_SHUFFLE, CREATE_NODE_SET_SHUFFLE_DEFAULT)) {
        Collections.shuffle(nodeList, random);
      }
    } else {
      nodeList = new ArrayList<>(liveNodes);
      Collections.shuffle(nodeList, random);
    }

    return nodeList;
  }


  private void modifyCollection(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    
    final String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    //the rest of the processing is based on writing cluster state properties
    //remove the property here to avoid any errors down the pipeline due to this property appearing
    String configName = (String) message.getProperties().remove(COLL_CONF);
    
    if(configName != null) {
      validateConfigOrThrowSolrException(configName);
      
      boolean isLegacyCloud =  Overseer.isLegacy(zkStateReader);
      createConfNode(configName, collectionName, isLegacyCloud);
      reloadCollection(null, new ZkNodeProps(NAME, collectionName), results);
    }
    
    overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));

    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean areChangesVisible = true;
    while (!timeout.hasTimedOut()) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      areChangesVisible = true;
      for (Map.Entry<String,Object> updateEntry : message.getProperties().entrySet()) {
        String updateKey = updateEntry.getKey();
        if (!updateKey.equals(ZkStateReader.COLLECTION_PROP)
            && !updateKey.equals(Overseer.QUEUE_OPERATION)
            && !collection.get(updateKey).equals(updateEntry.getValue())){
          areChangesVisible = false;
          break;
        }
      }
      if (areChangesVisible) break;
      Thread.sleep(100);
    }
    if (!areChangesVisible)
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not modify collection " + message);
  }

  void cleanupCollection(String collectionName, NamedList results) throws Exception {
    log.error("Cleaning up collection [" + collectionName + "]." );
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    commandMap.get(DELETE).call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);
  }

  Map<Position, String> identifyNodes(ClusterState clusterState,
                                      List<String> nodeList,
                                      ZkNodeProps message,
                                      List<String> shardNames,
                                      int repFactor) throws IOException {
    List<Map> rulesMap = (List) message.get("rule");
    if (rulesMap == null) {
      int i = 0;
      Map<Position, String> result = new HashMap<>();
      for (String aShard : shardNames) {
        for (int j = 0; j < repFactor; j++){
          result.put(new Position(aShard, j), nodeList.get(i % nodeList.size()));
          i++;
        }
      }
      return result;
    }

    List<Rule> rules = new ArrayList<>();
    for (Object map : rulesMap) rules.add(new Rule((Map) map));

    Map<String, Integer> sharVsReplicaCount = new HashMap<>();

    for (String shard : shardNames) sharVsReplicaCount.put(shard, repFactor);
    ReplicaAssigner replicaAssigner = new ReplicaAssigner(rules,
        sharVsReplicaCount,
        (List<Map>) message.get(SNITCH),
        new HashMap<>(),//this is a new collection. So, there are no nodes in any shard
        nodeList,
        overseer.getZkController().getCoreContainer(),
        clusterState);

    return replicaAssigner.getNodeMappings();
  }

  Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    Map<String, Replica> result = new HashMap<>();
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    while (true) {
      DocCollection coll = zkStateReader.getClusterState().getCollection(collectionName);
      for (String coreName : coreNames) {
        if (result.containsKey(coreName)) continue;
        for (Slice slice : coll.getSlices()) {
          for (Replica replica : slice.getReplicas()) {
            if (coreName.equals(replica.getStr(ZkStateReader.CORE_NAME_PROP))) {
              result.put(coreName, replica);
              break;
            }
          }
        }
      }
      
      if (result.size() == coreNames.size()) {
        return result;
      }
      if (timeout.hasTimedOut()) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out waiting to see all replicas: " + coreNames + " in cluster state.");
      }
      
      Thread.sleep(100);
    }
  }

  ZkNodeProps addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws KeeperException, InterruptedException {

    return ((AddReplicaCmd) commandMap.get(ADDREPLICA)).addReplica(clusterState, message, results, onComplete);
  }

  void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
                        String asyncId, Map<String, String> requestMap) {
    processResponses(results, shardHandler, abortOnError, msgOnError, asyncId, requestMap, Collections.emptySet());
  }

  void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
                                String asyncId, Map<String, String> requestMap, Set<String> okayExceptions) {
    //Processes all shard responses
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp, okayExceptions);
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

    //If request is async wait for the core admin to complete before returning
    if (asyncId != null) {
      waitForAsyncCallsToComplete(requestMap, results);
      requestMap.clear();
    }
  }


  void validateConfigOrThrowSolrException(String configName) throws KeeperException, InterruptedException {
    boolean isValid = zkStateReader.getZkClient().exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName, true);
    if(!isValid) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  void createConfNode(String configName, String coll, boolean isLegacyCloud) throws KeeperException, InterruptedException {
    
    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.debug("creating collections conf node {} ", collDir);
      byte[] data = Utils.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (zkStateReader.getZkClient().exists(collDir, true)) {
        zkStateReader.getZkClient().setData(collDir, data, true);
      } else {
        zkStateReader.getZkClient().makePath(collDir, data, true);
      }
    } else {
      if(isLegacyCloud){
        log.warn("Could not obtain config name");
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,"Unable to get config name");
      }
    }

  }
  
  private void collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                             NamedList results, Replica.State stateMatcher, String asyncId, Map<String, String> requestMap) {
    collectionCmd( message, params, results, stateMatcher, asyncId, requestMap, Collections.emptySet());
  }


  void collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                     NamedList results, Replica.State stateMatcher, String asyncId, Map<String, String> requestMap, Set<String> okayExceptions) {
    log.info("Executing Collection Cmd : " + params);
    String collectionName = message.getStr(NAME);
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(collectionName);
    
    for (Slice slice : coll.getSlices()) {
      sliceCmd(clusterState, params, stateMatcher, slice, shardHandler, asyncId, requestMap);
    }

    processResponses(results, shardHandler, false, null, asyncId, requestMap, okayExceptions);

  }

  void sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
                Slice slice, ShardHandler shardHandler, String asyncId, Map<String, String> requestMap) {

    for (Replica replica : slice.getReplicas()) {
      if (clusterState.liveNodesContain(replica.getStr(ZkStateReader.NODE_NAME_PROP))
          && (stateMatcher == null || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {

        // For thread safety, only simple clone the ModifiableSolrParams
        ModifiableSolrParams cloneParams = new ModifiableSolrParams();
        cloneParams.add(params);
        cloneParams.set(CoreAdminParams.CORE, replica.getStr(ZkStateReader.CORE_NAME_PROP));

        sendShardRequest(replica.getStr(ZkStateReader.NODE_NAME_PROP), cloneParams, shardHandler, asyncId, requestMap);
      }
    }
  }
  
  private void processResponse(NamedList results, ShardResponse srsp, Set<String> okayExceptions) {
    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);
  }

  @SuppressWarnings("unchecked")
  private void processResponse(NamedList results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    String rootThrowable = null;
    if (e instanceof RemoteSolrException) {
      rootThrowable = ((RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
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

  @SuppressWarnings("unchecked")
  private void waitForAsyncCallsToComplete(Map<String, String> requestMap, NamedList results) {
    for (String k:requestMap.keySet()) {
      log.debug("I am Waiting for :{}/{}", k, requestMap.get(k));
      results.add(requestMap.get(k), waitForCoreAdminAsyncCallToComplete(k, requestMap.get(k)));
    }
  }

  private NamedList waitForCoreAdminAsyncCallToComplete(String nodeName, String requestId) {
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList results = new NamedList();
          processResponse(results, srsp, Collections.emptySet());
          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
              break;
            }
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
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
    if (tpe != null) {
      if (!tpe.isShutdown()) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }
  }

  interface Cmd {
    void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception;
  }
}
