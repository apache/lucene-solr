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

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Assign.ReplicaCount;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.ReplicaAssigner.Position;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
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
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.stats.Snapshot;
import org.apache.solr.util.stats.Timer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Assign.getNodesForNewReplicas;
import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATESTATEFORMAT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.Utils.makeMap;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related
 * overseer messages.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler {

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

  private static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  public static final Map<String, Object> COLL_PROPS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.MAX_SHARDS_PER_NODE, "1",
      ZkStateReader.AUTO_ADD_REPLICAS, "false",
      DocCollection.RULE, null,
      SNITCH, null));

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Overseer overseer;
  private ShardHandlerFactory shardHandlerFactory;
  private String adminPath;
  private ZkStateReader zkStateReader;
  private String myId;
  private Overseer.Stats stats;
  private OverseerNodePrioritizer overseerPrioritizer;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  final private LockTree lockTree = new LockTree();

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
    this.overseerPrioritizer = overseerPrioritizer;
  }

  @Override
  @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
  @SuppressWarnings("unchecked")
  public SolrResponse processMessage(ZkNodeProps message, String operation) {
    log.info("OverseerCollectionMessageHandler.processMessage : "+ operation + " , "+ message.toString());

    NamedList results = new NamedList();
    try {
      CollectionParams.CollectionAction action = getCollectionAction(operation);
      switch (action) {
        case CREATE:
          createCollection(zkStateReader.getClusterState(), message, results);
          break;
        case DELETE:
          deleteCollection(message, results);
          break;
        case RELOAD:
          reloadCollection(message, results);
          break;
        case CREATEALIAS:
          createAlias(zkStateReader.getAliases(), message);
          break;
        case DELETEALIAS:
          deleteAlias(zkStateReader.getAliases(), message);
          break;
        case SPLITSHARD:
          splitShard(zkStateReader.getClusterState(), message, results);
          break;
        case DELETESHARD:
          deleteShard(zkStateReader.getClusterState(), message, results);
          break;
        case CREATESHARD:
          createShard(zkStateReader.getClusterState(), message, results);
          break;
        case DELETEREPLICA:
          deleteReplica(zkStateReader.getClusterState(), message, results);
          break;
        case MIGRATE:
          migrate(zkStateReader.getClusterState(), message, results);
          break;
        case ADDROLE:
          processRoleCommand(message, operation);
          break;
        case REMOVEROLE:
          processRoleCommand(message, operation);
          break;
        case ADDREPLICA:
          addReplica(zkStateReader.getClusterState(), message, results);
          break;
        case OVERSEERSTATUS:
          getOverseerStatus(message, results);
          break;
        case ADDREPLICAPROP:
          processReplicaAddPropertyCommand(message);
          break;
        case DELETEREPLICAPROP:
          processReplicaDeletePropertyCommand(message);
          break;
        case BALANCESHARDUNIQUE:
          balanceProperty(message);
          break;
        case REBALANCELEADERS:
          processRebalanceLeaders(message);
          break;
        case MODIFYCOLLECTION:
          overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));
          break;
        case MIGRATESTATEFORMAT:
          migrateStateFormat(message, results);
          break;
        case BACKUP:
          processBackupAction(message, results);
          break;
        case RESTORE:
          processRestoreAction(message, results);
          break;
        case MOCK_COLL_TASK:
        case MOCK_SHARD_TASK:
        case MOCK_REPLICA_TASK: {
          //only for test purposes
          Thread.sleep(message.getInt("sleep", 1));
          log.info("MOCK_TASK_EXECUTED time {} data {}",System.currentTimeMillis(), Utils.toJSONString(message));
          results.add("MOCK_FINISHED", System.currentTimeMillis());
          break;
        }
        default:
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

  private CollectionParams.CollectionAction getCollectionAction(String operation) {
    CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }
    return action;
  }

  //
  // TODO DWS: this class has gone out of control (too big); refactor to break it up
  //

  private void reloadCollection(ZkNodeProps message, NamedList results) {
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
  private void processRebalanceLeaders(ZkNodeProps message) throws KeeperException, InterruptedException {
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
  private void processReplicaAddPropertyCommand(ZkNodeProps message) throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));
  }

  private void processReplicaDeletePropertyCommand(ZkNodeProps message) throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));
  }

  private void balanceProperty(ZkNodeProps message) throws KeeperException, InterruptedException {
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


  @SuppressWarnings("unchecked")
  private void getOverseerStatus(ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String leaderNode = OverseerTaskProcessor.getLeaderNode(zkStateReader.getZkClient());
    results.add("leader", leaderNode);
    Stat stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue",null, stat, true);
    results.add("overseer_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/queue-work",null, stat, true);
    results.add("overseer_work_queue_size", stat.getNumChildren());
    stat = new Stat();
    zkStateReader.getZkClient().getData("/overseer/collection-queue-work",null, stat, true);
    results.add("overseer_collection_queue_size", stat.getNumChildren());

    NamedList overseerStats = new NamedList();
    NamedList collectionStats = new NamedList();
    NamedList stateUpdateQueueStats = new NamedList();
    NamedList workQueueStats = new NamedList();
    NamedList collectionQueueStats = new NamedList();
    for (Map.Entry<String, Overseer.Stat> entry : stats.getStats().entrySet()) {
      String key = entry.getKey();
      NamedList<Object> lst = new SimpleOrderedMap<>();
      if (key.startsWith("collection_"))  {
        collectionStats.add(key.substring(11), lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
        List<Overseer.FailedOp> failureDetails = stats.getFailureDetails(key);
        if (failureDetails != null) {
          List<SimpleOrderedMap<Object>> failures = new ArrayList<>();
          for (Overseer.FailedOp failedOp : failureDetails) {
            SimpleOrderedMap<Object> fail = new SimpleOrderedMap<>();
            fail.add("request", failedOp.req.getProperties());
            fail.add("response", failedOp.resp.getResponse());
            failures.add(fail);
          }
          lst.add("recent_failures", failures);
        }
      } else if (key.startsWith("/overseer/queue_"))  {
        stateUpdateQueueStats.add(key.substring(16), lst);
      } else if (key.startsWith("/overseer/queue-work_"))  {
        workQueueStats.add(key.substring(21), lst);
      } else if (key.startsWith("/overseer/collection-queue-work_"))  {
        collectionQueueStats.add(key.substring(32), lst);
      } else  {
        // overseer stats
        overseerStats.add(key, lst);
        int successes = stats.getSuccessCount(entry.getKey());
        int errors = stats.getErrorCount(entry.getKey());
        lst.add("requests", successes);
        lst.add("errors", errors);
      }
      Timer timer = entry.getValue().requestTime;
      Snapshot snapshot = timer.getSnapshot();
      lst.add("totalTime", timer.getSum());
      lst.add("avgRequestsPerMinute", timer.getMeanRate());
      lst.add("5minRateRequestsPerMinute", timer.getFiveMinuteRate());
      lst.add("15minRateRequestsPerMinute", timer.getFifteenMinuteRate());
      lst.add("avgTimePerRequest", timer.getMean());
      lst.add("medianRequestTime", snapshot.getMedian());
      lst.add("75thPctlRequestTime", snapshot.get75thPercentile());
      lst.add("95thPctlRequestTime", snapshot.get95thPercentile());
      lst.add("99thPctlRequestTime", snapshot.get99thPercentile());
      lst.add("999thPctlRequestTime", snapshot.get999thPercentile());
    }
    results.add("overseer_operations", overseerStats);
    results.add("collection_operations", collectionStats);
    results.add("overseer_queue", stateUpdateQueueStats);
    results.add("overseer_internal_queue", workQueueStats);
    results.add("collection_queue", collectionQueueStats);

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
  private void processRoleCommand(ZkNodeProps message, String operation) throws KeeperException, InterruptedException {
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map roles = null;
    String node = message.getStr("node");

    String roleName = message.getStr("role");
    boolean nodeExists = false;
    if(nodeExists = zkClient.exists(ZkStateReader.ROLES, true)){
      roles = (Map) Utils.fromJSON(zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true));
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
      zkClient.setData(ZkStateReader.ROLES, Utils.toJSON(roles),true);
    } else {
      zkClient.create(ZkStateReader.ROLES, Utils.toJSON(roles), CreateMode.PERSISTENT,true);
    }
    //if there are too many nodes this command may time out. And most likely dedicated
    // overseers are created when there are too many nodes  . So , do this operation in a separate thread
    new Thread(){
      @Override
      public void run() {
        try {
          overseerPrioritizer.prioritizeOverseerNodes(myId);
        } catch (Exception e) {
          log.error("Error in prioritizing Overseer",e);
        }

      }
    }.start();
  }

  @SuppressWarnings("unchecked")
  private void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);
    
    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = coll.getSlice(shard);
    if (slice == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Invalid shard name : " + shard + " in collection : " + collectionName);
    }
    Replica replica = slice.getReplica(replicaName);
    if (replica == null) {
      ArrayList<String> l = new ArrayList<>();
      for (Replica r : slice.getReplicas())
        l.add(r.getName());
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid replica : " + replicaName + " in shard/collection : "
          + shard + "/" + collectionName + " available replicas are " + StrUtils.join(l, ','));
    }
    
    // If users are being safe and only want to remove a shard if it is down, they can specify onlyIfDown=true
    // on the command.
    if (Boolean.parseBoolean(message.getStr(ONLY_IF_DOWN)) && replica.getState() != Replica.State.DOWN) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Attempted to remove replica : " + collectionName + "/" + shard + "/" + replicaName
              + " with onlyIfDown='true', but state is '" + replica.getStr(ZkStateReader.STATE_PROP) + "'");
    }

    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = null;
    if (asyncId != null) {
      requestMap = new HashMap<>(1, 1.0f);
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
    params.add(CoreAdminParams.CORE, core);

    params.set(CoreAdminParams.DELETE_INDEX, message.getBool(CoreAdminParams.DELETE_INDEX, true));
    params.set(CoreAdminParams.DELETE_INSTANCE_DIR, message.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, true));
    params.set(CoreAdminParams.DELETE_DATA_DIR, message.getBool(CoreAdminParams.DELETE_DATA_DIR, true));

    sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap);

    processResponses(results, shardHandler, false, null, asyncId, requestMap);

    //check if the core unload removed the corenode zk entry
    if (waitForCoreNodeGone(collectionName, shard, replicaName, 5000)) return;

    // try and ensure core info is removed from cluster state
    deleteCoreNode(collectionName, replicaName, replica, core);
    if (waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return;
    
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not  remove replica : " + collectionName + "/" + shard + "/" + replicaName);
  }

  private boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
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

  private void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws KeeperException, InterruptedException {
    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName);
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));
  }

  private void checkRequired(ZkNodeProps message, String... props) {
    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

  }

  private void deleteCollection(ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    final String collection = message.getStr(NAME);
    try {
      if (zkStateReader.getClusterState().getCollectionOrNull(collection) == null) {
        if (zkStateReader.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
          // if the collection is not in the clusterstate, but is listed in zk, do nothing, it will just
          // be removed in the finally - we cannot continue, because the below code will error if the collection
          // is not in the clusterstate
          return;
        }
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);

      String asyncId = message.getStr(ASYNC);
      Map<String, String> requestMap = null;
      if (asyncId != null) {
        requestMap = new HashMap<>();
      }
      
      Set<String> okayExceptions = new HashSet<>(1);
      okayExceptions.add(NonExistentCoreException.class.getName());
      
      collectionCmd(message, params, results, null, asyncId, requestMap, okayExceptions);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETE.toLower(), NAME, collection);
      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));

      // wait for a while until we don't see the collection
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (! timeout.hasTimedOut()) {
        Thread.sleep(100);
        removed = !zkStateReader.getClusterState().hasCollection(collection);
        if (removed) {
          Thread.sleep(500); // just a bit of time so it's more likely other
                             // readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + collection);
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

  private void migrateStateFormat(ZkNodeProps message, NamedList results)
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

  private void createAlias(Aliases aliases, ZkNodeProps message) {
    String aliasName = message.getStr(NAME);
    String collections = message.getStr("collections");
    
    Map<String,Map<String,String>> newAliasesMap = new HashMap<>();
    Map<String,String> newCollectionAliasesMap = new HashMap<>();
    Map<String,String> prevColAliases = aliases.getCollectionAliasMap();
    if (prevColAliases != null) {
      newCollectionAliasesMap.putAll(prevColAliases);
    }
    newCollectionAliasesMap.put(aliasName, collections);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes = Utils.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES, jsonBytes, true);
      
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

    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases;
    while (! timeout.hasTimedOut()) {
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

    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases = null;
    while (! timeout.hasTimedOut()) {
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
    String aliasName = message.getStr(NAME);

    Map<String,Map<String,String>> newAliasesMap = new HashMap<>();
    Map<String,String> newCollectionAliasesMap = new HashMap<>();
    newCollectionAliasesMap.putAll(aliases.getCollectionAliasMap());
    newCollectionAliasesMap.remove(aliasName);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes  = Utils.toJSON(newAliases.getAliasMap());
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

  private boolean createShard(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);

    log.info("Create shard invoked: {}", message);
    if (collectionName == null || sliceName == null)
      throw new SolrException(ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");
    int numSlices = 1;
    
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    DocCollection collection = clusterState.getCollection(collectionName);
    int repFactor = message.getInt(REPLICATION_FACTOR, collection.getInt(REPLICATION_FACTOR, 1));
    String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    List<ReplicaCount> sortedNodeList = getNodesForNewReplicas(clusterState, collectionName, sliceName, repFactor,
        createNodeSetStr, overseer.getZkController().getCoreContainer());
        
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));
    // wait for a while until we see the shard
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean created = false;
    while (! timeout.hasTimedOut()) {
      Thread.sleep(100);
      created = zkStateReader.getClusterState().getCollection(collectionName).getSlice(sliceName) != null;
      if (created) break;
    }
    if (!created)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create shard: " + message.getStr(NAME));
      
    String configName = message.getStr(COLL_CONF);

    String async = message.getStr(ASYNC);
    Map<String, String> requestMap = null;
    if (async != null) {
      requestMap = new HashMap<>(repFactor, 1.0f);
    }

    for (int j = 1; j <= repFactor; j++) {
      String nodeName = sortedNodeList.get(((j - 1)) % sortedNodeList.size()).nodeName;
      String shardName = collectionName + "_" + sliceName + "_replica" + j;
      log.info("Creating shard " + shardName + " as part of slice " + sliceName + " of collection " + collectionName
          + " on " + nodeName);
          
      // Need to create new params for each request
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
      params.set(CoreAdminParams.NAME, shardName);
      params.set(COLL_CONF, configName);
      params.set(CoreAdminParams.COLLECTION, collectionName);
      params.set(CoreAdminParams.SHARD, sliceName);
      params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);
      addPropertyParams(message, params);

      sendShardRequest(nodeName, params, shardHandler, async, requestMap);
    }
    
    processResponses(results, shardHandler, true, "Failed to create shard", async, requestMap, Collections.emptySet());
    
    log.info("Finished create command on all shards for collection: " + collectionName);
    
    return true;
  }


  private boolean splitShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    String collectionName = message.getStr("collection");
    String slice = message.getStr(ZkStateReader.SHARD_ID_PROP);
    
    log.info("Split shard invoked");
    String splitKey = message.getStr("split.key");
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    
    DocCollection collection = clusterState.getCollection(collectionName);
    DocRouter router = collection.getRouter() != null ? collection.getRouter() : DocRouter.DEFAULT;
    
    Slice parentSlice;
    
    if (slice == null) {
      if (router instanceof CompositeIdRouter) {
        Collection<Slice> searchSlices = router.getSearchSlicesSingle(splitKey, new ModifiableSolrParams(), collection);
        if (searchSlices.isEmpty()) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unable to find an active shard for split.key: " + splitKey);
        }
        if (searchSlices.size() > 1) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Splitting a split.key: " + splitKey + " which spans multiple shards is not supported");
        }
        parentSlice = searchSlices.iterator().next();
        slice = parentSlice.getName();
        log.info("Split by route.key: {}, parent shard is: {} ", splitKey, slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Split by route key can only be used with CompositeIdRouter or subclass. Found router: "
                + router.getClass().getName());
      }
    } else {
      parentSlice = collection.getSlice(slice);
    }
    
    if (parentSlice == null) {
      // no chance of the collection being null because ClusterState#getCollection(String) would have thrown
      // an exception already
      throw new SolrException(ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
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
    if (rangesStr != null) {
      String[] ranges = rangesStr.split(",");
      if (ranges.length == 0 || ranges.length == 1) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "There must be at least two ranges specified to split a shard");
      } else {
        subRanges = new ArrayList<>(ranges.length);
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
        List<DocRouter.Range> temp = new ArrayList<>(subRanges); // copy to preserve original order
        Collections.sort(temp);
        if (!range.equals(new DocRouter.Range(temp.get(0).min, temp.get(temp.size() - 1).max))) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Specified hash ranges: " + rangesStr + " do not cover the entire range of parent shard: " + range);
        }
        for (int i = 1; i < temp.size(); i++) {
          if (temp.get(i - 1).max + 1 != temp.get(i).min) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Specified hash ranges: " + rangesStr
                + " either overlap with each other or " + "do not cover the entire range of parent shard: " + range);
          }
        }
      }
    } else if (splitKey != null) {
      if (router instanceof CompositeIdRouter) {
        CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
        subRanges = compositeIdRouter.partitionRangeByKey(splitKey, range);
        if (subRanges.size() == 1) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "The split.key: " + splitKey
              + " has a hash range that is exactly equal to hash range of shard: " + slice);
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
          if (i < subRanges.size() - 1) rangesStr += ',';
        }
      }
    } else {
      // todo: fixed to two partitions?
      subRanges = router.partitionRange(2, range);
    }
    
    try {
      List<String> subSlices = new ArrayList<>(subRanges.size());
      List<String> subShardNames = new ArrayList<>(subRanges.size());
      String nodeName = parentShardLeader.getNodeName();
      for (int i = 0; i < subRanges.size(); i++) {
        String subSlice = slice + "_" + i;
        subSlices.add(subSlice);
        String subShardName = collectionName + "_" + subSlice + "_replica1";
        subShardNames.add(subShardName);
        
        Slice oSlice = collection.getSlice(subSlice);
        if (oSlice != null) {
          final Slice.State state = oSlice.getState();
          if (state == Slice.State.ACTIVE) {
            throw new SolrException(ErrorCode.BAD_REQUEST,
                "Sub-shard: " + subSlice + " exists in active state. Aborting split shard.");
          } else if (state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY) {
            // delete the shards
            for (String sub : subSlices) {
              log.info("Sub-shard: {} already exists therefore requesting its deletion", sub);
              Map<String,Object> propMap = new HashMap<>();
              propMap.put(Overseer.QUEUE_OPERATION, "deleteshard");
              propMap.put(COLLECTION_PROP, collectionName);
              propMap.put(SHARD_ID_PROP, sub);
              ZkNodeProps m = new ZkNodeProps(propMap);
              try {
                deleteShard(clusterState, m, new NamedList());
              } catch (Exception e) {
                throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to delete already existing sub shard: " + sub,
                    e);
              }
            }
          }
        }
      }
      
      final String asyncId = message.getStr(ASYNC);
      Map<String,String> requestMap = new HashMap<>();
      
      for (int i = 0; i < subRanges.size(); i++) {
        String subSlice = subSlices.get(i);
        String subShardName = subShardNames.get(i);
        DocRouter.Range subRange = subRanges.get(i);
        
        log.info("Creating slice " + subSlice + " of collection " + collectionName + " on " + nodeName);
        
        Map<String,Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD.toLower());
        propMap.put(ZkStateReader.SHARD_ID_PROP, subSlice);
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        propMap.put(ZkStateReader.SHARD_RANGE_PROP, subRange.toString());
        propMap.put(ZkStateReader.SHARD_STATE_PROP, Slice.State.CONSTRUCTION.toString());
        propMap.put(ZkStateReader.SHARD_PARENT_PROP, parentSlice.getName());
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
        
        // wait until we are able to see the new shard in cluster state
        waitForNewShard(collectionName, subSlice);
        
        // refresh cluster state
        clusterState = zkStateReader.getClusterState();
        
        log.info("Adding replica " + subShardName + " as part of slice " + subSlice + " of collection " + collectionName
            + " on " + nodeName);
        propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
        propMap.put(COLLECTION_PROP, collectionName);
        propMap.put(SHARD_ID_PROP, subSlice);
        propMap.put("node", nodeName);
        propMap.put(CoreAdminParams.NAME, subShardName);
        // copy over property params:
        for (String key : message.keySet()) {
          if (key.startsWith(COLL_PROP_PREFIX)) {
            propMap.put(key, message.getStr(key));
          }
        }
        // add async param
        if (asyncId != null) {
          propMap.put(ASYNC, asyncId);
        }
        addReplica(clusterState, new ZkNodeProps(propMap), results);
      }

      processResponses(results, shardHandler, true, "SPLITSHARD failed to create subshard leaders", asyncId, requestMap);
      
      for (String subShardName : subShardNames) {
        // wait for parent leader to acknowledge the sub-shard core
        log.info("Asking parent leader to wait for: " + subShardName + " to be alive on: " + nodeName);
        String coreNodeName = waitForCoreNodeName(collectionName, nodeName, subShardName);
        CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
        cmd.setCoreName(subShardName);
        cmd.setNodeName(nodeName);
        cmd.setCoreNodeName(coreNodeName);
        cmd.setState(Replica.State.ACTIVE);
        cmd.setCheckLive(true);
        cmd.setOnlyIfLeader(true);
        
        ModifiableSolrParams p = new ModifiableSolrParams(cmd.getParams());
        sendShardRequest(nodeName, p, shardHandler, asyncId, requestMap);
      }

      processResponses(results, shardHandler, true, "SPLITSHARD timed out waiting for subshard leaders to come up",
          asyncId, requestMap);
      
      log.info("Successfully created all sub-shards for collection " + collectionName + " parent shard: " + slice
          + " on: " + parentShardLeader);
          
      log.info("Splitting shard " + parentShardLeader.getName() + " as part of slice " + slice + " of collection "
          + collectionName + " on " + parentShardLeader);
          
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
      params.set(CoreAdminParams.CORE, parentShardLeader.getStr("core"));
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);
        params.add(CoreAdminParams.TARGET_CORE, subShardName);
      }
      params.set(CoreAdminParams.RANGES, rangesStr);
      
      sendShardRequest(parentShardLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

      processResponses(results, shardHandler, true, "SPLITSHARD failed to invoke SPLIT core admin command", asyncId,
          requestMap);
      
      log.info("Index on shard: " + nodeName + " split into two successfully");
      
      // apply buffered updates on sub-shards
      for (int i = 0; i < subShardNames.size(); i++) {
        String subShardName = subShardNames.get(i);
        
        log.info("Applying buffered updates on : " + subShardName);
        
        params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
        params.set(CoreAdminParams.NAME, subShardName);
        
        sendShardRequest(nodeName, params, shardHandler, asyncId, requestMap);
      }

      processResponses(results, shardHandler, true, "SPLITSHARD failed while asking sub shard leaders" +
          " to apply buffered updates", asyncId, requestMap);
      
      log.info("Successfully applied buffered updates on : " + subShardNames);
      
      // Replica creation for the new Slices
      
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores
      
      // TODO: Have replication factor decided in some other way instead of numShards for the parent
      
      int repFactor = parentSlice.getReplicas().size();
      
      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.
      
      // TODO: add smarter options that look at the current number of cores per
      // node?
      // for now we just go random
      Set<String> nodes = clusterState.getLiveNodes();
      List<String> nodeList = new ArrayList<>(nodes.size());
      nodeList.addAll(nodes);
      
      // TODO: Have maxShardsPerNode param for this operation?
      
      // Remove the node that hosts the parent shard for replica creation.
      nodeList.remove(nodeName);
      
      // TODO: change this to handle sharding a slice into > 2 sub-shards.


      Map<Position, String> nodeMap = identifyNodes(clusterState,
          new ArrayList<>(clusterState.getLiveNodes()),
          new ZkNodeProps(collection.getProperties()),
          subSlices, repFactor - 1);

      List<Map<String, Object>> replicas = new ArrayList<>((repFactor - 1) * 2);

        for (Map.Entry<Position, String> entry : nodeMap.entrySet()) {
          String sliceName = entry.getKey().shard;
          String subShardNodeName = entry.getValue();
          String shardName = collectionName + "_" + sliceName + "_replica" + (entry.getKey().index);

          log.info("Creating replica shard " + shardName + " as part of slice " + sliceName + " of collection "
              + collectionName + " on " + subShardNodeName);

          ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.SHARD_ID_PROP, sliceName,
              ZkStateReader.CORE_NAME_PROP, shardName,
              ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
              ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(subShardNodeName),
              ZkStateReader.NODE_NAME_PROP, subShardNodeName);
          Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));

          HashMap<String,Object> propMap = new HashMap<>();
          propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
          propMap.put(COLLECTION_PROP, collectionName);
          propMap.put(SHARD_ID_PROP, sliceName);
          propMap.put("node", subShardNodeName);
          propMap.put(CoreAdminParams.NAME, shardName);
          // copy over property params:
          for (String key : message.keySet()) {
            if (key.startsWith(COLL_PROP_PREFIX)) {
              propMap.put(key, message.getStr(key));
            }
          }
          // add async param
          if (asyncId != null) {
            propMap.put(ASYNC, asyncId);
          }
          // special flag param to instruct addReplica not to create the replica in cluster state again
          propMap.put(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, "true");

          replicas.add(propMap);
        }

      // we must set the slice state into recovery before actually creating the replica cores
      // this ensures that the logic inside Overseer to update sub-shard state to 'active'
      // always gets a chance to execute. See SOLR-7673

      if (repFactor == 1) {
        // switch sub shard states to 'active'
        log.info("Replication factor is 1 so switching shard states");
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        Map<String,Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        propMap.put(slice, Slice.State.INACTIVE.toString());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.ACTIVE.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(Utils.toJSON(m));
      } else {
        log.info("Requesting shard state be set to 'recovery'");
        DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());
        Map<String,Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.RECOVERY.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(Utils.toJSON(m));
      }

      // now actually create replica cores on sub shard nodes
      for (Map<String, Object> replica : replicas) {
        addReplica(clusterState, new ZkNodeProps(replica), results);
      }

      processResponses(results, shardHandler, true, "SPLITSHARD failed to create subshard replicas", asyncId, requestMap);
      
      log.info("Successfully created all replica shards for all sub-slices " + subSlices);
      
      commit(results, slice, parentShardLeader);
      
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

  private String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore) {
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

  private void waitForNewShard(String collectionName, String sliceName) throws KeeperException, InterruptedException {
    log.info("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
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
        log.info("Waited for {}ms for slice {} of collection {} to be available",
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

  private void deleteShard(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    
    log.info("Delete shard invoked");
    Slice slice = clusterState.getSlice(collectionName, sliceId);
    
    if (slice == null) {
      if (clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No shard with name " + sliceId + " exists for collection " + collectionName);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collectionName);
      }
    }
    // For now, only allow for deletions of Inactive slices or custom hashes (range==null).
    // TODO: Add check for range gaps on Slice deletion
    final Slice.State state = slice.getState();
    if (!(slice.getRange() == null || state == Slice.State.INACTIVE || state == Slice.State.RECOVERY
        || state == Slice.State.CONSTRUCTION)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "The slice: " + slice.getName() + " is currently " + state
          + ". Only non-active (or custom-hashed) slices can be deleted.");
    }
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = null;
    if (asyncId != null) {
      requestMap = new HashMap<>(slice.getReplicas().size(), 1.0f);
    }
    
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INDEX, message.getBool(CoreAdminParams.DELETE_INDEX, true));
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, message.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, true));
      params.set(CoreAdminParams.DELETE_DATA_DIR, message.getBool(CoreAdminParams.DELETE_DATA_DIR, true));

      sliceCmd(clusterState, params, null, slice, shardHandler, asyncId, requestMap);

      processResponses(results, shardHandler, true, "Failed to delete shard", asyncId, requestMap, Collections.emptySet());

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETESHARD.toLower(), ZkStateReader.COLLECTION_PROP,
          collectionName, ZkStateReader.SHARD_ID_PROP, sliceId);
      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));
      
      // wait for a while until we don't see the shard
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (! timeout.hasTimedOut()) {
        Thread.sleep(100);
        DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
        removed = collection.getSlice(sliceId) == null;
        if (removed) {
          Thread.sleep(100); // just a bit of time so it's more likely other readers see on return
          break;
        }
      }
      if (!removed) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Could not fully remove collection: " + collectionName + " shard: " + sliceId);
      }
      
      log.info("Successfully deleted collection: " + collectionName + ", shard: " + sliceId);
      
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Error executing delete operation for collection: " + collectionName + " shard: " + sliceId, e);
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

    String asyncId = null;
    if(message.containsKey(ASYNC) && message.get(ASYNC) != null)
      asyncId = message.getStr(ASYNC);

    for (Slice sourceSlice : sourceSlices) {
      for (Slice targetSlice : targetSlices) {
        log.info("Migrating source shard: {} to target shard: {} for split.key = " + splitKey, sourceSlice, targetSlice);
        migrateKey(clusterState, sourceCollection, sourceSlice, targetCollection, targetSlice, splitKey,
            timeout, results, asyncId, message);
      }
    }
  }

  private void migrateKey(ClusterState clusterState, DocCollection sourceCollection, Slice sourceSlice,
                          DocCollection targetCollection, Slice targetSlice,
                          String splitKey, int timeout,
                          NamedList results, String asyncId, ZkNodeProps message) throws KeeperException, InterruptedException {
    String tempSourceCollectionName = "split_" + sourceSlice.getName() + "_temp_" + targetSlice.getName();
    if (clusterState.hasCollection(tempSourceCollectionName)) {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      Map<String, Object> props = makeMap(
          Overseer.QUEUE_OPERATION, DELETE.toLower(),
          NAME, tempSourceCollectionName);

      try {
        deleteCollection(new ZkNodeProps(props), results);
        clusterState = zkStateReader.getClusterState();
      } catch (Exception e) {
        log.warn("Unable to clean up existing temporary collection: " + tempSourceCollectionName, e);
      }
    }

    CompositeIdRouter sourceRouter = (CompositeIdRouter) sourceCollection.getRouter();
    DocRouter.Range keyHashRange = sourceRouter.keyHashRange(splitKey);

    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();

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
    // For tracking async calls.
    Map<String, String> requestMap = new HashMap<>();

    log.info("Asking target leader node: " + targetLeader.getNodeName() + " core: "
        + targetLeader.getStr("core") + " to buffer updates");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTBUFFERUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

    processResponses(results, shardHandler, true, "MIGRATE failed to request node to buffer updates", asyncId, requestMap);

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.ADDROUTINGRULE.toLower(),
        COLLECTION_PROP, sourceCollection.getName(),
        SHARD_ID_PROP, sourceSlice.getName(),
        "routeKey", SolrIndexSplitter.getRouteKey(splitKey) + "!",
        "range", splitRange.toString(),
        "targetCollection", targetCollection.getName(),
        "expireAt", RoutingRule.makeExpiryAt(timeout));
    log.info("Adding routing rule: " + m);
    Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(m));

    // wait for a while until we see the new rule
    log.info("Waiting to see routing rule updated in clusterstate");
    TimeOut waitUntil = new TimeOut(60, TimeUnit.SECONDS);
    boolean added = false;
    while (! waitUntil.hasTimedOut()) {
      Thread.sleep(100);
      sourceCollection = zkStateReader.getClusterState().getCollection(sourceCollection.getName());
      sourceSlice = sourceCollection.getSlice(sourceSlice.getName());
      Map<String, RoutingRule> rules = sourceSlice.getRoutingRules();
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
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, CREATE.toLower(),
        NAME, tempSourceCollectionName,
        REPLICATION_FACTOR, 1,
        NUM_SLICES, 1,
        COLL_CONF, configName,
        CREATE_NODE_SET, sourceLeader.getNodeName());
    if (asyncId != null) {
      String internalAsyncId = asyncId + Math.abs(System.nanoTime());
      props.put(ASYNC, internalAsyncId);
    }

    log.info("Creating temporary collection: " + props);
    createCollection(clusterState, new ZkNodeProps(props), results);
    // refresh cluster state
    clusterState = zkStateReader.getClusterState();
    Slice tempSourceSlice = clusterState.getCollection(tempSourceCollectionName).getSlices().iterator().next();
    Replica tempSourceLeader = zkStateReader.getLeaderRetry(tempSourceCollectionName, tempSourceSlice.getName(), 120000);

    String tempCollectionReplica1 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica1";
    String coreNodeName = waitForCoreNodeName(tempSourceCollectionName,
        sourceLeader.getNodeName(), tempCollectionReplica1);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking source leader to wait for: " + tempCollectionReplica1 + " to be alive on: " + sourceLeader.getNodeName());
    CoreAdminRequest.WaitForState cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempCollectionReplica1);
    cmd.setNodeName(sourceLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(Replica.State.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    // we don't want this to happen asynchronously
    sendShardRequest(tempSourceLeader.getNodeName(), new ModifiableSolrParams(cmd.getParams()), shardHandler, null, null);

    processResponses(results, shardHandler, true, "MIGRATE failed to create temp collection leader" +
        " or timed out waiting for it to come up", asyncId, requestMap);

    log.info("Asking source leader to split index");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
    params.set(CoreAdminParams.CORE, sourceLeader.getStr("core"));
    params.add(CoreAdminParams.TARGET_CORE, tempSourceLeader.getStr("core"));
    params.set(CoreAdminParams.RANGES, splitRange.toString());
    params.set("split.key", splitKey);

    String tempNodeName = sourceLeader.getNodeName();

    sendShardRequest(tempNodeName, params, shardHandler, asyncId, requestMap);
    processResponses(results, shardHandler, true, "MIGRATE failed to invoke SPLIT core admin command", asyncId, requestMap);

    log.info("Creating a replica of temporary collection: {} on the target leader node: {}",
        tempSourceCollectionName, targetLeader.getNodeName());
    String tempCollectionReplica2 = tempSourceCollectionName + "_" + tempSourceSlice.getName() + "_replica2";
    props = new HashMap<>();
    props.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower());
    props.put(COLLECTION_PROP, tempSourceCollectionName);
    props.put(SHARD_ID_PROP, tempSourceSlice.getName());
    props.put("node", targetLeader.getNodeName());
    props.put(CoreAdminParams.NAME, tempCollectionReplica2);
    // copy over property params:
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        props.put(key, message.getStr(key));
      }
    }
    // add async param
    if(asyncId != null) {
      props.put(ASYNC, asyncId);
    }
    addReplica(clusterState, new ZkNodeProps(props), results);

    processResponses(results, shardHandler, true, "MIGRATE failed to create replica of " +
        "temporary collection in target leader node.", asyncId, requestMap);

    coreNodeName = waitForCoreNodeName(tempSourceCollectionName,
        targetLeader.getNodeName(), tempCollectionReplica2);
    // wait for the replicas to be seen as active on temp source leader
    log.info("Asking temp source leader to wait for: " + tempCollectionReplica2 + " to be alive on: " + targetLeader.getNodeName());
    cmd = new CoreAdminRequest.WaitForState();
    cmd.setCoreName(tempSourceLeader.getStr("core"));
    cmd.setNodeName(targetLeader.getNodeName());
    cmd.setCoreNodeName(coreNodeName);
    cmd.setState(Replica.State.ACTIVE);
    cmd.setCheckLive(true);
    cmd.setOnlyIfLeader(true);
    params = new ModifiableSolrParams(cmd.getParams());

    sendShardRequest(tempSourceLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

    processResponses(results, shardHandler, true, "MIGRATE failed to create temp collection" +
        " replica or timed out waiting for them to come up", asyncId, requestMap);

    log.info("Successfully created replica of temp source collection on target leader node");

    log.info("Requesting merge of temp source collection replica to target leader");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.MERGEINDEXES.toString());
    params.set(CoreAdminParams.CORE, targetLeader.getStr("core"));
    params.set(CoreAdminParams.SRC_CORE, tempCollectionReplica2);

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    String msg = "MIGRATE failed to merge " + tempCollectionReplica2 + " to "
        + targetLeader.getStr("core") + " on node: " + targetLeader.getNodeName();
    processResponses(results, shardHandler, true, msg, asyncId, requestMap);

    log.info("Asking target leader to apply buffered updates");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    processResponses(results, shardHandler, true, "MIGRATE failed to request node to apply buffered updates",
        asyncId, requestMap);

    try {
      log.info("Deleting temporary collection: " + tempSourceCollectionName);
      props = makeMap(
          Overseer.QUEUE_OPERATION, DELETE.toLower(),
          NAME, tempSourceCollectionName);
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

  private void sendShardRequest(String nodeName, ModifiableSolrParams params,
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

  private void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }
  }

  private void addPropertyParams(ZkNodeProps message, Map<String,Object> map) {
    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }
  }

  private static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random) {
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

  private void createCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    final String collectionName = message.getStr(NAME);
    log.info("Create collection {}", collectionName);
    if (clusterState.hasCollection(collectionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
    }

    String configName = getConfigName(collectionName, message);
    if (configName == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "No config set found to associate with the collection.");
    } else if (!validateConfig(configName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }

    try {
      // look at the replication factor and see if it matches reality
      // if it does not, find best nodes to create more cores

      int repFactor = message.getInt(REPLICATION_FACTOR, 1);

      ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
      final String async = message.getStr(ASYNC);

      Integer numSlices = message.getInt(NUM_SLICES, null);
      String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);
      List<String> shardNames = new ArrayList<>();
      if(ImplicitDocRouter.NAME.equals(router)){
        ClusterStateMutator.getShardNames(shardNames, message.getStr("shards", null));
        numSlices = shardNames.size();
      } else {
        if (numSlices == null ) {
          throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " is a required param (when using CompositeId router).");
        }
        ClusterStateMutator.getShardNames(numSlices, shardNames);
      }

      int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, 1);

      if (repFactor <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, REPLICATION_FACTOR + " must be greater than 0");
      }

      if (numSlices <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, NUM_SLICES + " must be > 0");
      }

      // we need to look at every node and see how many cores it serves
      // add our new cores to existing nodes serving the least number of cores
      // but (for now) require that each core goes on a distinct node.

      final List<String> nodeList = getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, RANDOM);
      Map<Position, String> positionVsNodes;
      if (nodeList.isEmpty()) {
        log.warn("It is unusual to create a collection ("+collectionName+") without cores.");

        positionVsNodes = new HashMap<>();
      } else {
        if (repFactor > nodeList.size()) {
          log.warn("Specified "
              + REPLICATION_FACTOR
              + " of "
              + repFactor
              + " on collection "
              + collectionName
              + " is higher than or equal to the number of Solr instances currently live or live and part of your " + CREATE_NODE_SET + "("
              + nodeList.size()
              + "). It's unusual to run two replica of the same slice on the same Solr-instance.");
        }
        
        int maxShardsAllowedToCreate = maxShardsPerNode * nodeList.size();
        int requestedShardsToCreate = numSlices * repFactor;
        if (maxShardsAllowedToCreate < requestedShardsToCreate) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Cannot create collection " + collectionName + ". Value of "
              + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
              + ", and the number of nodes currently live or live and part of your "+CREATE_NODE_SET+" is " + nodeList.size()
              + ". This allows a maximum of " + maxShardsAllowedToCreate
              + " to be created. Value of " + NUM_SLICES + " is " + numSlices
              + " and value of " + REPLICATION_FACTOR + " is " + repFactor
              + ". This requires " + requestedShardsToCreate
              + " shards to be created (higher than the allowed number)");
        }

        positionVsNodes = identifyNodes(clusterState, nodeList, message, shardNames, repFactor);
      }

      boolean isLegacyCloud =  Overseer.isLegacy(zkStateReader);

      createConfNode(configName, collectionName, isLegacyCloud);

      Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(message));

      // wait for a while until we don't see the collection
      TimeOut waitUntil = new TimeOut(30, TimeUnit.SECONDS);
      boolean created = false;
      while (! waitUntil.hasTimedOut()) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().hasCollection(collectionName);
        if(created) break;
      }
      if (!created)
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create collection: " + collectionName);

      if (nodeList.isEmpty()) {
        log.info("Finished create command for collection: {}", collectionName);
        return;
      }

      // For tracking async calls.
      Map<String, String> requestMap = new HashMap<>();


      log.info(formatString("Creating SolrCores for new collection {0}, shardNames {1} , replicationFactor : {2}",
          collectionName, shardNames, repFactor));
      Map<String,ShardRequest> coresToCreate = new LinkedHashMap<>();
      for (Map.Entry<Position, String> e : positionVsNodes.entrySet()) {
        Position position = e.getKey();
        String nodeName = e.getValue();
        String coreName = collectionName + "_" + position.shard + "_replica" + (position.index + 1);
        log.info(formatString("Creating core {0} as part of shard {1} of collection {2} on {3}"
            , coreName, position.shard, collectionName, nodeName));


        String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
        //in the new mode, create the replica in clusterstate prior to creating the core.
        // Otherwise the core creation fails
        if (!isLegacyCloud) {
          ZkNodeProps props = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.SHARD_ID_PROP, position.shard,
              ZkStateReader.CORE_NAME_PROP, coreName,
              ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
              ZkStateReader.BASE_URL_PROP, baseUrl);
          Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));
        }

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, coreName);
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, position.shard);
        params.set(ZkStateReader.NUM_SHARDS_PROP, numSlices);

        if (async != null) {
          String coreAdminAsyncId = async + Math.abs(System.nanoTime());
          params.add(ASYNC, coreAdminAsyncId);
          requestMap.put(nodeName, coreAdminAsyncId);
        }
        addPropertyParams(message, params);

        ShardRequest sreq = new ShardRequest();
        sreq.nodeName = nodeName;
        params.set("qt", adminPath);
        sreq.purpose = 1;
        sreq.shards = new String[]{baseUrl};
        sreq.actualShards = sreq.shards;
        sreq.params = params;

        if (isLegacyCloud) {
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        } else {
          coresToCreate.put(coreName, sreq);
        }
      }

      if(!isLegacyCloud) {
        // wait for all replica entries to be created
        Map<String, Replica> replicas = waitToSeeReplicasInState(collectionName, coresToCreate.keySet());
        for (Map.Entry<String, ShardRequest> e : coresToCreate.entrySet()) {
          ShardRequest sreq = e.getValue();
          sreq.params.set(CoreAdminParams.CORE_NODE_NAME, replicas.get(e.getKey()).getName());
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        }
      }

      processResponses(results, shardHandler, false, null, async, requestMap, Collections.emptySet());
      if(results.get("failure") != null && ((SimpleOrderedMap)results.get("failure")).size() > 0) {
        // Let's cleanup as we hit an exception
        // We shouldn't be passing 'results' here for the cleanup as the response would then contain 'success'
        // element, which may be interpreted by the user as a positive ack
        cleanupCollection(collectionName, new NamedList());
        log.info("Cleaned up  artifacts for failed create collection for [" + collectionName + "]");
      } else {
        log.debug("Finished create command on all shards for collection: "
            + collectionName);
      }
    } catch (SolrException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR, null, ex);
    }
  }


  private void cleanupCollection(String collectionName, NamedList results) throws KeeperException, InterruptedException {
    log.error("Cleaning up collection [" + collectionName + "]." );
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    deleteCollection(new ZkNodeProps(props), results);
  }

  private Map<Position, String> identifyNodes(ClusterState clusterState,
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

  private Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
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

  private void addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collection = message.getStr(COLLECTION_PROP);
    String node = message.getStr(CoreAdminParams.NODE);
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
    if (StringUtils.isBlank(coreName)) {
      coreName = message.getStr(CoreAdminParams.PROPERTY_PREFIX + CoreAdminParams.NAME);
    }
    
    final String asyncId = message.getStr(ASYNC);
    
    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    if (coll.getSlice(shard) == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Collection: " + collection + " shard: " + shard + " does not exist");
    }
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    boolean skipCreateReplicaInClusterState = message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false);

    // Kind of unnecessary, but it does put the logic of whether to override maxShardsPerNode in one place.
    if (!skipCreateReplicaInClusterState) {
      node = getNodesForNewReplicas(clusterState, collection, shard, 1, node,
          overseer.getZkController().getCoreContainer()).get(0).nodeName;
    }
    log.info("Node not provided, Identified {} for creating new replica", node);

    if (!clusterState.liveNodesContain(node)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Node: " + node + " is not live");
    }
    if (coreName == null) {
      coreName = Assign.buildCoreName(coll, shard);
    } else if (!skipCreateReplicaInClusterState) {
      //Validate that the core name is unique in that collection
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          String replicaCoreName = replica.getStr(CORE_NAME_PROP);
          if (coreName.equals(replicaCoreName)) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Another replica with the same core name already exists" +
                " for this collection");
          }
        }
      }
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    
    if (!Overseer.isLegacy(zkStateReader)) {
      if (!skipCreateReplicaInClusterState) {
        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(), ZkStateReader.COLLECTION_PROP,
            collection, ZkStateReader.SHARD_ID_PROP, shard, ZkStateReader.CORE_NAME_PROP, coreName,
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(), ZkStateReader.BASE_URL_PROP,
            zkStateReader.getBaseUrlForNodeName(node), ZkStateReader.NODE_NAME_PROP, node);
        Overseer.getStateUpdateQueue(zkStateReader.getZkClient()).offer(Utils.toJSON(props));
      }
      params.set(CoreAdminParams.CORE_NODE_NAME,
          waitToSeeReplicasInState(collection, Collections.singletonList(coreName)).get(coreName).getName());
    }
    
    String configName = zkStateReader.readConfigName(collection);
    String routeKey = message.getStr(ShardParams._ROUTE_);
    String dataDir = message.getStr(CoreAdminParams.DATA_DIR);
    String instanceDir = message.getStr(CoreAdminParams.INSTANCE_DIR);
    
    params.set(CoreAdminParams.ACTION, CoreAdminAction.CREATE.toString());
    params.set(CoreAdminParams.NAME, coreName);
    params.set(COLL_CONF, configName);
    params.set(CoreAdminParams.COLLECTION, collection);
    if (shard != null) {
      params.set(CoreAdminParams.SHARD, shard);
    } else if (routeKey != null) {
      Collection<Slice> slices = coll.getRouter().getSearchSlicesSingle(routeKey, null, coll);
      if (slices.isEmpty()) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No active shard serving _route_=" + routeKey + " found");
      } else {
        params.set(CoreAdminParams.SHARD, slices.iterator().next().getName());
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Specify either 'shard' or _route_ param");
    }
    if (dataDir != null) {
      params.set(CoreAdminParams.DATA_DIR, dataDir);
    }
    if (instanceDir != null) {
      params.set(CoreAdminParams.INSTANCE_DIR, instanceDir);
    }
    addPropertyParams(message, params);
    
    // For tracking async calls.
    Map<String,String> requestMap = new HashMap<>();
    sendShardRequest(node, params, shardHandler, asyncId, requestMap);

    processResponses(results, shardHandler, true, "ADDREPLICA failed to create replica", asyncId, requestMap);

    waitForCoreNodeName(collection, node, coreName);
  }

  private void processBackupAction(ZkNodeProps message, NamedList results) throws IOException, KeeperException, InterruptedException {
    String collectionName =  message.getStr(COLLECTION_PROP);
    String backupName =  message.getStr(NAME);
    String location = message.getStr("location");
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = new HashMap<>();
    Instant startTime = Instant.now();

    // note: we assume a shared files system to backup a collection, since a collection is distributed
    Path backupPath = Paths.get(location).resolve(backupName).toAbsolutePath();

    //Validating if the directory already exists.
    if (Files.exists(backupPath)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Backup directory already exists: " + backupPath);
    }
    Files.createDirectory(backupPath); // create now

    log.info("Starting backup of collection={} with backupName={} at location={}", collectionName, backupName,
        backupPath);

    for (Slice slice : zkStateReader.getClusterState().getCollection(collectionName).getActiveSlices()) {
      Replica replica = slice.getLeader();

      String coreName = replica.getStr(CORE_NAME_PROP);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.BACKUPCORE.toString());
      params.set(NAME, slice.getName());
      params.set("location", backupPath.toString()); // note: index dir will be here then the "snapshot." + slice name
      params.set(CORE_NAME_PROP, coreName);

      sendShardRequest(replica.getNodeName(), params, shardHandler, asyncId, requestMap);
      log.debug("Sent backup request to core={} for backupName={}", coreName, backupName);
    }
    log.debug("Sent backup requests to all shard leaders for backupName={}", backupName);

    processResponses(results, shardHandler, true, "Could not backup all replicas", asyncId, requestMap);

    log.info("Starting to backup ZK data for backupName={}", backupName);

    //Download the configs
    String configName = zkStateReader.readConfigName(collectionName);
    Path zkBackup =  backupPath.resolve("zk_backup");
    zkStateReader.getConfigManager().downloadConfigDir(configName, zkBackup.resolve("configs").resolve(configName));

    //Save the collection's state. Can be part of the monolithic clusterstate.json or a individual state.json
    //Since we don't want to distinguish we extract the state and back it up as a separate json
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    Files.write(zkBackup.resolve("collection_state.json"),
        Utils.toJSON(Collections.singletonMap(collectionName, collection)));

    Path propertiesPath = backupPath.resolve("backup.properties");
    Properties properties = new Properties();

    properties.put("backupName", backupName);
    properties.put("collection", collectionName);
    properties.put("collection.configName", configName);
    properties.put("startTime", startTime.toString());
    //TODO: Add MD5 of the configset. If during restore the same name configset exists then we can compare checksums to see if they are the same.
    //if they are not the same then we can throw an error or have an 'overwriteConfig' flag
    //TODO save numDocs for the shardLeader. We can use it to sanity check the restore.

    try (Writer os = Files.newBufferedWriter(propertiesPath, StandardCharsets.UTF_8)) {
      properties.store(os, "Snapshot properties file");
    }

    log.info("Completed backing up ZK data for backupName={}", backupName);
  }

  private void processRestoreAction(ZkNodeProps message, NamedList results) throws IOException, KeeperException, InterruptedException {
    // TODO maybe we can inherit createCollection's options/code
    String restoreCollectionName =  message.getStr(COLLECTION_PROP);
    String backupName =  message.getStr(NAME); // of backup
    String location = message.getStr("location");
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    String asyncId = message.getStr(ASYNC);
    Map<String, String> requestMap = new HashMap<>();

    Path backupPath = Paths.get(location).resolve(backupName).toAbsolutePath();
    if (!Files.exists(backupPath)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Couldn't restore since doesn't exist: " + backupPath);
    }
    Path backupZkPath =  backupPath.resolve("zk_backup");

    Properties properties = new Properties();
    try (Reader in = Files.newBufferedReader(backupPath.resolve("backup.properties"), StandardCharsets.UTF_8)) {
      properties.load(in);
    }

    String backupCollection = (String) properties.get("collection");
    byte[] data = Files.readAllBytes(backupZkPath.resolve("collection_state.json"));
    ClusterState backupClusterState = ClusterState.load(-1, data, Collections.emptySet());
    DocCollection backupCollectionState = backupClusterState.getCollection(backupCollection);

    //Upload the configs
    String configName = (String) properties.get(COLL_CONF);
    String restoreConfigName = message.getStr(COLL_CONF, configName);
    if (zkStateReader.getConfigManager().configExists(restoreConfigName)) {
      log.info("Using existing config {}", restoreConfigName);
      //TODO add overwrite option?
    } else {
      log.info("Uploading config {}", restoreConfigName);
      zkStateReader.getConfigManager().uploadConfigDir(backupZkPath.resolve("configs").resolve(configName), restoreConfigName);
    }

    log.info("Starting restore into collection={} with backup_name={} at location={}", restoreCollectionName, backupName,
        backupPath);

    //Create core-less collection
    {
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATE.toString());
      propMap.put("fromApi", "true"); // mostly true.  Prevents autoCreated=true in the collection state.

      // inherit settings from input API, defaulting to the backup's setting.  Ex: replicationFactor
      for (String collProp : COLL_PROPS.keySet()) {
        Object val = message.getProperties().getOrDefault(collProp, backupCollectionState.get(collProp));
        if (val != null) {
          propMap.put(collProp, val);
        }
      }

      propMap.put(NAME, restoreCollectionName);
      propMap.put(CREATE_NODE_SET, CREATE_NODE_SET_EMPTY); //no cores
      propMap.put(COLL_CONF, restoreConfigName);

      // router.*
      @SuppressWarnings("unchecked")
      Map<String, Object> routerProps = (Map<String, Object>) backupCollectionState.getProperties().get(DocCollection.DOC_ROUTER);
      for (Map.Entry<String, Object> pair : routerProps.entrySet()) {
        propMap.put(DocCollection.DOC_ROUTER + "." + pair.getKey(), pair.getValue());
      }

      Set<String> sliceNames = backupCollectionState.getActiveSlicesMap().keySet();
      if (backupCollectionState.getRouter() instanceof ImplicitDocRouter) {
        propMap.put(SHARDS_PROP, StrUtils.join(sliceNames, ','));
      } else {
        propMap.put(NUM_SLICES, sliceNames.size());
        // ClusterStateMutator.createCollection detects that "slices" is in fact a slice structure instead of a
        //   list of names, and if so uses this instead of building it.  We clear the replica list.
        Collection<Slice> backupSlices = backupCollectionState.getActiveSlices();
        Map<String,Slice> newSlices = new LinkedHashMap<>(backupSlices.size());
        for (Slice backupSlice : backupSlices) {
          newSlices.put(backupSlice.getName(),
              new Slice(backupSlice.getName(), Collections.emptyMap(), backupSlice.getProperties()));
        }
        propMap.put(SHARDS_PROP, newSlices);
      }

      createCollection(zkStateReader.getClusterState(), new ZkNodeProps(propMap), new NamedList());
      // note: when createCollection() returns, the collection exists (no race)
    }

    DocCollection restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    DistributedQueue inQueue = Overseer.getStateUpdateQueue(zkStateReader.getZkClient());

    //Mark all shards in CONSTRUCTION STATE while we restore the data
    {
      //TODO might instead createCollection accept an initial state?  Is there a race?
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.CONSTRUCTION.toString());
      }
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
      inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    // TODO how do we leverage the CREATE_NODE_SET / RULE / SNITCH logic in createCollection?

    ClusterState clusterState = zkStateReader.getClusterState();
    //Create one replica per shard and copy backed up data to it
    for (Slice slice: restoreCollection.getSlices()) {
      log.debug("Adding replica for shard={} collection={} ", slice.getName(), restoreCollection);
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, CREATESHARD);
      propMap.put(COLLECTION_PROP, restoreCollectionName);
      propMap.put(SHARD_ID_PROP, slice.getName());
      // add async param
      if (asyncId != null) {
        propMap.put(ASYNC, asyncId);
      }
      addPropertyParams(message, propMap);

      addReplica(clusterState, new ZkNodeProps(propMap), new NamedList());
    }

    //refresh the location copy of collection state
    restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    //Copy data from backed up index to each replica
    for (Slice slice: restoreCollection.getSlices()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.RESTORECORE.toString());
      params.set(NAME, "snapshot." + slice.getName());
      params.set("location", backupPath.toString());
      sliceCmd(clusterState, params, null, slice, shardHandler, asyncId, requestMap);
    }
    processResponses(new NamedList(), shardHandler, true, "Could not restore core", asyncId, requestMap);

    //Mark all shards in ACTIVE STATE
    {
      HashMap<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
      propMap.put(ZkStateReader.COLLECTION_PROP, restoreCollectionName);
      for (Slice shard : restoreCollection.getSlices()) {
        propMap.put(shard.getName(), Slice.State.ACTIVE.toString());
      }
      inQueue.offer(Utils.toJSON(new ZkNodeProps(propMap)));
    }

    //refresh the location copy of collection state
    restoreCollection = zkStateReader.getClusterState().getCollection(restoreCollectionName);

    //Add the remaining replicas for each shard
    Integer numReplicas = restoreCollection.getReplicationFactor();
    if (numReplicas != null && numReplicas > 1) {
      log.info("Adding replicas to restored collection={}", restoreCollection);

      for (Slice slice: restoreCollection.getSlices()) {
        for(int i=1; i<numReplicas; i++) {
          log.debug("Adding replica for shard={} collection={} ", slice.getName(), restoreCollection);
          HashMap<String, Object> propMap = new HashMap<>();
          propMap.put(COLLECTION_PROP, restoreCollectionName);
          propMap.put(SHARD_ID_PROP, slice.getName());
          // add async param
          if (asyncId != null) {
            propMap.put(ASYNC, asyncId);
          }
          addPropertyParams(message, propMap);

          addReplica(zkStateReader.getClusterState(), new ZkNodeProps(propMap), results);
        }
      }
    }

    log.info("Completed restoring collection={} backupName={}", restoreCollection, backupName);
  }

  private void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
                                String asyncId, Map<String, String> requestMap) {
    processResponses(results, shardHandler, abortOnError, msgOnError, asyncId, requestMap, Collections.emptySet());
  }

  private void processResponses(NamedList results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
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

  private String getConfigName(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    String configName = message.getStr(COLL_CONF);
    
    if (configName == null) {
      // if there is only one conf, use that
      List<String> configNames = null;
      try {
        configNames = zkStateReader.getZkClient().getChildren(ZkConfigManager.CONFIGS_ZKNODE, null, true);
        if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          log.info("Only one config set found in zk - using it:" + configName);
        } else if (configNames.contains(coll)) {
          configName = coll;
        }
      } catch (KeeperException.NoNodeException e) {

      }
    }
    return configName;
  }
  
  private boolean validateConfig(String configName) throws KeeperException, InterruptedException {
    return zkStateReader.getZkClient().exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName, true);
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  private void createConfNode(String configName, String coll, boolean isLegacyCloud) throws KeeperException, InterruptedException {
    
    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.info("creating collections conf node {} ", collDir);
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


  private void collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
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

  private void sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
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


 /* @Override
  public void markExclusiveTask(String collectionName, ZkNodeProps message) {
    if (collectionName != null) {
      synchronized (collectionWip) {
        collectionWip.add(collectionName);
      }
    }
  }

  @Override
  public void unmarkExclusiveTask(String collectionName, String operation, ZkNodeProps message) {
    if(collectionName != null) {
      synchronized (collectionWip) {
        collectionWip.remove(collectionName);
      }
    }
  }*/
/*
  @Override
  public ExclusiveMarking checkExclusiveMarking(String collectionName, ZkNodeProps message) {
    synchronized (collectionWip) {
      if(collectionWip.contains(collectionName))
        return ExclusiveMarking.NONEXCLUSIVE;
    }

    return ExclusiveMarking.NOTDETERMINED;
  }*/

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

}
