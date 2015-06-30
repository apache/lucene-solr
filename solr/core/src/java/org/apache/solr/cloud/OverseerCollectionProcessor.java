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

import java.io.Closeable;
import java.io.IOException;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.Assign.Node;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.ReplicaAssigner.Position;
import org.apache.solr.cloud.rule.Rule;
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
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.ClusterStatus;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.Snapshot;
import org.apache.solr.util.stats.Timer;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Assign.getNodesForNewShard;
import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
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
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;


public class OverseerCollectionProcessor implements Runnable, Closeable {

  public static final String NUM_SLICES = "numShards";
  
  static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = "createNodeSet.shuffle";
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

  public int maxParallelThreads = 10;

  public static final Map<String, Object> COLL_PROPS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.MAX_SHARDS_PER_NODE, "1",
      ZkStateReader.AUTO_ADD_REPLICAS, "false",
      DocCollection.RULE, null,
      SNITCH, null));

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

  public ExecutorService tpe ;

  private static Logger log = LoggerFactory
      .getLogger(OverseerCollectionProcessor.class);

  private DistributedQueue workQueue;
  private DistributedMap runningMap;
  private DistributedMap completedMap;
  private DistributedMap failureMap;

  // Set that maintains a list of all the tasks that are running. This is keyed on zk id of the task.
  final private Set runningTasks;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.
  final private Set collectionWip;

  // List of completed tasks. This is used to clean up workQueue in zk.
  final private HashMap<String, QueueEvent> completedTasks;

  private String myId;

  private final ShardHandlerFactory shardHandlerFactory;

  private String adminPath;

  private ZkStateReader zkStateReader;

  private boolean isClosed;

  private Overseer.Stats stats;

  // Set of tasks that have been picked up for processing but not cleaned up from zk work-queue.
  // It may contain tasks that have completed execution, have been entered into the completed/failed map in zk but not
  // deleted from the work-queue as that is a batched operation.
  final private Set<String> runningZKTasks;
  private final Object waitLock = new Object();
  private Overseer overseer;

  public OverseerCollectionProcessor(ZkStateReader zkStateReader, String myId,
                                     final ShardHandler shardHandler,
                                     String adminPath, Overseer.Stats stats, Overseer overseer) {
    this(zkStateReader, myId, shardHandler.getShardHandlerFactory(), adminPath, stats, Overseer.getCollectionQueue(zkStateReader.getZkClient(), stats),
        Overseer.getRunningMap(zkStateReader.getZkClient()),
        Overseer.getCompletedMap(zkStateReader.getZkClient()), Overseer.getFailureMap(zkStateReader.getZkClient()));
    this.overseer = overseer;
  }

  protected OverseerCollectionProcessor(ZkStateReader zkStateReader, String myId,
                                        final ShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Overseer.Stats stats,
                                        DistributedQueue workQueue,
                                        DistributedMap runningMap,
                                        DistributedMap completedMap,
                                        DistributedMap failureMap) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    this.stats = stats;
    this.runningZKTasks = new HashSet<>();
    this.runningTasks = new HashSet();
    this.collectionWip = new HashSet();
    this.completedTasks = new HashMap<>();
  }

  @Override
  public void run() {
    log.info("Process current queue of collection creations");
    LeaderStatus isLeader = amILeader();
    while (isLeader == LeaderStatus.DONT_KNOW) {
      log.debug("am_i_leader unclear {}", isLeader);
      isLeader = amILeader();  // not a no, not a yes, try ask again
    }

    String oldestItemInWorkQueue = null;
    // hasLeftOverItems - used for avoiding re-execution of async tasks that were processed by a previous Overseer.
    // This variable is set in case there's any task found on the workQueue when the OCP starts up and
    // the id for the queue tail is used as a marker to check for the task in completed/failed map in zk.
    // Beyond the marker, all tasks can safely be assumed to have never been executed.
    boolean hasLeftOverItems = true;

    try {
      oldestItemInWorkQueue = workQueue.getTailId();
    } catch (KeeperException e) {
      // We don't need to handle this. This is just a fail-safe which comes in handy in skipping already processed
      // async calls.
      SolrException.log(log, "", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (oldestItemInWorkQueue == null)
      hasLeftOverItems = false;
    else
      log.debug("Found already existing elements in the work-queue. Last element: {}", oldestItemInWorkQueue);

    try {
      prioritizeOverseerNodes();
    } catch (Exception e) {
      log.error("Unable to prioritize overseer ", e);
    }

    // TODO: Make maxThreads configurable.

    this.tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 100, 0L, TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>(),
        new DefaultSolrThreadFactory("OverseerThreadFactory"));
    try {
      while (!this.isClosed) {
        try {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          } else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try asking again
          }

          log.debug("Cleaning up work-queue. #Running tasks: {}", runningTasks.size());
          cleanUpWorkQueue();

          printTrackingMaps();

          boolean waited = false;

          while (runningTasks.size() > maxParallelThreads) {
            synchronized (waitLock) {
              waitLock.wait(100);//wait for 100 ms or till a task is complete
            }
            waited = true;
          }

          if (waited)
            cleanUpWorkQueue();

          List<QueueEvent> heads = workQueue.peekTopN(maxParallelThreads, runningZKTasks, 2000L);

          if (heads == null)
            continue;

          log.debug("Got {} tasks from work-queue : [{}]", heads.size(), heads.toString());

          if (isClosed) break;

          for (QueueEvent head : heads) {
            final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
            String collectionName = message.containsKey(COLLECTION_PROP) ?
                message.getStr(COLLECTION_PROP) : message.getStr(NAME);
            final String asyncId = message.getStr(ASYNC);
            if (hasLeftOverItems) {
              if (head.getId().equals(oldestItemInWorkQueue))
                hasLeftOverItems = false;
              if (asyncId != null && (completedMap.contains(asyncId) || failureMap.contains(asyncId))) {
                log.debug("Found already processed task in workQueue, cleaning up. AsyncId [{}]",asyncId );
                workQueue.remove(head);
                continue;
              }
            }

            if (!checkExclusivity(message, head.getId())) {
              log.debug("Exclusivity check failed for [{}]", message.toString());
              continue;
            }

            try {
              markTaskAsRunning(head, collectionName, asyncId, message);
              log.debug("Marked task [{}] as running", head.getId());
            } catch (KeeperException.NodeExistsException e) {
              // This should never happen
              log.error("Tried to pick up task [{}] when it was already running!", head.getId());
            } catch (InterruptedException e) {
              log.error("Thread interrupted while trying to pick task for execution.", head.getId());
              Thread.currentThread().interrupt();
            }

            log.info("Overseer Collection Processor: Get the message id:" + head.getId() + " message:" + message.toString());
            String operation = message.getStr(Overseer.QUEUE_OPERATION);
            Runner runner = new Runner(message,
                operation, head);
            tpe.execute(runner);
          }

        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
            log.warn("Overseer cannot talk to ZK");
            return;
          }
          SolrException.log(log, "", e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          SolrException.log(log, "", e);
        }
      }
    } finally {
      this.close();
    }
  }

  private boolean checkExclusivity(ZkNodeProps message, String id) throws KeeperException, InterruptedException {
    String collectionName = message.containsKey(COLLECTION_PROP) ?
        message.getStr(COLLECTION_PROP) : message.getStr(NAME);

    if(collectionName == null)
      return true;

    // CLUSTERSTATUS is always mutually exclusive
    //TODO deprecated remove this check .
    if(CLUSTERSTATUS.isEqual(message.getStr(Overseer.QUEUE_OPERATION)))
      return true;

    if(collectionWip.contains(collectionName))
      return false;

    if(runningZKTasks.contains(id))
      return false;

    return true;
  }

  private void cleanUpWorkQueue() throws KeeperException, InterruptedException {
    synchronized (completedTasks) {
      for (String id : completedTasks.keySet()) {
        workQueue.remove(completedTasks.get(id));
        runningZKTasks.remove(id);
      }
      completedTasks.clear();
    }
  }

  public void close() {
    isClosed = true;
    if(tpe != null) {
      if (!tpe.isShutdown()) {
        tpe.shutdown();
        try {
          tpe.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          log.warn("Thread interrupted while waiting for OCP threadpool close.");
          Thread.currentThread().interrupt();
        } finally {
          if (!tpe.isShutdown())
            tpe.shutdownNow();
        }
      }
    }
  }

  private synchronized void prioritizeOverseerNodes() throws KeeperException, InterruptedException {
    SolrZkClient zk = zkStateReader.getZkClient();
    if(!zk.exists(ZkStateReader.ROLES,true))return;
    Map m = (Map) ZkStateReader.fromJSON(zk.getData(ZkStateReader.ROLES, null, new Stat(), true));

    List overseerDesignates = (List) m.get("overseer");
    if(overseerDesignates==null || overseerDesignates.isEmpty()) return;
    String ldr = getLeaderNode(zk);
    if(overseerDesignates.contains(ldr)) return;
    log.info("prioritizing overseer nodes at {} overseer designates are {}", myId, overseerDesignates);
    List<String> electionNodes = getSortedElectionNodes(zk, OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE);
    if(electionNodes.size()<2) return;
    log.info("sorted nodes {}", electionNodes);

    String designateNodeId = null;
    for (String electionNode : electionNodes) {
      if(overseerDesignates.contains( LeaderElector.getNodeName(electionNode))){
        designateNodeId = electionNode;
        break;
      }
    }

    if(designateNodeId == null){
      log.warn("No live overseer designate ");
      return;
    }
    if(!designateNodeId.equals( electionNodes.get(1))) { //checking if it is already at no:1
      log.info("asking node {} to come join election at head", designateNodeId);
      invokeOverseerOp(designateNodeId, "rejoinAtHead"); //ask designate to come first
      log.info("asking the old first in line {} to rejoin election  ",electionNodes.get(1) );
      invokeOverseerOp(electionNodes.get(1), "rejoin");//ask second inline to go behind
    }
    //now ask the current leader to QUIT , so that the designate can takeover
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(
        ZkStateReader.toJSON(new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower(),
            "id",getLeaderId(zkStateReader.getZkClient()))));

  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(OverseerElectionContext.PATH + LeaderElector.ELECTION_NODE, null, true);
    } catch (Exception e) {
      log.warn("error ", e);
      return new ArrayList<>();
    }
    LeaderElector.sortSeqs(children);
    ArrayList<String> nodeNames = new ArrayList<>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));
    return nodeNames;
  }

  public static List<String> getSortedElectionNodes(SolrZkClient zk, String path) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zk.getChildren(path, null, true);
      LeaderElector.sortSeqs(children);
      return children;
    } catch (Exception e) {
      throw e;
    }

  }

  public static String getLeaderNode(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    String id = getLeaderId(zkClient);
    return id==null ?
        null:
        LeaderElector.getNodeName( id);
  }

  public static String getLeaderId(SolrZkClient zkClient) throws KeeperException,InterruptedException{
    byte[] data = null;
    try {
      data = zkClient.getData("/overseer_elect/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map m = (Map) ZkStateReader.fromJSON(data);
    return  (String) m.get("id");
  }

  private void invokeOverseerOp(String electionNode, String op) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.OVERSEEROP.toString());
    params.set("op", op);
    params.set("qt", adminPath);
    params.set("electionNode", electionNode);
    ShardRequest sreq = new ShardRequest();
    sreq.purpose = 1;
    String replica = zkStateReader.getBaseUrlForNodeName(LeaderElector.getNodeName(electionNode));
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    shardHandler.submit(sreq, replica, sreq.params);
    shardHandler.takeCompletedOrError();
  }

  protected LeaderStatus amILeader() {
    TimerContext timerContext = stats.time("collection_am_i_leader");
    boolean success = true;
    try {
      ZkNodeProps props = ZkNodeProps.load(zkStateReader.getZkClient().getData(
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
        stats.success("collection_am_i_leader");
      } else  {
        stats.error("collection_am_i_leader");
      }
    }
    log.info("According to ZK I (id=" + myId + ") am no longer a leader.");
    return LeaderStatus.NO;
  }

  @SuppressWarnings("unchecked")
  protected SolrResponse processMessage(ZkNodeProps message, String operation) {
    log.warn("OverseerCollectionProcessor.processMessage : "+ operation + " , "+ message.toString());

    NamedList results = new NamedList();
    try {
      // force update the cluster state
      zkStateReader.updateClusterState(true);
      CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(operation);
      if (action == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
      switch (action) {
        case CREATE:
          createCollection(zkStateReader.getClusterState(), message, results);
          break;
        case DELETE:
          deleteCollection(message, results);
          break;
        case RELOAD:
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());
          collectionCmd(zkStateReader.getClusterState(), message, params, results, Replica.State.ACTIVE);
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
        case CLUSTERSTATUS://TODO . deprecated. OCP does not need to do it .remove in a later release
          new ClusterStatus(zkStateReader, message).getClusterStatus(results);
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
          overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));
          break;
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

  @SuppressWarnings("unchecked")
  private void processRebalanceLeaders(ZkNodeProps message) throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP,
        NODE_NAME_PROP, BASE_URL_PROP, REJOIN_AT_HEAD_PROP);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
    params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
    params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REJOINLEADERELECTION.toString());
    params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
    params.set(NODE_NAME_PROP, message.getStr(NODE_NAME_PROP));
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
    DistributedQueue inQueue = Overseer.getInQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(ZkStateReader.toJSON(m));
  }

  private void processReplicaDeletePropertyCommand(ZkNodeProps message) throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getInQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(ZkStateReader.toJSON(m));
  }

  private void balanceProperty(ZkNodeProps message) throws KeeperException, InterruptedException {
    if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
              "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
    }
    SolrZkClient zkClient = zkStateReader.getZkClient();
    DistributedQueue inQueue = Overseer.getInQueue(zkClient);
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
    propMap.putAll(message.getProperties());
    inQueue.offer(ZkStateReader.toJSON(new ZkNodeProps(propMap)));
  }


  @SuppressWarnings("unchecked")
  private void getOverseerStatus(ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String leaderNode = getLeaderNode(zkStateReader.getZkClient());
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

  @SuppressWarnings("unchecked")
  private void getClusterStatus(ClusterState clusterState, ZkNodeProps message, NamedList results) throws KeeperException, InterruptedException {
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);

    // read aliases
    Aliases aliases = zkStateReader.getAliases();
    Map<String, List<String>> collectionVsAliases = new HashMap<>();
    Map<String, String> aliasVsCollections = aliases.getCollectionAliasMap();
    if (aliasVsCollections != null) {
      for (Map.Entry<String, String> entry : aliasVsCollections.entrySet()) {
        List<String> colls = StrUtils.splitSmart(entry.getValue(), ',');
        String alias = entry.getKey();
        for (String coll : colls) {
          if (collection == null || collection.equals(coll))  {
            List<String> list = collectionVsAliases.get(coll);
            if (list == null) {
              list = new ArrayList<>();
              collectionVsAliases.put(coll, list);
            }
            list.add(alias);
          }
        }
      }
    }

    Map roles = null;
    if (zkStateReader.getZkClient().exists(ZkStateReader.ROLES, true)) {
      roles = (Map) ZkStateReader.fromJSON(zkStateReader.getZkClient().getData(ZkStateReader.ROLES, null, null, true));
    }

    // convert cluster state into a map of writable types
    byte[] bytes = ZkStateReader.toJSON(clusterState);
    Map<String, Object> stateMap = (Map<String,Object>) ZkStateReader.fromJSON(bytes);

    Set<String> collections = new HashSet<>();
    String routeKey = message.getStr(ShardParams._ROUTE_);
    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);
    if (collection == null) {
      collections = new HashSet<>(clusterState.getCollections());
    } else  {
      collections = Collections.singleton(collection);
    }

    NamedList<Object> collectionProps = new SimpleOrderedMap<Object>();

    for (String name : collections) {
      Map<String, Object> collectionStatus = null;
      DocCollection clusterStateCollection = clusterState.getCollection(name);

      Set<String> requestedShards = new HashSet<>();
      if (routeKey != null) {
        DocRouter router = clusterStateCollection.getRouter();
        Collection<Slice> slices = router.getSearchSlices(routeKey, null, clusterStateCollection);
        for (Slice slice : slices) {
          requestedShards.add(slice.getName());
        }
      }
      if (shard != null) {
        requestedShards.add(shard);
      }

      if (clusterStateCollection.getStateFormat() > 1) {
        bytes = ZkStateReader.toJSON(clusterStateCollection);
        Map<String, Object> docCollection = (Map<String, Object>) ZkStateReader.fromJSON(bytes);
        collectionStatus = getCollectionStatus(docCollection, name, requestedShards);
      } else {
        collectionStatus = getCollectionStatus((Map<String, Object>) stateMap.get(name), name, requestedShards);
      }

      collectionStatus.put("znodeVersion", clusterStateCollection.getZNodeVersion());
      if (collectionVsAliases.containsKey(name) && !collectionVsAliases.get(name).isEmpty()) {
        collectionStatus.put("aliases", collectionVsAliases.get(name));
      }
      String configName = zkStateReader.readConfigName(name);
      collectionStatus.put("configName", configName);
      collectionProps.add(name, collectionStatus);
    }

    List<String> liveNodes = zkStateReader.getZkClient().getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true);

    // now we need to walk the collectionProps tree to cross-check replica state with live nodes
    crossCheckReplicaStateWithLiveNodes(liveNodes, collectionProps);

    NamedList<Object> clusterStatus = new SimpleOrderedMap<>();
    clusterStatus.add("collections", collectionProps);

    // read cluster properties
    Map clusterProps = zkStateReader.getClusterProps();
    if (clusterProps != null && !clusterProps.isEmpty())  {
      clusterStatus.add("properties", clusterProps);
    }

    // add the alias map too
    if (aliasVsCollections != null && !aliasVsCollections.isEmpty())  {
      clusterStatus.add("aliases", aliasVsCollections);
    }

    // add the roles map
    if (roles != null)  {
      clusterStatus.add("roles", roles);
    }

    // add live_nodes
    clusterStatus.add("live_nodes", liveNodes);

    results.add("cluster", clusterStatus);
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

  @SuppressWarnings("unchecked")
  private void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    String replicaName = message.getStr(REPLICA_PROP);
    
    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = coll.getSlice(shard);
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
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
    
    String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
    String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    
    // assume the core exists and try to unload it
    Map m = makeMap("qt", adminPath, CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString(), CoreAdminParams.CORE,
        core, CoreAdminParams.DELETE_INSTANCE_DIR, "true", CoreAdminParams.DELETE_DATA_DIR, "true");
        
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
    
    collectShardResponses(replica.getState() != Replica.State.ACTIVE ? new NamedList() : results, false, null,
        shardHandler);
        
    if (waitForCoreNodeGone(collectionName, shard, replicaName, 5000)) return;// check if the core unload removed the
                                                                              // corenode zk enry
    deleteCoreNode(collectionName, replicaName, replica, core); // try and ensure core info is removed from clusterstate
    if (waitForCoreNodeGone(collectionName, shard, replicaName, 30000)) return;
    
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not  remove replica : " + collectionName + "/" + shard + "/" + replicaName);
        
  }

  private boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    boolean deleted = false;
    while (System.nanoTime() < waitUntil) {
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
    final String collection = message.getStr(NAME);
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);
      collectionCmd(zkStateReader.getClusterState(), message, params, results,
          null);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          DELETE.toLower(), NAME, collection);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(
          ZkStateReader.toJSON(m));

      // wait for a while until we don't see the collection
      long now = System.nanoTime();
      long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (System.nanoTime() < timeout) {
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
      jsonBytes = ZkStateReader.toJSON(newAliases.getAliasMap());
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

    long now = System.nanoTime();
    long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases = null;
    while (System.nanoTime() < timeout) {
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

    long now = System.nanoTime();
    long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases = null;
    while (System.nanoTime() < timeout) {
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
    List<Node> sortedNodeList = getNodesForNewShard(clusterState, collectionName, sliceName, repFactor,
        createNodeSetStr, overseer.getZkController().getCoreContainer());
        
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));
    // wait for a while until we see the shard
    long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
    boolean created = false;
    while (System.nanoTime() < waitUntil) {
      Thread.sleep(100);
      created = zkStateReader.getClusterState().getCollection(collectionName).getSlice(sliceName) != null;
      if (created) break;
    }
    if (!created)
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create shard: " + message.getStr(NAME));
      
    String configName = message.getStr(COLL_CONF);
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
      
      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;
      
      shardHandler.submit(sreq, replica, sreq.params);
      
    }
    
    processResponses(results, shardHandler);
    
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
    
    Slice parentSlice = null;
    
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
      parentSlice = clusterState.getSlice(collectionName, slice);
    }
    
    if (parentSlice == null) {
      if (clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No shard with the specified name exists: " + slice);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No collection with the specified name exists: " + collectionName);
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
        
        Slice oSlice = clusterState.getSlice(collectionName, subSlice);
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
      
      // do not abort splitshard if the unloading fails
      // this can happen because the replicas created previously may be down
      // the only side effect of this is that the sub shard may end up having more replicas than we want
      collectShardResponses(results, false, null, shardHandler);
      
      final String asyncId = message.getStr(ASYNC);
      HashMap<String,String> requestMap = new HashMap<>();
      
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
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        inQueue.offer(ZkStateReader.toJSON(new ZkNodeProps(propMap)));
        
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
      
      collectShardResponses(results, true, "SPLITSHARD failed to create subshard leaders", shardHandler);
      
      completeAsyncRequest(asyncId, requestMap, results);
      
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
      
      collectShardResponses(results, true, "SPLITSHARD timed out waiting for subshard leaders to come up",
          shardHandler);
          
      completeAsyncRequest(asyncId, requestMap, results);
      
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
      
      collectShardResponses(results, true, "SPLITSHARD failed to invoke SPLIT core admin command", shardHandler);
      completeAsyncRequest(asyncId, requestMap, results);
      
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
      
      collectShardResponses(results, true, "SPLITSHARD failed while asking sub shard leaders to apply buffered updates",
          shardHandler);
          
      completeAsyncRequest(asyncId, requestMap, results);
      
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
      List<String> nodeList = new ArrayList<>(nodes.size());
      nodeList.addAll(nodes);
      
      // TODO: Have maxShardsPerNode param for this operation?
      
      // Remove the node that hosts the parent shard for replica creation.
      nodeList.remove(nodeName);
      
      // TODO: change this to handle sharding a slice into > 2 sub-shards.

      List<Map<String, Object>> replicas = new ArrayList<>((repFactor - 1) * 2);
      for (int i = 1; i <= subSlices.size(); i++) {
        Collections.shuffle(nodeList, RANDOM);
        String sliceName = subSlices.get(i - 1);
        for (int j = 2; j <= repFactor; j++) {
          String subShardNodeName = nodeList.get((repFactor * (i - 1) + (j - 2)) % nodeList.size());
          String shardName = collectionName + "_" + sliceName + "_replica" + (j);

          log.info("Creating replica shard " + shardName + " as part of slice " + sliceName + " of collection "
              + collectionName + " on " + subShardNodeName);

          ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.SHARD_ID_PROP, sliceName,
              ZkStateReader.CORE_NAME_PROP, shardName,
              ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
              ZkStateReader.BASE_URL_PROP, zkStateReader.getBaseUrlForNodeName(subShardNodeName),
              ZkStateReader.NODE_NAME_PROP, subShardNodeName);
          Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(props));

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
      }

      // we must set the slice state into recovery before actually creating the replica cores
      // this ensures that the logic inside Overseer to update sub-shard state to 'active'
      // always gets a chance to execute. See SOLR-7673

      if (repFactor == 1) {
        // switch sub shard states to 'active'
        log.info("Replication factor is 1 so switching shard states");
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        Map<String,Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        propMap.put(slice, Slice.State.INACTIVE.toString());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.ACTIVE.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(ZkStateReader.toJSON(m));
      } else {
        log.info("Requesting shard state be set to 'recovery'");
        DistributedQueue inQueue = Overseer.getInQueue(zkStateReader.getZkClient());
        Map<String,Object> propMap = new HashMap<>();
        propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
        for (String subSlice : subSlices) {
          propMap.put(subSlice, Slice.State.RECOVERY.toString());
        }
        propMap.put(ZkStateReader.COLLECTION_PROP, collectionName);
        ZkNodeProps m = new ZkNodeProps(propMap);
        inQueue.offer(ZkStateReader.toJSON(m));
      }

      // now actually create replica cores on sub shard nodes
      for (Map<String, Object> replica : replicas) {
        addReplica(clusterState, new ZkNodeProps(replica), results);
      }
      
      collectShardResponses(results, true,
          "SPLITSHARD failed to create subshard replicas", shardHandler);
          
      completeAsyncRequest(asyncId, requestMap, results);
      
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
      processResponse(results, null, coreUrl, updateResponse, slice);
    } catch (Exception e) {
      processResponse(results, e, coreUrl, updateResponse, slice);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }
  }


  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {

    try (HttpSolrClient client = new HttpSolrClient(url)) {
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
    long startTime = System.currentTimeMillis();
    int retryCount = 320;
    while (retryCount-- > 0) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Unable to find collection: " + collectionName + " in clusterstate");
      }
      Slice slice = collection.getSlice(sliceName);
      if (slice != null) {
        log.info("Waited for {} seconds for slice {} of collection {} to be available",
            (System.currentTimeMillis() - startTime) / 1000, sliceName, collectionName);
        return;
      }
      Thread.sleep(1000);
      zkStateReader.updateClusterState(true);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not find new slice " + sliceName + " in collection " + collectionName
            + " even after waiting for " + (System.currentTimeMillis() - startTime) / 1000 + " seconds"
    );
  }

  private void collectShardResponses(NamedList results, boolean abortOnError,
                                     String msgOnError,
                                     ShardHandler shardHandler) {
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
    String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    String sliceId = message.getStr(ZkStateReader.SHARD_ID_PROP);
    
    log.info("Delete shard invoked");
    Slice slice = clusterState.getSlice(collection, sliceId);
    
    if (slice == null) {
      if (clusterState.hasCollection(collection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "No shard with name " + sliceId + " exists for collection " + collection);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No collection with the specified name exists: " + collection);
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
    
    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INDEX, "true");
      sliceCmd(clusterState, params, null, slice, shardHandler);
      
      processResponses(results, shardHandler);
      
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETESHARD.toLower(), ZkStateReader.COLLECTION_PROP,
          collection, ZkStateReader.SHARD_ID_PROP, sliceId);
      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(m));
      
      // wait for a while until we don't see the shard
      long now = System.nanoTime();
      long timeout = now + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean removed = false;
      while (System.nanoTime() < timeout) {
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
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Error executing delete operation for collection: " + collection + " shard: " + sliceId, e);
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
    HashMap<String, String> requestMap = new HashMap<String, String>();

    log.info("Asking target leader node: " + targetLeader.getNodeName() + " core: "
        + targetLeader.getStr("core") + " to buffer updates");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTBUFFERUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));
    String nodeName = targetLeader.getNodeName();

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);

    collectShardResponses(results, true, "MIGRATE failed to request node to buffer updates",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.ADDROUTINGRULE.toLower(),
        COLLECTION_PROP, sourceCollection.getName(),
        SHARD_ID_PROP, sourceSlice.getName(),
        "routeKey", SolrIndexSplitter.getRouteKey(splitKey) + "!",
        "range", splitRange.toString(),
        "targetCollection", targetCollection.getName(),
        // TODO: look at using nanoTime here?
        "expireAt", String.valueOf(System.currentTimeMillis() + timeout));
    log.info("Adding routing rule: " + m);
    Overseer.getInQueue(zkStateReader.getZkClient()).offer(
        ZkStateReader.toJSON(m));

    // wait for a while until we see the new rule
    log.info("Waiting to see routing rule updated in clusterstate");
    long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    boolean added = false;
    while (System.nanoTime() < waitUntil) {
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
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, CREATE.toLower(),
        NAME, tempSourceCollectionName,
        REPLICATION_FACTOR, 1,
        NUM_SLICES, 1,
        COLL_CONF, configName,
        CREATE_NODE_SET, sourceLeader.getNodeName());
    if(asyncId != null) {
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

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection leader or timed out waiting for it to come up",
        shardHandler);

    log.info("Asking source leader to split index");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.SPLIT.toString());
    params.set(CoreAdminParams.CORE, sourceLeader.getStr("core"));
    params.add(CoreAdminParams.TARGET_CORE, tempSourceLeader.getStr("core"));
    params.set(CoreAdminParams.RANGES, splitRange.toString());
    params.set("split.key", splitKey);

    String tempNodeName = sourceLeader.getNodeName();

    sendShardRequest(tempNodeName, params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true, "MIGRATE failed to invoke SPLIT core admin command", shardHandler);
    completeAsyncRequest(asyncId, requestMap, results);

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

    collectShardResponses(results, true,
        "MIGRATE failed to create replica of temporary collection in target leader node.",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

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

    collectShardResponses(results, true,
        "MIGRATE failed to create temp collection replica or timed out waiting for them to come up",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);
    log.info("Successfully created replica of temp source collection on target leader node");

    log.info("Requesting merge of temp source collection replica to target leader");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.MERGEINDEXES.toString());
    params.set(CoreAdminParams.CORE, targetLeader.getStr("core"));
    params.set(CoreAdminParams.SRC_CORE, tempCollectionReplica2);

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true,
        "MIGRATE failed to merge " + tempCollectionReplica2 +
            " to " + targetLeader.getStr("core") + " on node: " + targetLeader.getNodeName(),
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

    log.info("Asking target leader to apply buffered updates");
    params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, targetLeader.getStr("core"));

    sendShardRequest(targetLeader.getNodeName(), params, shardHandler, asyncId, requestMap);
    collectShardResponses(results, true,
        "MIGRATE failed to request node to apply buffered updates",
        shardHandler);

    completeAsyncRequest(asyncId, requestMap, results);

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

  private void completeAsyncRequest(String asyncId, HashMap<String, String> requestMap, NamedList results) {
    if(asyncId != null) {
      waitForAsyncCallsToComplete(requestMap, results);
      requestMap.clear();
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

  private void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler, String asyncId, Map<String, String> requestMap) {
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

  private static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random) {
    // TODO: add smarter options that look at the current number of cores per
    // node?
    // for now we just go random (except when createNodeSet and createNodeSet.shuffle=false are passed in)

    List<String> nodeList;

    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null)?null:StrUtils.splitSmart(createNodeSetStr, ",", true);

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
    String collectionName = message.getStr(NAME);
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

      Map<Position, String> positionVsNodes = identifyNodes(clusterState, nodeList, message, shardNames, repFactor);
      boolean isLegacyCloud =  Overseer.isLegacy(zkStateReader.getClusterProps());

      createConfNode(configName, collectionName, isLegacyCloud);

      Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(message));

      // wait for a while until we don't see the collection
      long waitUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      boolean created = false;
      while (System.nanoTime() < waitUntil) {
        Thread.sleep(100);
        created = zkStateReader.getClusterState().getCollections().contains(message.getStr(NAME));
        if(created) break;
      }
      if (!created)
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not fully create collection: " + message.getStr(NAME));

      // For tracking async calls.
      HashMap<String, String> requestMap = new HashMap<String, String>();


      log.info(formatString("Creating SolrCores for new collection {0}, shardNames {1} , replicationFactor : {2}",
          collectionName, shardNames, repFactor));
      Map<String ,ShardRequest> coresToCreate = new LinkedHashMap<>();
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
          Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(props));
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

      processResponses(results, shardHandler);

      completeAsyncRequest(async, requestMap, results);

      log.info("Finished create command on all shards for collection: "
          + collectionName);

    } catch (SolrException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SolrException(ErrorCode.SERVER_ERROR, null, ex);
    }
  }

  private Map<Position, String> identifyNodes(ClusterState clusterState,
                                              List<String> nodeList,
                                              ZkNodeProps message,
                                              List<String> shardNames,
                                              int repFactor) throws IOException {
    List<Map> maps = (List) message.get("rule");
    if (maps == null) {
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
    for (Object map : maps) rules.add(new Rule((Map) map));

    Map<String, Integer> sharVsReplicaCount = new HashMap<>();

    for (String shard : shardNames) sharVsReplicaCount.put(shard, repFactor);
    ReplicaAssigner replicaAssigner = new ReplicaAssigner(rules,
        sharVsReplicaCount,
        (List<Map>) message.get(SNITCH),
        new HashMap<String, Map<String, Integer>>(),//this is a new collection. So, there are no nodes in any shard
        nodeList,
        overseer.getZkController().getCoreContainer(),
        clusterState);

    return replicaAssigner.getNodeMappings();
  }

  private Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    Map<String, Replica> result = new HashMap<>();
    long endTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
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
      if (System.nanoTime() > endTime) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out waiting to see all replicas: " + coreNames + " in cluster state.");
      }
      
      Thread.sleep(100);
    }
  }

  private void addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws KeeperException, InterruptedException {
    String collection = message.getStr(COLLECTION_PROP);
    String node = message.getStr("node");
    String shard = message.getStr(SHARD_ID_PROP);
    String coreName = message.getStr(CoreAdminParams.NAME);
    
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
    
    if (node == null) {
      
      node = getNodesForNewShard(clusterState, collection, shard, 1, null,
          overseer.getZkController().getCoreContainer()).get(0).nodeName;
      log.info("Node not provided, Identified {} for creating new replica", node);
    }
    
    if (!clusterState.liveNodesContain(node)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Node: " + node + " is not live");
    }
    if (coreName == null) {
      coreName = Assign.buildCoreName(coll, shard);
    } else if (!message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false)) {
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
    
    if (!Overseer.isLegacy(zkStateReader.getClusterProps())) {
      if (!message.getBool(SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, false)) {
        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower(), ZkStateReader.COLLECTION_PROP,
            collection, ZkStateReader.SHARD_ID_PROP, shard, ZkStateReader.CORE_NAME_PROP, coreName,
            ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(), ZkStateReader.BASE_URL_PROP,
            zkStateReader.getBaseUrlForNodeName(node), ZkStateReader.NODE_NAME_PROP, node);
        Overseer.getInQueue(zkStateReader.getZkClient()).offer(ZkStateReader.toJSON(props));
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
    HashMap<String,String> requestMap = new HashMap<>();
    sendShardRequest(node, params, shardHandler, asyncId, requestMap);
    
    collectShardResponses(results, true, "ADDREPLICA failed to create replica", shardHandler);
    
    completeAsyncRequest(asyncId, requestMap, results);
  }

  private void processResponses(NamedList results, ShardHandler shardHandler) {
    ShardResponse srsp;
    do {
      srsp = shardHandler.takeCompletedOrError();
      if (srsp != null) {
        processResponse(results, srsp);
      }
    } while (srsp != null);
  }

  private String getConfigName(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    String configName = message.getStr(OverseerCollectionProcessor.COLL_CONF);
    
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
      byte[] data = ZkStateReader.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
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

  private void collectionCmd(ClusterState clusterState, ZkNodeProps message, ModifiableSolrParams params, NamedList results, Replica.State stateMatcher) {
    log.info("Executing Collection Cmd : " + params);
    String collectionName = message.getStr(NAME);
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    
    DocCollection coll = clusterState.getCollection(collectionName);
    
    for (Map.Entry<String,Slice> entry : coll.getSlicesMap().entrySet()) {
      Slice slice = entry.getValue();
      sliceCmd(clusterState, params, stateMatcher, slice, shardHandler);
    }

    processResponses(results, shardHandler);

  }

  private void sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
                        Slice slice, ShardHandler shardHandler) {
    Map<String,Replica> shards = slice.getReplicasMap();
    Set<Map.Entry<String,Replica>> shardEntries = shards.entrySet();
    for (Map.Entry<String,Replica> shardEntry : shardEntries) {
      final ZkNodeProps node = shardEntry.getValue();
      if (clusterState.liveNodesContain(node.getStr(ZkStateReader.NODE_NAME_PROP)) 
          && (stateMatcher == null || Replica.State.getState(node.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
        // For thread safety, only simple clone the ModifiableSolrParams
        ModifiableSolrParams cloneParams = new ModifiableSolrParams();
        cloneParams.add(params);
        cloneParams.set(CoreAdminParams.CORE, node.getStr(ZkStateReader.CORE_NAME_PROP));

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

  @SuppressWarnings("unchecked")
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

  public boolean isClosed() {
    return isClosed;
  }

  @SuppressWarnings("unchecked")
  private void waitForAsyncCallsToComplete(Map<String, String> requestMap, NamedList results) {
    for(String k:requestMap.keySet()) {
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
          processResponse(results, srsp);
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

  @SuppressWarnings("unchecked")
  private void markTaskAsRunning(QueueEvent head, String collectionName,
                                 String asyncId, ZkNodeProps message)
      throws KeeperException, InterruptedException {
    synchronized (runningZKTasks) {
      runningZKTasks.add(head.getId());
    }

    synchronized (runningTasks) {
      runningTasks.add(head.getId());
    }
    //TODO deprecated remove this check .
    if (!CLUSTERSTATUS.isEqual(message.getStr(Overseer.QUEUE_OPERATION)) && collectionName != null) {
      synchronized (collectionWip) {
        collectionWip.add(collectionName);
      }
    }

    if(asyncId != null)
      runningMap.put(asyncId, null);
  }
  
  protected class Runner implements Runnable {
    ZkNodeProps message;
    String operation;
    SolrResponse response;
    QueueEvent head;
  
    public Runner(ZkNodeProps message, String operation, QueueEvent head) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      response = null;
    }


    @Override
    public void run() {

      final TimerContext timerContext = stats.time("collection_" + operation);

      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String collectionName = message.containsKey(COLLECTION_PROP) ?
          message.getStr(COLLECTION_PROP) : message.getStr(NAME);

      try {
        try {
          log.debug("Runner processing {}", head.getId());
          response = processMessage(message, operation);
        } finally {
          timerContext.stop();
          updateStats();
        }

        if(asyncId != null) {
          if (response != null && (response.getResponse().get("failure") != null 
              || response.getResponse().get("exception") != null)) {
            failureMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated failed map for task with zkid:[{}]", head.getId());
          } else {
            completedMap.put(asyncId, SolrResponse.serializable(response));
            log.debug("Updated completed map for task with zkid:[{}]", head.getId());
          }
        } else {
          head.setBytes(SolrResponse.serializable(response));
          log.debug("Completed task:[{}]", head.getId());
        }

        markTaskComplete(head.getId(), asyncId, collectionName);
        log.debug("Marked task [{}] as completed.", head.getId());
        printTrackingMaps();

        log.info("Overseer Collection Processor: Message id:" + head.getId() +
            " complete, response:" + response.getResponse().toString());
        success = true;
      } catch (KeeperException e) {
        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        // Reset task from tracking data structures so that it can be retried.
        resetTaskWithException(head.getId(), asyncId, collectionName);
        log.warn("Resetting task {} as the thread was interrupted.", head.getId());
        Thread.currentThread().interrupt();
      } finally {
        if(!success) {
          // Reset task from tracking data structures so that it can be retried.
          resetTaskWithException(head.getId(), asyncId, collectionName);
        }
        synchronized (waitLock){
          waitLock.notifyAll();
        }
      }
    }

    private void markTaskComplete(String id, String asyncId, String collectionName)
        throws KeeperException, InterruptedException {
      synchronized (completedTasks) {
        completedTasks.put(id, head);
      }

      synchronized (runningTasks) {
        runningTasks.remove(id);
      }

      if(asyncId != null)
        runningMap.remove(asyncId);

      if(!CLUSTERSTATUS.isEqual(operation) && collectionName != null) {
        synchronized (collectionWip) {
          collectionWip.remove(collectionName);
        }
      }
    }

    private void resetTaskWithException(String id, String asyncId, String collectionName) {
      log.warn("Resetting task: {}, requestid: {}, collectionName: {}", id, asyncId, collectionName);
      try {
        if (asyncId != null)
          runningMap.remove(asyncId);

        synchronized (runningTasks) {
          runningTasks.remove(id);
        }

        if (!CLUSTERSTATUS.isEqual(operation) && collectionName != null) {
          synchronized (collectionWip) {
            collectionWip.remove(collectionName);
          }
        }
      } catch (KeeperException e) {
        SolrException.log(log, "", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

    }

    private void updateStats() {
      if (isSuccessful()) {
        stats.success("collection_" + operation);
      } else {
        stats.error("collection_" + operation);
        stats.storeFailureDetails("collection_" + operation, message, response);
      }
    }

    private boolean isSuccessful() {
      if(response == null)
        return false;
      return !(response.getResponse().get("failure") != null || response.getResponse().get("exception") != null);
    }
  }

  private void printTrackingMaps() {
    if(log.isDebugEnabled()) {
      synchronized (runningTasks) {
        log.debug("RunningTasks: {}", runningTasks.toString());
      }
      synchronized (completedTasks) {
        log.debug("CompletedTasks: {}", completedTasks.keySet().toString());
      }
      synchronized (runningZKTasks) {
        log.debug("RunningZKTasks: {}", runningZKTasks.toString());
      }
    }
  }


  String getId(){
    return myId;
  }


}
