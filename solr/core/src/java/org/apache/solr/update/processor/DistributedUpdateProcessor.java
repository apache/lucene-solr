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
package org.apache.solr.update.processor;

import static org.apache.solr.common.params.CommonParams.DISTRIB;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Slice.State;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.ForwardNode;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// NOT mt-safe... create a new processor for each add thread
// TODO: we really should not wait for distrib after local? unless a certain replication factor is asked for
public class DistributedUpdateProcessor extends UpdateRequestProcessor {

  final static String PARAM_WHITELIST_CTX_KEY = DistributedUpdateProcessor.class + "PARAM_WHITELIST_CTX_KEY";
  public static final String DISTRIB_FROM_SHARD = "distrib.from.shard";
  public static final String DISTRIB_FROM_COLLECTION = "distrib.from.collection";
  public static final String DISTRIB_FROM_PARENT = "distrib.from.parent";
  public static final String DISTRIB_FROM = "distrib.from";
  public static final String DISTRIB_INPLACE_PREVVERSION = "distrib.inplace.prevversion";
  private static final String TEST_DISTRIB_SKIP_SERVERS = "test.distrib.skip.servers";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Request forwarded to a leader of a different shard will be retried up to this amount of times by default
   */
  static final int MAX_RETRIES_ON_FORWARD_DEAULT = Integer.getInteger("solr.retries.on.forward",  25);
  /**
   * Requests from leader to it's followers will be retried this amount of times by default
   */
  static final int MAX_RETRIES_TO_FOLLOWERS_DEFAULT = Integer.getInteger("solr.retries.to.followers", 3);

  /**
   * Values this processor supports for the <code>DISTRIB_UPDATE_PARAM</code>.
   * This is an implementation detail exposed solely for tests.
   * 
   * @see DistributingUpdateProcessorFactory#DISTRIB_UPDATE_PARAM
   */
  public static enum DistribPhase {
    NONE, TOLEADER, FROMLEADER;

    public static DistribPhase parseParam(final String param) {
      if (param == null || param.trim().isEmpty()) {
        return NONE;
      }
      try {
        return valueOf(param);
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST, "Illegal value for " + 
           DISTRIB_UPDATE_PARAM + ": " + param, e);
      }
    }
  }

  public static final String COMMIT_END_POINT = "commit_end_point";
  public static final String LOG_REPLAY = "log_replay";

  // used to assert we don't call finish more than once, see finish()
  private boolean finished = false;

  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final UpdateRequestProcessor next;
  private final AtomicUpdateDocumentMerger docMerger;

  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private boolean returnVersions;

  private NamedList<Object> addsResponse = null;
  private NamedList<Object> deleteResponse = null;
  private NamedList<Object> deleteByQueryResponse = null;
  private CharsRefBuilder scratch;

  private final SchemaField idField;

  private SolrCmdDistributor cmdDistrib;

  private final boolean zkEnabled;

  private final CloudDescriptor cloudDesc;
  private final String collection;
  private final ZkController zkController;

  // these are setup at the start of each request processing
  // method in this update processor
  private boolean isLeader = true;
  private boolean forwardToLeader = false;
  private boolean isSubShardLeader = false;
  private List<Node> nodes;
  private Set<String> skippedCoreNodeNames;
  private boolean isIndexChanged = false;

  /**
   * Number of times requests forwarded to some other shard's leader can be retried
   */
  private final int maxRetriesOnForward = MAX_RETRIES_ON_FORWARD_DEAULT;
  /**
   * Number of times requests from leaders to followers can be retried
   */
  private final int maxRetriesToFollowers = MAX_RETRIES_TO_FOLLOWERS_DEFAULT;

  private UpdateCommand updateCommand;  // the current command this processor is working on.

  //used for keeping track of replicas that have processed an add/update from the leader
  private RollupRequestReplicationTracker rollupReplicationTracker = null;
  private LeaderRequestReplicationTracker leaderReplicationTracker = null;

  // should we clone the document before sending it to replicas?
  // this is set to true in the constructor if the next processors in the chain
  // are custom and may modify the SolrInputDocument racing with its serialization for replication
  private final boolean cloneRequiredOnLeader;
  private final Replica.Type replicaType;

  public DistributedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    this(req, rsp, new AtomicUpdateDocumentMerger(req), next);
  }

  /** Specification of AtomicUpdateDocumentMerger is currently experimental.
   * @lucene.experimental
   */
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger, UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.next = next;
    this.docMerger = docMerger;
    this.idField = req.getSchema().getUniqueKeyField();
    // version init

    this.ulog = req.getCore().getUpdateHandler().getUpdateLog();
    this.vinfo = ulog == null ? null : ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;
    returnVersions = req.getParams().getBool(UpdateParams.VERSIONS ,false);

    // TODO: better way to get the response, or pass back info to it?
    // SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;

    // this should always be used - see filterParams
    DistributedUpdateProcessorFactory.addParamToDistributedRequestWhitelist
      (this.req, UpdateParams.UPDATE_CHAIN, TEST_DISTRIB_SKIP_SERVERS, CommonParams.VERSION_FIELD,
          UpdateParams.EXPUNGE_DELETES, UpdateParams.OPTIMIZE, UpdateParams.MAX_OPTIMIZE_SEGMENTS);

    CoreContainer cc = req.getCore().getCoreContainer();

    this.zkEnabled  = cc.isZooKeeperAware();
    zkController = cc.getZkController();
    if (zkEnabled) {
      cmdDistrib = new SolrCmdDistributor(cc.getUpdateShardHandler());
    }
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();

    if (cloudDesc != null) {
      collection = cloudDesc.getCollectionName();
      replicaType = cloudDesc.getReplicaType();
    } else {
      collection = null;
      replicaType = Replica.Type.NRT;
    }

    boolean shouldClone = false;
    UpdateRequestProcessor nextInChain = next;
    while (nextInChain != null)  {
      Class<? extends UpdateRequestProcessor> klass = nextInChain.getClass();
      if (klass != LogUpdateProcessorFactory.LogUpdateProcessor.class
          && klass != RunUpdateProcessor.class
          && klass != TolerantUpdateProcessor.class)  {
        shouldClone = true;
        break;
      }
      nextInChain = nextInChain.next;
    }
    cloneRequiredOnLeader = shouldClone;
  }

  private List<Node> setupRequest(String id, SolrInputDocument doc) {
    return setupRequest(id, doc, null);
  }

  private List<Node> setupRequest(String id, SolrInputDocument doc, String route) {
    // if we are in zk mode...
    if (!zkEnabled) {
      return null;
    }

    assert TestInjection.injectUpdateRandomPause();

    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
      forwardToLeader = false;
      return null;
    }

    ClusterState cstate = zkController.getClusterState();
    DocCollection coll = cstate.getCollection(collection);
    Slice slice = coll.getRouter().getTargetSlice(id, doc, route, req.getParams(), coll);

    if (slice == null) {
      // No slice found.  Most strict routers will have already thrown an exception, so a null return is
      // a signal to use the slice of this core.
      // TODO: what if this core is not in the targeted collection?
      String shardId = cloudDesc.getShardId();
      slice = coll.getSlice(shardId);
      if (slice == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
      }
    }

    DistribPhase phase =
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    if (DistribPhase.FROMLEADER == phase && !couldIbeSubShardLeader(coll)) {
      if (cloudDesc.isLeader()) {
        // locally we think we are leader but the request says it came FROMLEADER
        // that could indicate a problem, let the full logic below figure it out
      } else {

        assert TestInjection.injectFailReplicaRequests();

        isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
        forwardToLeader = false;
        return null;
      }
    }

    String shardId = slice.getName();

    try {
      // Not equivalent to getLeaderProps, which  retries to find a leader.
      // Replica leader = slice.getLeader();
      Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, shardId);
      isLeader = leaderReplica.getName().equals(cloudDesc.getCoreNodeName());

      if (!isLeader) {
        isSubShardLeader = amISubShardLeader(coll, slice, id, doc);
        if (isSubShardLeader) {
          shardId = cloudDesc.getShardId();
          leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, shardId);
        }
      }

      doDefensiveChecks(phase);

      // if request is coming from another collection then we want it to be sent to all replicas
      // even if its phase is FROMLEADER
      String fromCollection = updateCommand.getReq().getParams().get(DISTRIB_FROM_COLLECTION);

      if (DistribPhase.FROMLEADER == phase && !isSubShardLeader && fromCollection == null) {
        // we are coming from the leader, just go local - add no urls
        forwardToLeader = false;
        return null;
      } else if (isLeader || isSubShardLeader) {
        // that means I want to forward onto my replicas...
        // so get the replicas...
        forwardToLeader = false;
        ClusterState clusterState = zkController.getZkStateReader().getClusterState();
        String leaderCoreNodeName = leaderReplica.getName();
        List<Replica> replicas = clusterState.getCollection(collection)
            .getSlice(shardId)
            .getReplicas(EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG));
        replicas.removeIf((replica) -> replica.getName().equals(leaderCoreNodeName));
        if (replicas.isEmpty()) {
          return null;
        }

        // check for test param that lets us miss replicas
        String[] skipList = req.getParams().getParams(TEST_DISTRIB_SKIP_SERVERS);
        Set<String> skipListSet = null;
        if (skipList != null) {
          skipListSet = new HashSet<>(skipList.length);
          skipListSet.addAll(Arrays.asList(skipList));
          log.info("test.distrib.skip.servers was found and contains:" + skipListSet);
        }

        List<Node> nodes = new ArrayList<>(replicas.size());
        skippedCoreNodeNames = new HashSet<>();
        ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
        for (Replica replica: replicas) {
          String coreNodeName = replica.getName();
          if (skipList != null && skipListSet.contains(replica.getCoreUrl())) {
            log.info("check url:" + replica.getCoreUrl() + " against:" + skipListSet + " result:true");
          } else if(zkShardTerms.registered(coreNodeName) && zkShardTerms.skipSendingUpdatesTo(coreNodeName)) {
            log.debug("skip url:{} cause its term is less than leader", replica.getCoreUrl());
            skippedCoreNodeNames.add(replica.getName());
          } else if (!clusterState.getLiveNodes().contains(replica.getNodeName()) || replica.getState() == Replica.State.DOWN) {
            skippedCoreNodeNames.add(replica.getName());
          } else {
            nodes.add(new StdNode(new ZkCoreNodeProps(replica), collection, shardId, maxRetriesToFollowers));
          }
        }
        return nodes;

      } else {
        // I need to forward on to the leader...
        forwardToLeader = true;
        return Collections.singletonList(
            new ForwardNode(new ZkCoreNodeProps(leaderReplica), zkController.getZkStateReader(), collection, shardId, maxRetriesOnForward));
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
    }
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  private boolean couldIbeSubShardLeader(DocCollection coll) {
    // Could I be the leader of a shard in "construction/recovery" state?
    String myShardId = cloudDesc.getShardId();
    Slice mySlice = coll.getSlice(myShardId);
    State state = mySlice.getState();
    return state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  private boolean amISubShardLeader(DocCollection coll, Slice parentSlice, String id, SolrInputDocument doc) throws InterruptedException {
    // Am I the leader of a shard in "construction/recovery" state?
    String myShardId = cloudDesc.getShardId();
    Slice mySlice = coll.getSlice(myShardId);
    final State state = mySlice.getState();
    if (state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY) {
      Replica myLeader = zkController.getZkStateReader().getLeaderRetry(collection, myShardId);
      boolean amILeader = myLeader.getName().equals(cloudDesc.getCoreNodeName());
      if (amILeader) {
        // Does the document belong to my hash range as well?
        DocRouter.Range myRange = mySlice.getRange();
        if (myRange == null) myRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (parentSlice != null)  {
          boolean isSubset = parentSlice.getRange() != null && myRange.isSubsetOf(parentSlice.getRange());
          return isSubset && coll.getRouter().isTargetSlice(id, doc, req.getParams(), myShardId, coll);
        } else  {
          // delete by query case -- as long as I am a sub shard leader we're fine
          return true;
        }
      }
    }
    return false;
  }
  
  private List<Node> getReplicaNodesForLeader(String shardId, Replica leaderReplica) {
    ClusterState clusterState = zkController.getZkStateReader().getClusterState();
    String leaderCoreNodeName = leaderReplica.getName();
    List<Replica> replicas = clusterState.getCollection(collection)
        .getSlice(shardId)
        .getReplicas(EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG));
    replicas.removeIf((replica) -> replica.getName().equals(leaderCoreNodeName));
    if (replicas.isEmpty()) {
      return null;
    }

    // check for test param that lets us miss replicas
    String[] skipList = req.getParams().getParams(TEST_DISTRIB_SKIP_SERVERS);
    Set<String> skipListSet = null;
    if (skipList != null) {
      skipListSet = new HashSet<>(skipList.length);
      skipListSet.addAll(Arrays.asList(skipList));
      log.info("test.distrib.skip.servers was found and contains:" + skipListSet);
    }

    List<Node> nodes = new ArrayList<>(replicas.size());
    skippedCoreNodeNames = new HashSet<>();
    ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
    for (Replica replica : replicas) {
      String coreNodeName = replica.getName();
      if (skipList != null && skipListSet.contains(replica.getCoreUrl())) {
        log.info("check url:" + replica.getCoreUrl() + " against:" + skipListSet + " result:true");
      } else if (zkShardTerms.registered(coreNodeName) && zkShardTerms.skipSendingUpdatesTo(coreNodeName)) {
        log.debug("skip url:{} cause its term is less than leader", replica.getCoreUrl());
        skippedCoreNodeNames.add(replica.getName());
      } else if (!clusterState.getLiveNodes().contains(replica.getNodeName())
          || replica.getState() == Replica.State.DOWN) {
        skippedCoreNodeNames.add(replica.getName());
      } else {
        nodes.add(new StdNode(new ZkCoreNodeProps(replica), collection, shardId));
      }
    }
    return nodes;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  private List<Node> getSubShardLeaders(DocCollection coll, String shardId, String docId, SolrInputDocument doc) {
    Collection<Slice> allSlices = coll.getSlices();
    List<Node> nodes = null;
    for (Slice aslice : allSlices) {
      final Slice.State state = aslice.getState();
      if (state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY)  {
        DocRouter.Range myRange = coll.getSlice(shardId).getRange();
        if (myRange == null) myRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        boolean isSubset = aslice.getRange() != null && aslice.getRange().isSubsetOf(myRange);
        if (isSubset &&
            (docId == null // in case of deletes
            || coll.getRouter().isTargetSlice(docId, doc, req.getParams(), aslice.getName(), coll))) {
          Replica sliceLeader = aslice.getLeader();
          // slice leader can be null because node/shard is created zk before leader election
          if (sliceLeader != null && zkController.getClusterState().liveNodesContain(sliceLeader.getNodeName()))  {
            if (nodes == null) nodes = new ArrayList<>();
            ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(sliceLeader);
            nodes.add(new StdNode(nodeProps, coll.getName(), aslice.getName()));
          }
        }
      }
    }
    return nodes;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#MIGRATE} */
  private List<Node> getNodesByRoutingRules(ClusterState cstate, DocCollection coll, String id, SolrInputDocument doc)  {
    DocRouter router = coll.getRouter();
    List<Node> nodes = null;
    if (router instanceof CompositeIdRouter)  {
      CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
      String myShardId = cloudDesc.getShardId();
      Slice slice = coll.getSlice(myShardId);
      Map<String, RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules != null) {

        // delete by query case
        if (id == null) {
          for (Entry<String, RoutingRule> entry : routingRules.entrySet()) {
            String targetCollectionName = entry.getValue().getTargetCollectionName();
            final DocCollection docCollection = cstate.getCollectionOrNull(targetCollectionName);
            if (docCollection != null && docCollection.getActiveSlicesArr().length > 0) {
              final Slice[] activeSlices = docCollection.getActiveSlicesArr();
              Slice any = activeSlices[0];
              if (nodes == null) nodes = new ArrayList<>();
              nodes.add(new StdNode(new ZkCoreNodeProps(any.getLeader())));
            }
          }
          return nodes;
        }

        String routeKey = SolrIndexSplitter.getRouteKey(id);
        if (routeKey != null) {
          RoutingRule rule = routingRules.get(routeKey + "!");
          if (rule != null) {
            if (! rule.isExpired()) {
              List<DocRouter.Range> ranges = rule.getRouteRanges();
              if (ranges != null && !ranges.isEmpty()) {
                int hash = compositeIdRouter.sliceHash(id, doc, null, coll);
                for (DocRouter.Range range : ranges) {
                  if (range.includes(hash)) {
                    DocCollection targetColl = cstate.getCollection(rule.getTargetCollectionName());
                    Collection<Slice> activeSlices = targetColl.getRouter().getSearchSlicesSingle(id, null, targetColl);
                    if (activeSlices == null || activeSlices.isEmpty()) {
                      throw new SolrException(ErrorCode.SERVER_ERROR,
                          "No active slices serving " + id + " found for target collection: " + rule.getTargetCollectionName());
                    }
                    Replica targetLeader = targetColl.getLeader(activeSlices.iterator().next().getName());
                    nodes = new ArrayList<>(1);
                    nodes.add(new StdNode(new ZkCoreNodeProps(targetLeader)));
                    break;
                  }
                }
              }
            } else  {
              ReentrantLock ruleExpiryLock = req.getCore().getRuleExpiryLock();
              if (!ruleExpiryLock.isLocked()) {
                try {
                  if (ruleExpiryLock.tryLock(10, TimeUnit.MILLISECONDS)) {
                    log.info("Going to expire routing rule");
                    try {
                      Map<String, Object> map = Utils.makeMap(Overseer.QUEUE_OPERATION, OverseerAction.REMOVEROUTINGRULE.toLower(),
                          ZkStateReader.COLLECTION_PROP, collection,
                          ZkStateReader.SHARD_ID_PROP, myShardId,
                          "routeKey", routeKey + "!");
                      zkController.getOverseer().offerStateUpdate(Utils.toJSON(map));
                    } catch (KeeperException e) {
                      log.warn("Exception while removing routing rule for route key: " + routeKey, e);
                    } catch (Exception e) {
                      log.error("Exception while removing routing rule for route key: " + routeKey, e);
                    } finally {
                      ruleExpiryLock.unlock();
                    }
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
            }
          }
        }
      }
    }
    return nodes;
  }

  private void doDefensiveChecks(DistribPhase phase) {
    boolean isReplayOrPeersync = (updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    if (isReplayOrPeersync) return;

    String from = req.getParams().get(DISTRIB_FROM);
    ClusterState clusterState = zkController.getClusterState();

    DocCollection docCollection = clusterState.getCollection(collection);
    Slice mySlice = docCollection.getSlice(cloudDesc.getShardId());
    boolean localIsLeader = cloudDesc.isLeader();
    if (DistribPhase.FROMLEADER == phase && localIsLeader && from != null) { // from will be null on log replay
      String fromShard = req.getParams().get(DISTRIB_FROM_PARENT);
      if (fromShard != null) {
        if (mySlice.getState() == Slice.State.ACTIVE)  {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but we are in active state");
        }
        // shard splitting case -- check ranges to see if we are a sub-shard
        Slice fromSlice = docCollection.getSlice(fromShard);
        DocRouter.Range parentRange = fromSlice.getRange();
        if (parentRange == null) parentRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (mySlice.getRange() != null && !mySlice.getRange().isSubsetOf(parentRange)) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but parent hash range is not superset of my range");
        }
      } else {
        String fromCollection = req.getParams().get(DISTRIB_FROM_COLLECTION); // is it because of a routing rule?
        if (fromCollection == null)  {
          log.error("Request says it is coming from leader, but we are the leader: " + req.getParamString());
          SolrException solrExc = new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Request says it is coming from leader, but we are the leader");
          solrExc.setMetadata("cause", "LeaderChanged");
          throw solrExc;
        }
      }
    }

    int count = 0;
    while (((isLeader && !localIsLeader) || (isSubShardLeader && !localIsLeader)) && count < 5) {
      count++;
      // re-getting localIsLeader since we published to ZK first before setting localIsLeader value
      localIsLeader = cloudDesc.isLeader();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if ((isLeader && !localIsLeader) || (isSubShardLeader && !localIsLeader)) {
      log.error("ClusterState says we are the leader, but locally we don't think so");
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
          "ClusterState says we are the leader (" + zkController.getBaseUrl()
              + "/" + req.getCore().getName() + "), but locally we don't think so. Request came from " + from);
    }
  }


  // used for deleteByQuery to get the list of nodes this leader should forward to
  private List<Node> setupRequestForDBQ() {
    List<Node> nodes = null;
    String shardId = cloudDesc.getShardId();

    try {
      Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, shardId);
      isLeader = leaderReplica.getName().equals(cloudDesc.getCoreNodeName());

      // TODO: what if we are no longer the leader?

      forwardToLeader = false;
      List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
          .getReplicaProps(collection, shardId, leaderReplica.getName(), null, Replica.State.DOWN, EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG));
      if (replicaProps != null) {
        nodes = new ArrayList<>(replicaProps.size());
        for (ZkCoreNodeProps props : replicaProps) {
          nodes.add(new StdNode(props, collection, shardId));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

    return nodes;
  }


  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {

    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;

    if (zkEnabled) {
      zkCheck();
      nodes = setupRequest(cmd.getHashableId(), cmd.getSolrInputDocument());
    } else {
      isLeader = getNonZkLeaderAssumption(req);
    }

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);

    // If we were sent a previous version, set this to the AddUpdateCommand (if not already set)
    if (!cmd.isInPlaceUpdate()) {
      cmd.prevVersion = cmd.getReq().getParams().getLong(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, -1);
    }
    // TODO: if minRf > 1 and we know the leader is the only active replica, we could fail
    // the request right here but for now I think it is better to just return the status
    // to the client that the minRf wasn't reached and let them handle it    

    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd = versionAdd(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    if (zkEnabled && isLeader && !isSubShardLeader)  {
      DocCollection coll = zkController.getClusterState().getCollection(collection);
      List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getHashableId(), cmd.getSolrInputDocument());
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_PARENT, cloudDesc.getShardId());
        cmdDistrib.distribAdd(cmd, subShardLeaders, params, true);
      }
      final List<Node> nodesByRoutingRules = getNodesByRoutingRules(zkController.getClusterState(), coll, cmd.getHashableId(), cmd.getSolrInputDocument());
      if (nodesByRoutingRules != null && !nodesByRoutingRules.isEmpty())  {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_COLLECTION, collection);
        params.set(DISTRIB_FROM_SHARD, cloudDesc.getShardId());
        cmdDistrib.distribAdd(cmd, nodesByRoutingRules, params, true);
      }
    }

    if (nodes != null) {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_UPDATE_PARAM,
          (isLeader || isSubShardLeader ?
              DistribPhase.FROMLEADER.toString() :
              DistribPhase.TOLEADER.toString()));
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      if (req.getParams().get(UpdateRequest.MIN_REPFACT) != null) {
        // TODO: Kept for rolling upgrades only. Should be removed in Solr 9
        params.set(UpdateRequest.MIN_REPFACT, req.getParams().get(UpdateRequest.MIN_REPFACT));
      }

      if (cmd.isInPlaceUpdate()) {
        params.set(DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));

        // Use synchronous=true so that a new connection is used, instead
        // of the update being streamed through an existing streaming client.
        // When using a streaming client, the previous update
        // and the current in-place update (that depends on the previous update), if reordered
        // in the stream, can result in the current update being bottled up behind the previous
        // update in the stream and can lead to degraded performance.
        cmdDistrib.distribAdd(cmd, nodes, params, true, rollupReplicationTracker, leaderReplicationTracker);
      } else {
        cmdDistrib.distribAdd(cmd, nodes, params, false, rollupReplicationTracker, leaderReplicationTracker);
      }
    }

    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && idField != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<>(1);
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRefBuilder();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }

    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.

  }

  // helper method, processAdd was getting a bit large.
  // Sets replicationTracker = null if we aren't the leader
  // We have two possibilities here:
  //
  // 1> we are a leader: Allocate a LeaderTracker and, if we're getting the original request, a RollupTracker
  // 2> we're a follower: allocat a RollupTracker
  //
  private void checkReplicationTracker(UpdateCommand cmd) {
    if (zkEnabled == false) {
      rollupReplicationTracker = null; // never need one of these in stand-alone
      leaderReplicationTracker = null;
      return;
    }

    SolrParams rp = cmd.getReq().getParams();
    String distribUpdate = rp.get(DISTRIB_UPDATE_PARAM);
    // Ok,we're receiving the original request, we need a rollup tracker, but only one so we accumulate over the
    // course of a batch.
    if ((distribUpdate == null || DistribPhase.NONE.toString().equals(distribUpdate)) &&
        rollupReplicationTracker == null) {
      rollupReplicationTracker = new RollupRequestReplicationTracker();
    }
    // If we're a leader, we need a leader replication tracker, so let's do that. If there are multiple docs in
    // a batch we need to use the _same_ leader replication tracker.
    if (isLeader && leaderReplicationTracker == null) {
      leaderReplicationTracker = new LeaderRequestReplicationTracker(
          req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId());
    }
  }


  @Override
  protected void doClose() {
    if (cmdDistrib != null) {
      cmdDistrib.close();
    }
  }
 
  // TODO: optionally fail if n replicas are not reached...
  private void doFinish() {
    boolean shouldUpdateTerms = isLeader && isIndexChanged;
    if (shouldUpdateTerms) {
      ZkShardTerms zkShardTerms = zkController.getShardTerms(cloudDesc.getCollectionName(), cloudDesc.getShardId());
      if (skippedCoreNodeNames != null) {
        zkShardTerms.ensureTermsIsHigher(cloudDesc.getCoreNodeName(), skippedCoreNodeNames);
      }
      zkController.getShardTerms(collection, cloudDesc.getShardId()).ensureHighestTermsAreNotZero();
    }
    // TODO: if not a forward and replication req is not specified, we could
    // send in a background thread

    cmdDistrib.finish();    
    List<Error> errors = cmdDistrib.getErrors();
    // TODO - we may need to tell about more than one error...

    List<Error> errorsForClient = new ArrayList<>(errors.size());
    Set<String> replicasShouldBeInLowerTerms = new HashSet<>();
    for (final SolrCmdDistributor.Error error : errors) {
      
      if (error.req.node instanceof ForwardNode) {
        // if it's a forward, any fail is a problem - 
        // otherwise we assume things are fine if we got it locally
        // until we start allowing min replication param
        errorsForClient.add(error);
        continue;
      }

      // else...
      
      // for now we don't error - we assume if it was added locally, we
      // succeeded 
      if (log.isWarnEnabled()) {
        log.warn("Error sending update to " + error.req.node.getBaseUrl(), error.e);
      }
      
      // Since it is not a forward request, for each fail, try to tell them to
      // recover - the doc was already added locally, so it should have been
      // legit

      DistribPhase phase = DistribPhase.parseParam(error.req.uReq.getParams().get(DISTRIB_UPDATE_PARAM));
      if (phase != DistribPhase.FROMLEADER)
        continue; // don't have non-leaders try to recovery other nodes

      // commits are special -- they can run on any node irrespective of whether it is a leader or not
      // we don't want to run recovery on a node which missed a commit command
      if (error.req.uReq.getParams().get(COMMIT_END_POINT) != null)
        continue;

      final String replicaUrl = error.req.node.getUrl();

      // if the remote replica failed the request because of leader change (SOLR-6511), then fail the request
      String cause = (error.e instanceof SolrException) ? ((SolrException)error.e).getMetadata("cause") : null;
      if ("LeaderChanged".equals(cause)) {
        // let's just fail this request and let the client retry? or just call processAdd again?
        log.error("On "+cloudDesc.getCoreNodeName()+", replica "+replicaUrl+
            " now thinks it is the leader! Failing the request to let the client retry! "+error.e);
        errorsForClient.add(error);
        continue;
      }

      String collection = null;
      String shardId = null;

      if (error.req.node instanceof StdNode) {
        StdNode stdNode = (StdNode)error.req.node;
        collection = stdNode.getCollection();
        shardId = stdNode.getShardId();

        // before we go setting other replicas to down, make sure we're still the leader!
        String leaderCoreNodeName = null;
        Exception getLeaderExc = null;
        Replica leaderProps = null;
        try {
            leaderProps = zkController.getZkStateReader().getLeader(collection, shardId);
          if (leaderProps != null) {
            leaderCoreNodeName = leaderProps.getName();
          }
        } catch (Exception exc) {
          getLeaderExc = exc;
        }
        if (leaderCoreNodeName == null) {
          log.warn("Failed to determine if {} is still the leader for collection={} shardId={} " +
                  "before putting {} into leader-initiated recovery",
              cloudDesc.getCoreNodeName(), collection, shardId, replicaUrl, getLeaderExc);
        }

        List<ZkCoreNodeProps> myReplicas = zkController.getZkStateReader().getReplicaProps(collection,
            cloudDesc.getShardId(), cloudDesc.getCoreNodeName());
        boolean foundErrorNodeInReplicaList = false;
        if (myReplicas != null) {
          for (ZkCoreNodeProps replicaProp : myReplicas) {
            if (((Replica) replicaProp.getNodeProps()).getName().equals(((Replica)stdNode.getNodeProps().getNodeProps()).getName()))  {
              foundErrorNodeInReplicaList = true;
              break;
            }
          }
        }

        if (leaderCoreNodeName != null && cloudDesc.getCoreNodeName().equals(leaderCoreNodeName) // we are still same leader
            && foundErrorNodeInReplicaList // we found an error for one of replicas
            && !stdNode.getNodeProps().getCoreUrl().equals(leaderProps.getCoreUrl())) { // we do not want to put ourself into LIR
          try {
            String coreNodeName = ((Replica) stdNode.getNodeProps().getNodeProps()).getName();
            // if false, then the node is probably not "live" anymore
            // and we do not need to send a recovery message
            Throwable rootCause = SolrException.getRootCause(error.e);
            log.error("Setting up to try to start recovery on replica {} with url {} by increasing leader term", coreNodeName, replicaUrl, rootCause);
            replicasShouldBeInLowerTerms.add(coreNodeName);
          } catch (Exception exc) {
            Throwable setLirZnodeFailedCause = SolrException.getRootCause(exc);
            log.error("Leader failed to set replica " +
                error.req.node.getUrl() + " state to DOWN due to: " + setLirZnodeFailedCause, setLirZnodeFailedCause);
          }
        } else {
          // not the leader anymore maybe or the error'd node is not my replica?
          if (!foundErrorNodeInReplicaList) {
            log.warn("Core "+cloudDesc.getCoreNodeName()+" belonging to "+collection+" "+
                cloudDesc.getShardId()+", does not have error'd node " + stdNode.getNodeProps().getCoreUrl() + " as a replica. " +
                "No request recovery command will be sent!");
            if (!shardId.equals(cloudDesc.getShardId())) {
              // some replicas on other shard did not receive the updates (ex: during splitshard),
              // exception must be notified to clients
              errorsForClient.add(error);
            }
          } else {
            log.warn("Core " + cloudDesc.getCoreNodeName() + " is no longer the leader for " + collection + " "
                + shardId + " or we tried to put ourself into LIR, no request recovery command will be sent!");
          }
        }
      }
    }
    if (!replicasShouldBeInLowerTerms.isEmpty()) {
      zkController.getShardTerms(cloudDesc.getCollectionName(), cloudDesc.getShardId())
          .ensureTermsIsHigher(cloudDesc.getCoreNodeName(), replicasShouldBeInLowerTerms);
    }
    handleReplicationFactor();
    if (0 < errorsForClient.size()) {
      throw new DistributedUpdatesAsyncException(errorsForClient);
    }
  }
 
  /**
   * If necessary, include in the response the achieved replication factor
   */
  @SuppressWarnings("deprecation")
  private void handleReplicationFactor() {
    if (leaderReplicationTracker != null || rollupReplicationTracker != null) {
      int achievedRf = Integer.MAX_VALUE;

      if (leaderReplicationTracker != null) {

        achievedRf = leaderReplicationTracker.getAchievedRf();

        // Transfer this to the rollup tracker if it exists
        if (rollupReplicationTracker != null) {
          rollupReplicationTracker.testAndSetAchievedRf(achievedRf);
        }
      }

      // Rollup tracker has accumulated stats.
      if (rollupReplicationTracker != null) {
        achievedRf = rollupReplicationTracker.getAchievedRf();
      }
      if (req.getParams().get(UpdateRequest.MIN_REPFACT) != null) {
        // Unused, but kept for back compatibility. To be removed in Solr 9
        rsp.getResponseHeader().add(UpdateRequest.MIN_REPFACT, Integer.parseInt(req.getParams().get(UpdateRequest.MIN_REPFACT)));
      }
      rsp.getResponseHeader().add(UpdateRequest.REPFACT, achievedRf);
      rollupReplicationTracker = null;
      leaderReplicationTracker = null;

    }
  }

  // must be synchronized by bucket
  private void doLocalAdd(AddUpdateCommand cmd) throws IOException {
    super.processAdd(cmd);
    isIndexChanged = true;
  }

  // must be synchronized by bucket
  private void doLocalDelete(DeleteUpdateCommand cmd) throws IOException {
    super.processDelete(cmd);
    isIndexChanged = true;
  }

  public static int bucketHash(BytesRef idBytes) {
    assert idBytes != null;
    return Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);
  }

  /**
   * @return whether or not to drop this cmd
   * @throws IOException If there is a low-level I/O error.
   */
  protected boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    BytesRef idBytes = cmd.getIndexedId();

    if (idBytes == null) {
      super.processAdd(cmd);
      return false;
    }

    if (vinfo == null) {
      if (AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Atomic document updates are not supported unless <updateLog/> is configured");
      } else {
        super.processAdd(cmd);
        return false;
      }
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash
    // here)
    int bucketHash = bucketHash(idBytes);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    // TODO: don't reuse update commands any more!
    long versionOnUpdate = cmd.getVersion();

    if (versionOnUpdate == 0) {
      SolrInputField versionField = cmd.getSolrInputDocument().getField(CommonParams.VERSION_FIELD);
      if (versionField != null) {
        Object o = versionField.getValue();
        versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      } else {
        // Find the version
        String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
        versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
      }
    }

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;
    boolean forwardedFromCollection = cmd.getReq().getParams().get(DISTRIB_FROM_COLLECTION) != null;

    VersionBucket bucket = vinfo.bucket(bucketHash);

    long dependentVersionFound = -1;
    // if this is an in-place update, check and wait if we should be waiting for a previous update (on which
    // this update depends), before entering the synchronized block
    if (!leaderLogic && cmd.isInPlaceUpdate()) {
      dependentVersionFound = waitForDependentUpdates(cmd, versionOnUpdate, isReplayOrPeersync, bucket);
      if (dependentVersionFound == -1) {
        // it means the document has been deleted by now at the leader. drop this update
        return true;
      }
    }

    vinfo.lockForUpdate();
    if (bucket.tryLock()) {
      try {
        bucket.signalAll();
        // just in case anyone is waiting let them know that we have a new update
        // we obtain the version when synchronized and then do the add so we can ensure that
        // if version1 < version2 then version1 is actually added before version2.

        // even if we don't store the version field, synchronizing on the bucket
        // will enable us to know what version happened first, and thus enable
        // realtime-get to work reliably.
        // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
        // there may be other reasons in the future for a version on the commands

        if (versionsStored) {

          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            if (forwardedFromCollection && ulog.getState() == UpdateLog.State.ACTIVE) {
              // forwarded from a collection but we are not buffering so strip original version and apply our own
              // see SOLR-5308
              log.info("Removing version field from doc: " + cmd.getPrintableId());
              cmd.solrDoc.remove(CommonParams.VERSION_FIELD);
              versionOnUpdate = 0;
            }

            getUpdatedDocument(cmd, versionOnUpdate);

            // leaders can also be in buffering state during "migrate" API call, see SOLR-5308
            if (forwardedFromCollection && ulog.getState() != UpdateLog.State.ACTIVE
                && isReplayOrPeersync == false) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              log.info("Leader logic applied but update log is buffering: " + cmd.getPrintableId());
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.add(cmd);
              return true;
            }

            if (versionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if (versionOnUpdate == foundVersion || (versionOnUpdate < 0 && foundVersion < 0)
                  || (versionOnUpdate == 1 && foundVersion > 0)) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getPrintableId()
                    + " expected=" + versionOnUpdate + " actual=" + foundVersion);
              }
            }

            long version = vinfo.getNewClock();
            cmd.setVersion(version);
            cmd.getSolrInputDocument().setField(CommonParams.VERSION_FIELD, version);
            bucket.updateHighest(version);
          } else {
            // The leader forwarded us this update.
            cmd.setVersion(versionOnUpdate);

            if (shouldBufferUpdate(cmd, isReplayOrPeersync, ulog.getState())) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.add(cmd);
              return true;
            }

            if (cmd.isInPlaceUpdate()) {
              long prev = cmd.prevVersion;
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion == null || Math.abs(lastVersion) < prev) {
                // this was checked for (in waitForDependentUpdates()) before entering the synchronized block.
                // So we shouldn't be here, unless what must've happened is:
                // by the time synchronization block was entered, the prev update was deleted by DBQ. Since
                // now that update is not in index, the vinfo.lookupVersion() is possibly giving us a version
                // from the deleted list (which might be older than the prev update!)
                UpdateCommand fetchedFromLeader = fetchFullUpdateFromLeader(cmd, versionOnUpdate);

                if (fetchedFromLeader instanceof DeleteUpdateCommand) {
                  log.info("In-place update of {} failed to find valid lastVersion to apply to, and the document"
                      + " was deleted at the leader subsequently.", idBytes.utf8ToString());
                  versionDelete((DeleteUpdateCommand) fetchedFromLeader);
                  return true;
                } else {
                  assert fetchedFromLeader instanceof AddUpdateCommand;
                  // Newer document was fetched from the leader. Apply that document instead of this current in-place
                  // update.
                  log.info(
                      "In-place update of {} failed to find valid lastVersion to apply to, forced to fetch full doc from leader: {}",
                      idBytes.utf8ToString(), fetchedFromLeader);

                  // Make this update to become a non-inplace update containing the full document obtained from the
                  // leader
                  cmd.solrDoc = ((AddUpdateCommand) fetchedFromLeader).solrDoc;
                  cmd.prevVersion = -1;
                  cmd.setVersion((long) cmd.solrDoc.getFieldValue(CommonParams.VERSION_FIELD));
                  assert cmd.isInPlaceUpdate() == false;
                }
              } else {
                if (lastVersion != null && Math.abs(lastVersion) > prev) {
                  // this means we got a newer full doc update and in that case it makes no sense to apply the older
                  // inplace update. Drop this update
                  log.info("Update was applied on version: " + prev + ", but last version I have is: " + lastVersion
                      + ". Dropping current update.");
                  return true;
                } else {
                  // We're good, we should apply this update. First, update the bucket's highest.
                  if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
                    bucket.updateHighest(versionOnUpdate);
                  }
                }
              }
            } else {
              // if we aren't the leader, then we need to check that updates were not re-ordered
              if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
                // we're OK... this update has a version higher than anything we've seen
                // in this bucket so far, so we know that no reordering has yet occurred.
                bucket.updateHighest(versionOnUpdate);
              } else {
                // there have been updates higher than the current update. we need to check
                // the specific version for this id.
                Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
                if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                  // This update is a repeat, or was reordered. We need to drop this update.
                  log.debug("Dropping add update due to version {}", idBytes.utf8ToString());
                  return true;
                }
              }
            }
            if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
            }
          }
        }

        boolean willDistrib = isLeader && nodes != null && nodes.size() > 0;

        SolrInputDocument clonedDoc = null;
        if (willDistrib && cloneRequiredOnLeader) {
          clonedDoc = cmd.solrDoc.deepCopy();
        }

        // TODO: possibly set checkDeleteByQueries as a flag on the command?
        doLocalAdd(cmd);

        if (willDistrib && cloneRequiredOnLeader) {
          cmd.solrDoc = clonedDoc;
        }
      } finally {

        bucket.unlock();

        vinfo.unlockForUpdate();
      }
      return false;

    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to get version bucket lock in " + bucket.getLockTimeoutMs() + " ms");
    }
  }

  @VisibleForTesting
  boolean shouldBufferUpdate(AddUpdateCommand cmd, boolean isReplayOrPeersync, UpdateLog.State state) {
    if (state == UpdateLog.State.APPLYING_BUFFERED
        && !isReplayOrPeersync
        && !cmd.isInPlaceUpdate()) {
      // this a new update sent from the leader, it contains whole document therefore it won't depend on other updates
      return false;
    }

    return state != UpdateLog.State.ACTIVE && isReplayOrPeersync == false;
  }

  /**
   * This method checks the update/transaction logs and index to find out if the update ("previous update") that the current update
   * depends on (in the case that this current update is an in-place update) has already been completed. If not,
   * this method will wait for the missing update until it has arrived. If it doesn't arrive within a timeout threshold,
   * then this actively fetches from the leader.
   * 
   * @return -1 if the current in-place should be dropped, or last found version if previous update has been indexed.
   */
  private long waitForDependentUpdates(AddUpdateCommand cmd, long versionOnUpdate,
                               boolean isReplayOrPeersync, VersionBucket bucket) throws IOException {
    long lastFoundVersion = 0;
    TimeOut waitTimeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    vinfo.lockForUpdate();
    if (bucket.tryLock()) {
      try {
        Long lookedUpVersion = vinfo.lookupVersion(cmd.getIndexedId());
        lastFoundVersion = lookedUpVersion == null ? 0L : lookedUpVersion;

        if (Math.abs(lastFoundVersion) < cmd.prevVersion) {
          log.debug("Re-ordered inplace update. version={}, prevVersion={}, lastVersion={}, replayOrPeerSync={}, id={}",
              (cmd.getVersion() == 0 ? versionOnUpdate : cmd.getVersion()), cmd.prevVersion, lastFoundVersion,
              isReplayOrPeersync, cmd.getPrintableId());
        }

        while (Math.abs(lastFoundVersion) < cmd.prevVersion && !waitTimeout.hasTimedOut()) {
          bucket.awaitNanos(waitTimeout.timeLeft(TimeUnit.NANOSECONDS));
          lookedUpVersion = vinfo.lookupVersion(cmd.getIndexedId());
          lastFoundVersion = lookedUpVersion == null ? 0L : lookedUpVersion;
        }
      } finally {

        bucket.unlock();

        vinfo.unlockForUpdate();
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to get version bucket lock in " + bucket.getLockTimeoutMs() + " ms");
    }

    if (Math.abs(lastFoundVersion) > cmd.prevVersion) {
      // This must've been the case due to a higher version full update succeeding concurrently, while we were waiting or
      // trying to index this partial update. Since a full update more recent than this partial update has succeeded,
      // we can drop the current update.
      if (log.isDebugEnabled()) {
        log.debug("Update was applied on version: {}, but last version I have is: {}"
            + ". Current update should be dropped. id={}", cmd.prevVersion, lastFoundVersion, cmd.getPrintableId());
      }
      return -1;
    } else if (Math.abs(lastFoundVersion) == cmd.prevVersion) {
      assert 0 < lastFoundVersion : "prevVersion " + cmd.prevVersion + " found but is a delete!";
      if (log.isDebugEnabled()) {
        log.debug("Dependent update found. id={}", cmd.getPrintableId());
      }
      return lastFoundVersion;
    }

    // We have waited enough, but dependent update didn't arrive. Its time to actively fetch it from leader
    log.info("Missing update, on which current in-place update depends on, hasn't arrived. id={}, looking for version={}, last found version={}", 
        cmd.getPrintableId(), cmd.prevVersion, lastFoundVersion);
    
    UpdateCommand missingUpdate = fetchFullUpdateFromLeader(cmd, versionOnUpdate);
    if (missingUpdate instanceof DeleteUpdateCommand) {
      log.info("Tried to fetch document {} from the leader, but the leader says document has been deleted. " 
          + "Deleting the document here and skipping this update: Last found version: {}, was looking for: {}", cmd.getPrintableId(), lastFoundVersion, cmd.prevVersion);
      versionDelete((DeleteUpdateCommand)missingUpdate);
      return -1;
    } else {
      assert missingUpdate instanceof AddUpdateCommand;
      log.debug("Fetched the document: {}", ((AddUpdateCommand)missingUpdate).getSolrInputDocument());
      versionAdd((AddUpdateCommand)missingUpdate);
      log.info("Added the fetched document, id="+((AddUpdateCommand)missingUpdate).getPrintableId()+", version="+missingUpdate.getVersion());
    }
    return missingUpdate.getVersion();
  }

  /**
   * This method is used when an update on which a particular in-place update has been lost for some reason. This method
   * sends a request to the shard leader to fetch the latest full document as seen on the leader.
   * @return AddUpdateCommand containing latest full doc at shard leader for the given id, or null if not found.
   */
  private UpdateCommand fetchFullUpdateFromLeader(AddUpdateCommand inplaceAdd, long versionOnUpdate) throws IOException {
    String id = inplaceAdd.getPrintableId();
    UpdateShardHandler updateShardHandler = inplaceAdd.getReq().getCore().getCoreContainer().getUpdateShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(DISTRIB, false);
    params.set("getInputDocument", id);
    params.set("onlyIfActive", true);
    SolrRequest<SimpleSolrResponse> ur = new GenericSolrRequest(METHOD.GET, "/get", params);

    String leaderUrl = req.getParams().get(DISTRIB_FROM);
    
    if (leaderUrl == null) {
      // An update we're dependent upon didn't arrive! This is unexpected. Perhaps likely our leader is
      // down or partitioned from us for some reason. Lets force refresh cluster state, and request the
      // leader for the update.
      if (zkController == null) { // we should be in cloud mode, but wtf? could be a unit test
        throw new SolrException(ErrorCode.SERVER_ERROR, "Can't find document with id=" + id + ", but fetching from leader "
            + "failed since we're not in cloud mode.");
      }
      Replica leader;
      try {
        leader = zkController.getZkStateReader().getLeaderRetry(collection, cloudDesc.getShardId());
      } catch (InterruptedException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Exception during fetching from leader.", e);
      }
      leaderUrl = leader.getCoreUrl();
    }

    NamedList<Object> rsp;
    try {
      ur.setBasePath(leaderUrl);
      rsp = updateShardHandler.getUpdateOnlyHttpClient().request(ur);
    } catch (SolrServerException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error during fetching [" + id +
          "] from leader (" + leaderUrl + "): ", e);
    }
    Object inputDocObj = rsp.get("inputDocument");
    Long version = (Long)rsp.get("version");
    SolrInputDocument leaderDoc = (SolrInputDocument) inputDocObj;

    if (leaderDoc == null) {
      // this doc was not found (deleted) on the leader. Lets delete it here as well.
      DeleteUpdateCommand del = new DeleteUpdateCommand(inplaceAdd.getReq());
      del.setIndexedId(inplaceAdd.getIndexedId());
      del.setId(inplaceAdd.getIndexedId().utf8ToString());
      del.setVersion((version == null || version == 0)? -versionOnUpdate: version);
      return del;
    }

    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.solrDoc = leaderDoc;
    cmd.setVersion((long)leaderDoc.getFieldValue(CommonParams.VERSION_FIELD));
    return cmd;
  }
  
  // TODO: may want to switch to using optimistic locking in the future for better concurrency
  // that's why this code is here... need to retry in a loop closely around/in versionAdd
  boolean getUpdatedDocument(AddUpdateCommand cmd, long versionOnUpdate) throws IOException {
    if (!AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) return false;

    Set<String> inPlaceUpdatedFields = AtomicUpdateDocumentMerger.computeInPlaceUpdatableFields(cmd);
    if (inPlaceUpdatedFields.size() > 0) { // non-empty means this is suitable for in-place updates
      if (docMerger.doInPlaceUpdateMerge(cmd, inPlaceUpdatedFields)) {
        return true;
      } else {
        // in-place update failed, so fall through and re-try the same with a full atomic update
      }
    }
    
    // full (non-inplace) atomic update
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    BytesRef id = cmd.getIndexedId();
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(cmd.getReq().getCore(), id);

    if (oldDoc == null) {
      // create a new doc by default if an old one wasn't found
      if (versionOnUpdate <= 0) {
        oldDoc = new SolrInputDocument();
      } else {
        // could just let the optimistic locking throw the error
        throw new SolrException(ErrorCode.CONFLICT, "Document not found for update.  id=" + cmd.getPrintableId());
      }
    } else {
      oldDoc.remove(CommonParams.VERSION_FIELD);
    }


    cmd.solrDoc = docMerger.merge(sdoc, oldDoc);
    return true;
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    
    assert TestInjection.injectFailUpdateRequests();
    
    updateCommand = cmd;

    if (!cmd.isDeleteById()) {
      doDeleteByQuery(cmd);
    } else {
      doDeleteById(cmd);
    }
  }

  // Implementing min_rf here was a bit tricky. When a request comes in for a delete by id to a replica that does _not_
  // have any documents specified by those IDs, the request is not forwarded to any other replicas on that shard. Thus
  // we have to spoof the replicationTracker and set the achieved rf to the number of active replicas.
  //
  private void doDeleteById(DeleteUpdateCommand cmd) throws IOException {
    if (zkEnabled) {
      zkCheck();
      nodes = setupRequest(cmd.getId(), null, cmd.getRoute());
    } else {
      isLeader = getNonZkLeaderAssumption(req);
    }

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);

    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd  = versionDelete(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    if (zkEnabled && isLeader && !isSubShardLeader)  {
      DocCollection coll = zkController.getClusterState().getCollection(collection);
      List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getId(), null);
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_PARENT, cloudDesc.getShardId());
        cmdDistrib.distribDelete(cmd, subShardLeaders, params, true, null, null);
      }

      final List<Node> nodesByRoutingRules = getNodesByRoutingRules(zkController.getClusterState(), coll, cmd.getId(), null);
      if (nodesByRoutingRules != null && !nodesByRoutingRules.isEmpty())  {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_COLLECTION, collection);
        params.set(DISTRIB_FROM_SHARD, cloudDesc.getShardId());
        cmdDistrib.distribDelete(cmd, nodesByRoutingRules, params, true, null, null);
      }
    }

    if (nodes != null) {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_UPDATE_PARAM,
          (isLeader || isSubShardLeader ? DistribPhase.FROMLEADER.toString()
              : DistribPhase.TOLEADER.toString()));
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      if (req.getParams().get(UpdateRequest.MIN_REPFACT) != null) {
        // TODO: Kept for rolling upgrades only. Remove in Solr 9
        params.add(UpdateRequest.MIN_REPFACT, req.getParams().get(UpdateRequest.MIN_REPFACT));
      }
      cmdDistrib.distribDelete(cmd, nodes, params, false, rollupReplicationTracker, leaderReplicationTracker);
    }

    // cmd.getIndexId == null when delete by query
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && cmd.getIndexedId() != null && idField != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<>(1);
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRefBuilder();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  /** @see DistributedUpdateProcessorFactory#addParamToDistributedRequestWhitelist */
  @SuppressWarnings("unchecked")
  protected ModifiableSolrParams filterParams(SolrParams params) {
    ModifiableSolrParams fparams = new ModifiableSolrParams();
    
    Set<String> whitelist = (Set<String>) this.req.getContext().get(PARAM_WHITELIST_CTX_KEY);
    assert null != whitelist : "whitelist can't be null, constructor adds to it";

    for (String p : whitelist) {
      passParam(params, fparams, p);
    }
    return fparams;
  }

  private void passParam(SolrParams params, ModifiableSolrParams fparams, String param) {
    String[] values = params.getParams(param);
    if (values != null) {
      for (String value : values) {
        fparams.add(param, value);
      }
    }
  }

  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {

    // even in non zk mode, tests simulate updates from a leader
    if(!zkEnabled) {
      isLeader = getNonZkLeaderAssumption(req);
    } else {
      zkCheck();
    }

    // NONE: we are the first to receive this deleteByQuery
    //       - it must be forwarded to the leader of every shard
    // TO:   we are a leader receiving a forwarded deleteByQuery... we must:
    //       - block all updates (use VersionInfo)
    //       - flush *all* updates going to our replicas
    //       - forward the DBQ to our replicas and wait for the response
    //       - log + execute the local DBQ
    // FROM: we are a replica receiving a DBQ from our leader
    //       - log + execute the local DBQ
    DistribPhase phase = DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    DocCollection coll = zkEnabled
      ? zkController.getClusterState().getCollection(collection) : null;

    if (zkEnabled && DistribPhase.NONE == phase) {
      if (rollupReplicationTracker == null) {
        rollupReplicationTracker = new RollupRequestReplicationTracker();
      }
      boolean leaderForAnyShard = false;  // start off by assuming we are not a leader for any shard

      ModifiableSolrParams outParams = new ModifiableSolrParams(filterParams(req.getParams()));
      outParams.set(DISTRIB_UPDATE_PARAM, DistribPhase.TOLEADER.toString());
      outParams.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      SolrParams params = req.getParams();
      String route = params.get(ShardParams._ROUTE_);
      Collection<Slice> slices = coll.getRouter().getSearchSlices(route, params, coll);

      List<Node> leaders =  new ArrayList<>(slices.size());
      for (Slice slice : slices) {
        String sliceName = slice.getName();
        Replica leader;
        try {
          leader = zkController.getZkStateReader().getLeaderRetry(collection, sliceName);
        } catch (InterruptedException e) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Exception finding leader for shard " + sliceName, e);
        }

        // TODO: What if leaders changed in the meantime?
        // should we send out slice-at-a-time and if a node returns "hey, I'm not a leader" (or we get an error because it went down) then look up the new leader?

        // Am I the leader for this slice?
        ZkCoreNodeProps coreLeaderProps = new ZkCoreNodeProps(leader);
        String leaderCoreNodeName = leader.getName();
        String coreNodeName = cloudDesc.getCoreNodeName();
        isLeader = coreNodeName.equals(leaderCoreNodeName);

        if (isLeader) {
          // don't forward to ourself
          leaderForAnyShard = true;
        } else {
          leaders.add(new ForwardNode(coreLeaderProps, zkController.getZkStateReader(), collection, sliceName, maxRetriesOnForward));
        }
      }

      outParams.remove("commit"); // this will be distributed from the local commit


      if (params.get(UpdateRequest.MIN_REPFACT) != null) {
        // TODO: Kept this for rolling upgrades. Remove in Solr 9
        outParams.add(UpdateRequest.MIN_REPFACT, req.getParams().get(UpdateRequest.MIN_REPFACT));
      }
      cmdDistrib.distribDelete(cmd, leaders, outParams, false, rollupReplicationTracker, null);

      if (!leaderForAnyShard) {
        return;
      }

      // change the phase to TOLEADER so we look up and forward to our own replicas (if any)
      phase = DistribPhase.TOLEADER;
    }

    List<Node> replicas = null;

    if (zkEnabled && DistribPhase.TOLEADER == phase) {
      // This core should be a leader
      isLeader = true;
      replicas = setupRequestForDBQ();
    } else if (DistribPhase.FROMLEADER == phase) {
      isLeader = false;
    }
    

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);

    if (vinfo == null) {
      super.processDelete(cmd);
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;
    versionDeleteByQuery(cmd);
    if (zkEnabled)  {
      // forward to all replicas
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(CommonParams.VERSION_FIELD, Long.toString(cmd.getVersion()));
      params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
      params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      boolean someReplicas = false;
      boolean subShardLeader = false;
      try {
        subShardLeader = amISubShardLeader(coll, null, null, null);
        if (subShardLeader)  {
          String myShardId = cloudDesc.getShardId();
          Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
              collection, myShardId);
          // DBQ forwarded to NRT and TLOG replicas
          List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
              .getReplicaProps(collection, myShardId, leaderReplica.getName(), null, Replica.State.DOWN, EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG));
          if (replicaProps != null) {
            final List<Node> myReplicas = new ArrayList<>(replicaProps.size());
            for (ZkCoreNodeProps replicaProp : replicaProps) {
              myReplicas.add(new StdNode(replicaProp, collection, myShardId));
            }
            cmdDistrib.distribDelete(cmd, myReplicas, params, false, rollupReplicationTracker, leaderReplicationTracker);
            someReplicas = true;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
      }
      if (leaderLogic) {
        List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), null, null);
        if (subShardLeaders != null)  {
          cmdDistrib.distribDelete(cmd, subShardLeaders, params, true, rollupReplicationTracker, leaderReplicationTracker);
        }
        final List<Node> nodesByRoutingRules = getNodesByRoutingRules(zkController.getClusterState(), coll, null, null);
        if (nodesByRoutingRules != null && !nodesByRoutingRules.isEmpty())  {
          params = new ModifiableSolrParams(filterParams(req.getParams()));
          params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
          params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
              zkController.getBaseUrl(), req.getCore().getName()));
          params.set(DISTRIB_FROM_COLLECTION, collection);
          params.set(DISTRIB_FROM_SHARD, cloudDesc.getShardId());

          cmdDistrib.distribDelete(cmd, nodesByRoutingRules, params, true, rollupReplicationTracker, leaderReplicationTracker);
        }
        if (replicas != null) {
          cmdDistrib.distribDelete(cmd, replicas, params, false, rollupReplicationTracker, leaderReplicationTracker);
          someReplicas = true;
        }
      }

      if (someReplicas)  {
        cmdDistrib.blockAndDoRetries();
      }
    }


    if (returnVersions && rsp != null) {
      if (deleteByQueryResponse == null) {
        deleteByQueryResponse = new NamedList<>(1);
        rsp.add("deleteByQuery",deleteByQueryResponse);
      }
      deleteByQueryResponse.add(cmd.getQuery(), cmd.getVersion());
    }
  }

  protected void versionDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;

    if (!leaderLogic && versionOnUpdate == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    vinfo.blockUpdates();
    try {

      if (versionsStored) {
        if (leaderLogic) {
          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          // TODO update versions in all buckets

          doLocalDelete(cmd);

        } else {
          cmd.setVersion(-versionOnUpdate);

          if (ulog.getState() != UpdateLog.State.ACTIVE && isReplayOrPeersync == false) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.deleteByQuery(cmd);
            return;
          }

          if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
            // TLOG replica not leader, don't write the DBQ to IW
            cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
          }
          doLocalDelete(cmd);
        }
      }

      // since we don't know which documents were deleted, the easiest thing to do is to invalidate
      // all real-time caches (i.e. UpdateLog) which involves also getting a new version of the IndexReader
      // (so cache misses will see up-to-date data)

    } finally {
      vinfo.unblockUpdates();
    }
  }

  // internal helper method to tell if we are the leader for an add or deleteById update
  // NOTE: not called by this class!
  boolean isLeader(UpdateCommand cmd) {
    updateCommand = cmd;

    if (zkEnabled) {
      zkCheck();
      if (cmd instanceof AddUpdateCommand) {
        AddUpdateCommand acmd = (AddUpdateCommand)cmd;
        nodes = setupRequest(acmd.getHashableId(), acmd.getSolrInputDocument());
      } else if (cmd instanceof DeleteUpdateCommand) {
        DeleteUpdateCommand dcmd = (DeleteUpdateCommand)cmd;
        nodes = setupRequest(dcmd.getId(), null);
      }
    } else {
      isLeader = getNonZkLeaderAssumption(req);
    }

    return isLeader;
  }

  private void zkCheck() {

    // Streaming updates can delay shutdown and cause big update reorderings (new streams can't be
    // initiated, but existing streams carry on).  This is why we check if the CC is shutdown.
    // See SOLR-8203 and loop HdfsChaosMonkeyNothingIsSafeTest (and check for inconsistent shards) to test.
    if (req.getCore().getCoreContainer().isShutDown()) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "CoreContainer is shutting down.");
    }

    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      // for log reply or peer sync, we don't need to be connected to ZK
      return;
    }

    if (!zkController.getZkClient().getConnectionManager().isLikelyExpired()) {
      return;
    }
    
    throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Cannot talk to ZooKeeper - Updates are disabled.");
  }

  protected boolean versionDelete(DeleteUpdateCommand cmd) throws IOException {

    BytesRef idBytes = cmd.getIndexedId();

    if (vinfo == null || idBytes == null) {
      super.processDelete(cmd);
      return false;
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash
    // here)
    int bucketHash = bucketHash(idBytes);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(CommonParams.VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    long signedVersionOnUpdate = versionOnUpdate;
    versionOnUpdate = Math.abs(versionOnUpdate); // normalize to positive version

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;
    boolean forwardedFromCollection = cmd.getReq().getParams().get(DISTRIB_FROM_COLLECTION) != null;

    if (!leaderLogic && versionOnUpdate == 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    if (bucket.tryLock()) {
      try {
        if (versionsStored) {
          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            if (forwardedFromCollection && ulog.getState() == UpdateLog.State.ACTIVE) {
              // forwarded from a collection but we are not buffering so strip original version and apply our own
              // see SOLR-5308
              log.info("Removing version field from doc: " + cmd.getId());
              versionOnUpdate = signedVersionOnUpdate = 0;
            }

            // leaders can also be in buffering state during "migrate" API call, see SOLR-5308
            if (forwardedFromCollection && ulog.getState() != UpdateLog.State.ACTIVE
                && !isReplayOrPeersync) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              log.info("Leader logic applied but update log is buffering: " + cmd.getId());
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.delete(cmd);
              return true;
            }

            if (signedVersionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if ((signedVersionOnUpdate == foundVersion) || (signedVersionOnUpdate < 0 && foundVersion < 0)
                  || (signedVersionOnUpdate == 1 && foundVersion > 0)) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getId() + " expected="
                    + signedVersionOnUpdate + " actual=" + foundVersion);
              }
            }

            long version = vinfo.getNewClock();
            cmd.setVersion(-version);
            bucket.updateHighest(version);
          } else {
            cmd.setVersion(-versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && isReplayOrPeersync == false) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.delete(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occurred.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update. we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered. We need to drop this update.
                log.debug("Dropping delete update due to version {}", idBytes.utf8ToString());
                return true;
              }
            }

            if (!isSubShardLeader && replicaType == Replica.Type.TLOG && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              cmd.setFlags(cmd.getFlags() | UpdateCommand.IGNORE_INDEXWRITER);
            }
          }
        }

        doLocalDelete(cmd);
        return false;
      } finally {
        bucket.unlock();
        vinfo.unlockForUpdate();
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unable to get version bucket lock in " + bucket.getLockTimeoutMs() + " ms");
    }
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    
    assert TestInjection.injectFailUpdateRequests();
    
    updateCommand = cmd;
    List<Node> nodes = null;
    Replica leaderReplica = null;
    if (zkEnabled) {
      zkCheck();
      try {
        leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, cloudDesc.getShardId());
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Exception finding leader for shard " + cloudDesc.getShardId(), e);
      }
      isLeader = leaderReplica.getName().equals(cloudDesc.getCoreNodeName());
      
      nodes = getCollectionUrls(collection, EnumSet.of(Replica.Type.TLOG,Replica.Type.NRT), true);
      if (nodes == null) {
        // This could happen if there are only pull replicas
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
            "Unable to distribute commit operation. No replicas available of types " + Replica.Type.TLOG + " or " + Replica.Type.NRT);
      }

      nodes.removeIf((node) -> node.getNodeProps().getNodeName().equals(zkController.getNodeName())
          && node.getNodeProps().getCoreName().equals(req.getCore().getName()));
    }
    
    if (!zkEnabled || (!isLeader && req.getParams().get(COMMIT_END_POINT, "").equals("replicas"))) {
      if (replicaType == Replica.Type.TLOG) {

        if (isLeader) {
          long commitVersion = vinfo.getNewClock();
          cmd.setVersion(commitVersion);
          doLocalCommit(cmd);
        } else {
          assert TestInjection.waitForInSyncWithLeader(req.getCore(),
              zkController, collection, cloudDesc.getShardId()) : "Core " + req.getCore() + " not in sync with leader";
        }

      } else if (replicaType == Replica.Type.PULL) {
        log.warn("Commit not supported on replicas of type " + Replica.Type.PULL);
      } else {
        // NRT replicas will always commit
        if (vinfo != null) {
          long commitVersion = vinfo.getNewClock();
          cmd.setVersion(commitVersion);
        }
  
        doLocalCommit(cmd);
      }
    } else {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));

      List<Node> useNodes = null;
      if (req.getParams().get(COMMIT_END_POINT) == null) {
        useNodes = nodes;
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.TOLEADER.toString());
        params.set(COMMIT_END_POINT, "leaders");
        if (useNodes != null) {
          params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
              zkController.getBaseUrl(), req.getCore().getName()));
          cmdDistrib.distribCommit(cmd, useNodes, params);
          cmdDistrib.blockAndDoRetries();
        }
      }

      if (isLeader) {
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());

        params.set(COMMIT_END_POINT, "replicas");

        useNodes = getReplicaNodesForLeader(cloudDesc.getShardId(), leaderReplica);

        if (useNodes != null) {
          params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
              zkController.getBaseUrl(), req.getCore().getName()));

          cmdDistrib.distribCommit(cmd, useNodes, params);
        }
        // NRT replicas will always commit
        if (vinfo != null) {
          long commitVersion = vinfo.getNewClock();
          cmd.setVersion(commitVersion);
        }

        doLocalCommit(cmd);
        if (useNodes != null) {
          cmdDistrib.blockAndDoRetries();
        }
      }
    }

  }

  private void doLocalCommit(CommitUpdateCommand cmd) throws IOException {
    if (vinfo != null) {
      vinfo.lockForUpdate();
    }
    try {

      if (ulog == null || ulog.getState() == UpdateLog.State.ACTIVE || (cmd.getFlags() & UpdateCommand.REPLAY) != 0) {
        super.processCommit(cmd);
      } else {
        log.info("Ignoring commit while not ACTIVE - state: " + ulog.getState() + " replay: " + ((cmd.getFlags() & UpdateCommand.REPLAY) != 0));
      }

    } finally {
      if (vinfo != null) {
        vinfo.unlockForUpdate();
      }
    }
  }
  
  @Override
  public void finish() throws IOException {
    assert ! finished : "lifecycle sanity check";
    finished = true;
    
    if (zkEnabled) doFinish();
    
    if (next != null && nodes == null) next.finish();
  }

  private List<Node> getCollectionUrls(String collection, EnumSet<Replica.Type> types, boolean onlyLeaders) {
    ClusterState clusterState = zkController.getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (collection == null || docCollection.getSlicesMap() == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + clusterState);
    }
    Map<String,Slice> slices = docCollection.getSlicesMap();
    final List<Node> urls = new ArrayList<>(slices.size());
    for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
      Slice replicas = slices.get(sliceEntry.getKey());
      if (onlyLeaders) {
        Replica replica = docCollection.getLeader(replicas.getName());
        if (replica != null) {
          ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(replica);
          urls.add(new StdNode(nodeProps, collection, replicas.getName()));
        }
        continue;
      }
      Map<String,Replica> shardMap = replicas.getReplicasMap();
      
      for (Entry<String,Replica> entry : shardMap.entrySet()) {
        if (!types.contains(entry.getValue().getType())) {
          continue;
        }
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
        if (clusterState.liveNodesContain(nodeProps.getNodeName())) {
          urls.add(new StdNode(nodeProps, collection, replicas.getName()));
        }
      }
    }
    if (urls.isEmpty()) {
      return null;
    }
    return urls;
  }

  /**
   * Returns a boolean indicating whether or not the caller should behave as
   * if this is the "leader" even when ZooKeeper is not enabled.  
   * (Even in non zk mode, tests may simulate updates to/from a leader)
   */
  public static boolean getNonZkLeaderAssumption(SolrQueryRequest req) {
    DistribPhase phase = 
      DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    // if we have been told we are coming from a leader, then we are 
    // definitely not the leader.  Otherwise assume we are.
    return DistribPhase.FROMLEADER != phase;
  }

  public static final class DistributedUpdatesAsyncException extends SolrException {
    public final List<Error> errors;
    public DistributedUpdatesAsyncException(List<Error> errors) {
      super(buildCode(errors), buildMsg(errors), null);
      this.errors = errors;

      // create a merged copy of the metadata from all wrapped exceptions
      NamedList<String> metadata = new NamedList<String>();
      for (Error error : errors) {
        if (error.e instanceof SolrException) {
          SolrException e = (SolrException) error.e;
          NamedList<String> eMeta = e.getMetadata();
          if (null != eMeta) {
            metadata.addAll(eMeta);
          }
        }
      }
      if (0 < metadata.size()) {
        this.setMetadata(metadata);
      }
    }

    /** Helper method for constructor */
    private static int buildCode(List<Error> errors) {
      assert null != errors;
      assert 0 < errors.size();

      int minCode = Integer.MAX_VALUE;
      int maxCode = Integer.MIN_VALUE;
      for (Error error : errors) {
        log.trace("REMOTE ERROR: {}", error);
        minCode = Math.min(error.statusCode, minCode);
        maxCode = Math.max(error.statusCode, maxCode);
      }
      if (minCode == maxCode) {
        // all codes are consistent, use that...
        return minCode;
      } else if (400 <= minCode && maxCode < 500) {
        // all codes are 4xx, use 400
        return ErrorCode.BAD_REQUEST.code;
      } 
      // ...otherwise use sensible default
      return ErrorCode.SERVER_ERROR.code;
    }
    
    /** Helper method for constructor */
    private static String buildMsg(List<Error> errors) {
      assert null != errors;
      assert 0 < errors.size();
      
      if (1 == errors.size()) {
        return "Async exception during distributed update: " + errors.get(0).e.getMessage();
      } else {
        StringBuilder buf = new StringBuilder(errors.size() + " Async exceptions during distributed update: ");
        for (Error error : errors) {
          buf.append("\n");
          buf.append(error.e.getMessage());
        }
        return buf.toString();
      }
    }
  }


  //    Keeps track of the replication factor achieved for a distributed update request
  //    originated in this distributed update processor. A RollupReplicationTracker is the only tracker that will
  //    persist across sub-requests.
  //
  //   Note that the replica that receives the original request has the only RollupReplicationTracker that exists for the
  //   lifetime of the batch. The leader for each shard keeps track of its own achieved replication for its shard
  //   and attaches that to the response to the originating node (i.e. the one with the RollupReplicationTracker).
  //   Followers in general do not need a tracker of any sort with the sole exception of the RollupReplicationTracker
  //   allocated on the original node that receives the top-level request.
  //
  //   DeleteById is tricky. Since the docs are sent one at a time, there has to be some fancy dancing. In the
  //   deleteById case, here are the rules:
  //
  //   If I'm leader, there are two possibilities:
  //     1> I got the original request. This is the hard one. There are two sub-cases:
  //     a> Some document in the request is deleted from the shard I lead. In this case my computed replication
  //        factor counts.
  //     b> No document in the packet is deleted from my shard. In that case I have nothing to say about the
  //        achieved replication factor.
  //
  //     2> I'm a leader and I got the request from some other replica. In this case I can be certain of a couple of things:
  //       a> The document in the request will be deleted from my shard
  //       b> my replication factor counts.
  //
  //   Even the DeleteById case follows the rules for whether a RollupReplicaitonTracker is allocated.
  //   This doesn't matter when it comes to delete-by-query since all leaders get the sub-request.


  public static class RollupRequestReplicationTracker {

    private int achievedRf = Integer.MAX_VALUE;

    public int getAchievedRf() {
      return achievedRf;
    }

    // We want to report only the minimun _ever_ achieved...
    public void testAndSetAchievedRf(int rf) {
      this.achievedRf = Math.min(this.achievedRf, rf);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("RollupRequestReplicationTracker")
          .append(" achievedRf: ")
          .append(achievedRf);
      return sb.toString();
    }
  }


  // Allocate a LeaderRequestReplicatinTracker if (and only if) we're a leader. If the request comes in to the leader
  // at first, allocate both one of these and a RollupRequestReplicationTracker.
  //
  // Since these are leader-only, all they really have to do is track the individual update request for this shard
  // and return it to be added to the rollup tracker. Which is kind of simple since we get an onSuccess method in
  // SolrCmdDistributor

  public static class LeaderRequestReplicationTracker {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Since we only allocate one of these on the leader and, by definition, the leader has been found and is running,
    // we have a replication factor of one by default.
    private int achievedRf = 1;

    private final String myShardId;

    public LeaderRequestReplicationTracker(String shardId) {
      this.myShardId = shardId;
    }

    // gives the replication factor that was achieved for this request
    public int getAchievedRf() {
      return achievedRf;
    }

    public void trackRequestResult(Node node, boolean success) {
      if (log.isDebugEnabled()) {
        log.debug("trackRequestResult({}): success? {}, shardId={}", node, success, myShardId);
      }

      if (success) {
        ++achievedRf;
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder("LeaderRequestReplicationTracker");
      sb.append(", achievedRf=")
          .append(getAchievedRf())
          .append(" for shard ")
          .append(myShardId);
      return sb.toString();
    }
  }
}
