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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class DistributedZkUpdateProcessor extends DistributedUpdateProcessor {

  private final CloudDescriptor cloudDesc;
  private final ZkController zkController;
  private final SolrCmdDistributor cmdDistrib;
  protected List<SolrCmdDistributor.Node> nodes;
  private Set<String> skippedCoreNodeNames;
  private final String collection;
  private boolean readOnlyCollection = false;

  // The cached immutable clusterState for the update... usually refreshed for each individual update.
  // Different parts of this class used to request current clusterState views, which lead to subtle bugs and race conditions
  // such as SOLR-13815 (live split data loss.)  Most likely, the only valid reasons for updating clusterState should be on
  // certain types of failure + retry.
  // Note: there may be other races related to
  //   1) cluster topology change across multiple adds
  //   2) use of methods directly on zkController that use a different clusterState
  //   3) in general, not controlling carefully enough exactly when our view of clusterState is updated
  protected ClusterState clusterState;

  // should we clone the document before sending it to replicas?
  // this is set to true in the constructor if the next processors in the chain
  // are custom and may modify the SolrInputDocument racing with its serialization for replication
  private final boolean cloneRequiredOnLeader;

  //used for keeping track of replicas that have processed an add/update from the leader
  private RollupRequestReplicationTracker rollupReplicationTracker = null;
  private LeaderRequestReplicationTracker leaderReplicationTracker = null;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DistributedZkUpdateProcessor(SolrQueryRequest req,
                                      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
    CoreContainer cc = req.getCore().getCoreContainer();
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    zkController = cc.getZkController();
    cmdDistrib = new SolrCmdDistributor(cc.getUpdateShardHandler());
    cloneRequiredOnLeader = isCloneRequiredOnLeader(next);
    collection = cloudDesc.getCollectionName();
    clusterState = zkController.getClusterState();
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll != null) {
      // check readOnly property in coll state
      readOnlyCollection = coll.isReadOnly();
    }
  }

  private boolean isReadOnly() {
    return readOnlyCollection || req.getCore().readOnly;
  }

  private boolean isCloneRequiredOnLeader(UpdateRequestProcessor next) {
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
    return shouldClone;
  }

  @Override
  protected Replica.Type computeReplicaType() {
    // can't use cloudDesc since this is called by super class, before the constructor instantiates cloudDesc.
    return req.getCore().getCoreDescriptor().getCloudDescriptor().getReplicaType();
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    clusterState = zkController.getClusterState();

    assert TestInjection.injectFailUpdateRequests();

    if (isReadOnly()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }

    updateCommand = cmd;

    List<SolrCmdDistributor.Node> nodes = null;
    Replica leaderReplica = null;
    zkCheck();
    try {
      leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, cloudDesc.getShardId());
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Exception finding leader for shard " + cloudDesc.getShardId(), e);
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

    if (!isLeader && req.getParams().get(COMMIT_END_POINT, "").equals("replicas")) {
      if (replicaType == Replica.Type.PULL) {
        log.warn("Commit not supported on replicas of type " + Replica.Type.PULL);
      } else if (replicaType == Replica.Type.NRT) {
        doLocalCommit(cmd);
      }
    } else {
      // zk
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));

      List<SolrCmdDistributor.Node> useNodes = null;
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

        doLocalCommit(cmd);

        if (useNodes != null) {
          cmdDistrib.blockAndDoRetries();
        }
      }
    }
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    clusterState = zkController.getClusterState();

    assert TestInjection.injectFailUpdateRequests();

    if (isReadOnly()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }

    setupRequest(cmd);

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);

    super.processAdd(cmd);
  }

  @Override
  protected void doDistribAdd(AddUpdateCommand cmd) throws IOException {

    if (isLeader && !isSubShardLeader)  {
      DocCollection coll = clusterState.getCollection(collection);
      List<SolrCmdDistributor.Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getRootIdUsingRouteParam(), cmd.getSolrInputDocument());
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_PARENT, cloudDesc.getShardId());
        cmdDistrib.distribAdd(cmd, subShardLeaders, params, true);
      }
      final List<SolrCmdDistributor.Node> nodesByRoutingRules = getNodesByRoutingRules(clusterState, coll, cmd.getRootIdUsingRouteParam(), cmd.getSolrInputDocument());
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
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    clusterState = zkController.getClusterState();

    if (isReadOnly()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }

    super.processDelete(cmd);
  }

  @Override
  protected void doDeleteById(DeleteUpdateCommand cmd) throws IOException {
    setupRequest(cmd);

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);

    super.doDeleteById(cmd);
  }

  @Override
  protected void doDistribDeleteById(DeleteUpdateCommand cmd) throws IOException {
    if (isLeader && !isSubShardLeader)  {
      DocCollection coll = clusterState.getCollection(collection);
      List<SolrCmdDistributor.Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getId(), null);
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set(DISTRIB_FROM_PARENT, cloudDesc.getShardId());
        cmdDistrib.distribDelete(cmd, subShardLeaders, params, true, null, null);
      }

      final List<SolrCmdDistributor.Node> nodesByRoutingRules = getNodesByRoutingRules(clusterState, coll, cmd.getId(), null);
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
  }

  @Override
  protected void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    zkCheck();

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

    DocCollection coll = clusterState.getCollection(collection);

    if (DistribPhase.NONE == phase) {
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

      List<SolrCmdDistributor.Node> leaders =  new ArrayList<>(slices.size());
      for (Slice slice : slices) {
        String sliceName = slice.getName();
        Replica leader;
        try {
          leader = zkController.getZkStateReader().getLeaderRetry(collection, sliceName);
        } catch (InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Exception finding leader for shard " + sliceName, e);
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
          leaders.add(new SolrCmdDistributor.ForwardNode(coreLeaderProps, zkController.getZkStateReader(), collection, sliceName, maxRetriesOnForward));
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
    List<SolrCmdDistributor.Node> replicas = null;

    if (DistribPhase.TOLEADER == phase) {
      // This core should be a leader
      isLeader = true;
      replicas = setupRequestForDBQ();
    } else if (DistribPhase.FROMLEADER == phase) {
      isLeader = false;
    }

    // check if client has requested minimum replication factor information. will set replicationTracker to null if
    // we aren't the leader or subShardLeader
    checkReplicationTracker(cmd);
    super.doDeleteByQuery(cmd, replicas, coll);
  }

  @Override
  protected void doDistribDeleteByQuery(DeleteUpdateCommand cmd, List<SolrCmdDistributor.Node> replicas,
                                        DocCollection coll) throws IOException {

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;

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
      if (subShardLeader) {
        String myShardId = cloudDesc.getShardId();
        Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
            collection, myShardId);
        // DBQ forwarded to NRT and TLOG replicas
        List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
            .getReplicaProps(collection, myShardId, leaderReplica.getName(), null, Replica.State.DOWN, EnumSet.of(Replica.Type.NRT, Replica.Type.TLOG));
        if (replicaProps != null) {
          final List<SolrCmdDistributor.Node> myReplicas = new ArrayList<>(replicaProps.size());
          for (ZkCoreNodeProps replicaProp : replicaProps) {
            myReplicas.add(new SolrCmdDistributor.StdNode(replicaProp, collection, myShardId));
          }
          cmdDistrib.distribDelete(cmd, myReplicas, params, false, rollupReplicationTracker, leaderReplicationTracker);
          someReplicas = true;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    if (leaderLogic) {
      List<SolrCmdDistributor.Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), null, null);
      if (subShardLeaders != null) {
        cmdDistrib.distribDelete(cmd, subShardLeaders, params, true, rollupReplicationTracker, leaderReplicationTracker);
      }
      final List<SolrCmdDistributor.Node> nodesByRoutingRules = getNodesByRoutingRules(clusterState, coll, null, null);
      if (nodesByRoutingRules != null && !nodesByRoutingRules.isEmpty()) {
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

    if (someReplicas) {
      cmdDistrib.blockAndDoRetries();
    }
  }

  // used for deleteByQuery to get the list of nodes this leader should forward to
  private List<SolrCmdDistributor.Node> setupRequestForDBQ() {
    List<SolrCmdDistributor.Node> nodes = null;
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
          nodes.add(new SolrCmdDistributor.StdNode(props, collection, shardId));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

    return nodes;
  }

  @Override
  protected String getLeaderUrl(String id) {
    // try get leader from req params, fallback to zk lookup if not found.
    String distribFrom = req.getParams().get(DISTRIB_FROM);
    if(distribFrom != null) {
      return distribFrom;
    }
    return getLeaderUrlZk(id);
  }

  private String getLeaderUrlZk(String id) {
    // An update we're dependent upon didn't arrive! This is unexpected. Perhaps likely our leader is
    // down or partitioned from us for some reason. Lets force refresh cluster state, and request the
    // leader for the update.
    if (zkController == null) { // we should be in cloud mode, but wtf? could be a unit test
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find document with id=" + id + ", but fetching from leader "
          + "failed since we're not in cloud mode.");
    }
    try {
      return zkController.getZkStateReader().getLeaderRetry(collection, cloudDesc.getShardId()).getCoreUrl();
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception during fetching from leader.", e);
    }
  }

  @Override
  void setupRequest(UpdateCommand cmd) {
    updateCommand = cmd;
    zkCheck();
    if (cmd instanceof AddUpdateCommand) {
      AddUpdateCommand acmd = (AddUpdateCommand)cmd;
      nodes = setupRequest(acmd.getRootIdUsingRouteParam(), acmd.getSolrInputDocument());
    } else if (cmd instanceof DeleteUpdateCommand) {
      DeleteUpdateCommand dcmd = (DeleteUpdateCommand)cmd;
      nodes = setupRequest(dcmd.getId(), null);
    }
  }

  protected List<SolrCmdDistributor.Node> setupRequest(String id, SolrInputDocument doc) {
    return setupRequest(id, doc, null);
  }

  protected List<SolrCmdDistributor.Node> setupRequest(String id, SolrInputDocument doc, String route) {
    // if we are in zk mode...

    assert TestInjection.injectUpdateRandomPause();

    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
      forwardToLeader = false;
      return null;
    }

    clusterState = zkController.getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    Slice slice = coll.getRouter().getTargetSlice(id, doc, route, req.getParams(), coll);

    if (slice == null) {
      // No slice found.  Most strict routers will have already thrown an exception, so a null return is
      // a signal to use the slice of this core.
      // TODO: what if this core is not in the targeted collection?
      String shardId = cloudDesc.getShardId();
      slice = coll.getSlice(shardId);
      if (slice == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
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

        List<SolrCmdDistributor.Node> nodes = new ArrayList<>(replicas.size());
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
            nodes.add(new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(replica), collection, shardId, maxRetriesToFollowers));
          }
        }
        return nodes;

      } else {
        // I need to forward on to the leader...
        forwardToLeader = true;
        return Collections.singletonList(
            new SolrCmdDistributor.ForwardNode(new ZkCoreNodeProps(leaderReplica), zkController.getZkStateReader(), collection, shardId, maxRetriesOnForward));
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
  }

  @Override
  protected boolean shouldCloneCmdDoc() {
    boolean willDistrib = isLeader && nodes != null && nodes.size() > 0;
    return willDistrib & cloneRequiredOnLeader;
  }

  // helper method, processAdd was getting a bit large.
  // Sets replicationTracker = null if we aren't the leader
  // We have two possibilities here:
  //
  // 1> we are a leader: Allocate a LeaderTracker and, if we're getting the original request, a RollupTracker
  // 2> we're a follower: allocat a RollupTracker
  //
  private void checkReplicationTracker(UpdateCommand cmd) {

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


  private List<SolrCmdDistributor.Node> getCollectionUrls(String collection, EnumSet<Replica.Type> types, boolean onlyLeaders) {
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (collection == null || docCollection.getSlicesMap() == null) {
      throw new ZooKeeperException(SolrException.ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + clusterState);
    }
    Map<String,Slice> slices = docCollection.getSlicesMap();
    final List<SolrCmdDistributor.Node> urls = new ArrayList<>(slices.size());
    for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
      Slice replicas = slices.get(sliceEntry.getKey());
      if (onlyLeaders) {
        Replica replica = docCollection.getLeader(replicas.getName());
        if (replica != null) {
          ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(replica);
          urls.add(new SolrCmdDistributor.StdNode(nodeProps, collection, replicas.getName()));
        }
        continue;
      }
      Map<String,Replica> shardMap = replicas.getReplicasMap();

      for (Map.Entry<String,Replica> entry : shardMap.entrySet()) {
        if (!types.contains(entry.getValue().getType())) {
          continue;
        }
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
        if (clusterState.liveNodesContain(nodeProps.getNodeName())) {
          urls.add(new SolrCmdDistributor.StdNode(nodeProps, collection, replicas.getName()));
        }
      }
    }
    if (urls.isEmpty()) {
      return null;
    }
    return urls;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  private boolean couldIbeSubShardLeader(DocCollection coll) {
    // Could I be the leader of a shard in "construction/recovery" state?
    String myShardId = cloudDesc.getShardId();
    Slice mySlice = coll.getSlice(myShardId);
    Slice.State state = mySlice.getState();
    return state == Slice.State.CONSTRUCTION || state == Slice.State.RECOVERY;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  protected boolean amISubShardLeader(DocCollection coll, Slice parentSlice, String id, SolrInputDocument doc) throws InterruptedException {
    // Am I the leader of a shard in "construction/recovery" state?
    String myShardId = cloudDesc.getShardId();
    Slice mySlice = coll.getSlice(myShardId);
    final Slice.State state = mySlice.getState();
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

  protected List<SolrCmdDistributor.Node> getReplicaNodesForLeader(String shardId, Replica leaderReplica) {
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

    List<SolrCmdDistributor.Node> nodes = new ArrayList<>(replicas.size());
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
        nodes.add(new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(replica), collection, shardId));
      }
    }
    return nodes;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#SPLITSHARD} */
  protected List<SolrCmdDistributor.Node> getSubShardLeaders(DocCollection coll, String shardId, String docId, SolrInputDocument doc) {
    Collection<Slice> allSlices = coll.getSlices();
    List<SolrCmdDistributor.Node> nodes = null;
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
          if (sliceLeader != null && clusterState.liveNodesContain(sliceLeader.getNodeName()))  {
            if (nodes == null) nodes = new ArrayList<>();
            ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(sliceLeader);
            nodes.add(new SolrCmdDistributor.StdNode(nodeProps, coll.getName(), aslice.getName()));
          }
        }
      }
    }
    return nodes;
  }

  /** For {@link org.apache.solr.common.params.CollectionParams.CollectionAction#MIGRATE} */
  protected List<SolrCmdDistributor.Node> getNodesByRoutingRules(ClusterState cstate, DocCollection coll, String id, SolrInputDocument doc)  {
    DocRouter router = coll.getRouter();
    List<SolrCmdDistributor.Node> nodes = null;
    if (router instanceof CompositeIdRouter)  {
      CompositeIdRouter compositeIdRouter = (CompositeIdRouter) router;
      String myShardId = cloudDesc.getShardId();
      Slice slice = coll.getSlice(myShardId);
      Map<String, RoutingRule> routingRules = slice.getRoutingRules();
      if (routingRules != null) {

        // delete by query case
        if (id == null) {
          for (Map.Entry<String, RoutingRule> entry : routingRules.entrySet()) {
            String targetCollectionName = entry.getValue().getTargetCollectionName();
            final DocCollection docCollection = cstate.getCollectionOrNull(targetCollectionName);
            if (docCollection != null && docCollection.getActiveSlicesArr().length > 0) {
              final Slice[] activeSlices = docCollection.getActiveSlicesArr();
              Slice any = activeSlices[0];
              if (nodes == null) nodes = new ArrayList<>();
              nodes.add(new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(any.getLeader())));
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
                      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                          "No active slices serving " + id + " found for target collection: " + rule.getTargetCollectionName());
                    }
                    Replica targetLeader = targetColl.getLeader(activeSlices.iterator().next().getName());
                    nodes = new ArrayList<>(1);
                    nodes.add(new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(targetLeader)));
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

    DocCollection docCollection = clusterState.getCollection(collection);
    Slice mySlice = docCollection.getSlice(cloudDesc.getShardId());
    boolean localIsLeader = cloudDesc.isLeader();
    if (DistribPhase.FROMLEADER == phase && localIsLeader && from != null) { // from will be null on log replay
      String fromShard = req.getParams().get(DISTRIB_FROM_PARENT);
      if (fromShard != null) {
        if (mySlice.getState() == Slice.State.ACTIVE)  {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but we are in active state");
        }
        // shard splitting case -- check ranges to see if we are a sub-shard
        Slice fromSlice = docCollection.getSlice(fromShard);
        DocRouter.Range parentRange = fromSlice.getRange();
        if (parentRange == null) parentRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (mySlice.getRange() != null && !mySlice.getRange().isSubsetOf(parentRange)) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but parent hash range is not superset of my range");
        }
      } else {
        String fromCollection = req.getParams().get(DISTRIB_FROM_COLLECTION); // is it because of a routing rule?
        if (fromCollection == null)  {
          log.error("Request says it is coming from leader, but we are the leader: " + req.getParamString());
          SolrException solrExc = new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Request says it is coming from leader, but we are the leader");
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
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "ClusterState says we are the leader (" + zkController.getBaseUrl()
              + "/" + req.getCore().getName() + "), but locally we don't think so. Request came from " + from);
    }
  }

  @Override
  protected void doClose() {
    if (cmdDistrib != null) {
      cmdDistrib.close();
    }
  }

  @Override
  public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
    clusterState = zkController.getClusterState();

    if (isReadOnly()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }
    super.processMergeIndexes(cmd);
  }

  @Override
  public void processRollback(RollbackUpdateCommand cmd) throws IOException {
    clusterState = zkController.getClusterState();

    if (isReadOnly()) {
      throw new SolrException(ErrorCode.FORBIDDEN, "Collection " + collection + " is read-only.");
    }
    super.processRollback(cmd);
  }

  // TODO: optionally fail if n replicas are not reached...
  protected void doDistribFinish() {
    clusterState = zkController.getClusterState();

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
    List<SolrCmdDistributor.Error> errors = cmdDistrib.getErrors();
    // TODO - we may need to tell about more than one error...

    List<SolrCmdDistributor.Error> errorsForClient = new ArrayList<>(errors.size());
    Set<String> replicasShouldBeInLowerTerms = new HashSet<>();
    for (final SolrCmdDistributor.Error error : errors) {

      if (error.req.node instanceof SolrCmdDistributor.ForwardNode) {
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

      if (error.req.node instanceof SolrCmdDistributor.StdNode) {
        SolrCmdDistributor.StdNode stdNode = (SolrCmdDistributor.StdNode)error.req.node;
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

  private void zkCheck() {

    // Streaming updates can delay shutdown and cause big update reorderings (new streams can't be
    // initiated, but existing streams carry on).  This is why we check if the CC is shutdown.
    // See SOLR-8203 and loop HdfsChaosMonkeyNothingIsSafeTest (and check for inconsistent shards) to test.
    if (req.getCore().getCoreContainer().isShutDown()) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "CoreContainer is shutting down.");
    }

    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      // for log reply or peer sync, we don't need to be connected to ZK
      return;
    }

    if (!zkController.getZkClient().getConnectionManager().isLikelyExpired()) {
      return;
    }

    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Cannot talk to ZooKeeper - Updates are disabled.");
  }
}
