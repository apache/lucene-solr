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

import static org.apache.solr.cloud.Overseer.*;
import static org.apache.solr.cloud.OverseerCollectionProcessor.*;
import static org.apache.solr.common.cloud.DocCollection.*;
import static org.apache.solr.common.cloud.ZkStateReader.*;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonParams.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestSyncShard;
import org.apache.solr.cloud.DistributedQueue;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
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
import org.apache.solr.handler.BlobHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.CreateMode;
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
      case BALANCESHARDUNIQUE: {
        this.handleBalanceShardUnique(req, rsp);
        break;
      }
      case REBALANCELEADERS: {
        this.handleBalanceLeaders(req, rsp);
        break;
      }
      default: {
          throw new RuntimeException("Unknown action: " + action);
      }
    }

    rsp.setHttpCaching(false);
  }


  private void handleBalanceLeaders(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP);

    String collectionName = req.getParams().get(COLLECTION_PROP);
    if (StringUtils.isBlank(collectionName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "The " + COLLECTION_PROP + " is required for the REASSIGNLEADERS command."));
    }
    coreContainer.getZkController().getZkStateReader().updateClusterState(true);
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    DocCollection dc = clusterState.getCollection(collectionName);
    if (dc == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
    }
    Map<String, String> currentRequests = new HashMap<>();
    int max = req.getParams().getInt(MAX_AT_ONCE_PROP, Integer.MAX_VALUE);
    if (max <= 0) max = Integer.MAX_VALUE;
    int maxWaitSecs = req.getParams().getInt(MAX_WAIT_SECONDS_PROP, 60);
    NamedList<Object> results = new NamedList<>();

    boolean keepGoing = true;
    for (Slice slice : dc.getSlices()) {
      insurePreferredIsLeader(req, results, slice, currentRequests);
      if (currentRequests.size() == max) {
        log.info("Queued " + max + " leader reassignments, waiting for some to complete.");
        keepGoing = waitForLeaderChange(currentRequests, maxWaitSecs, false, results);
        if (keepGoing == false) {
          break; // If we've waited longer than specified, don't continue to wait!
        }
      }
    }
    if (keepGoing == true) {
      keepGoing = waitForLeaderChange(currentRequests, maxWaitSecs, true, results);
    }
    if (keepGoing == true) {
      log.info("All leader reassignments completed.");
    } else {
      log.warn("Exceeded specified timeout of ." + maxWaitSecs + "' all leaders may not have been reassigned");
    }

    rsp.getValues().addAll(results);
  }

  private void insurePreferredIsLeader(SolrQueryRequest req, NamedList<Object> results,
                                              Slice slice, Map<String, String> currentRequests) throws KeeperException, InterruptedException {
    final String inactivePreferreds = "inactivePreferreds";
    final String alreadyLeaders = "alreadyLeaders";
    String collectionName = req.getParams().get(COLLECTION_PROP);

    for (Replica replica : slice.getReplicas()) {
      // Tell the replica to become the leader if we're the preferred leader AND active AND not the leader already
      if (replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false) == false) {
        continue;
      }
      // OK, we are the preferred leader, are we the actual leader?
      if (replica.getBool(LEADER_PROP, false)) {
        //We're a preferred leader, but we're _also_ the leader, don't need to do anything.
        NamedList<Object> noops = (NamedList<Object>) results.get(alreadyLeaders);
        if (noops == null) {
          noops = new NamedList<>();
          results.add(alreadyLeaders, noops);
        }
        NamedList<Object> res = new NamedList<>();
        res.add("status", "success");
        res.add("msg", "Already leader");
        res.add("shard", slice.getName());
        res.add("nodeName", replica.getNodeName());
        noops.add(replica.getName(), res);
        return; // already the leader, do nothing.
      }

      // We're the preferred leader, but someone else is leader. Only become leader if we're active.
      if (replica.getState() != Replica.State.ACTIVE) {
        NamedList<Object> inactives = (NamedList<Object>) results.get(inactivePreferreds);
        if (inactives == null) {
          inactives = new NamedList<>();
          results.add(inactivePreferreds, inactives);
        }
        NamedList<Object> res = new NamedList<>();
        res.add("status", "skipped");
        res.add("msg", "Node is a referredLeader, but it's inactive. Skipping");
        res.add("shard", slice.getName());
        res.add("nodeName", replica.getNodeName());
        inactives.add(replica.getName(), res);
        return; // Don't try to become the leader if we're not active!
      }

      // Replica is the preferred leader but not the actual leader, do something about that.
      // "Something" is
      // 1> if the preferred leader isn't first in line, tell it to re-queue itself.
      // 2> tell the actual leader to re-queue itself.

      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();

      List<String> electionNodes = OverseerCollectionProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
          ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

      if (electionNodes.size() < 2) { // if there's only one node in the queue, should already be leader and we shouldn't be here anyway.
        log.warn("Rebalancing leaders and slice " + slice.getName() + " has less than two elements in the leader " +
            "election queue, but replica " + replica.getName() + " doesn't think it's the leader. Do nothing");
        return;
      }

      // Ok, the sorting for election nodes is a bit strange. If the sequence numbers are the same, then the whole
      // string is used, but that sorts nodes with the same sequence number by their session IDs from ZK.
      // While this is determinate, it's not quite what we need, so re-queue nodes that aren't us and are
      // watching the leader node..

      String firstWatcher = electionNodes.get(1);

      if (LeaderElector.getNodeName(firstWatcher).equals(replica.getName()) == false) {
        makeReplicaFirstWatcher(collectionName, slice, replica);
      }

      String coreName = slice.getReplica(LeaderElector.getNodeName(electionNodes.get(0))).getStr(CORE_NAME_PROP);
      rejoinElection(collectionName, slice, electionNodes.get(0), coreName, false);
      waitForNodeChange(collectionName, slice, electionNodes.get(0));


      return; // Done with this slice, skip the rest of the replicas.
    }
  }
  // Put the replica in at the head of the queue and send all nodes with the same sequence number to the back of the list
  void makeReplicaFirstWatcher(String collectionName, Slice slice, Replica replica)
      throws KeeperException, InterruptedException {

    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    List<String> electionNodes = OverseerCollectionProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
        ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

    // First, queue up the preferred leader at the head of the queue.
    int newSeq = -1;
    for (String electionNode : electionNodes) {
      if (LeaderElector.getNodeName(electionNode).equals(replica.getName())) {
        String coreName = slice.getReplica(LeaderElector.getNodeName(electionNode)).getStr(CORE_NAME_PROP);
        rejoinElection(collectionName, slice, electionNode, coreName, true);
        newSeq = waitForNodeChange(collectionName, slice, electionNode);
        break;
      }
    }
    if (newSeq == -1) {
      return; // let's not continue if we didn't get what we expect. Possibly we're offline etc..
    }

    List<String> electionNodesTmp = OverseerCollectionProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
        ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));


    // Now find other nodes that have the same sequence number as this node and re-queue them at the end of the queue.
    electionNodes = OverseerCollectionProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
        ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

    for (String thisNode : electionNodes) {
      if (LeaderElector.getSeq(thisNode) > newSeq) {
        break;
      }
      if (LeaderElector.getNodeName(thisNode).equals(replica.getName())) {
        continue;
      }
      if (LeaderElector.getSeq(thisNode) == newSeq) {
        String coreName = slice.getReplica(LeaderElector.getNodeName(thisNode)).getStr(CORE_NAME_PROP);
        rejoinElection(collectionName, slice, thisNode, coreName, false);
        waitForNodeChange(collectionName, slice, thisNode);
      }
    }
  }

  int waitForNodeChange(String collectionName, Slice slice, String electionNode) throws InterruptedException, KeeperException {
    String nodeName = LeaderElector.getNodeName(electionNode);
    int oldSeq = LeaderElector.getSeq(electionNode);
     for (int idx = 0; idx < 600; ++idx) {
      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      List<String> electionNodes = OverseerCollectionProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
          ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));
      for (String testNode : electionNodes) {
        if (LeaderElector.getNodeName(testNode).equals(nodeName) && oldSeq != LeaderElector.getSeq(testNode)) {
          return LeaderElector.getSeq(testNode);
        }
      }

      Thread.sleep(100);
    }
    return -1;
  }
  private void rejoinElection(String collectionName, Slice slice, String electionNode, String core,
                              boolean rejoinAtHead) throws KeeperException, InterruptedException {
    Replica replica = slice.getReplica(LeaderElector.getNodeName(electionNode));
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(COLLECTION_PROP, collectionName);
    propMap.put(SHARD_ID_PROP, slice.getName());
    propMap.put(Overseer.QUEUE_OPERATION, REBALANCELEADERS.toLower());
    propMap.put(CORE_NAME_PROP, core);
    propMap.put(NODE_NAME_PROP, replica.getName());
    propMap.put(ZkStateReader.BASE_URL_PROP, replica.getProperties().get(ZkStateReader.BASE_URL_PROP));
    propMap.put(REJOIN_AT_HEAD_PROP, Boolean.toString(rejoinAtHead)); // Get ourselves to be first in line.
    propMap.put(ELECTION_NODE_PROP, electionNode);
    String asyncId = REBALANCELEADERS.toLower() + "_" + core + "_" + Math.abs(System.nanoTime());
    propMap.put(ASYNC, asyncId);
    ZkNodeProps m = new ZkNodeProps(propMap);
    SolrQueryResponse rspIgnore = new SolrQueryResponse(); // I'm constructing my own response
    handleResponse(REBALANCELEADERS.toLower(), m, rspIgnore); // Want to construct my own response here.
  }

  // currentAsyncIds - map of request IDs and reporting data (value)
  // maxWaitSecs - How long are we going to wait? Defaults to 30 seconds.
  // waitForAll - if true, do not return until all assignments have been made.
  // results - a place to stash results for reporting back to the user.
  //
  private boolean waitForLeaderChange(Map<String, String> currentAsyncIds, final int maxWaitSecs,
                                      Boolean waitForAll, NamedList<Object> results)
      throws KeeperException, InterruptedException {

    if (currentAsyncIds.size() == 0) return true;

    for (int idx = 0; idx < maxWaitSecs * 10; ++idx) {
      Iterator<Map.Entry<String, String>> iter = currentAsyncIds.entrySet().iterator();
      boolean foundChange = false;
      while (iter.hasNext()) {
        Map.Entry<String, String> pair = iter.next();
        String asyncId = pair.getKey();
        if (coreContainer.getZkController().getOverseerFailureMap().contains(asyncId)) {
          coreContainer.getZkController().getOverseerFailureMap().remove(asyncId);
          NamedList<Object> fails = (NamedList<Object>) results.get("failures");
          if (fails == null) {
            fails = new NamedList<>();
            results.add("failures", fails);
          }
          NamedList<Object> res = new NamedList<>();
          res.add("status", "failed");
          res.add("msg", "Failed to assign '" + pair.getValue() + "' to be leader");
          fails.add(asyncId.substring(REBALANCELEADERS.toLower().length()), res);
          iter.remove();
          foundChange = true;
        } else if (coreContainer.getZkController().getOverseerCompletedMap().contains(asyncId)) {
          coreContainer.getZkController().getOverseerCompletedMap().remove(asyncId);
          NamedList<Object> successes = (NamedList<Object>) results.get("successes");
          if (successes == null) {
            successes = new NamedList<>();
            results.add("successes", successes);
          }
          NamedList<Object> res = new NamedList<>();
          res.add("status", "success");
          res.add("msg", "Assigned '" + pair.getValue() + "' to be leader");
          successes.add(asyncId.substring(REBALANCELEADERS.toLower().length()), res);
          iter.remove();
          foundChange = true;
        }
      }
      // We're done if we're processing a few at a time or all requests are processed.
      if ((foundChange && waitForAll == false) || currentAsyncIds.size() == 0) {
        return true;
      }
      Thread.sleep(100); //TODO: Is there a better thing to do than sleep here?
    }
    return false;
  }
  private void handleAddReplicaProp(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP, PROPERTY_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_VALUE_PROP);


    Map<String, Object> map = ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    copyIfNotNull(req.getParams(), map, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP,
        SHARD_UNIQUE, PROPERTY_VALUE_PROP);

    String property = (String) map.get(PROPERTY_PROP);
    if (property.startsWith(OverseerCollectionProcessor.COLL_PROP_PREFIX) == false) {
      property = OverseerCollectionProcessor.COLL_PROP_PREFIX + property;
    }

    boolean uniquePerSlice = Boolean.parseBoolean((String) map.get(SHARD_UNIQUE));

    // Check if we're trying to set a property with parameters that allow us to set the property on multiple replicas
    // in a slice on properties that are known to only be one-per-slice and error out if so.
    if (StringUtils.isNotBlank((String)map.get(SHARD_UNIQUE)) &&
        SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(property.toLowerCase(Locale.ROOT)) &&
        uniquePerSlice == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer replica property command received for property " + property +
              " with the " + SHARD_UNIQUE +
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



  private void handleBalanceShardUnique(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP, PROPERTY_PROP);
    Boolean shardUnique = Boolean.parseBoolean(req.getParams().get(SHARD_UNIQUE));
    String prop = req.getParams().get(PROPERTY_PROP).toLowerCase(Locale.ROOT);
    if (StringUtils.startsWith(prop, OverseerCollectionProcessor.COLL_PROP_PREFIX) == false) {
      prop = OverseerCollectionProcessor.COLL_PROP_PREFIX + prop;
    }

    if (shardUnique == false &&
        SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(prop) == false) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Balancing properties amongst replicas in a slice requires that"
      + " the property be pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'. " +
      " Property: " + prop + " shardUnique: " + Boolean.toString(shardUnique));
    }

    Map<String, Object> map = ZkNodeProps.makeMap(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
    copyIfNotNull(req.getParams(), map, COLLECTION_PROP, PROPERTY_PROP, ONLY_ACTIVE_NODES, SHARD_UNIQUE);

    handleResponse(BALANCESHARDUNIQUE.toLower(), new ZkNodeProps(map), rsp);
  }

  private void handleOverseerStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    Map<String, Object> props = ZkNodeProps.makeMap(
        Overseer.QUEUE_OPERATION, OVERSEERSTATUS.toLower());
    handleResponse(OVERSEERSTATUS.toLower(), new ZkNodeProps(props), rsp);
  }

  private void handleProp(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    req.getParams().required().check(NAME);
    String name = req.getParams().get(NAME);
    String val = req.getParams().get(VALUE_LONG);
    coreContainer.getZkController().getZkStateReader().setClusterProperty(name, val);
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
    String name = req.getParams().required().get(NAME);
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        RELOAD.toLower(), NAME, name);

    handleResponse(RELOAD.toLower(), m, rsp);
  }
  
  private void handleSyncShardAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException, SolrServerException, IOException {
    log.info("Syncing shard : " + req.getParamString());
    String collection = req.getParams().required().get("collection");
    String shard = req.getParams().required().get("shard");
    
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    
    ZkNodeProps leaderProps = clusterState.getLeader(collection, shard);
    ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
    
    ;
    try (HttpSolrClient client = new HttpSolrClient(nodeProps.getBaseUrl())) {
      client.setConnectionTimeout(15000);
      client.setSoTimeout(60000);
      RequestSyncShard reqSyncShard = new CoreAdminRequest.RequestSyncShard();
      reqSyncShard.setCollection(collection);
      reqSyncShard.setShard(shard);
      reqSyncShard.setCoreName(nodeProps.getCoreName());
      client.request(reqSyncShard);
    }
  }
  
  private void handleCreateAliasAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws Exception {
    log.info("Create alias action : " + req.getParamString());
    String name = req.getParams().required().get(NAME);
    String collections = req.getParams().required().get("collections");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        CREATEALIAS.toLower(), NAME, name, "collections",
        collections);
    
    handleResponse(CREATEALIAS.toLower(), m, rsp);
  }
  
  private void handleDeleteAliasAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws Exception {
    log.info("Delete alias action : " + req.getParamString());
    String name = req.getParams().required().get(NAME);
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        DELETEALIAS.toLower(), NAME, name);
    
    handleResponse(DELETEALIAS.toLower(), m, rsp);
  }

  private void handleDeleteAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Deleting Collection : " + req.getParamString());

    String name = req.getParams().required().get(NAME);
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        DELETE.toLower(), NAME, name);

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
    String name = req.getParams().required().get(NAME);
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
        NAME,
        REPLICATION_FACTOR,
         COLL_CONF,
         NUM_SLICES,
         MAX_SHARDS_PER_NODE,
         CREATE_NODE_SET, CREATE_NODE_SET_SHUFFLE,
         SHARDS_PROP,
         ASYNC,
         DocCollection.STATE_FORMAT,
         AUTO_ADD_REPLICAS,
        "router.");
    if(props.get(DocCollection.STATE_FORMAT) == null){
      props.put(DocCollection.STATE_FORMAT,"2");
    }
    addRuleMap(req.getParams(), props, "rule");
    addRuleMap(req.getParams(), props, "snitch");

    if(SYSTEM_COLL.equals(name)){
      //We must always create asystem collection with only a single shard
      props.put(NUM_SLICES,1);
      props.remove(SHARDS_PROP);
      createSysConfigSet();

    }
    copyPropertiesIfNotNull(req.getParams(), props);

    ZkNodeProps m = new ZkNodeProps(props);
    handleResponse(CREATE.toLower(), m, rsp);
  }

  private void addRuleMap(SolrParams params, Map<String, Object> props, String key) {
    String[] rules = params.getParams(key);
    if(rules!= null && rules.length >0){
      ArrayList<Map> l = new ArrayList<>();
      for (String rule : rules) l.add(Rule.parseRule(rule));
      props.put(key, l);
    }
  }

  private void createSysConfigSet() throws KeeperException, InterruptedException {
    SolrZkClient zk = coreContainer.getZkController().getZkStateReader().getZkClient();
    createNodeIfNotExists(zk,ZkStateReader.CONFIGS_ZKNODE, null);
    createNodeIfNotExists(zk,ZkStateReader.CONFIGS_ZKNODE+"/"+SYSTEM_COLL, null);
    createNodeIfNotExists(zk,ZkStateReader.CONFIGS_ZKNODE+"/"+SYSTEM_COLL+"/schema.xml", BlobHandler.SCHEMA.replaceAll("'","\"").getBytes(StandardCharsets.UTF_8));
    createNodeIfNotExists(zk, ZkStateReader.CONFIGS_ZKNODE + "/" + SYSTEM_COLL + "/solrconfig.xml", BlobHandler.CONF.replaceAll("'", "\"").getBytes(StandardCharsets.UTF_8));
  }

  public static void createNodeIfNotExists(SolrZkClient zk, String path, byte[] data) throws KeeperException, InterruptedException {
    if(!zk.exists(path, true)){
      //create the config znode
      try {
        zk.create(path,data, CreateMode.PERSISTENT,true);
      } catch (KeeperException.NodeExistsException e) {
        //no problem . race condition. carry on the good work
      }
    }
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
    if (!ImplicitDocRouter.NAME.equals(((Map) clusterState.getCollection(req.getParams().get(COLLECTION_PROP)).get(DOC_ROUTER)).get(NAME)))
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
    copyPropertiesIfNotNull(req.getParams(), props);
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
    NamedList<Object> results = new NamedList<>();
    Set<String> collections = coreContainer.getZkController().getZkStateReader().getClusterState().getCollections();
    List<String> collectionList = new ArrayList<>();
    for (String collection : collections) {
      collectionList.add(collection);
    }
    results.add("collections", collectionList);
    SolrResponse response = new OverseerSolrResponse(results);

    rsp.getValues().addAll(response.getResponse());
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
  public static final String SYSTEM_COLL =".system";

}
