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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.LEADER_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_AT_ONCE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_WAIT_SECONDS_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REBALANCELEADERS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

class RebalanceLeaders {
  final SolrQueryRequest req;
  final SolrQueryResponse rsp;
  final CollectionsHandler collectionsHandler;
  final CoreContainer coreContainer;

  RebalanceLeaders(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler collectionsHandler) {
    this.req = req;
    this.rsp = rsp;
    this.collectionsHandler = collectionsHandler;
    coreContainer = collectionsHandler.getCoreContainer();
  }

  void execute() throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP);

    String collectionName = req.getParams().get(COLLECTION_PROP);
    if (StringUtils.isBlank(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "The " + COLLECTION_PROP + " is required for the REASSIGNLEADERS command."));
    }
    coreContainer.getZkController().getZkStateReader().updateClusterState(true);
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    DocCollection dc = clusterState.getCollection(collectionName);
    if (dc == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
    }
    Map<String, String> currentRequests = new HashMap<>();
    int max = req.getParams().getInt(MAX_AT_ONCE_PROP, Integer.MAX_VALUE);
    if (max <= 0) max = Integer.MAX_VALUE;
    int maxWaitSecs = req.getParams().getInt(MAX_WAIT_SECONDS_PROP, 60);
    NamedList<Object> results = new NamedList<>();

    boolean keepGoing = true;
    for (Slice slice : dc.getSlices()) {
      insurePreferredIsLeader(results, slice, currentRequests);
      if (currentRequests.size() == max) {
        CollectionsHandler.log.info("Queued " + max + " leader reassignments, waiting for some to complete.");
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
      CollectionsHandler.log.info("All leader reassignments completed.");
    } else {
      CollectionsHandler.log.warn("Exceeded specified timeout of ." + maxWaitSecs + "' all leaders may not have been reassigned");
    }

    rsp.getValues().addAll(results);
  }

  private void insurePreferredIsLeader(NamedList<Object> results,
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
        CollectionsHandler.log.warn("Rebalancing leaders and slice " + slice.getName() + " has less than two elements in the leader " +
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
    propMap.put(QUEUE_OPERATION, REBALANCELEADERS.toLower());
    propMap.put(CORE_NAME_PROP, core);
    propMap.put(NODE_NAME_PROP, replica.getName());
    propMap.put(ZkStateReader.BASE_URL_PROP, replica.getProperties().get(ZkStateReader.BASE_URL_PROP));
    propMap.put(REJOIN_AT_HEAD_PROP, Boolean.toString(rejoinAtHead)); // Get ourselves to be first in line.
    propMap.put(ELECTION_NODE_PROP, electionNode);
    String asyncId = REBALANCELEADERS.toLower() + "_" + core + "_" + Math.abs(System.nanoTime());
    propMap.put(ASYNC, asyncId);
    ZkNodeProps m = new ZkNodeProps(propMap);
    SolrQueryResponse rspIgnore = new SolrQueryResponse(); // I'm constructing my own response
    collectionsHandler.handleResponse(REBALANCELEADERS.toLower(), m, rspIgnore); // Want to construct my own response here.
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


}
