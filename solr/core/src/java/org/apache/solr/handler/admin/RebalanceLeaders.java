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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.LEADER_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_AT_ONCE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_WAIT_SECONDS_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REBALANCELEADERS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;


/**
 * The end point for the collections API REBALANCELEADERS call that actually does the work.
 * <p>
 * Overview:
 * <p>
 * The leader election process is that each replica of a shard watches one, and only one other replica via
 * ephemeral nodes in ZooKeeper. When the node being watched goes down, the node watching it is sent a notification
 * and, if the node being watched is the leader, the node getting the notification assumes leadership.
 * <p>
 * ZooKeeper's ephemeral nodes get a monotonically increasing "sequence number" that defines it's position in the queue
 * <p>
 * So to force a particular node to become a leader it must have a watch on the leader. This can lead to two nodes
 * having the same sequence number. Say the process is this
 * replica1 is the leader (seq 1)
 * replica3 is on a Solr node that happens to be started next, it watches the leader (seq2)
 * replica2 is on the next Solr node started. It will _also_ watch the leader, it's sequence number is 2 exactly
 * like replica3s
 * <p>
 * This is true on startup, but can also be a consequence of, say, a replica going into recovery. It's no longer
 * eligible to become leader, so will be put at the end of the queue by default. So there's code to put it in the
 * queue with the same sequence number as the current second replica.
 * <p>
 * To compilcate matters further, when the nodes are sorted (see  OverseerTaskProcessor.getSortedElectionNodes)
 * the primary sort is on the sequence number, secondary sort on the session ID. So the preferredLeader may
 * or may not be second in that list.
 * <p>
 * what all this means is that when the REBALANCELEADER command is issued, this class examines the election queue and
 * performs just three things for each shard in the collection:
 * <p>
 * 1> insures that the preferredLeader is watching the leader (rejoins the election queue at the head)
 * <p>
 * 2> if there are two ephemeral nodes with the same sequence number watching the leader, and if one of them is the
 * preferredLeader it will send the _other_ node to the end of the queue (rejoins it)
 * <p>
 * 3> rejoins the zeroth entry in the list at the end of the queue, which triggers the watch on the preferredLeader
 * replica which then takes over leadership
 * <p>
 * All this of course assuming the preferedLeader is alive and well and is assigned for any given shard.
 */

class RebalanceLeaders {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrQueryRequest req;
  final SolrQueryResponse rsp;
  final CollectionsHandler collectionsHandler;
  final CoreContainer coreContainer;
  private final Set<String> asyncRequests = new HashSet<>();
  final static String INACTIVE_PREFERREDS = "inactivePreferreds";
  final static String ALREADY_LEADERS = "alreadyLeaders";
  final static String SUMMARY = "Summary";
  @SuppressWarnings({"rawtypes"})
  final SimpleOrderedMap results = new SimpleOrderedMap();
  final Map<String, String> pendingOps = new HashMap<>();
  private String collectionName;


  RebalanceLeaders(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler collectionsHandler) {
    this.req = req;
    this.rsp = rsp;
    this.collectionsHandler = collectionsHandler;
    coreContainer = collectionsHandler.getCoreContainer();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  void execute() throws KeeperException, InterruptedException {
    DocCollection dc = checkParams();


    int max = req.getParams().getInt(MAX_AT_ONCE_PROP, Integer.MAX_VALUE);
    if (max <= 0) max = Integer.MAX_VALUE;
    int maxWaitSecs = req.getParams().getInt(MAX_WAIT_SECONDS_PROP, 60);


    // If there are a maximum number of simultaneous requests specified, we have to pause when we have that many
    // outstanding requests and wait for at least one to finish before going on the the next rebalance.
    boolean keepGoing = true;
    for (Slice slice : dc.getSlices()) {
      ensurePreferredIsLeader(slice);
      if (asyncRequests.size() == max) {
        log.info("Queued {} leader reassignments, waiting for some to complete.", max);
        keepGoing = waitAsyncRequests(maxWaitSecs, false);
        if (keepGoing == false) {
          break; // If we've waited longer than specified, don't continue to wait!
        }
      }
    }
    if (keepGoing == true) {
      keepGoing = waitAsyncRequests(maxWaitSecs, true);
    }
    if (keepGoing == true) {
      log.info("All leader reassignments completed.");
    } else {
      log.warn("Exceeded specified timeout of '{}' all leaders may not have been reassigned'", maxWaitSecs);
    }

    checkLeaderStatus();
    SimpleOrderedMap summary = new SimpleOrderedMap();
    if (pendingOps.size() == 0) {
      summary.add("Success", "All active replicas with the preferredLeader property set are leaders");
    } else {
      summary.add("Failure", "Not all active replicas with preferredLeader property are leaders");
    }
    rsp.getValues().add(SUMMARY, summary); // we want this first.

    rsp.getValues().addAll(results);
  }

  // Insure that ll required parameters are there and the doc colection exists.
  private DocCollection checkParams() throws KeeperException, InterruptedException {
    req.getParams().required().check(COLLECTION_PROP);

    collectionName = req.getParams().get(COLLECTION_PROP);
    if (StringUtils.isBlank(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT, "The " + COLLECTION_PROP + " is required for the Rebalance Leaders command."));
    }
    coreContainer.getZkController().getZkStateReader().forceUpdateCollection(collectionName);
    ClusterState clusterState = coreContainer.getZkController().getClusterState();

    DocCollection dc = clusterState.getCollection(collectionName);
    if (dc == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
    }
    return dc;
  }

  // Once we've done all the fiddling with the queues, check on the way out to see if all the active preferred
  // leaders that we intended to change are in fact the leaders.
  private void checkLeaderStatus() throws InterruptedException, KeeperException {
    for (int idx = 0; pendingOps.size() > 0 && idx < 600; ++idx) {
      ClusterState clusterState = coreContainer.getZkController().getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();
      DocCollection dc = clusterState.getCollection(collectionName);
      for (Slice slice : dc.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (replica.isActive(liveNodes) && replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false)) {
            if (replica.getBool(LEADER_PROP, false)) {
              if (pendingOps.containsKey(slice.getName())) {
                // Record for return that the leader changed successfully
                pendingOps.remove(slice.getName());
                addToSuccesses(slice, replica);
                break;
              }
            }
          }
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
      coreContainer.getZkController().getZkStateReader().forciblyRefreshAllClusterStateSlow();
    }
    addAnyFailures();
  }

  // The process is:
  // if the replica with preferredLeader is already the leader, do nothing
  // Otherwise:
  // > if two nodes have the same sequence number and both point to the current leader, we presume that we've just
  //   moved it, move the one that does _not_ have the preferredLeader to the end of the list.
  // > move the current leader to the end of the list. This _should_ mean that the current ephemeral node in the
  //   leader election queue is removed and the only remaining node watching it is triggered to become leader.
  private void ensurePreferredIsLeader(Slice slice) throws KeeperException, InterruptedException {
    for (Replica replica : slice.getReplicas()) {
      // Tell the replica to become the leader if we're the preferred leader AND active AND not the leader already
      if (replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false) == false) {
        continue;
      }
      // OK, we are the preferred leader, are we the actual leader?
      if (replica.getBool(LEADER_PROP, false)) {
        //We're a preferred leader, but we're _also_ the leader, don't need to do anything.
        addAlreadyLeaderToResults(slice, replica);
        return; // already the leader, do nothing.
      }
      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      // We're the preferred leader, but someone else is leader. Only become leader if we're active.
      if (replica.isActive(zkStateReader.getClusterState().getLiveNodes()) == false) {
        addInactiveToResults(slice, replica);
        return; // Don't try to become the leader if we're not active!
      }

      List<String> electionNodes = OverseerTaskProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
          ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

      if (electionQueueInBadState(electionNodes, slice, replica)) {
        return;
      }

      // Replica is the preferred leader but not the actual leader, do something about that.
      // "Something" is
      // 1> if the preferred leader isn't first in line, tell it to re-queue itself.
      // 2> tell the actual leader to re-queue itself.

      // Ok, the sorting for election nodes is a bit strange. If the sequence numbers are the same, then the whole
      // string is used, but that sorts nodes with the same sequence number by their session IDs from ZK.
      // While this is determinate, it's not quite what we need, so re-queue nodes that aren't us and are
      // watching the leader node..


      String firstWatcher = electionNodes.get(1);

      if (LeaderElector.getNodeName(firstWatcher).equals(replica.getName()) == false) {
        makeReplicaFirstWatcher(slice, replica);
      }

      // This replica should be the leader at the end of the day, so let's record that information to check at the end
      pendingOps.put(slice.getName(), replica.getName());
      String leaderElectionNode = electionNodes.get(0);
      String coreName = slice.getReplica(LeaderElector.getNodeName(leaderElectionNode)).getStr(CORE_NAME_PROP);
      rejoinElectionQueue(slice, leaderElectionNode, coreName, false);
      waitForNodeChange(slice, leaderElectionNode);

      return; // Done with this slice, skip the rest of the replicas.
    }
  }

  // Check that the election queue has some members! There really should be two or more for this to make any sense,
  // if there's only one we can't change anything.
  private boolean electionQueueInBadState(List<String> electionNodes, Slice slice, Replica replica) {
    if (electionNodes.size() < 2) { // if there's only one node in the queue, should already be leader and we shouldn't be here anyway.
      log.warn("Rebalancing leaders and slice {} has less than two elements in the leader election queue, but replica {} doesn't think it's the leader."
          , slice.getName(), replica.getName());
      return true;
    }

    return false;
  }

  // Provide some feedback to the user about what actually happened, or in this case where no action was
  // possible
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addInactiveToResults(Slice slice, Replica replica) {
    SimpleOrderedMap inactives = (SimpleOrderedMap) results.get(INACTIVE_PREFERREDS);
    if (inactives == null) {
      inactives = new SimpleOrderedMap();
      results.add(INACTIVE_PREFERREDS, inactives);
    }
    SimpleOrderedMap res = new SimpleOrderedMap();
    res.add("status", "skipped");
    res.add("msg", "Replica " + replica.getName() + " is a referredLeader for shard " + slice.getName() + ", but is inactive. No change necessary");
    inactives.add(replica.getName(), res);
  }

  // Provide some feedback to the user about what actually happened, or in this case where no action was
  // necesary since this preferred replica was already the leader
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addAlreadyLeaderToResults(Slice slice, Replica replica) {
    SimpleOrderedMap alreadyLeaders = (SimpleOrderedMap) results.get(ALREADY_LEADERS);
    if (alreadyLeaders == null) {
      alreadyLeaders = new SimpleOrderedMap();
      results.add(ALREADY_LEADERS, alreadyLeaders);
    }
    SimpleOrderedMap res = new SimpleOrderedMap();
    res.add("status", "skipped");
    res.add("msg", "Replica " + replica.getName() + " is already the leader for shard " + slice.getName() + ". No change necessary");
    alreadyLeaders.add(replica.getName(), res);
  }

  // Put the replica in at the head of the queue and send all nodes with the same sequence number to the back of the list
  // There can be "ties", i.e. replicas in the queue with the same sequence number. Sorting doesn't necessarily sort
  // the one we most care about first. So put the node we _don't care about at the end of the election queue_

  void makeReplicaFirstWatcher(Slice slice, Replica replica)
      throws KeeperException, InterruptedException {

    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    List<String> electionNodes = OverseerTaskProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
        ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

    // First, queue up the preferred leader watching the leader if it isn't already
    int secondSeq = Integer.MAX_VALUE;

    int candidateSeq = -1;
    for (int idx = 1; idx < electionNodes.size(); ++idx) {
      String candidate = electionNodes.get(idx);
      secondSeq = Math.min(secondSeq, LeaderElector.getSeq(candidate));
      if (LeaderElector.getNodeName(candidate).equals(replica.getName())) {
        candidateSeq = LeaderElector.getSeq(candidate);
      }
    }
    int newSeq = -1;
    if (candidateSeq == secondSeq) {
      // the preferredLeader is already watching the leader, no need to move it around.
      newSeq = secondSeq;
    } else {
      for (String electionNode : electionNodes) {
        if (LeaderElector.getNodeName(electionNode).equals(replica.getName())) {
          // Make the preferred leader watch the leader.
          String coreName = slice.getReplica(LeaderElector.getNodeName(electionNode)).getStr(CORE_NAME_PROP);
          rejoinElectionQueue(slice, electionNode, coreName, true);
          newSeq = waitForNodeChange(slice, electionNode);
          break;
        }
      }
    }
    if (newSeq == -1) {
      return; // let's not continue if we didn't get what we expect. Possibly we're offline etc..
    }

    // Now find other nodes that have the same sequence number as this node and re-queue them at the end of the queue.
    electionNodes = OverseerTaskProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
        ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));

    for (String thisNode : electionNodes) {
      if (LeaderElector.getSeq(thisNode) > newSeq) {
        break;
      }
      if (LeaderElector.getNodeName(thisNode).equals(replica.getName())) {
        continue;
      }
      // We won't get here for the preferredLeader node
      if (LeaderElector.getSeq(thisNode) == newSeq) {
        String coreName = slice.getReplica(LeaderElector.getNodeName(thisNode)).getStr(CORE_NAME_PROP);
        rejoinElectionQueue(slice, thisNode, coreName, false);
        waitForNodeChange(slice, thisNode);
      }
    }
  }

  // We're just waiting for the electionNode to rejoin the queue with a _different_ node, indicating that any
  // requeueing we've done has happened.
  int waitForNodeChange(Slice slice, String electionNode) throws InterruptedException, KeeperException {
    String nodeName = LeaderElector.getNodeName(electionNode);
    int oldSeq = LeaderElector.getSeq(electionNode);
    for (int idx = 0; idx < 600; ++idx) {

      ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
      List<String> electionNodes = OverseerTaskProcessor.getSortedElectionNodes(zkStateReader.getZkClient(),
          ZkStateReader.getShardLeadersElectPath(collectionName, slice.getName()));
      for (String testNode : electionNodes) {
        if (LeaderElector.getNodeName(testNode).equals(nodeName) && oldSeq != LeaderElector.getSeq(testNode)) {
          return LeaderElector.getSeq(testNode);
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
      zkStateReader.forciblyRefreshAllClusterStateSlow();
    }
    return -1;
  }

  // Move an election node to some other place in the queue. If rejoinAtHead==false, then at the end, otherwise
  // the new node should point at the leader.
  private void rejoinElectionQueue(Slice slice, String electionNode, String core, boolean rejoinAtHead)
      throws KeeperException, InterruptedException {
    Replica replica = slice.getReplica(LeaderElector.getNodeName(electionNode));
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(COLLECTION_PROP, collectionName);
    propMap.put(SHARD_ID_PROP, slice.getName());
    propMap.put(QUEUE_OPERATION, REBALANCELEADERS.toLower());
    propMap.put(CORE_NAME_PROP, core);
    propMap.put(CORE_NODE_NAME_PROP, replica.getName());
    propMap.put(ZkStateReader.NODE_NAME_PROP, replica.getNodeName());
    propMap.put(ZkStateReader.BASE_URL_PROP, coreContainer.getZkController().getZkStateReader().getBaseUrlForNodeName(replica.getNodeName()));
    propMap.put(REJOIN_AT_HEAD_PROP, Boolean.toString(rejoinAtHead)); // Get ourselves to be first in line.
    propMap.put(ELECTION_NODE_PROP, electionNode);
    String asyncId = REBALANCELEADERS.toLower() + "_" + core + "_" + Math.abs(System.nanoTime());
    propMap.put(ASYNC, asyncId);
    asyncRequests.add(asyncId);

    collectionsHandler.sendToOCPQueue(new ZkNodeProps(propMap)); // ignore response; we construct our own
  }

  // maxWaitSecs - How long are we going to wait? Defaults to 30 seconds.
  // waitForAll - if true, do not return until all requests have been processed. "Processed" could mean failure!
  //

  private boolean waitAsyncRequests(final int maxWaitSecs, Boolean waitForAll)
      throws KeeperException, InterruptedException {

    if (asyncRequests.size() == 0) {
      return true;
    }

    for (int idx = 0; idx < maxWaitSecs * 10; ++idx) {
      Iterator<String> iter = asyncRequests.iterator();
      boolean foundChange = false;
      while (iter.hasNext()) {
        String asyncId = iter.next();
        if (coreContainer.getZkController().getOverseerFailureMap().contains(asyncId)) {
          coreContainer.getZkController().getOverseerFailureMap().remove(asyncId);
          coreContainer.getZkController().clearAsyncId(asyncId);
          iter.remove();
          foundChange = true;
        } else if (coreContainer.getZkController().getOverseerCompletedMap().contains(asyncId)) {
          coreContainer.getZkController().getOverseerCompletedMap().remove(asyncId);
          coreContainer.getZkController().clearAsyncId(asyncId);
          iter.remove();
          foundChange = true;
        }
      }
      // We're done if we're processing a few at a time or all requests are processed.
      // We don't want to change, say, 100s of leaders simultaneously. So if the request specifies some limit,
      // and we're at that limit, we want to return to the caller so it can immediately add another request.
      // That's the purpose of the first clause here. Otherwise, of course, just return if all requests are
      // processed.
      if ((foundChange && waitForAll == false) || asyncRequests.size() == 0) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
    // If we get here, we've timed out waiting.
    return false;
  }

  // If we actually changed the leader, we should send that fact back in the response.
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addToSuccesses(Slice slice, Replica replica) {
    SimpleOrderedMap successes = (SimpleOrderedMap) results.get("successes");
    if (successes == null) {
      successes = new SimpleOrderedMap();
      results.add("successes", successes);
    }
    if (log.isInfoEnabled()) {
      log.info("Successfully changed leader of shard {} to replica {}", slice.getName(), replica.getName());
    }
    SimpleOrderedMap res = new SimpleOrderedMap();
    res.add("status", "success");
    res.add("msg", "Successfully changed leader of slice " + slice.getName() + " to " + replica.getName());
    successes.add(slice.getName(), res);
  }

  // If for any reason we were supposed to change leadership, that should be recorded in changingLeaders. Any
  // time we verified that the change actually occurred, that entry should have been removed. So report anything
  // left over as a failure.
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void addAnyFailures() {
    if (pendingOps.size() == 0) {
      return;
    }
    SimpleOrderedMap fails = new SimpleOrderedMap();
    results.add("failures", fails);

    for (Map.Entry<String, String> ent : pendingOps.entrySet()) {
      if (log.isInfoEnabled()) {
        log.info("Failed to change leader of shard {} to replica {}", ent.getKey(), ent.getValue());
      }
      SimpleOrderedMap res = new SimpleOrderedMap();
      res.add("status", "failed");
      res.add("msg", String.format(Locale.ROOT, "Could not change leder for slice %s to %s", ent.getKey(), ent.getValue()));
      fails.add(ent.getKey(), res);

    }
  }
}
