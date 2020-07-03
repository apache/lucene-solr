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
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer cc;
  private final SyncStrategy syncStrategy;

  private volatile boolean isClosed = false;

  public ShardLeaderElectionContext(LeaderElector leaderElector,
                                    final String shardId, final String collection,
                                    final String coreNodeName, ZkNodeProps props, ZkController zkController, CoreContainer cc) {
    super(leaderElector, shardId, collection, coreNodeName, props,
        zkController);
    this.cc = cc;
    syncStrategy = new SyncStrategy(cc);
  }

  @Override
  public void close() {
    super.close();
    this.isClosed = true;
    syncStrategy.close();
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    try (SolrCore core = cc.getCore(coreName)) {
      if (core != null) {
        core.getCoreDescriptor().getCloudDescriptor().setLeader(false);
      }
    }

    super.cancelElection();
  }

  @Override
  public ElectionContext copy() {
    return new ShardLeaderElectionContext(leaderElector, shardId, collection, id, leaderProps, zkController, cc);
  }

  /*
   * weAreReplacement: has someone else been the leader already?
   */
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStart) throws KeeperException,
      InterruptedException, IOException {
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    ActionThrottle lt;
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        // shutdown or removed
        return;
      }
      MDCLoggingContext.setCore(core);
      lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();
    }

    try {
      lt.minimumWaitBetweenActions();
      lt.markAttemptingAction();


      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

      log.info("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId, weAreReplacement, leaderVoteWait);

      if (isClosed) {
        // Solr is shutting down or the ZooKeeper session expired while waiting for replicas. If the later,
        // we cannot be sure we are still the leader, so we should bail out. The OnReconnect handler will
        // re-register the cores and handle a new leadership election.
        return;
      }

      Replica.Type replicaType;
      String coreNodeName;
      boolean setTermToMax = false;
      try (SolrCore core = cc.getCore(coreName)) {

        if (core == null) {
          return;
        }

        replicaType = core.getCoreDescriptor().getCloudDescriptor().getReplicaType();
        coreNodeName = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
        // should I be leader?
        ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
        if (zkShardTerms.registered(coreNodeName) && !zkShardTerms.canBecomeLeader(coreNodeName)) {
          if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, coreNodeName, leaderVoteWait)) {
            rejoinLeaderElection(core);
            return;
          } else {
            // only log an error if this replica win the election
            setTermToMax = true;
          }
        }

        if (isClosed) {
          return;
        }

        log.info("I may be the new leader - try and sync");

        // we are going to attempt to be the leader
        // first cancel any current recovery
        core.getUpdateHandler().getSolrCoreState().cancelRecovery();

        PeerSync.PeerSyncResult result = null;
        boolean success = false;
        try {
          result = syncStrategy.sync(zkController, core, leaderProps, weAreReplacement);
          success = result.isSuccess();
        } catch (Exception e) {
          SolrException.log(log, "Exception while trying to sync", e);
          result = PeerSync.PeerSyncResult.failure();
        }

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

        if (!success) {
          boolean hasRecentUpdates = false;
          if (ulog != null) {
            // TODO: we could optimize this if necessary
            try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
              hasRecentUpdates = !recentUpdates.getVersions(1).isEmpty();
            }
          }

          if (!hasRecentUpdates) {
            // we failed sync, but we have no versions - we can't sync in that case
            // - we were active
            // before, so become leader anyway if no one else has any versions either
            if (result.getOtherHasVersions().orElse(false)) {
              log.info("We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader");
              success = false;
            } else {
              log.info(
                  "We failed sync, but we have no versions - we can't sync in that case - we were active before, so become leader anyway");
              success = true;
            }
          }
        }

        // solrcloud_debug
        if (log.isDebugEnabled()) {
          try {
            RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
            SolrIndexSearcher searcher = searchHolder.get();
            try {
              if (log.isDebugEnabled()) {
                log.debug("{} synched {}", core.getCoreContainer().getZkController().getNodeName()
                    , searcher.count(new MatchAllDocsQuery()));
              }
            } finally {
              searchHolder.decref();
            }
          } catch (Exception e) {
            log.error("Error in solrcloud_debug block", e);
          }
        }
        if (!success) {
          rejoinLeaderElection(core);
          return;
        }

      }

      boolean isLeader = true;
      if (!isClosed) {
        try {
          if (replicaType == Replica.Type.TLOG) {
            // stop replicate from old leader
            zkController.stopReplicationFromLeader(coreName);
            if (weAreReplacement) {
              try (SolrCore core = cc.getCore(coreName)) {
                Future<UpdateLog.RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().recoverFromCurrentLog();
                if (future != null) {
                  log.info("Replaying tlog before become new leader");
                  future.get();
                } else {
                  log.info("New leader does not have old tlog to replay");
                }
              }
            }
          }
          // in case of leaderVoteWait timeout, a replica with lower term can win the election
          if (setTermToMax) {
            log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) {}"
                , "without being up-to-date with the previous leader", coreNodeName);
            zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreNodeName);
          }
          super.runLeaderProcess(weAreReplacement, 0);


          assert shardId != null;

          ZkNodeProps zkNodes = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
                  ZkStateReader.SHARD_ID_PROP, shardId,
                  ZkStateReader.COLLECTION_PROP, collection,
                  ZkStateReader.BASE_URL_PROP, leaderProps.get(ZkStateReader.BASE_URL_PROP),
                  ZkStateReader.NODE_NAME_PROP, leaderProps.get(ZkStateReader.NODE_NAME_PROP),
                  ZkStateReader.CORE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NAME_PROP),
                  ZkStateReader.CORE_NODE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NODE_NAME_PROP),
                  ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
          assert zkController != null;
          assert zkController.getOverseer() != null;
          zkController.getOverseer().offerStateUpdate(Utils.toJSON(zkNodes));

          try (SolrCore core = cc.getCore(coreName)) {
            if (core != null) {
              core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
              publishActiveIfRegisteredAndNotActive(core);
            } else {
              log.info("No SolrCore found, will not become leader: {} {}", ZkCoreNodeProps.getCoreUrl(leaderProps), shardId);
              return;
            }
          }
          if (log.isInfoEnabled()) {
            log.info("I am the new leader: {} {}", ZkCoreNodeProps.getCoreUrl(leaderProps), shardId);
          }

          // we made it as leader - send any recovery requests we need to
          syncStrategy.requestRecoveries();

        } catch (SessionExpiredException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "ZK session expired - cancelling election for " + collection + " " + shardId);
        } catch (Exception e) {
          isLeader = false;
          SolrException.log(log, "There was a problem trying to register as the leader", e);

          try (SolrCore core = cc.getCore(coreName)) {

            if (core == null) {
              if (log.isDebugEnabled()) {
                log.debug("SolrCore not found: {} in {}", coreName, cc.getLoadedCoreNames());
              }
              return;
            }

            core.getCoreDescriptor().getCloudDescriptor().setLeader(false);

            // we could not publish ourselves as leader - try and rejoin election
            try {
              rejoinLeaderElection(core);
            } catch (SessionExpiredException exc) {
              throw new SolrException(ErrorCode.SERVER_ERROR,
                  "ZK session expired - cancelling election for " + collection + " " + shardId);
            }
          }
        }
      } else {
        cancelElection();
      }
    } finally {
      MDCLoggingContext.clear();
    }
  }

  /**
   * Wait for other replicas with higher terms participate in the electioon
   *
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, String coreNodeName, int timeout) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (!isClosed && !cc.isShutDown()) {
      if (System.nanoTime() > timeoutAt) {
        log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (core_term:{}, highest_term:{})",
            timeout, coreNodeName, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
        return true;
      }
      if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
        log.info("Can't become leader, other replicas with higher term participated in leader election");
        return false;
      }
      Thread.sleep(500L);
    }
    return false;
  }

  /**
   * Do other replicas with higher term participated in the election
   *
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreNodeName) {
    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) return false;

    long replicaTerm = zkShardTerms.getTerm(coreNodeName);
    boolean isRecovering = zkShardTerms.isRecovering(coreNodeName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreNodeName)) continue;

      if (clusterState.getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());

        if (isRecovering && !isOtherReplicaRecovering) return true;
        if (otherTerm > replicaTerm) return true;
      }
    }
    return false;
  }

  public void publishActiveIfRegisteredAndNotActive(SolrCore core) throws Exception {
    if (log.isDebugEnabled()) log.debug("We have become the leader after core registration but are not in an ACTIVE state - publishing ACTIVE");
    zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
  }

  private Replica getReplica(ClusterState clusterState, String collectionName, String replicaName) {
    if (clusterState == null) return null;
    final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
    if (docCollection == null) return null;
    return docCollection.getReplica(replicaName);
  }

  private void rejoinLeaderElection(SolrCore core)
      throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election
    if (cc.isShutDown()) {
      log.debug("Not rejoining election because CoreContainer is closed");
      return;
    }

    log.info("There may be a better leader candidate than us - going back into recovery");

    cancelElection();

    core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());

    leaderElector.joinElection(this, true);
  }

}
