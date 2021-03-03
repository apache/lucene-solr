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

import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer cc;
  private final SyncStrategy syncStrategy;

  protected final String shardId;

  protected final String collection;
  protected final LeaderElector leaderElector;


  private final ZkController zkController;

  public ShardLeaderElectionContext(LeaderElector leaderElector,
                                    final String shardId, final String collection,
                                    Replica props, ZkController zkController, CoreContainer cc, CoreDescriptor cd) {
    super(cd.getName(), ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                    + "/leader_elect/" + shardId,  ZkStateReader.getShardLeadersPath(
            collection, shardId), props, cd,
            zkController.getZkClient());
    this.cc = cc;
    this.syncStrategy = new SyncStrategy(cc);
    this.shardId = shardId;
    this.leaderElector = leaderElector;
    this.zkController = zkController;
    this.collection = collection;
  }

  @Override
  protected void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
  }

  @Override
  public ElectionContext copy() {
    return new ShardLeaderElectionContext(leaderElector, shardId, collection, leaderProps, zkController, cc, cd);
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }

  /*
   * weAreReplacement: has someone else been the leader already?
   */
  @Override
  boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStart) throws KeeperException,
          InterruptedException, IOException {

    String coreName = leaderProps.getName();

    log.info("Run leader process for shard [{}] election, first step is to try and sync with the shard", context.leaderProps.getSlice());
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        log.info("No SolrCore found, cannot become leader {}", coreName);
        throw new AlreadyClosedException("No SolrCore found, cannot become leader " + coreName);
      }
//      if (core.isClosing() || core.getCoreContainer().isShutDown()) {
//        log.info("We are closed, will not become leader");
//        closed = true;
//        cancelElection();
//        return false;
//      }

        core.getSolrCoreState().cancelRecovery(false, false);

        ActionThrottle lt;

        lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();

        lt.minimumWaitBetweenActions();
        lt.markAttemptingAction();

        int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

        if (log.isDebugEnabled()) {
          log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId, weAreReplacement, leaderVoteWait);
        }

        if (core.getUpdateHandler().getUpdateLog() == null) {
          log.error("No UpdateLog found - cannot sync");
          throw new SolrException(ErrorCode.SERVER_ERROR, "Replica with no update log configured cannot be leader");
        }

        Replica.Type replicaType;
        boolean setTermToMax = false;

        CoreDescriptor cd = core.getCoreDescriptor();
        CloudDescriptor cloudCd = cd.getCloudDescriptor();
        replicaType = cloudCd.getReplicaType();
        // should I be leader?

        if (log.isDebugEnabled()) {
          log.debug("Check zkShardTerms");
        }
        ZkShardTerms zkShardTerms = zkController.getShardTermsOrNull(collection, shardId);
        try {
          if (zkShardTerms != null) {
            //  guarantees that a live-fetch will be enough for this core to see max term published
            if (log.isDebugEnabled()) log.debug("refresh shard terms for core {}", coreName);
            zkShardTerms.refreshTerms(false, -1);
          }
        } catch (Exception e) {
          log.error("Exception while looking at refreshing shard terms", e);
        }

        if (zkShardTerms != null && zkShardTerms.registered(coreName) && !zkShardTerms.canBecomeLeader(coreName)) {
          boolean onlyLiveReplica = true;
          DocCollection coll = zkController.zkStateReader.getClusterState().getCollectionOrNull(collection);

          if (coll != null) {
            Slice slice = coll.getSlice(shardId);
            if (slice != null) {
              for (Replica replica : slice) {
                if (replica.getName().equals(coreName)) {
                  continue;
                }
                if (zkController.zkStateReader.isNodeLive(replica.getNodeName())) {
                  onlyLiveReplica = false;
                  break;
                }
              }
            }
          }

          if (!onlyLiveReplica) {
            if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, coreName, leaderVoteWait)) {
              rejoinLeaderElection(core);
              return false;
            }
          }

          // only log an error if this replica win the election
          setTermToMax = true;
        }

        log.info("I may be the new leader - try and sync");

        PeerSync.PeerSyncResult result = null;
        boolean success = false;
//        if (core.getCoreContainer().isShutDown()) {
//          return false;
//        }
        result = syncStrategy.sync(zkController, core, leaderProps, weAreReplacement);
        log.info("Sync strategy sync result {}", result);
        success = result.isSuccess();

        if (!success) {
          boolean hasRecentUpdates = false;

          UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
          if (ulog != null) {
            // TODO: we could optimize this if necessary
            try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
              hasRecentUpdates = recentUpdates != null && !recentUpdates.getVersions(1).isEmpty();
            }
          }

          log.warn("Checking for recent versions in the update log", hasRecentUpdates);
          if (!hasRecentUpdates) {
            // we failed sync, but we have no versions - we can't sync in that case
            // - we were active
            // before, so become leader anyway if no one else has any versions either
            if (result.getOtherHasVersions().orElse(false)) {
              log.info("We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader {}", coreName);
              rejoinLeaderElection(core);
              return false;
            } else {
              log.info("We failed sync, but we have no versions - we can't sync in that case - we did not find versions on other replicas, so become leader anyway {}", coreName);
              success = true;
            }
          }
        }
        // solrcloud_debug
//        if (log.isDebugEnabled()) {
//          try {
//            RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//            SolrIndexSearcher searcher = searchHolder.get();
//            try {
//              log.debug(core.getCoreContainer().getZkController().getNodeName() + " synched " + searcher.count(new MatchAllDocsQuery()));
//            } finally {
//              searchHolder.decref();
//            }
//          } catch (Exception e) {
//            ParWork.propagateInterrupt(e);
//            throw new SolrException(ErrorCode.SERVER_ERROR, e);
//          }
//        }
        if (!success) {
          log.info("Sync with potential leader failed, rejoining election {} ...", coreName);
          rejoinLeaderElection(core);
          return false;
        }

        if (replicaType == Replica.Type.TLOG) {
          // stop replicate from old leader
          zkController.stopReplicationFromLeader(coreName);
          if (weAreReplacement) {

            Future<UpdateLog.RecoveryInfo> future = core.getUpdateHandler().getUpdateLog().recoverFromCurrentLog();
            if (future != null) {
              log.info("Replaying tlog before become new leader");
              future.get();
            } else {
              log.info("New leader does not have old tlog to replay");
            }

          }
        }
        // in case of leaderVoteWait timeout, a replica with lower term can win the election
        if (setTermToMax) {
          log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " + "without being up-to-date with the previous leader", coreName);
          try {
            zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreName);
          } catch (Exception e) {
            log.error("Exception trying to set shard terms equal to leader", e);
          }
        }

        boolean leaderSuccess = super.runLeaderProcess(context, weAreReplacement, 0);
        if (!leaderSuccess) {
          return false;
        }

        ZkNodeProps zkNodes = ZkNodeProps
            .fromKeyVals(StatePublisher.OPERATION, OverseerAction.STATE.toLower(), ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.CORE_NAME_PROP, leaderProps.getName(),
                "id", cd.getCoreProperties().get("collId") + "-" + cd.getCoreProperties().get("id"),
                ZkStateReader.STATE_PROP, "leader");

        log.info("I am the new leader, publishing as active: " + leaderProps.getCoreUrl() + " " + shardId);

        zkController.publish(zkNodes);

      } catch (AlreadyClosedException e) {
        log.info("Already closed, bailing..");
        cancelElection();
        if (e instanceof  RuntimeException) {
          throw e;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted, bailing..");
        ParWork.propagateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (Exception e) {
        SolrException.log(log, "There was a problem trying to register as the leader", e);

        cancelElection();
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }

    return true;
  }

  /**
   * Wait for other replicas with higher terms participate in the electioon
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, String coreNodeName, int timeout) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (true) {
      if (System.nanoTime() > timeoutAt) {
        log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (core_term:{}, highest_term:{})",
            timeout, coreNodeName, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
        return true;
      }
      if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
        log.info("Can't become leader, other replicas with higher term participated in leader election {}", zkShardTerms);
        return false;
      }

      // TODO: if we know eveyrone has already particpated, we should bail early...
      
      Thread.sleep(50L);
    }
  }

  /**
   * Do other replicas with higher term participated in the election
   *
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreName) throws InterruptedException {
    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) return false;

    long replicaTerm = zkShardTerms.getTerm(coreName);
    boolean isRecovering = zkShardTerms.isRecovering(coreName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreName)) continue;
      if (zkController.getZkStateReader().getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());
        if (isRecovering && !isOtherReplicaRecovering) return true;
        if (otherTerm > replicaTerm) return true;
      }
    }
    return false;
  }

  private void rejoinLeaderElection(SolrCore core)
          throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election

    log.info("There may be a better leader candidate than us - will cancel election, rejoin election, and kick off recovery {}", core.getName());

    leaderElector.retryElection(false);

    core.getSolrCoreState().doRecovery(core);
  }

  public String getShardId() {
    return shardId;
  }

  public String getCollection() {
    return collection;
  }

}

