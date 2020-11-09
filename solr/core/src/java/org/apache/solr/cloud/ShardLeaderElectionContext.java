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

import net.sf.saxon.trans.Err;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
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

  protected final String shardId;

  protected final String collection;
  protected final LeaderElector leaderElector;

  private volatile boolean isClosed = false;

  private final ZkController zkController;

  public ShardLeaderElectionContext(LeaderElector leaderElector,
                                    final String shardId, final String collection,
                                    final String coreNodeName, Replica props, ZkController zkController, CoreContainer cc) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                    + "/leader_elect/" + shardId,  ZkStateReader.getShardLeadersPath(
            collection, shardId), props,
            zkController.getZkClient());
    assert ObjectReleaseTracker.track(this);
    this.cc = cc;
    this.syncStrategy = new SyncStrategy(cc);
    this.shardId = shardId;
    this.leaderElector = leaderElector;
    this.zkController = zkController;
    this.collection = collection;
  }

  @Override
  public void close() {
    super.close();
    IOUtils.closeQuietly(syncStrategy);
    this.isClosed = true;
    assert ObjectReleaseTracker.release(this);
  }

  @Override
  protected void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    try {
      try (SolrCore core = cc.getCore(coreName)) {
        if (core != null) {
          core.getCoreDescriptor().getCloudDescriptor().setLeader(false);
        }
      }
    } catch (AlreadyClosedException e) {
      // okay
    }
  }

  @Override
  public ElectionContext copy() {
    return new ShardLeaderElectionContext(leaderElector, shardId, collection, id, leaderProps, zkController, cc);
  }



  public LeaderElector getLeaderElector() {
    return leaderElector;
  }

  /*
   * weAreReplacement: has someone else been the leader already?
   */
  @Override
  void runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStart) throws KeeperException,
          InterruptedException, IOException {

    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);

    log.info("Run leader process for shard election {}", coreName);

    ActionThrottle lt;
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        log.error("No SolrCore found, cannot become leader {}", coreName);
        throw new SolrException(ErrorCode.SERVER_ERROR, "No SolrCore found, cannot become leader " + coreName);
      }
      MDCLoggingContext.setCore(core);
      lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();

      lt.minimumWaitBetweenActions();
      lt.markAttemptingAction();

      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();

      if (log.isDebugEnabled()) log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId, weAreReplacement, leaderVoteWait);

      Replica.Type replicaType;
      String coreNodeName;
      boolean setTermToMax = false;

      CoreDescriptor cd = core.getCoreDescriptor();
      CloudDescriptor cloudCd = cd.getCloudDescriptor();
      replicaType = cloudCd.getReplicaType();
      // should I be leader?
      ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
      if (zkShardTerms.registered(coreName) && !zkShardTerms.canBecomeLeader(coreName)) {
        if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, coreName, leaderVoteWait)) {
          rejoinLeaderElection(core);
          return;
        } else {
          // only log an error if this replica win the election
          setTermToMax = true;
        }
      }


      log.info("I may be the new leader - try and sync");

      // nocommit
      // we are going to attempt to be the leader
      // first cancel any current recovery
      core.getUpdateHandler().getSolrCoreState().cancelRecovery();

      PeerSync.PeerSyncResult result = null;
      boolean success = false;
      try {
        result = syncStrategy.sync(zkController, core, leaderProps, weAreReplacement);
        success = result.isSuccess();
      } catch (Exception e) {
        ParWork.propagateInterrupt("Exception while trying to sync", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();

      if (!success) {

        log.warn("Our sync attempt failed ulog={}", ulog);
        boolean hasRecentUpdates = false;
        if (ulog != null) {
          // TODO: we could optimize this if necessary
          try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
            hasRecentUpdates = !recentUpdates.getVersions(1).isEmpty();
          }
        }

        log.warn("Checking for recent versions in the update log", hasRecentUpdates);
        if (!hasRecentUpdates) {
          // we failed sync, but we have no versions - we can't sync in that case
          // - we were active
          // before, so become leader anyway if no one else has any versions either
          if (result.getOtherHasVersions().orElse(false)) {
            log.info("We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader");
              rejoinLeaderElection(core);
              return;
          } else {
            log.info("We failed sync, but we have no versions - we can't sync in that case - we were active before, so become leader anyway");
            success = true;
          }
        }
      }
      log.info("Our sync attempt succeeded");

      // solrcloud_debug
      if (log.isDebugEnabled()) {
        try {
          RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
          SolrIndexSearcher searcher = searchHolder.get();
          try {
            log.debug(core.getCoreContainer().getZkController().getNodeName() + " synched " + searcher.count(new MatchAllDocsQuery()));
          } finally {
            searchHolder.decref();
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
      if (!success) {
        log.info("Sync with potential leader failed, rejoining election ...");
        rejoinLeaderElection(core);
        return;
      }

      try {
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
          zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreName);
        }

        super.runLeaderProcess(context, weAreReplacement, 0);

        assert shardId != null;

        core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
        publishActive(core);
        ZkNodeProps zkNodes = ZkNodeProps
            .fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(), ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.BASE_URL_PROP,
                leaderProps.get(ZkStateReader.BASE_URL_PROP), ZkStateReader.NODE_NAME_PROP, leaderProps.get(ZkStateReader.NODE_NAME_PROP), ZkStateReader.CORE_NAME_PROP,
                leaderProps.get(ZkStateReader.CORE_NAME_PROP), ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
        assert zkController != null;
        assert zkController.getOverseer() != null;

        log.info("Publish leader state");
        zkController.getOverseer().offerStateUpdate(Utils.toJSON(zkNodes));

        log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);

      } catch (AlreadyClosedException | InterruptedException e) {
        ParWork.propagateInterrupt("Already closed or interrupted, bailing..", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (SessionExpiredException e) {
        throw e;
      } catch (Exception e) {
        SolrException.log(log, "There was a problem trying to register as the leader", e);

        core.getCoreDescriptor().getCloudDescriptor().setLeader(false);

        // we could not publish ourselves as leader - try and rejoin election

        rejoinLeaderElection(core);
      }


    } catch (AlreadyClosedException e) {
      log.info("CoreContainer is shutting down, won't become leader");
    } finally {
      MDCLoggingContext.clear();
    }
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
        log.info("Can't become leader, other replicas with higher term participated in leader election");
        return false;
      }

      // TODO: if we know eveyrone has already particpated, we should bail early...
      
      Thread.sleep(500L);
    }
  }

  /**
   * Do other replicas with higher term participated in the election
   *
   * @return true if other replicas with higher term participated in the election, false if otherwise
   */
  private boolean replicasWithHigherTermParticipated(ZkShardTerms zkShardTerms, String coreNodeName) throws InterruptedException {
    ClusterState clusterState = zkController.getClusterState();
    DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    if (slices == null) return false;

    long replicaTerm = zkShardTerms.getTerm(coreNodeName);
    boolean isRecovering = zkShardTerms.isRecovering(coreNodeName);

    for (Replica replica : slices.getReplicas()) {
      if (replica.getName().equals(coreNodeName)) continue;
      if (zkController.getZkStateReader().getLiveNodes().contains(replica.getNodeName())) {
        long otherTerm = zkShardTerms.getTerm(replica.getName());
        boolean isOtherReplicaRecovering = zkShardTerms.isRecovering(replica.getName());
        if (isRecovering && !isOtherReplicaRecovering) return true;
        if (otherTerm > replicaTerm) return true;
      }
    }
    return false;
  }

  public void publishActive(SolrCore core) throws Exception {
    if (log.isDebugEnabled()) log.debug("publishing ACTIVE on becoming leader");
    zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE, true, false);
  }

  private void rejoinLeaderElection(SolrCore core)
          throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election

    log.info("There may be a better leader candidate than us - will cancel election, rejoin election, and kick off recovery");

    cancelElection();

    this.isClosed = false;
    super.closed = false;
    leaderElector.joinElection(this, true);

    core.getUpdateHandler().getSolrCoreState().doRecovery(zkController.getCoreContainer(), core.getCoreDescriptor());
  }

  public String getShardId() {
    return shardId;
  }

  public String getCollection() {
    return collection;
  }

}

