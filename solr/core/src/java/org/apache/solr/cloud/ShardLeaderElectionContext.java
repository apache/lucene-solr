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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
                                    final String coreNodeName, ZkNodeProps props, ZkController zkController, CoreContainer cc) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                    + "/leader_elect/" + shardId,  ZkStateReader.getShardLeadersPath(
            collection, shardId), props,
            zkController.getZkClient());
    this.cc = cc;
    this.syncStrategy = new SyncStrategy(cc);
    this.shardId = shardId;
    this.leaderElector = leaderElector;
    this.zkController = zkController;
    this.collection = collection;
  }

  @Override
  public void close() {
    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(() -> super.close());
      closer.collect(() -> {
        try {
          cancelElection();
        } catch (Exception e) {
          ParWork.propegateInterrupt(e);
          log.error("Exception canceling election", e);
        }
      });
      closer.collect(syncStrategy);
      closer.addCollect("shardLeaderElectionContextClose");
    }

    this.isClosed = true;
  }

  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
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
    if (isClosed()) {
      return;
    }

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

      log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId,
              weAreReplacement, leaderVoteWait);

//      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
//              ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection);
//      try {
//        zkController.getOverseer().offerStateUpdate(Utils.toJSON(m));
//      } catch (Exception e1) {
//        ParWork.propegateInterrupt(e1);
//        throw new SolrException(ErrorCode.SERVER_ERROR, e1);
//      }

      if (isClosed()) {
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
        CoreDescriptor cd = core.getCoreDescriptor();
        CloudDescriptor cloudCd = cd.getCloudDescriptor();
        replicaType = cloudCd.getReplicaType();
        coreNodeName = cloudCd.getCoreNodeName();
        // should I be leader?
        ZkShardTerms zkShardTerms = zkController.getShardTerms(collection, shardId);
        if (zkShardTerms.registered(coreNodeName) && !zkShardTerms.canBecomeLeader(coreNodeName)) {
          if (!waitForEligibleBecomeLeaderAfterTimeout(zkShardTerms, cd, leaderVoteWait)) {
            rejoinLeaderElection(core);
            return;
          } else {
            // only log an error if this replica win the election
            setTermToMax = true;
          }
        }

        if (isClosed()) {
          return;
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
          ParWork.propegateInterrupt(e);
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
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
              log.info(
                      "We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader");
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
              log.debug(core.getCoreContainer().getZkController().getNodeName() + " synched "
                      + searcher.count(new MatchAllDocsQuery()));
            } finally {
              searchHolder.decref();
            }
          } catch (Exception e) {
            ParWork.propegateInterrupt(e);
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
        if (!success) {
          rejoinLeaderElection(core);
          return;
        }

      }
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
            log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " +
                    "without being up-to-date with the previous leader", coreNodeName);
            zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreNodeName);
          }
          super.runLeaderProcess(context, weAreReplacement, 0);

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
              return;
            }
          }
          log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);

        } catch (AlreadyClosedException | InterruptedException e) {
          log.info("Already closed or interrupted, bailing..");
        } catch (Exception e) {
          SolrException.log(log, "There was a problem trying to register as the leader", e);
          ParWork.propegateInterrupt(e);
          if(e instanceof IOException
                  || (e instanceof KeeperException && (!(e instanceof SessionExpiredException)))) {

            try (SolrCore core = cc.getCore(coreName)) {

              if (core == null) {
                if (log.isDebugEnabled())
                  log.debug("SolrCore not found:" + coreName + " in " + cc.getLoadedCoreNames());
                return;
              }
              core.getCoreDescriptor().getCloudDescriptor().setLeader(false);

              // we could not publish ourselves as leader - try and rejoin election
              try {
                rejoinLeaderElection(core);
              } catch (Exception exc) {
                ParWork.propegateInterrupt(e);
                throw new SolrException(ErrorCode.SERVER_ERROR, e);
              }
            }
          } else {
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
      }

    } finally {
      MDCLoggingContext.clear();
    }
  }

  /**
   * Wait for other replicas with higher terms participate in the electioon
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, CoreDescriptor cd, int timeout) throws InterruptedException {
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    AtomicReference<Boolean> foundHigherTerm = new AtomicReference<>();
    try {
      zkController.getZkStateReader().waitForState(cd.getCollectionName(), timeout, TimeUnit.MILLISECONDS, (n,c) -> foundForHigherTermReplica(zkShardTerms, cd, foundHigherTerm));
    } catch (TimeoutException e) {
      log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (" +
                      "core_term:{}, highest_term:{})",
              timeout, cd, zkShardTerms.getTerm(coreNodeName), zkShardTerms.getHighestTerm());
      return true;
    }

    return false;
  }

  private boolean foundForHigherTermReplica(ZkShardTerms zkShardTerms, CoreDescriptor cd, AtomicReference<Boolean> foundHigherTerm) {
    String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    if (replicasWithHigherTermParticipated(zkShardTerms, coreNodeName)) {
      log.info("Can't become leader, other replicas with higher term participated in leader election");
      foundHigherTerm.set(true);
      return true;
    }

    return false;
  }

  /**
   * Do other replicas with higher term participated in the election
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
    zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE, true, false);
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

    core.getUpdateHandler().getSolrCoreState().doRecovery(zkController.getCoreContainer(), core.getCoreDescriptor());

    leaderElector.joinElection(this, true);
  }

  public String getShardId() {
    return shardId;
  }

  public String getCollection() {
    return collection;
  }

  @Override
  public boolean isClosed() {
    return closed || cc.isShutDown() || zkController.getZkClient().isConnected() == false;
  }
}

