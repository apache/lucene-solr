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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
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
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

public abstract class ElectionContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  volatile String leaderSeqPath;
  private SolrZkClient zkClient;

  public ElectionContext(final String coreNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps, final SolrZkClient zkClient) {
    assert zkClient != null;
    this.id = coreNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
    this.zkClient = zkClient;
  }
  
  public void close() {

  }
  
  public void cancelElection() throws InterruptedException, KeeperException {
    if (leaderSeqPath != null) {
      try {
        log.debug("Canceling election {}", leaderSeqPath);
        zkClient.delete(leaderSeqPath, -1, true);
      } catch (NoNodeException e) {
        // fine
        log.debug("cancelElection did not find election node to remove {}", leaderSeqPath);
      }
    } else {
      log.debug("cancelElection skipped as this context has not been initialized");
    }
  }

  abstract void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException, InterruptedException, IOException;

  public void checkIfIamLeaderFired() {}

  public void joinedElectionFired() {}

  public  ElectionContext copy(){
    throw new UnsupportedOperationException("copy");
  }
}

class ShardLeaderElectionContextBase extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrZkClient zkClient;
  protected String shardId;
  protected String collection;
  protected LeaderElector leaderElector;
  protected ZkStateReader zkStateReader;
  protected ZkController zkController;
  private Integer leaderZkNodeParentVersion;

  // Prevents a race between cancelling and becoming leader.
  private final Object lock = new Object();

  public ShardLeaderElectionContextBase(LeaderElector leaderElector,
      final String shardId, final String collection, final String coreNodeName,
      ZkNodeProps props, ZkController zkController) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + "/leader_elect/" + shardId, ZkStateReader.getShardLeadersPath(
        collection, shardId), props, zkController.getZkClient());
    this.leaderElector = leaderElector;
    this.zkStateReader = zkController.getZkStateReader();
    this.zkClient = zkStateReader.getZkClient();
    this.zkController = zkController;
    this.shardId = shardId;
    this.collection = collection;
    
    String parent = Paths.get(leaderPath).getParent().toString();
    ZkCmdExecutor zcmd = new ZkCmdExecutor(30000);
    // only if /collections/{collection} exists already do we succeed in creating this path
    log.info("make sure parent is created {}", parent);
    try {
      zcmd.ensureExists(parent, (byte[])null, CreateMode.PERSISTENT, zkClient, 2);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    synchronized (lock) {
      if (leaderZkNodeParentVersion != null) {
        try {
          // We need to be careful and make sure we *only* delete our own leader registration node.
          // We do this by using a multi and ensuring the parent znode of the leader registration node
          // matches the version we expect - there is a setData call that increments the parent's znode
          // version whenever a leader registers.
          log.debug("Removing leader registration node on cancel: {} {}", leaderPath, leaderZkNodeParentVersion);
          List<Op> ops = new ArrayList<>(2);
          ops.add(Op.check(Paths.get(leaderPath).getParent().toString(), leaderZkNodeParentVersion));
          ops.add(Op.delete(leaderPath, -1));
          zkClient.multi(ops, true);
        } catch (KeeperException.NoNodeException nne) {
          // no problem
          log.debug("No leader registration node found to remove: {}", leaderPath);
        } catch (KeeperException.BadVersionException bve) {
          log.info("Cannot remove leader registration node because the current registered node is not ours: {}", leaderPath);
          // no problem
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          SolrException.log(log, e);
        }
        leaderZkNodeParentVersion = null;
      } else {
        log.info("No version found for ephemeral leader parent node, won't remove previous leader registration.");
      }
    }
  }
  
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away

    String parent = Paths.get(leaderPath).getParent().toString();
    try {
      RetryUtil.retryOnThrowable(NodeExistsException.class, 60000, 5000, () -> {
        synchronized (lock) {
          log.info("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);
          List<Op> ops = new ArrayList<>(2);

          // We use a multi operation to get the parent nodes version, which will
          // be used to make sure we only remove our own leader registration node.
          // The setData call used to get the parent version is also the trigger to
          // increment the version. We also do a sanity check that our leaderSeqPath exists.

          ops.add(Op.check(leaderSeqPath, -1));
          ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
          ops.add(Op.setData(parent, null, -1));
          List<OpResult> results;

          results = zkClient.multi(ops, true);
          for (OpResult result : results) {
            if (result.getType() == ZooDefs.OpCode.setData) {
              SetDataResult dresult = (SetDataResult) result;
              Stat stat = dresult.getStat();
              leaderZkNodeParentVersion = stat.getVersion();
              return;
            }
          }
          assert leaderZkNodeParentVersion != null;
        }
      });
    } catch (NoNodeException e) {
      log.info("Will not register as leader because it seems the election is no longer taking place.");
      return;
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", t);
    } 
    
    assert shardId != null;
    boolean isAlreadyLeader = false;
    if (zkStateReader.getClusterState() != null &&
        zkStateReader.getClusterState().getCollection(collection).getSlice(shardId).getReplicas().size() < 2) {
      Replica leader = zkStateReader.getLeader(collection, shardId);
      if (leader != null
          && leader.getBaseUrl().equals(leaderProps.get(ZkStateReader.BASE_URL_PROP))
          && leader.getCoreName().equals(leaderProps.get(ZkStateReader.CORE_NAME_PROP))) {
        isAlreadyLeader = true;
      }
    }
    if (!isAlreadyLeader) {
      ZkNodeProps m = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
          ZkStateReader.SHARD_ID_PROP, shardId,
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.BASE_URL_PROP, leaderProps.get(ZkStateReader.BASE_URL_PROP),
          ZkStateReader.CORE_NAME_PROP, leaderProps.get(ZkStateReader.CORE_NAME_PROP),
          ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
      assert zkController != null;
      assert zkController.getOverseer() != null;
      zkController.getOverseer().offerStateUpdate(Utils.toJSON(m));
    }
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }

  Integer getLeaderZkNodeParentVersion() {
    synchronized (lock) {
      return leaderZkNodeParentVersion;
    }
  }
}

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
    this.isClosed  = true;
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
      if (core == null ) {
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
      
      log.debug("Running the leader process for shard={} and weAreReplacement={} and leaderVoteWait={}", shardId, weAreReplacement, leaderVoteWait);
      if (zkController.getClusterState().getCollection(collection).getSlice(shardId).getReplicas().size() > 1) {
        // Clear the leader in clusterstate. We only need to worry about this if there is actually more than one replica.
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
            ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection);
        zkController.getOverseer().getStateUpdateQueue().offer(Utils.toJSON(m));
      }

      boolean allReplicasInLine = false;
      if (!weAreReplacement) {
        allReplicasInLine = waitForReplicasToComeUp(leaderVoteWait);
      } else {
        allReplicasInLine = areAllReplicasParticipating();
      }
      
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
        
        if (weAreReplacement) {
          // wait a moment for any floating updates to finish
          try {
            Thread.sleep(2500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, e);
          }
        }

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
            if (result.getOtherHasVersions().orElse(false))  {
              log.info("We failed sync, but we have no versions - we can't sync in that case. But others have some versions, so we should not become leader");
              success = false;
            } else  {
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
            log.error("WARNING: Potential data loss -- Replica {} became leader after timeout (leaderVoteWait) " +
                "without being up-to-date with the previous leader", coreNodeName);
            zkController.getShardTerms(collection, shardId).setTermEqualsToLeader(coreNodeName);
          }
          super.runLeaderProcess(weAreReplacement, 0);
          try (SolrCore core = cc.getCore(coreName)) {
            if (core != null) {
              core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
              publishActiveIfRegisteredAndNotActive(core);
            } else {
              return;
            }
          }
          log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);
          
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
              log.debug("SolrCore not found:" + coreName + " in " + cc.getLoadedCoreNames());
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
   * @return true if after {@code timeout} there are no other replicas with higher term participate in the election,
   * false if otherwise
   */
  private boolean waitForEligibleBecomeLeaderAfterTimeout(ZkShardTerms zkShardTerms, String coreNodeName, int timeout) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (!isClosed && !cc.isShutDown()) {
      if (System.nanoTime() > timeoutAt) {
        log.warn("After waiting for {}ms, no other potential leader was found, {} try to become leader anyway (" +
                "core_term:{}, highest_term:{})",
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
      if (core.getCoreDescriptor().getCloudDescriptor().hasRegistered()) {
        ZkStateReader zkStateReader = zkController.getZkStateReader();
        zkStateReader.forceUpdateCollection(collection);
        ClusterState clusterState = zkStateReader.getClusterState();
        Replica rep = getReplica(clusterState, collection, leaderProps.getStr(ZkStateReader.CORE_NODE_NAME_PROP));
        if (rep == null) return;
        if (rep.getState() != Replica.State.ACTIVE || core.getCoreDescriptor().getCloudDescriptor().getLastPublished() != Replica.State.ACTIVE) {
          log.debug("We have become the leader after core registration but are not in an ACTIVE state - publishing ACTIVE");
          zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
        }
      }
  }
  
  private Replica getReplica(ClusterState clusterState, String collectionName, String replicaName) {
    if (clusterState == null) return null;
    final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
    if (docCollection == null) return null;
    return docCollection.getReplica(replicaName);
  }

  // returns true if all replicas are found to be up, false if not
  private boolean waitForReplicasToComeUp(int timeoutms) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    final String shardsElectZkPath = electionPath + LeaderElector.ELECTION_NODE;
    
    DocCollection docCollection = zkController.getClusterState().getCollectionOrNull(collection);
    Slice slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
    int cnt = 0;
    while (!isClosed && !cc.isShutDown()) {
      // wait for everyone to be up
      if (slices != null) {
        int found = 0;
        try {
          found = zkClient.getChildren(shardsElectZkPath, null, true).size();
        } catch (KeeperException e) {
          if (e instanceof KeeperException.SessionExpiredException) {
            // if the session has expired, then another election will be launched, so
            // quit here
            throw new SolrException(ErrorCode.SERVER_ERROR,
                                    "ZK session expired - cancelling election for " + collection + " " + shardId);
          }
          SolrException.log(log,
              "Error checking for the number of election participants", e);
        }
        
        // on startup and after connection timeout, wait for all known shards
        if (found >= slices.getReplicas(EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT)).size()) {
          log.info("Enough replicas found to continue.");
          return true;
        } else {
          if (cnt % 40 == 0) {
            log.info("Waiting until we see more replicas up for shard {}: total={}"
              + " found={}"
              + " timeoutin={}ms",
                shardId, slices.getReplicas(EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT)).size(), found,
                TimeUnit.MILLISECONDS.convert(timeoutAt - System.nanoTime(), TimeUnit.NANOSECONDS));
          }
        }
        
        if (System.nanoTime() > timeoutAt) {
          log.info("Was waiting for replicas to come up, but they are taking too long - assuming they won't come back till later");
          return false;
        }
      } else {
        log.warn("Shard not found: " + shardId + " for collection " + collection);

        return false;

      }
      
      Thread.sleep(500);
      docCollection = zkController.getClusterState().getCollectionOrNull(collection);
      slices = (docCollection == null) ? null : docCollection.getSlice(shardId);
      cnt++;
    }
    return false;
  }
  
  // returns true if all replicas are found to be up, false if not
  private boolean areAllReplicasParticipating() throws InterruptedException {
    final String shardsElectZkPath = electionPath + LeaderElector.ELECTION_NODE;
    final DocCollection docCollection = zkController.getClusterState().getCollectionOrNull(collection);
    
    if (docCollection != null && docCollection.getSlice(shardId) != null) {
      final Slice slices = docCollection.getSlice(shardId);
      int found = 0;
      try {
        found = zkClient.getChildren(shardsElectZkPath, null, true).size();
      } catch (KeeperException e) {
        if (e instanceof KeeperException.SessionExpiredException) {
          // if the session has expired, then another election will be launched, so
          // quit here
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "ZK session expired - cancelling election for " + collection + " " + shardId);
        }
        SolrException.log(log, "Error checking for the number of election participants", e);
      }
      
      if (found >= slices.getReplicasMap().size()) {
        log.debug("All replicas are ready to participate in election.");
        return true;
      }
      
    } else {
      log.warn("Shard not found: " + shardId + " for collection " + collection);
      
      return false;
    }
    
    return false;
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

final class OverseerElectionContext extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient zkClient;
  private final Overseer overseer;
  private volatile boolean isClosed = false;

  public OverseerElectionContext(SolrZkClient zkClient, Overseer overseer, final String zkNodeName) {
    super(zkNodeName, Overseer.OVERSEER_ELECT, Overseer.OVERSEER_ELECT + "/leader", null, zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
    try {
      new ZkCmdExecutor(zkClient.getZkClientTimeout()).ensureExists(Overseer.OVERSEER_ELECT, zkClient);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException,
      InterruptedException {
    if (isClosed) {
      return;
    }
    log.info("I am going to be the leader {}", id);
    final String id = leaderSeqPath
        .substring(leaderSeqPath.lastIndexOf("/") + 1);
    ZkNodeProps myProps = new ZkNodeProps(ID, id);

    zkClient.makePath(leaderPath, Utils.toJSON(myProps),
        CreateMode.EPHEMERAL, true);
    if(pauseBeforeStartMs >0){
      try {
        Thread.sleep(pauseBeforeStartMs);
      } catch (InterruptedException e) {
        Thread.interrupted();
        log.warn("Wait interrupted ", e);
      }
    }
    synchronized (this) {
      if (!this.isClosed && !overseer.getZkController().getCoreContainer().isShutDown()) {
        overseer.start(id);
      }
    }
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    overseer.close();
  }
  
  @Override
  public synchronized void close() {
    this.isClosed  = true;
    overseer.close();
  }

  @Override
  public ElectionContext copy() {
    return new OverseerElectionContext(zkClient, overseer ,id);
  }
  
  @Override
  public void joinedElectionFired() {
    overseer.close();
  }
  
  @Override
  public void checkIfIamLeaderFired() {
    // leader changed - close the overseer
    overseer.close();
  }

}
