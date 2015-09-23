package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.RetryUtil.RetryCmd;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class ElectionContext implements Closeable {
  static Logger log = LoggerFactory.getLogger(ElectionContext.class);
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  volatile String leaderSeqPath;
  private SolrZkClient zkClient;

  public ElectionContext(final String coreNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps, final SolrZkClient zkClient) {
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
        log.info("Canceling election {}", leaderSeqPath);
        zkClient.delete(leaderSeqPath, -1, true);
      } catch (NoNodeException e) {
        // fine
        log.info("cancelElection did not find election node to remove {}", leaderSeqPath);
      }
    } else {
      log.info("cancelElection skipped as this context has not been initialized");
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
  private static Logger log = LoggerFactory
      .getLogger(ShardLeaderElectionContextBase.class);
  protected final SolrZkClient zkClient;
  protected String shardId;
  protected String collection;
  protected LeaderElector leaderElector;
  protected volatile Integer leaderZkNodeParentVersion;
  
  public ShardLeaderElectionContextBase(LeaderElector leaderElector,
      final String shardId, final String collection, final String coreNodeName,
      ZkNodeProps props, ZkStateReader zkStateReader) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + "/leader_elect/" + shardId, ZkStateReader.getShardLeadersPath(
        collection, shardId), props, zkStateReader.getZkClient());
    this.leaderElector = leaderElector;
    this.zkClient = zkStateReader.getZkClient();
    this.shardId = shardId;
    this.collection = collection;

    try {
      new ZkCmdExecutor(zkStateReader.getZkClient().getZkClientTimeout())
          .ensureExists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection,
              zkClient);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    if (leaderZkNodeParentVersion != null) {
      try {
        // We need to be careful and make sure we *only* delete our own leader registration node.
        // We do this by using a multi and ensuring the parent znode of the leader registration node
        // matches the version we expect - there is a setData call that increments the parent's znode
        // version whenever a leader registers.
        log.info("Removing leader registration node on cancel: {} {}", leaderPath, leaderZkNodeParentVersion);
        List<Op> ops = new ArrayList<>(2);
        ops.add(Op.check(new Path(leaderPath).getParent().toString(), leaderZkNodeParentVersion));
        ops.add(Op.delete(leaderPath, -1));
        zkClient.multi(ops, true);
      } catch (KeeperException.NoNodeException nne) {
        // no problem
        log.info("No leader registration node found to remove: {}", leaderPath);
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
    super.cancelElection();
  }
  
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away
    final String parent = new Path(leaderPath).getParent().toString();
    ZkCmdExecutor zcmd = new ZkCmdExecutor(30000);
    zcmd.ensureExists(parent, zkClient);
    final byte[] json = Utils.toJSON(leaderProps);
    try {
      RetryUtil.retryOnThrowable(NodeExistsException.class, 60000, 5000, new RetryCmd() {
        
        @Override
        public void execute() throws InterruptedException, KeeperException {
          log.info("Creating leader registration node", leaderPath);
          List<Op> ops = new ArrayList<>(2);
          
          // We use a multi operation to get the parent nodes version, which will
          // be used to make sure we only remove our own leader registration node.
          // The setData call used to get the parent version is also the trigger to
          // increment the version. We also do a sanity check that our leaderSeqPath exists.
          
          ops.add(Op.check(leaderSeqPath, -1));
          ops.add(Op.create(leaderPath, json, zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
          // we set the json on the parent too for back compat, see SOLR-7844
          ops.add(Op.setData(parent, json, -1));
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
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", t);
    } 
    
    assert shardId != null;
    ZkNodeProps m = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION,
        OverseerAction.LEADER.toLower(), ZkStateReader.SHARD_ID_PROP, shardId,
        ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.BASE_URL_PROP,
        leaderProps.getProperties().get(ZkStateReader.BASE_URL_PROP),
        ZkStateReader.CORE_NAME_PROP,
        leaderProps.getProperties().get(ZkStateReader.CORE_NAME_PROP),
        ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
    Overseer.getInQueue(zkClient).offer(Utils.toJSON(m));
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }  
}

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private static Logger log = LoggerFactory.getLogger(ShardLeaderElectionContext.class);
  
  private final ZkController zkController;
  private final CoreContainer cc;
  private final SyncStrategy syncStrategy;

  private volatile boolean isClosed = false;
  
  public ShardLeaderElectionContext(LeaderElector leaderElector, 
      final String shardId, final String collection,
      final String coreNodeName, ZkNodeProps props, ZkController zkController, CoreContainer cc) {
    super(leaderElector, shardId, collection, coreNodeName, props,
        zkController.getZkStateReader());
    this.zkController = zkController;
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
        throw new SolrException(ErrorCode.SERVER_ERROR, "SolrCore not found:" + coreName + " in " + cc.getCoreNames());
      }
      MDCLoggingContext.setCore(core);
      lt = core.getUpdateHandler().getSolrCoreState().getLeaderThrottle();
    }

    try {
      lt.minimumWaitBetweenActions();
      lt.markAttemptingAction();
      
      log.info("Running the leader process for shard " + shardId);
      // clear the leader in clusterstate
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.LEADER.toLower(),
          ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP, collection);
      Overseer.getInQueue(zkClient).offer(Utils.toJSON(m));
      
      int leaderVoteWait = cc.getZkController().getLeaderVoteWait();
      if (!weAreReplacement) {
        waitForReplicasToComeUp(leaderVoteWait);
      }
      
      if (isClosed) {
        // Solr is shutting down or the ZooKeeper session expired while waiting for replicas. If the later, 
        // we cannot be sure we are still the leader, so we should bail out. The OnReconnect handler will 
        // re-register the cores and handle a new leadership election.
        return;
      }
      
      try (SolrCore core = cc.getCore(coreName)) {
        
        if (core == null) {
          cancelElection();
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "SolrCore not found:" + coreName + " in " + cc.getCoreNames());
        }
        
        // should I be leader?
        if (weAreReplacement && !shouldIBeLeader(leaderProps, core, weAreReplacement)) {
          rejoinLeaderElection(core);
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
        
        boolean success = false;
        try {
          success = syncStrategy.sync(zkController, core, leaderProps, weAreReplacement);
        } catch (Exception e) {
          SolrException.log(log, "Exception while trying to sync", e);
          success = false;
        }
        
        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        
        if (!success) {
          boolean hasRecentUpdates = false;
          if (ulog != null) {
            // TODO: we could optimize this if necessary
            UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates();
            try {
              hasRecentUpdates = !recentUpdates.getVersions(1).isEmpty();
            } finally {
              recentUpdates.close();
            }
          }
          
          if (!hasRecentUpdates) {
            // we failed sync, but we have no versions - we can't sync in that case
            // - we were active
            // before, so become leader anyway
            log.info(
                "We failed sync, but we have no versions - we can't sync in that case - we were active before, so become leader anyway");
            success = true;
          }
        }
        
        // solrcloud_debug
        if (log.isDebugEnabled()) {
          try {
            RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
            SolrIndexSearcher searcher = searchHolder.get();
            try {
              log.debug(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " synched "
                  + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
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
        
        log.info("I am the new leader: " + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);
        core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
      }
      
      boolean isLeader = true;
      if (!isClosed) {
        try {
          super.runLeaderProcess(weAreReplacement, 0);
        } catch (Exception e) {
          isLeader = false;
          SolrException.log(log, "There was a problem trying to register as the leader", e);
          
          try (SolrCore core = cc.getCore(coreName)) {
            
            if (core == null) {
              log.debug("SolrCore not found:" + coreName + " in " + cc.getCoreNames());
              return;
            }
            
            core.getCoreDescriptor().getCloudDescriptor().setLeader(false);
            
            // we could not publish ourselves as leader - try and rejoin election
            rejoinLeaderElection(core);
          }
        }
        
        if (isLeader) {
          // check for any replicas in my shard that were set to down by the previous leader
          try {
            startLeaderInitiatedRecoveryOnReplicas(coreName);
          } catch (Exception exc) {
            // don't want leader election to fail because of
            // an error trying to tell others to recover
          }
        }
      } else {
        cancelElection();
      }
    } finally {
      MDCLoggingContext.clear();
    }
  }
  
  private void startLeaderInitiatedRecoveryOnReplicas(String coreName) throws Exception {
    try (SolrCore core = cc.getCore(coreName)) {
      CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
      String coll = cloudDesc.getCollectionName();
      String shardId = cloudDesc.getShardId();
      String coreNodeName = cloudDesc.getCoreNodeName();

      if (coll == null || shardId == null) {
        log.error("Cannot start leader-initiated recovery on new leader (core="+
           coreName+",coreNodeName=" + coreNodeName + ") because collection and/or shard is null!");
        return;
      }
      
      String znodePath = zkController.getLeaderInitiatedRecoveryZnodePath(coll, shardId);
      List<String> replicas = null;
      try {
        replicas = zkClient.getChildren(znodePath, null, false);
      } catch (NoNodeException nne) {
        // this can be ignored
      }
      
      if (replicas != null && replicas.size() > 0) {
        for (String replicaCoreNodeName : replicas) {
          
          if (coreNodeName.equals(replicaCoreNodeName))
            continue; // added safe-guard so we don't mark this core as down
          
          final Replica.State lirState = zkController.getLeaderInitiatedRecoveryState(coll, shardId, replicaCoreNodeName);
          if (lirState == Replica.State.DOWN || lirState == Replica.State.RECOVERY_FAILED) {
            log.info("After core={} coreNodeName={} was elected leader, a replica coreNodeName={} was found in state: "
                + lirState.toString() + " and needing recovery.", coreName, coreNodeName, replicaCoreNodeName);
            List<ZkCoreNodeProps> replicaProps =  
                zkController.getZkStateReader().getReplicaProps(collection, shardId, coreNodeName);
            
            if (replicaProps != null && replicaProps.size() > 0) {                
              ZkCoreNodeProps coreNodeProps = null;
              for (ZkCoreNodeProps p : replicaProps) {
                if (((Replica)p.getNodeProps()).getName().equals(replicaCoreNodeName)) {
                  coreNodeProps = p;
                  break;
                }
              }
              
              zkController.ensureReplicaInLeaderInitiatedRecovery(cc,
                  collection, shardId, coreNodeProps, core.getCoreDescriptor(),
                  false /* forcePublishState */);
            }              
          }
        }
      }
    } // core gets closed automagically    
  }

  private void waitForReplicasToComeUp(int timeoutms) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    final String shardsElectZkPath = electionPath + LeaderElector.ELECTION_NODE;
    
    Slice slices = zkController.getClusterState().getSlice(collection, shardId);
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
        if (found >= slices.getReplicasMap().size()) {
          log.info("Enough replicas found to continue.");
          return;
        } else {
          if (cnt % 40 == 0) {
            log.info("Waiting until we see more replicas up for shard {}: total={}"
              + " found={}"
              + " timeoutin={}ms",
                shardId, slices.getReplicasMap().size(), found,
                TimeUnit.MILLISECONDS.convert(timeoutAt - System.nanoTime(), TimeUnit.NANOSECONDS));
          }
        }
        
        if (System.nanoTime() > timeoutAt) {
          log.info("Was waiting for replicas to come up, but they are taking too long - assuming they won't come back till later");
          return;
        }
      } else {
        log.warn("Shard not found: " + shardId + " for collection " + collection);

        return;

      }
      
      Thread.sleep(500);
      slices = zkController.getClusterState().getSlice(collection, shardId);
      cnt++;
    }
  }

  private void rejoinLeaderElection(SolrCore core)
      throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election
    if (cc.isShutDown()) {
      log.info("Not rejoining election because CoreContainer is closed");
      return;
    }
    
    log.info("There may be a better leader candidate than us - going back into recovery");
    
    cancelElection();
    
    core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
    
    leaderElector.joinElection(this, true);
  }

  private boolean shouldIBeLeader(ZkNodeProps leaderProps, SolrCore core, boolean weAreReplacement) {
    log.info("Checking if I should try and be the leader.");
    
    if (isClosed) {
      log.info("Bailing on leader process because we have been closed");
      return false;
    }
    
    if (!weAreReplacement) {
      // we are the first node starting in the shard - there is a configurable wait
      // to make sure others participate in sync and leader election, we can be leader
      return true;
    }
    
    if (core.getCoreDescriptor().getCloudDescriptor().getLastPublished() == Replica.State.ACTIVE) {
      
      // maybe active but if the previous leader marked us as down and
      // we haven't recovered, then can't be leader
      final Replica.State lirState = zkController.getLeaderInitiatedRecoveryState(collection, shardId,
          core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
      if (lirState == Replica.State.DOWN || lirState == Replica.State.RECOVERING) {
        log.warn("Although my last published state is Active, the previous leader marked me "+core.getName()
            + " as " + lirState.toString()
            + " and I haven't recovered yet, so I shouldn't be the leader.");
        return false;
      }
      
      log.info("My last published State was Active, it's okay to be the leader.");
      return true;
    }
    log.info("My last published State was "
        + core.getCoreDescriptor().getCloudDescriptor().getLastPublished()
        + ", I won't be the leader.");
    // TODO: and if no one is a good candidate?
    
    return false;
  }
  
}

final class OverseerElectionContext extends ElectionContext {
  private static Logger log = LoggerFactory.getLogger(OverseerElectionContext.class);
  private final SolrZkClient zkClient;
  private Overseer overseer;
  public static final String PATH = "/overseer_elect";

  public OverseerElectionContext(SolrZkClient zkClient, Overseer overseer, final String zkNodeName) {
    super(zkNodeName,PATH , PATH+"/leader", null, zkClient);
    this.overseer = overseer;
    this.zkClient = zkClient;
    try {
      new ZkCmdExecutor(zkClient.getZkClientTimeout()).ensureExists("/overseer_elect", zkClient);
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
    log.info("I am going to be the leader {}", id);
    final String id = leaderSeqPath
        .substring(leaderSeqPath.lastIndexOf("/") + 1);
    ZkNodeProps myProps = new ZkNodeProps("id", id);

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
    
    overseer.start(id);
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
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
