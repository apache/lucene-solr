package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.MatchAllDocsQuery;
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
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    if( leaderSeqPath != null ){
      try {
        log.info("canceling election {}",leaderSeqPath );
        zkClient.delete(leaderSeqPath, -1, true);
      } catch (NoNodeException e) {
        // fine
        log.warn("cancelElection did not find election node to remove {}" ,leaderSeqPath);
      }
    } else {
      log.warn("cancelElection skipped as this context has not been initialized");
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
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait just a bit
    // to see if it goes away
    try {
      RetryUtil.retryOnThrowable(NodeExistsException.class, 15000, 1000,
          new RetryCmd() {
            
            @Override
            public void execute() throws Throwable {
              zkClient.makePath(leaderPath, ZkStateReader.toJSON(leaderProps),
                  CreateMode.EPHEMERAL, true);
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
        Overseer.OverseerAction.LEADER.toLower(), ZkStateReader.SHARD_ID_PROP, shardId,
        ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.BASE_URL_PROP,
        leaderProps.getProperties().get(ZkStateReader.BASE_URL_PROP),
        ZkStateReader.CORE_NAME_PROP,
        leaderProps.getProperties().get(ZkStateReader.CORE_NAME_PROP),
        ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);
    Overseer.getInQueue(zkClient).offer(ZkStateReader.toJSON(m));    
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
    log.info("Running the leader process for shard " + shardId);
    
    String coreName = leaderProps.getStr(ZkStateReader.CORE_NAME_PROP);
    
    // clear the leader in clusterstate
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, Overseer.OverseerAction.LEADER.toLower(),
        ZkStateReader.SHARD_ID_PROP, shardId, ZkStateReader.COLLECTION_PROP,
        collection);
    Overseer.getInQueue(zkClient).offer(ZkStateReader.toJSON(m));
    
    int leaderVoteWait = cc.getZkController().getLeaderVoteWait();
    if (!weAreReplacement) {
      waitForReplicasToComeUp(weAreReplacement, leaderVoteWait);
    }

    try (SolrCore core = cc.getCore(coreName)) {

      if (core == null) {
        cancelElection();
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "Fatal Error, SolrCore not found:" + coreName + " in "
                + cc.getCoreNames());
      }
      
      // should I be leader?
      if (weAreReplacement && !shouldIBeLeader(leaderProps, core, weAreReplacement)) {
        rejoinLeaderElection(leaderSeqPath, core);
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
          log.info("We failed sync, but we have no versions - we can't sync in that case - we were active before, so become leader anyway");
          success = true;
        }
      }
      
      // solrcloud_debug
      if (log.isDebugEnabled()) {
        try {
          RefCounted<SolrIndexSearcher> searchHolder = core
              .getNewestSearcher(false);
          SolrIndexSearcher searcher = searchHolder.get();
          try {
            log.debug(core.getCoreDescriptor().getCoreContainer()
                .getZkController().getNodeName()
                + " synched "
                + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
          } finally {
            searchHolder.decref();
          }
        } catch (Exception e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, null, e);
        }
      }
      if (!success) {
        rejoinLeaderElection(leaderSeqPath, core);
        return;
      }

      log.info("I am the new leader: "
          + ZkCoreNodeProps.getCoreUrl(leaderProps) + " " + shardId);
      core.getCoreDescriptor().getCloudDescriptor().setLeader(true);
    }

    boolean isLeader = true;
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
        rejoinLeaderElection(leaderSeqPath, core);
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
          
          String lirState = zkController.getLeaderInitiatedRecoveryState(coll, shardId, replicaCoreNodeName);
          if (ZkStateReader.DOWN.equals(lirState) || ZkStateReader.RECOVERY_FAILED.equals(lirState)) {
            log.info("After core={} coreNodeName={} was elected leader, a replica coreNodeName={} was found in state: "
                + lirState + " and needing recovery.", coreName, coreNodeName, replicaCoreNodeName);
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
              
              LeaderInitiatedRecoveryThread lirThread = 
                  new LeaderInitiatedRecoveryThread(zkController,
                                                    cc,
                                                    collection,
                                                    shardId,
                                                    coreNodeProps,
                                                    120);
              zkController.ensureReplicaInLeaderInitiatedRecovery(
                  collection, shardId, coreNodeProps.getCoreUrl(), coreNodeProps, false);
              
              ExecutorService executor = cc.getUpdateShardHandler().getUpdateExecutor();
              executor.execute(lirThread);
            }              
          }
        }
      }
    } // core gets closed automagically    
  }

  private void waitForReplicasToComeUp(boolean weAreReplacement, int timeoutms) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutms, TimeUnit.MILLISECONDS);
    final String shardsElectZkPath = electionPath + LeaderElector.ELECTION_NODE;
    
    Slice slices = zkController.getClusterState().getSlice(collection, shardId);
    int cnt = 0;
    while (true && !isClosed && !cc.isShutDown()) {
      // wait for everyone to be up
      if (slices != null) {
        int found = 0;
        try {
          found = zkClient.getChildren(shardsElectZkPath, null, true).size();
        } catch (KeeperException e) {
          SolrException.log(log,
              "Error checking for the number of election participants", e);
        }
        
        // on startup and after connection timeout, wait for all known shards
        if (found >= slices.getReplicasMap().size()) {
          log.info("Enough replicas found to continue.");
          return;
        } else {
          if (cnt % 40 == 0) {
            log.info("Waiting until we see more replicas up for shard " + shardId + ": total="
              + slices.getReplicasMap().size() + " found=" + found
              + " timeoutin=" + (timeoutAt - System.nanoTime() / (float)(10^9)) + "ms");
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
      // System.out.println("###### waitForReplicasToComeUp  : slices=" + slices + " all=" + zkController.getClusterState().getCollectionStates() );
      cnt++;
    }
  }

  private void rejoinLeaderElection(String leaderSeqPath, SolrCore core)
      throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election
    if (cc.isShutDown()) {
      log.info("Not rejoining election because CoreContainer is close");
      return;
    }
    
    log.info("There may be a better leader candidate than us - going back into recovery");
    
    cancelElection();
    
    core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
    
    leaderElector.joinElection(this, true);
  }

  private boolean shouldIBeLeader(ZkNodeProps leaderProps, SolrCore core, boolean weAreReplacement) {
    log.info("Checking if I (core={},coreNodeName={}) should try and be the leader.", core.getName(),
        core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
    
    if (isClosed) {
      log.info("Bailing on leader process because we have been closed");
      return false;
    }
    
    if (!weAreReplacement) {
      // we are the first node starting in the shard - there is a configurable wait
      // to make sure others participate in sync and leader election, we can be leader
      return true;
    }
    
    if (core.getCoreDescriptor().getCloudDescriptor().getLastPublished().equals(ZkStateReader.ACTIVE)) {
      
      // maybe active but if the previous leader marked us as down and
      // we haven't recovered, then can't be leader
      String lirState = zkController.getLeaderInitiatedRecoveryState(collection, shardId,
          core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
      if (ZkStateReader.DOWN.equals(lirState) || ZkStateReader.RECOVERING.equals(lirState)) {
        log.warn("Although my last published state is Active, the previous leader marked me "+core.getName()
            + " as " + lirState
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

    zkClient.makePath(leaderPath, ZkStateReader.toJSON(myProps),
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
