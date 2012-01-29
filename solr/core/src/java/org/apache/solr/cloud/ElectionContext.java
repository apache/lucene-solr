package org.apache.solr.cloud;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;

/**
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

public abstract class ElectionContext {
  
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;
  
  public ElectionContext(final String shardZkNodeName,
      final String electionPath, final String leaderPath, final ZkNodeProps leaderProps) {
    this.id = shardZkNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderProps = leaderProps;
  }
  
  abstract void runLeaderProcess(String leaderSeqPath, boolean weAreReplacement) throws KeeperException, InterruptedException, IOException;
}

class ShardLeaderElectionContextBase extends ElectionContext {
  
  protected final SolrZkClient zkClient;
  protected String shardId;
  protected String collection;
  protected LeaderElector leaderElector;

  public ShardLeaderElectionContextBase(LeaderElector leaderElector, final String shardId,
      final String collection, final String shardZkNodeName, ZkNodeProps props, ZkStateReader zkStateReader) {
    super(shardZkNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/leader_elect/"
        + shardId, ZkStateReader.getShardLeadersPath(collection, shardId),
        props);
    this.leaderElector = leaderElector;
    this.zkClient = zkStateReader.getZkClient();
    this.shardId = shardId;
    this.collection = collection;
  }

  @Override
  void runLeaderProcess(String leaderSeqPath, boolean weAreReplacement)
      throws KeeperException, InterruptedException, IOException {

    try {
      zkClient.makePath(leaderPath,
          leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
          CreateMode.EPHEMERAL, true);
    } catch (NodeExistsException e) {
      // if a previous leader ephemeral still exists for some reason, try and
      // remove it
      zkClient.delete(leaderPath, -1, true);
      zkClient.makePath(leaderPath,
          leaderProps == null ? null : ZkStateReader.toJSON(leaderProps),
          CreateMode.EPHEMERAL, true);
    }
  } 

}

// add core container and stop passing core around...
final class ShardLeaderElectionContext extends ShardLeaderElectionContextBase {
  private ZkController zkController;
  private CoreContainer cc;
  private SyncStrategy syncStrategy = new SyncStrategy();
  
  public ShardLeaderElectionContext(LeaderElector leaderElector, 
      final String shardId, final String collection,
      final String shardZkNodeName, ZkNodeProps props, ZkController zkController, CoreContainer cc) {
    super(leaderElector, shardId, collection, shardZkNodeName, props,
        zkController.getZkStateReader());
    this.zkController = zkController;
    this.cc = cc;
  }
  
  @Override
  void runLeaderProcess(String leaderSeqPath, boolean weAreReplacement)
      throws KeeperException, InterruptedException, IOException {
    if (cc != null) {
      SolrCore core = null;
      String coreName = leaderProps.get(ZkStateReader.CORE_NAME_PROP);
      try {
        core = cc.getCore(coreName);
        if (core == null) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Core not found:" + coreName);
        }
        // should I be leader?
        if (weAreReplacement && !shouldIBeLeader(leaderProps)) {
          // System.out.println("there is a better leader candidate it appears");
          rejoinLeaderElection(leaderSeqPath, core);
          return;
        }
        
        if (weAreReplacement) {
          if (zkClient.exists(leaderPath, true)) {
            zkClient.delete(leaderPath, -1, true);
          }
//          System.out.println("I may be the new Leader:" + leaderPath
//              + " - I need to try and sync");
          boolean success = syncStrategy.sync(zkController, core, leaderProps);
          if (!success) {
            // TODO: what if no one can be the leader in a loop?
            // perhaps we look down the list and if no one is active, we
            // accept leader role anyhow
            core.getUpdateHandler().getSolrCoreState().doRecovery(core);
            
            rejoinLeaderElection(leaderSeqPath, core);
            return;
          } 
        }
        
        // If I am going to be the leader I have to be active
        
        core.getUpdateHandler().getSolrCoreState().cancelRecovery();
        zkController.publish(core, ZkStateReader.ACTIVE);
        
      } finally {
        if (core != null) {
          core.close();
        }
      }
      
    }
    
    super.runLeaderProcess(leaderSeqPath, weAreReplacement);
  }

  private void rejoinLeaderElection(String leaderSeqPath, SolrCore core)
      throws InterruptedException, KeeperException, IOException {
    // remove our ephemeral and re join the election
   // System.out.println("sync failed, delete our election node:"
   //     + leaderSeqPath);
    zkController.publish(core, ZkStateReader.DOWN);
    zkClient.delete(leaderSeqPath, -1, true);
    
    core.getUpdateHandler().getSolrCoreState().doRecovery(core);
    
    leaderElector.joinElection(this);
  }
  
  private boolean shouldIBeLeader(ZkNodeProps leaderProps) {
    CloudState cloudState = zkController.getZkStateReader().getCloudState();
    Map<String,Slice> slices = cloudState.getSlices(this.collection);
    Slice slice = slices.get(shardId);
    Map<String,ZkNodeProps> shards = slice.getShards();
    boolean foundSomeoneElseActive = false;
    for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {
      String state = shard.getValue().get(ZkStateReader.STATE_PROP);

      if (new ZkCoreNodeProps(shard.getValue()).getCoreUrl().equals(
              new ZkCoreNodeProps(leaderProps).getCoreUrl())) {
        if (state.equals(ZkStateReader.ACTIVE)
          && cloudState.liveNodesContain(shard.getValue().get(
              ZkStateReader.NODE_NAME_PROP))) {
          // we are alive
          return true;
        }
      }
      
      if ((state.equals(ZkStateReader.ACTIVE))
          && cloudState.liveNodesContain(shard.getValue().get(
              ZkStateReader.NODE_NAME_PROP))
          && !new ZkCoreNodeProps(shard.getValue()).getCoreUrl().equals(
              new ZkCoreNodeProps(leaderProps).getCoreUrl())) {
        foundSomeoneElseActive = true;
      }
    }
    
    return !foundSomeoneElseActive;
  }
  
}

final class OverseerElectionContext extends ElectionContext {
  
  private final SolrZkClient zkClient;
  private final ZkStateReader stateReader;

  public OverseerElectionContext(final String zkNodeName, SolrZkClient zkClient, ZkStateReader stateReader) {
    super(zkNodeName, "/overseer_elect", "/overseer_elect/leader", null);
    this.zkClient = zkClient;
    this.stateReader = stateReader;
  }

  @Override
  void runLeaderProcess(String leaderSeqPath, boolean weAreReplacement) throws KeeperException, InterruptedException {
    
    final String id = leaderSeqPath.substring(leaderSeqPath.lastIndexOf("/")+1);
    ZkNodeProps myProps = new ZkNodeProps("id", id);

    try {
      zkClient.makePath(leaderPath,
          ZkStateReader.toJSON(myProps),
          CreateMode.EPHEMERAL, true);
    } catch (NodeExistsException e) {
      // if a previous leader ephemeral still exists for some reason, try and
      // remove it
      zkClient.delete(leaderPath, -1, true);
      zkClient.makePath(leaderPath,
          ZkStateReader.toJSON(myProps),
          CreateMode.EPHEMERAL, true);
    }
  
    new Overseer(zkClient, stateReader, id);
  }
  
}
