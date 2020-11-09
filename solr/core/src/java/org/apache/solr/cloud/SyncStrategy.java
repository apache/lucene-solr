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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.PeerSync;
import org.apache.solr.update.UpdateShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.DISTRIB;

public class SyncStrategy implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final ShardHandler shardHandler;

  private volatile boolean isClosed;

  private volatile ZkController zkController;
  
  public SyncStrategy(CoreContainer cc) {
    // don't track currently - can be left open by a late election,
    // but currently holds no resources to release anyway
    // assert ObjectReleaseTracker.track(this);
    shardHandler = cc.getShardHandlerFactory().getShardHandler();
  }
  
  private static class ShardCoreRequest extends ShardRequest {
    String coreName;
    public String baseUrl;
  }
  
  public PeerSync.PeerSyncResult sync(ZkController zkController, SolrCore core, Replica leaderProps) {
    return sync(zkController, core, leaderProps, false);
  }
  
  public PeerSync.PeerSyncResult sync(ZkController zkController, SolrCore core, Replica leaderProps,
      boolean peerSyncOnlyWithActive) {
    if (SKIP_AUTO_RECOVERY) {
      return PeerSync.PeerSyncResult.success();
    }

    if (log.isInfoEnabled()) {
      log.info("Sync replicas to {}", ZkCoreNodeProps.getCoreUrl(leaderProps));
    }

    if (core.getUpdateHandler().getUpdateLog() == null) {
      log.error("No UpdateLog found - cannot sync");
      return PeerSync.PeerSyncResult.failure();
    }
    this.zkController = zkController;
    return syncReplicas(zkController, core, leaderProps, peerSyncOnlyWithActive);
  }

  private PeerSync.PeerSyncResult syncReplicas(ZkController zkController, SolrCore core,
      Replica leaderProps, boolean peerSyncOnlyWithActive) {

    boolean success = false;
    PeerSync.PeerSyncResult result = null;
    assert core != null;
    assert core.getCoreDescriptor() != null;
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();

    // first sync ourselves - we are the potential leader after all
    try {
      result = syncWithReplicas(zkController, core, leaderProps, collection,
          shardId, peerSyncOnlyWithActive);
      success = result.isSuccess();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      SolrException.log(log, "Sync Failed", e);
    }
    try {
      
      if (success) {
        log.info("Sync Success - now sync replicas to me");
        
        syncToMe(zkController, collection, shardId, leaderProps, core.getCoreDescriptor(), core.getUpdateHandler().getUpdateLog().getNumRecordsToKeep());
        
      } else {
        log.info("Leader's attempt to sync with shard failed, moving to the next candidate");
        // lets see who seems ahead...
      }
      
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      SolrException.log(log, "Sync Failed", e);
    }
    
    return result == null ? PeerSync.PeerSyncResult.failure() : result;
  }
  
  private PeerSync.PeerSyncResult syncWithReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps props, String collection, String shardId, boolean peerSyncOnlyWithActive) throws Exception {

    List<Replica> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,core.getCoreDescriptor().getName());
    
    if (nodes == null) {
      // I have no replicas
      return PeerSync.PeerSyncResult.success();
    }
    
    List<String> syncWith = new ArrayList<>(nodes.size());
    for (Replica node : nodes) {
      syncWith.add(node.getCoreUrl());
    }
    
    // if we can't reach a replica for sync, we still consider the overall sync a success
    // TODO: as an assurance, we should still try and tell the sync nodes that we couldn't reach
    // to recover once more?
    // Fingerprinting here is off because the we currently rely on having at least one of the nodes return "true", and if replicas are out-of-sync
    // we still need to pick one as leader.  A followup sync from the replica to the new leader (with fingerprinting on) should then fail and
    // initiate recovery-by-replication.
    try (PeerSync peerSync = new PeerSync(core, syncWith, core.getUpdateHandler().getUpdateLog().getNumRecordsToKeep(), true, peerSyncOnlyWithActive, false)) {
      return peerSync.sync();
    }
  }
  
  private void syncToMe(ZkController zkController, String collection,
                        String shardId, Replica leaderProps, CoreDescriptor cd,
                        int nUpdates) {

    // sync everyone else
    // TODO: we should do this in parallel at least
    List<Replica> nodes = zkController
        .getZkStateReader()
        .getReplicaProps(collection, shardId,
            cd.getName());
    if (nodes == null) {
      if (log.isInfoEnabled()) {
        log.info("{} has no replicas", ZkCoreNodeProps.getCoreUrl(leaderProps));
      }
      return;
    }


    for (Replica node : nodes) {
      try {
        if (log.isInfoEnabled()) {
          log.info("{}: try and ask {} to sync", ZkCoreNodeProps.getCoreUrl(leaderProps), node.getCoreUrl());
        }
        
        requestSync(node.getBaseUrl(), node.getCoreUrl(), leaderProps.getCoreUrl(), node.getName(), nUpdates);
        
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        SolrException.log(log, "Error syncing replica to leader", e);
      }
    }
    
    if (nodes.size() > 0) {
      for (; ; ) {
        ShardResponse srsp = shardHandler.takeCompletedOrError();
        if (srsp == null) break;
        boolean success = handleResponse(srsp);
        if (srsp.getException() != null) {
          SolrException.log(log, "Sync request error: " + srsp.getException());
        }

        if (!success) {
          if (log.isInfoEnabled()) {
            log.info("{}: Sync failed - replica ({}) should try to recover.", ZkCoreNodeProps.getCoreUrl(leaderProps), srsp.getShardAddress());
          }
        } else {
          if (log.isInfoEnabled()) {
            log.info("{}: sync completed with {}", ZkCoreNodeProps.getCoreUrl(leaderProps), srsp.getShardAddress());
          }
        }
      }
    }

  }
  
  private boolean handleResponse(ShardResponse srsp) {
    NamedList<Object> response = srsp.getSolrResponse().getResponse();
    // TODO: why does this return null sometimes?
    if (response == null) {
      return false;
    }
    Boolean success = (Boolean) response.get("sync");
    
    if (success == null) {
      success = false;
    }
    
    return success;
  }

  private void requestSync(String baseUrl, String replica, String leaderUrl, String coreName, int nUpdates) {
    //TODO should we use peerSyncWithLeader instead?
    ShardCoreRequest sreq = new ShardCoreRequest();
    sreq.coreName = coreName;
    sreq.baseUrl = baseUrl;
    sreq.purpose = 1;
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set(DISTRIB,false);
    sreq.params.set("getVersions",Integer.toString(nUpdates));
    sreq.params.set("sync",leaderUrl);
    
    shardHandler.submit(sreq, replica, sreq.params);
  }
  
  public void close() {
    this.isClosed = true;
    // don't track currently - can be left open by a late election,
    // but currently holds no resources to release anyway
    // assert ObjectReleaseTracker.release(this);
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }
}
