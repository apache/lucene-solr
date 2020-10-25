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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
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

public class SyncStrategy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final ShardHandler shardHandler;

  private volatile boolean isClosed;
  
  private final HttpClient client;

  private final ExecutorService updateExecutor;
  
  private final List<RecoveryRequest> recoveryRequests = new ArrayList<>();
  
  private static class RecoveryRequest {
    ZkNodeProps leaderProps;
    String baseUrl;
    String coreName;
  }
  
  public SyncStrategy(CoreContainer cc) {
    UpdateShardHandler updateShardHandler = cc.getUpdateShardHandler();
    client = updateShardHandler.getDefaultHttpClient();
    shardHandler = ((HttpShardHandlerFactory)cc.getShardHandlerFactory()).getShardHandler(cc.getUpdateShardHandler().getDefaultHttpClient());
    updateExecutor = updateShardHandler.getUpdateExecutor();
  }
  
  private static class ShardCoreRequest extends ShardRequest {
    String coreName;
    public String baseUrl;
  }
  
  public PeerSync.PeerSyncResult sync(ZkController zkController, SolrCore core, ZkNodeProps leaderProps) {
    return sync(zkController, core, leaderProps, false);
  }
  
  public PeerSync.PeerSyncResult sync(ZkController zkController, SolrCore core, ZkNodeProps leaderProps,
      boolean peerSyncOnlyWithActive) {
    if (SKIP_AUTO_RECOVERY) {
      return PeerSync.PeerSyncResult.success();
    }

    if (isClosed) {
      log.warn("Closed, skipping sync up.");
      return PeerSync.PeerSyncResult.failure();
    }

    recoveryRequests.clear();

    if (log.isInfoEnabled()) {
      log.info("Sync replicas to {}", ZkCoreNodeProps.getCoreUrl(leaderProps));
    }

    if (core.getUpdateHandler().getUpdateLog() == null) {
      log.error("No UpdateLog found - cannot sync");
      return PeerSync.PeerSyncResult.failure();
    }

    return syncReplicas(zkController, core, leaderProps, peerSyncOnlyWithActive);
  }
  
  private PeerSync.PeerSyncResult syncReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps, boolean peerSyncOnlyWithActive) {
    if (isClosed) {
      log.info("We have been closed, won't sync with replicas");
      return PeerSync.PeerSyncResult.failure();
    }
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
      SolrException.log(log, "Sync Failed", e);
    }
    try {
      if (isClosed) {
        log.info("We have been closed, won't attempt to sync replicas back to leader");
        return PeerSync.PeerSyncResult.failure();
      }
      
      if (success) {
        log.info("Sync Success - now sync replicas to me");
        
        syncToMe(zkController, collection, shardId, leaderProps, core.getCoreDescriptor(), core.getUpdateHandler().getUpdateLog().getNumRecordsToKeep());
        
      } else {
        log.info("Leader's attempt to sync with shard failed, moving to the next candidate");
        // lets see who seems ahead...
      }
      
    } catch (Exception e) {
      SolrException.log(log, "Sync Failed", e);
    }
    
    return result == null ? PeerSync.PeerSyncResult.failure() : result;
  }
  
  private PeerSync.PeerSyncResult syncWithReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps props, String collection, String shardId, boolean peerSyncOnlyWithActive) throws Exception {
    List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
    
    if (isClosed) {
      log.info("We have been closed, won't sync with replicas");
      return PeerSync.PeerSyncResult.failure();
    }
    
    if (nodes == null) {
      // I have no replicas
      return PeerSync.PeerSyncResult.success();
    }
    
    List<String> syncWith = new ArrayList<>(nodes.size());
    for (ZkCoreNodeProps node : nodes) {
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
                        String shardId, ZkNodeProps leaderProps, CoreDescriptor cd,
                        int nUpdates) {
    
    if (isClosed) {
      log.info("We have been closed, won't sync replicas to me.");
      return;
    }
    
    // sync everyone else
    // TODO: we should do this in parallel at least
    List<ZkCoreNodeProps> nodes = zkController
        .getZkStateReader()
        .getReplicaProps(collection, shardId,
            cd.getCloudDescriptor().getCoreNodeName());
    if (nodes == null) {
      if (log.isInfoEnabled()) {
        log.info("{} has no replicas", ZkCoreNodeProps.getCoreUrl(leaderProps));
      }
      return;
    }

    ZkCoreNodeProps zkLeader = new ZkCoreNodeProps(leaderProps);
    for (ZkCoreNodeProps node : nodes) {
      try {
        if (log.isInfoEnabled()) {
          log.info("{}: try and ask {} to sync", ZkCoreNodeProps.getCoreUrl(leaderProps), node.getCoreUrl());
        }
        
        requestSync(node.getBaseUrl(), node.getCoreUrl(), zkLeader.getCoreUrl(), node.getCoreName(), nUpdates);
        
      } catch (Exception e) {
        SolrException.log(log, "Error syncing replica to leader", e);
      }
    }
    
    
    for(;;) {
      ShardResponse srsp = shardHandler.takeCompletedOrError();
      if (srsp == null) break;
      boolean success = handleResponse(srsp);
      if (srsp.getException() != null) {
        SolrException.log(log, "Sync request error: " + srsp.getException());
      }
      
      if (!success) {
        if (log.isInfoEnabled()) {
          log.info("{}: Sync failed - we will ask replica ({}) to recover."
              , ZkCoreNodeProps.getCoreUrl(leaderProps), srsp.getShardAddress());
        }
        if (isClosed) {
          log.info("We have been closed, don't request that a replica recover");
        } else {
          RecoveryRequest rr = new RecoveryRequest();
          rr.leaderProps = leaderProps;
          rr.baseUrl = ((ShardCoreRequest) srsp.getShardRequest()).baseUrl;
          rr.coreName = ((ShardCoreRequest) srsp.getShardRequest()).coreName;
          recoveryRequests.add(rr);
        }
      } else {
        if (log.isInfoEnabled()) {
          log.info("{}: sync completed with {}", ZkCoreNodeProps.getCoreUrl(leaderProps), srsp.getShardAddress());
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
  }
  
  public void requestRecoveries() {
    for (RecoveryRequest rr : recoveryRequests) {
      try {
        requestRecovery(rr.leaderProps, rr.baseUrl, rr.coreName);
      } catch (SolrServerException | IOException e) {
        log.error("Problem requesting that a replica recover", e);
      }
    }
  }
  
  private void requestRecovery(final ZkNodeProps leaderProps, final String baseUrl, final String coreName) throws SolrServerException, IOException {
    Thread thread = new Thread() {
      {
        setDaemon(true);
      }
      @Override
      public void run() {
        
        if (isClosed) {
          log.info("We have been closed, won't request recovery");
          return;
        }
        RequestRecovery recoverRequestCmd = new RequestRecovery();
        recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
        recoverRequestCmd.setCoreName(coreName);
        
        try (HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
            .withHttpClient(SyncStrategy.this.client)
            .withConnectionTimeout(30000)
            .withSocketTimeout(120000)
            .build()) {
          client.request(recoverRequestCmd);
        } catch (Throwable t) {
          SolrException.log(log, ZkCoreNodeProps.getCoreUrl(leaderProps) + ": Could not tell a replica to recover", t);
          if (t instanceof Error) {
            throw (Error) t;
          }
        }
      }
    };
    updateExecutor.execute(thread);
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }
}
