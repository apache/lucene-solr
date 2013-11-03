package org.apache.solr.cloud;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.update.PeerSync;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncStrategy {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  
  private final ShardHandler shardHandler;
  
  private ThreadPoolExecutor recoveryCmdExecutor = new ThreadPoolExecutor(
      0, Integer.MAX_VALUE, 5, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>(), new DefaultSolrThreadFactory(
          "recoveryCmdExecutor"));

  private volatile boolean isClosed;
  
  private final HttpClient client;
  {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 10000);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 20);
    params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 15000);
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 60000);
    params.set(HttpClientUtil.PROP_USE_RETRY, false);
    client = HttpClientUtil.createClient(params);
  }
  
  public SyncStrategy() {
    shardHandler = new HttpShardHandlerFactory().getShardHandler(client);
  }
  
  private static class ShardCoreRequest extends ShardRequest {
    String coreName;
    public String baseUrl;
  }
  
  public boolean sync(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    if (SKIP_AUTO_RECOVERY) {
      return true;
    }
    if (isClosed) {
      log.warn("Closed, skipping sync up.");
      return false;
    }
    log.info("Sync replicas to " + ZkCoreNodeProps.getCoreUrl(leaderProps));

    if (core.getUpdateHandler().getUpdateLog() == null) {
      log.error("No UpdateLog found - cannot sync");
      return false;
    }
    boolean success = syncReplicas(zkController, core, leaderProps);
    return success;
  }
  
  private boolean syncReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps leaderProps) {
    boolean success = false;
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();

    if (isClosed) {
      log.info("We have been closed, won't sync with replicas");
      return false;
    }
    
    // first sync ourselves - we are the potential leader after all
    try {
      success = syncWithReplicas(zkController, core, leaderProps, collection,
          shardId);
    } catch (Exception e) {
      SolrException.log(log, "Sync Failed", e);
    }
    try {
      if (isClosed) {
        log.info("We have been closed, won't attempt to sync replicas back to leader");
        return false;
      }
      
      if (success) {
        log.info("Sync Success - now sync replicas to me");
        
        syncToMe(zkController, collection, shardId, leaderProps, core.getCoreDescriptor());
        
      } else {
        log.info("Leader's attempt to sync with shard failed, moving to the next candidate");
        // lets see who seems ahead...
      }
      
    } catch (Exception e) {
      SolrException.log(log, "Sync Failed", e);
    }
    
    return success;
  }
  
  private boolean syncWithReplicas(ZkController zkController, SolrCore core,
      ZkNodeProps props, String collection, String shardId) {
    List<ZkCoreNodeProps> nodes = zkController.getZkStateReader()
        .getReplicaProps(collection, shardId,core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName(),
            props.getStr(ZkStateReader.CORE_NAME_PROP));
    
    if (nodes == null) {
      // I have no replicas
      return true;
    }
    
    List<String> syncWith = new ArrayList<String>();
    for (ZkCoreNodeProps node : nodes) {
      syncWith.add(node.getCoreUrl());
    }
    
    // if we can't reach a replica for sync, we still consider the overall sync a success
    // TODO: as an assurance, we should still try and tell the sync nodes that we couldn't reach
    // to recover once more?
    PeerSync peerSync = new PeerSync(core, syncWith, core.getUpdateHandler().getUpdateLog().numRecordsToKeep, true, true);
    return peerSync.sync();
  }
  
  private void syncToMe(ZkController zkController, String collection,
      String shardId, ZkNodeProps leaderProps, CoreDescriptor cd) {
    
    // sync everyone else
    // TODO: we should do this in parallel at least
    List<ZkCoreNodeProps> nodes = zkController
        .getZkStateReader()
        .getReplicaProps(collection, shardId,
            cd.getCloudDescriptor().getCoreNodeName(),
            leaderProps.getStr(ZkStateReader.CORE_NAME_PROP));
    if (nodes == null) {
      log.info(ZkCoreNodeProps.getCoreUrl(leaderProps) + " has no replicas");
      return;
    }

    ZkCoreNodeProps zkLeader = new ZkCoreNodeProps(leaderProps);
    for (ZkCoreNodeProps node : nodes) {
      try {
        log.info(ZkCoreNodeProps.getCoreUrl(leaderProps) + ": try and ask " + node.getCoreUrl() + " to sync");
        
        requestSync(node.getBaseUrl(), node.getCoreUrl(), zkLeader.getCoreUrl(), node.getCoreName());
        
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
         try {
           log.info(ZkCoreNodeProps.getCoreUrl(leaderProps) + ": Sync failed - asking replica (" + srsp.getShardAddress() + ") to recover.");
           if (isClosed) {
             log.info("We have been closed, don't request that a replica recover");
           } else {
             requestRecovery(leaderProps, ((ShardCoreRequest)srsp.getShardRequest()).baseUrl, ((ShardCoreRequest)srsp.getShardRequest()).coreName);
           }
         } catch (Throwable t) {
           SolrException.log(log, ZkCoreNodeProps.getCoreUrl(leaderProps) + ": Could not tell a replica to recover", t);
         }
      } else {
        log.info(ZkCoreNodeProps.getCoreUrl(leaderProps) + ": " + " sync completed with " + srsp.getShardAddress());
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

  private void requestSync(String baseUrl, String replica, String leaderUrl, String coreName) {
    ShardCoreRequest sreq = new ShardCoreRequest();
    sreq.coreName = coreName;
    sreq.baseUrl = baseUrl;
    sreq.purpose = 1;
    // TODO: this sucks
    if (replica.startsWith("http://"))
      replica = replica.substring(7);
    sreq.shards = new String[]{replica};
    sreq.actualShards = sreq.shards;
    sreq.params = new ModifiableSolrParams();
    sreq.params.set("qt","/get");
    sreq.params.set("distrib",false);
    sreq.params.set("getVersions",Integer.toString(100));
    sreq.params.set("sync",leaderUrl);
    
    shardHandler.submit(sreq, replica, sreq.params);
  }
  
  public void close() {
    this.isClosed = true;
    try {
      client.getConnectionManager().shutdown();
    } catch (Throwable e) {
      SolrException.log(log, e);
    }
    try {
      ExecutorUtil.shutdownNowAndAwaitTermination(recoveryCmdExecutor);
    } catch (Throwable e) {
      SolrException.log(log, e);
    }
  }
  
  private void requestRecovery(final ZkNodeProps leaderProps, final String baseUrl, final String coreName) throws SolrServerException, IOException {
    Thread thread = new Thread() {
      {
        setDaemon(true);
      }
      @Override
      public void run() {
        RequestRecovery recoverRequestCmd = new RequestRecovery();
        recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
        recoverRequestCmd.setCoreName(coreName);
        
        HttpSolrServer server = new HttpSolrServer(baseUrl, client);
        try {
          server.setConnectionTimeout(15000);
          server.setSoTimeout(60000);
          server.request(recoverRequestCmd);
        } catch (Throwable t) {
          SolrException.log(log, ZkCoreNodeProps.getCoreUrl(leaderProps) + ": Could not tell a replica to recover", t);
        } finally {
          server.shutdown();
        }
      }
    };
    recoveryCmdExecutor.execute(thread);
  }
  
  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }
}
