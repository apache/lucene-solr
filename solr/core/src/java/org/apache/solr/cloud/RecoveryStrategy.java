package org.apache.solr.cloud;

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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.PrepRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.RequestHandlers.LazyRequestHandlerWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryStrategy extends Thread {
  private static final int MAX_RETRIES = 100;
  private static final int INTERRUPTED = 101;
  private static final int START_TIMEOUT = 100;
  
  private static final String REPLICATION_HANDLER = "/replication";

  private static Logger log = LoggerFactory.getLogger(RecoveryStrategy.class);

  private volatile boolean close = false;

  private ZkController zkController;
  private String baseUrl;
  private String coreZkNodeName;
  private ZkStateReader zkStateReader;
  private volatile String coreName;
  private int retries;
  private SolrCore core;
  
  public RecoveryStrategy(SolrCore core) {
    this.core = core;
    this.coreName = core.getName();
    setName("RecoveryThread");
    zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();
    coreZkNodeName = zkController.getNodeName() + "_" + coreName;
    
  }
  
  // make sure any threads stop retrying
  public void close() {
    close = true;
    interrupt();
  }

  
  private void recoveryFailed(final SolrCore core,
      final ZkController zkController, final String baseUrl,
      final String shardZkNodeName, final CoreDescriptor cd) {
    SolrException.log(log, "Recovery failed - I give up.");
    zkController.publishAsRecoveryFailed(baseUrl, cd,
        shardZkNodeName, core.getName());
    close = true;
  }
  
  private void replicate(String nodeName, SolrCore core, String shardZkNodeName, ZkNodeProps leaderprops, String baseUrl)
      throws SolrServerException, IOException {
    // start buffer updates to tran log
    // and do recovery - either replay via realtime get (eventually)
    // or full index replication
   
    String leaderBaseUrl = leaderprops.get(ZkStateReader.BASE_URL_PROP);
    ZkCoreNodeProps leaderCNodeProps = new ZkCoreNodeProps(leaderprops);
    String leaderUrl = leaderCNodeProps.getCoreUrl();
    String leaderCoreName = leaderCNodeProps.getCoreName();
    
    log.info("Attempt to replicate from " + leaderUrl);
    
    // if we are the leader, either we are trying to recover faster
    // then our ephemeral timed out or we are the only node
    if (!leaderBaseUrl.equals(baseUrl)) {
      
      CommonsHttpSolrServer server = new CommonsHttpSolrServer(leaderBaseUrl);
      server.setSoTimeout(15000);
      PrepRecovery prepCmd = new PrepRecovery();
      prepCmd.setCoreName(leaderCoreName);
      prepCmd.setNodeName(nodeName);
      prepCmd.setCoreNodeName(shardZkNodeName);
      
      server.request(prepCmd);
      server.shutdown();
      
      // use rep handler directly, so we can do this sync rather than async
      SolrRequestHandler handler = core.getRequestHandler(REPLICATION_HANDLER);
      if (handler instanceof LazyRequestHandlerWrapper) {
        handler = ((LazyRequestHandlerWrapper)handler).getWrappedHandler();
      }
      ReplicationHandler replicationHandler = (ReplicationHandler) handler;
      
      if (replicationHandler == null) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
            "Skipping recovery, no " + REPLICATION_HANDLER + " handler found");
      }
      
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl + "replication");
      
      if (close) retries = INTERRUPTED; 
      boolean success = replicationHandler.doFetch(solrParams, true); // TODO: look into making sure fore=true does not download files we already have

      if (!success) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Replication for recovery failed.");
      }
      
      // solrcloud_debug
//      try {
//        RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//        SolrIndexSearcher searcher = searchHolder.get();
//        try {
//          System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " replicated "
//              + searcher.search(new MatchAllDocsQuery(), 1).totalHits + " from " + leaderUrl + " gen:" + core.getDeletionPolicy().getLatestCommit().getGeneration() + " data:" + core.getDataDir());
//        } finally {
//          searchHolder.decref();
//        }
//      } catch (Exception e) {
//        
//      }
    }
  }
  
  @Override
  public void run() {
    boolean replayed = false;
    boolean succesfulRecovery = false;
    
    while (!succesfulRecovery && !close && !isInterrupted()) {
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      if (ulog == null) return;
      
      ulog.bufferUpdates();
      replayed = false;
      CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
      try {
        zkController.publish(core, ZkStateReader.RECOVERING);
        
        ZkNodeProps leaderprops = zkStateReader.getLeaderProps(
            cloudDesc.getCollectionName(), cloudDesc.getShardId());
        
        // System.out.println("recover " + shardZkNodeName + " against " +
        // leaderprops);
        replicate(zkController.getNodeName(), core, coreZkNodeName,
            leaderprops, ZkCoreNodeProps.getCoreUrl(baseUrl, coreName));
        
        replay(ulog);
        replayed = true;
        
        // if there are pending recovery requests, don't advert as active
        zkController.publishAsActive(baseUrl, core.getCoreDescriptor(), coreZkNodeName,
            coreName);
        
        succesfulRecovery = true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Recovery was interrupted", e);
        retries = INTERRUPTED;
      } catch (Throwable t) {
        SolrException.log(log, "Error while trying to recover", t);
      } finally {
        if (!replayed) {
          try {
            ulog.dropBufferedUpdates();
          } catch (Throwable t) {
            SolrException.log(log, "", t);
          }
        }
        
      }
      
      if (!succesfulRecovery) {
        // lets pause for a moment and we need to try again...
        // TODO: we don't want to retry for some problems?
        // Or do a fall off retry...
        try {

          SolrException.log(log, "Recovery failed - trying again...");
          retries++;
          if (retries >= MAX_RETRIES) {
            if (retries == INTERRUPTED) {

            } else {
              // TODO: for now, give up after 10 tries - should we do more?
              recoveryFailed(core, zkController, baseUrl, coreZkNodeName,
                  core.getCoreDescriptor());
            }
            break;
          }
          
        } catch (Exception e) {
          SolrException.log(log, "", e);
        }
        
        try {
          Thread.sleep(Math.min(START_TIMEOUT * retries, 60000));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Recovery was interrupted", e);
          retries = INTERRUPTED;
        }
      }
      
      log.info("Finished recovery process");
      
    }
  }

  private Future<RecoveryInfo> replay(UpdateLog ulog)
      throws InterruptedException, ExecutionException, TimeoutException {
    Future<RecoveryInfo> future = ulog.applyBufferedUpdates();
    if (future == null) {
      // no replay needed\
      log.info("No replay needed");
    } else {
      // wait for replay
      future.get();
    }
    
    // solrcloud_debug
//    try {
//      RefCounted<SolrIndexSearcher> searchHolder = core.getNewestSearcher(false);
//      SolrIndexSearcher searcher = searchHolder.get();
//      try {
//        System.out.println(core.getCoreDescriptor().getCoreContainer().getZkController().getNodeName() + " replayed "
//            + searcher.search(new MatchAllDocsQuery(), 1).totalHits);
//      } finally {
//        searchHolder.decref();
//      }
//    } catch (Exception e) {
//      
//    }
    
    return future;
  }

}
