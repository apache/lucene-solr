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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.PrepRecovery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.RequestHandlers.LazyRequestHandlerWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateLog.RecoveryInfo;
import org.apache.solr.update.UpdateLog.State;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryStrat {
  private static final int MAX_RETRIES = 10;

  private static final String REPLICATION_HANDLER = "/replication";

  private static Logger log = LoggerFactory.getLogger(RecoveryStrat.class);
  
  private volatile RecoveryListener recoveryListener;

  private volatile boolean close = false;
  
  private final AtomicInteger recoveryAttempts = new AtomicInteger();
  private final AtomicInteger recoverySuccesses = new AtomicInteger();
  
  private static final Lock RECOVERY_LOCK = new ReentrantLock();
  
  // for now, just for tests
  public interface RecoveryListener {
    public void startRecovery();
    public void finishedReplication();
    public void finishedRecovery();
  }
  
  // make sure any threads stop retrying
  public void close() {
    close = true;
  }
  
  // TODO: we want to be pretty noisy if we don't properly recover?
  public void recover(final SolrCore core) {
    
    log.info("Start recovery process");
    if (recoveryListener != null) recoveryListener.startRecovery();
    
    final ZkController zkController = core.getCoreDescriptor()
        .getCoreContainer().getZkController();
    final ZkStateReader zkStateReader = zkController.getZkStateReader();
    final String shardUrl = zkController.getShardUrl(core.getName());
    final String shardZkNodeName = zkController.getNodeName() + "_"
        + core.getName();
    final CloudDescriptor cloudDesc = core.getCoreDescriptor()
        .getCloudDescriptor();
    
    zkController.publishAsRecoverying(shardUrl, cloudDesc, shardZkNodeName);
    
    Thread thread = new Thread() {
      {
        setDaemon(true);
      }
      
      @Override
      public void run() {
        RECOVERY_LOCK.lock();
        try {
          UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
          // TODO: consider any races issues here - if we failed though, an
          // assert
          // exception is thrown if we are already buffering...
          if (ulog.getState() != UpdateLog.State.BUFFERING) {
            ulog.bufferUpdates();
          }
          
          boolean succesfulRecovery = false;
          int retries = 0;
          while (!succesfulRecovery && !close) {
            recoveryAttempts.incrementAndGet();
            try {
              
              String leaderUrl = zkStateReader.getLeaderUrl(
                  cloudDesc.getCollectionName(), cloudDesc.getShardId());
              System.out.println("leaderUrl:" + leaderUrl);
              System.out.println("shardUrl:" + shardUrl);
              
              replicate(core, shardZkNodeName, leaderUrl,
                  leaderUrl.equals(shardUrl));
              
              // nocommit:
              RefCounted<SolrIndexSearcher> searcher = core.getSearcher(true,
                  true, null);
              SolrIndexSearcher is = searcher.get();
              int afterReplicateDocs = 0;
              if (!core.isClosed()) {
                afterReplicateDocs = is.search(new MatchAllDocsQuery(), 1).totalHits;
                searcher.decref();
              }
              
              System.out.println("apply buffered updates");
              replay(core);
              System.out.println("replay done");
              
              // nocommit: remove this
              searcher = core.getSearcher(true, true, null);
              int afterReplayDocs = searcher.get().search(
                  new MatchAllDocsQuery(), 1).totalHits;
              searcher.decref();
              
              if (recoveryListener != null) recoveryListener.finishedRecovery();
              
              zkController
                  .publishAsActive(shardUrl, cloudDesc, shardZkNodeName);
              
              System.out.println("url: " + shardUrl + " docs after replicate: "
                  + afterReplicateDocs + " docs after replay:"
                  + afterReplayDocs + " leader:" + leaderUrl);
              // TODO: what if the problem was in onFinish.run which sets the
              // state?
              succesfulRecovery = true;
              recoverySuccesses.incrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              log.error("Recovery was interrupted", e);
              retries = MAX_RETRIES;
            } catch (Exception e) {
              log.error("Error while trying to recover", e);
            }
            
            if (!succesfulRecovery) {
              // lets pause for a moment and we need to try again...
              // TODO: we don't want to retry for some problems?
              // Or do a fall off retry...
              log.error("Recovery failed - trying again...");
              retries++;
              if (retries >= MAX_RETRIES) {
                // nocommit: for now, give up after 10 tries
                close = true;
              }
              
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          }
          log.info("Finished recovery process");
          System.out.println("recovery done: " + succesfulRecovery);
        } finally {
          RECOVERY_LOCK.unlock();
        }
      }
      
      private Future<RecoveryInfo> replay(final SolrCore core)
          throws InterruptedException, ExecutionException {
        Future<RecoveryInfo> future = core.getUpdateHandler().getUpdateLog()
            .applyBufferedUpdates();
        if (future == null) {
          // no replay needed\
          log.info("No replay needed");
        } else {
          // wait for replay
          future.get();
        }
        return future;
      }
    };
    thread.start();
  }
  
  private void replicate(SolrCore core, String shardZkNodeName, String leaderUrl, boolean iamleader)
      throws SolrServerException, IOException {
    System.out.println("replicate");
    // start buffer updates to tran log
    // and do recovery - either replay via realtime get (eventually)
    // or full index replication
   
    // if we are the leader, either we are trying to recover faster
    // then our ephemeral timed out or we are the only node
    if (!iamleader) {
      
      CommonsHttpSolrServer server = new CommonsHttpSolrServer(leaderUrl);
      System.out.println("send prep cmd");
      PrepRecovery prepCmd = new PrepRecovery();
      prepCmd.setAction(CoreAdminAction.PREPRECOVERY);
      // nocommit: the replica core name may not matcher the leader core name!
      prepCmd.setCoreName(core.getName());
      prepCmd.setNodeName(shardZkNodeName);
      
      server.request(prepCmd);
      
      
      // use rep handler directly, so we can do this sync rather than async
      SolrRequestHandler handler = core.getRequestHandler(REPLICATION_HANDLER);
      if (handler instanceof LazyRequestHandlerWrapper) {
        handler = ((LazyRequestHandlerWrapper)handler).getWrappedHandler();
      }
      ReplicationHandler replicationHandler = (ReplicationHandler) handler;
      
      if (replicationHandler == null) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Skipping recovery, no /replication handler found");
      }
      
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl + "replication");
      
      replicationHandler.doFetch(solrParams);
      
      
      System.out.println("recover from: " + leaderUrl + " ");
      
      if (recoveryListener != null) recoveryListener.finishedReplication();
    }
  }
  
  public RecoveryListener getRecoveryListener() {
    return recoveryListener;
  }

  public void setRecoveryListener(RecoveryListener recoveryListener) {
    this.recoveryListener = recoveryListener;
  }

  public AtomicInteger getRecoveryAttempts() {
    return recoveryAttempts;
  }

  public AtomicInteger getRecoverySuccesses() {
    return recoverySuccesses;
  }
}
