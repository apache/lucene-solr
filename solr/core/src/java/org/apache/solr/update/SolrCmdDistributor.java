package org.apache.solr.update;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequestExt;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;

// TODO: we are not really using the buffering anymore due to DistribUpdateProc...
// we might want to bring back a form of slots...
public class SolrCmdDistributor {
  // TODO: shut this thing down
  static ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  
  static HttpClient client;
  
  static {
    MultiThreadedHttpConnectionManager mgr = new MultiThreadedHttpConnectionManager();
    mgr.getParams().setDefaultMaxConnectionsPerHost(8);
    mgr.getParams().setMaxTotalConnections(200);
    client = new HttpClient(mgr);
  }
  
  CompletionService<Request> completionService;
  Set<Future<Request>> pending;
  
  //private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;

  //private final SchemaField idField;
  
  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 100;
  
  private List<AddUpdateCommand> alist;
  private ArrayList<DeleteUpdateCommand> dlist;

  public static class CmdRequest {
    public List<ShardInfo> shardInfos;
    public ModifiableSolrParams params;
    // we have to retry updates that are just being forwarded to the leader
    public boolean forwarding;
    
    public CmdRequest(List<ShardInfo> shardInfos, ModifiableSolrParams params) {
      this.shardInfos = shardInfos;
      this.params = params;
    }
    
    public CmdRequest(List<ShardInfo> shardInfos, ModifiableSolrParams params, boolean forwarding) {
      this(shardInfos, params);
      this.forwarding = forwarding;
    }
    
    public void updateUrlsForRetry() {
      for (ShardInfo shardInfo : shardInfos) {
        if (shardInfo.retryUrl != null) {
          shardInfo.url = shardInfo.retryUrl.getRetryUrl();
        }
      }
    }
  }
  
  public static class ShardInfo {
    public String url;
    public RetryUrl retryUrl;
  }
  
  public static interface RetryUrl {
    String getRetryUrl();
  }
  
  public SolrCmdDistributor(SolrQueryResponse rsp) {
    this.rsp = rsp;
  }
  
  public void finish(CmdRequest cmdRequest) {

    // piggyback on any outstanding adds or deletes if possible.
    flushAdds(1, null, cmdRequest);
    flushDeletes(1, null, cmdRequest);

    checkResponses(true, cmdRequest);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, CmdRequest cmdRequest) throws IOException {
    checkResponses(false, cmdRequest);
    
    if (cmd.isDeleteById()) {
      doDelete(cmd, cmdRequest);
    } else {
      // TODO: query must be broadcast to all ??
      doDelete(cmd, cmdRequest);
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, CmdRequest cmdRequest) throws IOException {
    
    checkResponses(false, cmdRequest);
    
    // make sure any pending deletes are flushed
    flushDeletes(1, null, cmdRequest);
    
    // TODO: this is brittle
    // need to make a clone since these commands may be reused
    AddUpdateCommand clone = new AddUpdateCommand(null);
    
    clone.solrDoc = cmd.solrDoc;
    clone.commitWithin = cmd.commitWithin;
    clone.overwrite = cmd.overwrite;
    clone.setVersion(cmd.getVersion());
    
    // nocommit: review as far as SOLR-2685
    // clone.indexedId = cmd.indexedId;
    // clone.doc = cmd.doc;
    

    if (alist == null) {
      alist = new ArrayList<AddUpdateCommand>(2);
    }
    alist.add(clone);
    
    flushAdds(maxBufferedAddsPerServer, null, cmdRequest);
  }
  
  public void distribCommit(CommitUpdateCommand cmd, CmdRequest cmdRequest)
      throws IOException {
    
    // Wait for all outstanding repsonses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true, cmdRequest);
    
    // piggyback on any outstanding adds or deletes if possible.
    // TODO: review this
    flushAdds(1, cmd, cmdRequest);
    
    flushDeletes(1, cmd, cmdRequest);
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(cmdRequest.params);

    addCommit(ureq, cmd);
    submit(ureq, cmdRequest);
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    // nocommit
    if (/* cmd.waitFlush || */cmd.waitSearcher) {
      checkResponses(true, cmdRequest);
    }
  }
  
  private void doDelete(DeleteUpdateCommand cmd, CmdRequest cmdRequest) throws IOException {
    
    flushAdds(1, null, cmdRequest);
    
    if (dlist == null) {
      dlist = new ArrayList<DeleteUpdateCommand>(2);
    }
    dlist.add(clone(cmd));
    
    flushDeletes(maxBufferedDeletesPerServer, null, cmdRequest);
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    // nocommit
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher);
  }
  
  boolean flushAdds(int limit, CommitUpdateCommand ccmd, CmdRequest cmdRequest) {
    // check for pending deletes
    if (alist == null || alist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(cmdRequest.params);
    
    addCommit(ureq, ccmd);
    
    for (AddUpdateCommand cmd : alist) {
      ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
    }
    
    alist = null;
    submit(ureq, cmdRequest);
    return true;
  }
  
  boolean flushDeletes(int limit, CommitUpdateCommand ccmd, CmdRequest cmdRequest) {
    // check for pending deletes
    if (dlist == null || dlist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(cmdRequest.params);

    addCommit(ureq, ccmd);
    
    for (DeleteUpdateCommand cmd : dlist) {
      if (cmd.isDeleteById()) {
        ureq.deleteById(cmd.getId(), cmd.getVersion());
      } else {
        ureq.deleteByQuery(cmd.query);
      }
    }
    
    dlist = null;
    submit(ureq, cmdRequest);
    return true;
  }
  
  private DeleteUpdateCommand clone(DeleteUpdateCommand cmd) {
    DeleteUpdateCommand c = (DeleteUpdateCommand)cmd.clone();
    // TODO: shouldnt the clone do this?
    c.setFlags(cmd.getFlags());
    c.setVersion(cmd.getVersion());
    return c;
  }
  
  static class Request {
    // TODO: we may need to look at deep cloning this?
    CmdRequest cmdRequest;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    Exception exception;
    String url;
    int retries = 0;
  }
  
  void submit(UpdateRequestExt ureq, CmdRequest cmdRequest) {
    Request sreq = new Request();
    sreq.cmdRequest = cmdRequest;
    sreq.ureq = ureq;
    submit(sreq);
  }
  
  void submit(final Request sreq) {
    if (completionService == null) {
      completionService = new ExecutorCompletionService<Request>(commExecutor);
      pending = new HashSet<Future<Request>>();
    }

    for (final ShardInfo shardInfo : sreq.cmdRequest.shardInfos) {
      // TODO: when we break up shards, we might forward
      // to self again - makes things simple here, but we could
      // also have realized this before, done the req locally, and
      // removed self from this list.
      
      Callable<Request> task = new Callable<Request>() {
        @Override
        public Request call() throws Exception {
          Request clonedRequest = new Request();
          clonedRequest.cmdRequest = sreq.cmdRequest;
          clonedRequest.ureq = sreq.ureq;
          clonedRequest.url = shardInfo.url;
          try {
            // TODO: what about https?
            String url;
            if (!shardInfo.url.startsWith("http://")) {
              url = "http://" + shardInfo.url;
            } else {
              url = shardInfo.url;
            }

            SolrServer server = new CommonsHttpSolrServer(url, client);
            clonedRequest.ursp = server.request(clonedRequest.ureq);
            
            // currently no way to get the request body.
          } catch (Exception e) {
            clonedRequest.exception = e;
            if (e instanceof SolrException) {
              clonedRequest.rspCode = ((SolrException) e).code();
            } else {
              clonedRequest.rspCode = -1;
            }
          }
          return clonedRequest;
        }
      };
      
      pending.add(completionService.submit(task));
    }
  }
  
  void checkResponses(boolean block, CmdRequest cmdRequest) {
    
    int expectedResponses = pending == null ? 0 : pending.size();
    int nonConnectionErrors = 0;
    int failed = 0;
    Request failedFowardingRequest = null;
    while (pending != null && pending.size() > 0) {
      try {
        Future<Request> future = block ? completionService.take()
            : completionService.poll();
        if (future == null) return;
        pending.remove(future);
        
        try {
          Request sreq = future.get();
          if (sreq.rspCode != 0) {
            // error during request
            Exception e = sreq.exception;
            
            // if it failed due to connect, assume we simply have not yet
            // learned it is down TODO: how about if we are cut off? Are we assuming too much?
            // the problem is there are other exceptions thrown due to a machine going down mid connection... I've
            // seen Interrupted exceptions.
            
            // nocommit:
            // we have to match against the msg...:(
            if (!e.getMessage().contains(
                "java.net.ConnectException: Connection refused")
                || e.getMessage().contains(
                    "java.net.SocketException: Connection reset")) nonConnectionErrors++;

            failed++;
            // use the first exception encountered
            // TODO: perhaps we should do more?
            if (rsp.getException() == null) {
              
              String newMsg = "shard update error (" + sreq.cmdRequest.shardInfos + "):"
                  + e.getMessage();
              if (e instanceof SolrException) {
                SolrException se = (SolrException) e;
                e = new SolrException(ErrorCode.getErrorCode(se.code()),
                    newMsg, se.getCause());
              } else {
                e = new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    newMsg, e);
              }
              rsp.setException(e);
            }
            
            if (cmdRequest.forwarding) {
              // this shold be fine because forwarding requests are only to one shard
              failedFowardingRequest = sreq;
            }
            
            SolrException.logOnce(SolrCore.log, "shard update error " + sreq.url + " ("
                + sreq.cmdRequest.shardInfos + ")", sreq.exception);
          }
          
        } catch (ExecutionException e) {
          // shouldn't happen since we catch exceptions ourselves
          SolrException.log(SolrCore.log,
              "error sending update request to shard", e);
        }
        
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
            "interrupted waiting for shard update response", e);
      }
    }
//    if (failed > 0) {
//      System.out.println("expected:" + expectedResponses + " failed:" + failed + " failedAfterConnect:" + nonConnectionErrors + " forward:" + forwarding);
//    }
    // TODO: this is a somewhat weak success guarantee - if the request was successful on every replica considered up
    // and that does not return a connect exception, it was successful.
    //should we optionally fail when there is only a single leader for a shard? (no replication)
    
    // TODO: now we should tell those that failed to try and recover?

    if (failed > 0 && nonConnectionErrors == 0) {
      if (failed == expectedResponses && cmdRequest.forwarding) {
        // this is a pure forwarding request (single url for the leader) and it fully failed -
        // don't reset the exception - TODO: most likely there is now a new
        // leader - first we should retry the request...
        
        // TODO: we really need to clean this up and apis that allow it...
        if (failedFowardingRequest != null) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }

          failedFowardingRequest.cmdRequest.updateUrlsForRetry();
          failedFowardingRequest.retries++;

          if (failedFowardingRequest.retries < 10) {
            rsp.setException(null);
            submit(failedFowardingRequest);
            checkResponses(block, cmdRequest);
          }
        }
      } else {
        // System.out.println("clear exception");
        rsp.setException(null);
      }
    }
  }
}
