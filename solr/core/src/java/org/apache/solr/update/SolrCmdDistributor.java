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
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequestExt;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;

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
  //private final SolrQueryResponse rsp;

  //private final SchemaField idField;
  
  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 100;
  
  private List<AddUpdateCommand> alist;
  private ArrayList<DeleteUpdateCommand> dlist;

  Response response = new Response();
  
  public static class ShardInfo {
    public String url;
    public RetryUrl retryUrl;
    @Override
    public String toString() {
      return "ShardInfo [url=" + url + "]";
    }
  }
  
  public static interface RetryUrl {
    String getRetryUrl();
  }
  
  public SolrCmdDistributor() {
    //this.rsp = rsp;
  }
  
  public Response finish(List<String> urls, ModifiableSolrParams params) {

    // piggyback on any outstanding adds or deletes if possible.
    flushAdds(1, null, urls, params);
    flushDeletes(1, null, urls, params);

    return checkResponses(true, urls);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<String> urls, ModifiableSolrParams params) throws IOException {
    checkResponses(false, urls);
    
    if (cmd.isDeleteById()) {
      doDelete(cmd, urls, params);
    } else {
      // TODO: query must be broadcast to all ??
      doDelete(cmd, urls, params);
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<String> urls, ModifiableSolrParams params) throws IOException {
    
    checkResponses(false, urls);
    
    // make sure any pending deletes are flushed
    flushDeletes(1, null, urls, params);
    
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
    
    flushAdds(maxBufferedAddsPerServer, null, urls, params);
  }
  
  public void distribCommit(CommitUpdateCommand cmd, List<String> urls, ModifiableSolrParams params)
      throws IOException {
    
    // Wait for all outstanding repsonses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true, urls);
    
    // piggyback on any outstanding adds or deletes if possible.
    // TODO: review this
    flushAdds(1, cmd, urls, params);
    
    flushDeletes(1, cmd, urls, params);
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(params);

    addCommit(ureq, cmd);
    submit(ureq, urls);
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    // nocommit
    if (/* cmd.waitFlush || */cmd.waitSearcher) {
      checkResponses(true, urls);
    }
  }
  
  private void doDelete(DeleteUpdateCommand cmd, List<String> urls, ModifiableSolrParams params) throws IOException {
    
    flushAdds(1, null, urls, params);
    
    if (dlist == null) {
      dlist = new ArrayList<DeleteUpdateCommand>(2);
    }
    dlist.add(clone(cmd));
    
    flushDeletes(maxBufferedDeletesPerServer, null, urls, params);
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    // nocommit
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher);
  }
  
  boolean flushAdds(int limit, CommitUpdateCommand ccmd, List<String> urls, ModifiableSolrParams params) {
    // check for pending deletes
    if (alist == null || alist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(params);
    
    addCommit(ureq, ccmd);
    
    for (AddUpdateCommand cmd : alist) {
      ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
    }
    
    alist = null;
    submit(ureq, urls);
    return true;
  }
  
  boolean flushDeletes(int limit, CommitUpdateCommand ccmd, List<String> urls, ModifiableSolrParams params) {
    // check for pending deletes
    if (dlist == null || dlist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(params);

    addCommit(ureq, ccmd);
    
    for (DeleteUpdateCommand cmd : dlist) {
      if (cmd.isDeleteById()) {
        ureq.deleteById(cmd.getId(), cmd.getVersion());
      } else {
        ureq.deleteByQuery(cmd.query);
      }
    }
    
    dlist = null;
    submit(ureq, urls);
    return true;
  }
  
  private DeleteUpdateCommand clone(DeleteUpdateCommand cmd) {
    DeleteUpdateCommand c = (DeleteUpdateCommand)cmd.clone();
    // TODO: shouldnt the clone do this?
    c.setFlags(cmd.getFlags());
    c.setVersion(cmd.getVersion());
    return c;
  }
  
  public static class Request {
    // TODO: we may need to look at deep cloning this?
    public List<String> urls;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    Exception exception;
    String url;
    int retries = 0;
  }
  
  void submit(UpdateRequestExt ureq, List<String> urls) {
    Request sreq = new Request();
    sreq.urls = urls;
    sreq.ureq = ureq;
    submit(sreq);
  }
  
 public void submit(final Request sreq) {
    if (completionService == null) {
      completionService = new ExecutorCompletionService<Request>(commExecutor);
      pending = new HashSet<Future<Request>>();
    }

    for (final String url : sreq.urls) {
      // TODO: when we break up shards, we might forward
      // to self again - makes things simple here, but we could
      // also have realized this before, done the req locally, and
      // removed self from this list.
      
      Callable<Request> task = new Callable<Request>() {
        @Override
        public Request call() throws Exception {
          Request clonedRequest = new Request();
          clonedRequest.urls = sreq.urls;
          clonedRequest.ureq = sreq.ureq;
          clonedRequest.url = url;
          clonedRequest.retries = sreq.retries;
          
          try {
            // TODO: what about https?
            String fullUrl;
            if (!url.startsWith("http://")) {
              fullUrl = "http://" + url;
            } else {
              fullUrl = url;
            }

            CommonsHttpSolrServer server = new CommonsHttpSolrServer(fullUrl, client);
   
            //server.setSoTimeout(1000);
            //server.setConnectionTimeout(1000);
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
  
  Response checkResponses(boolean block, List<String> urls) {


    while (pending != null && pending.size() > 0) {
      try {
        Future<Request> future = block ? completionService.take()
            : completionService.poll();
        if (future == null) return null;
        pending.remove(future);
        
        try {
          Request sreq = future.get();
          if (sreq.rspCode != 0) {
            // error during request
            Exception e = sreq.exception;
            Error error = new Error();
            error.e = e;
            error.url = sreq.url;
            response.errors.add(error);
            response.sreq = sreq;
            SolrException.logOnce(SolrCore.log, "shard update error " + sreq.url + " ("
                + sreq.urls + ")", sreq.exception);
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

    return response;
    
  }
  
  public static class Response {
    public Request sreq;
    public List<Error> errors = new ArrayList<Error>();
  }
  
  public static class Error {
    public String url;
    public Exception e;
  }
}
