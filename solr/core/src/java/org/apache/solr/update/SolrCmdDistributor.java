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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.DistributedUpdateProcessor;

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
  
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;

  private final SchemaField idField;
  
  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 100;
  
  private List<AddUpdateCommand> alist;
  private ArrayList<DeleteUpdateCommand> dlist;
  
  public SolrCmdDistributor(SolrQueryRequest req,
      SolrQueryResponse rsp) {
    this.req = req;
    this.rsp = rsp;
    this.idField = req.getSchema().getUniqueKeyField();
  }
  
  public void finish(List<String> shards) {

    // piggyback on any outstanding adds or deletes if possible.
    flushAdds(1, null, shards);
    flushDeletes(1, null, shards);

    checkResponses(true);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<String> shards) throws IOException {
    checkResponses(false);
    
    if (cmd.id != null) {
      doDelete(cmd, shards);
    } else if (cmd.query != null) {
      // TODO: query must be broadcast to all ??
      doDelete(cmd, shards);
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<String> shards) throws IOException {
    
    checkResponses(false);
    
    SolrInputDocument doc = cmd.getSolrInputDocument();
    SolrInputField field = doc.getField(idField.getName());
    if (field == null) {
      throw new RuntimeException("no id field found");
    }
    
    // make sure any pending deletes are flushed
    flushDeletes(1, null, shards);
    
    // TODO: this is brittle
    // need to make a clone since these commands may be reused
    AddUpdateCommand clone = new AddUpdateCommand(req);
    
    clone.solrDoc = cmd.solrDoc;
    clone.commitWithin = cmd.commitWithin;
    clone.overwrite = cmd.overwrite;
    
    // nocommit: review as far as SOLR-2685
    // clone.indexedId = cmd.indexedId;
    // clone.doc = cmd.doc;
    

    if (alist == null) {
      alist = new ArrayList<AddUpdateCommand>(2);
    }
    alist.add(clone);
    
    flushAdds(maxBufferedAddsPerServer, null, shards);
  }
  
  public void distribCommit(CommitUpdateCommand cmd, List<String> shards)
      throws IOException {
    
    // Wait for all outstanding repsonses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true);
    
    // piggyback on any outstanding adds or deletes if possible.
    // TODO: review this
    flushAdds(1, cmd, shards);
    
    flushDeletes(1, cmd, shards);
    
    UpdateRequestExt ureq = new UpdateRequestExt();

    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }
    passOnParams(ureq);
    addCommit(ureq, cmd);
    submit(ureq, shards);
    
    // if (next != null && shardStr == null) next.processCommit(cmd);
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    // nocommit
    if (/* cmd.waitFlush || */cmd.waitSearcher) {
      checkResponses(true);
    }
  }

  private void passOnParams(UpdateRequestExt ureq) {
    String seenLeader = req.getParams().get(
        DistributedUpdateProcessor.SEEN_LEADER);
    if (seenLeader != null) {
      ureq.getParams().add(DistributedUpdateProcessor.SEEN_LEADER, seenLeader);
    }
    String updateChain = req.getParams().get(UpdateParams.UPDATE_CHAIN);
    if (updateChain != null) {
      ureq.getParams().add(UpdateParams.UPDATE_CHAIN, updateChain);
    }
  }
  
  private void doDelete(DeleteUpdateCommand cmd, List<String> shards) throws IOException {
    
    flushAdds(1, null, shards);
    
    if (dlist == null) {
      dlist = new ArrayList<DeleteUpdateCommand>(2);
    }
    dlist.add(clone(cmd));
    
    flushDeletes(maxBufferedDeletesPerServer, null, shards);
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    // nocommit
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher);
  }
  
  boolean flushAdds(int limit, CommitUpdateCommand ccmd, List<String> urls) {
    // check for pending deletes
    if (alist == null || alist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on seen leader
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }
    
    passOnParams(ureq);
    addCommit(ureq, ccmd);
    
    for (AddUpdateCommand cmd : alist) {
      ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
    }
    
    alist = null;
    submit(ureq, urls);
    return true;
  }
  
  boolean flushDeletes(int limit, CommitUpdateCommand ccmd, List<String> shards) {
    // check for pending deletes
    if (dlist == null || dlist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on version
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }
    
    passOnParams(ureq);
    addCommit(ureq, ccmd);
    
    for (DeleteUpdateCommand cmd : dlist) {
      if (cmd.id != null) {
        ureq.deleteById(cmd.id);
      }
      if (cmd.query != null) {
        ureq.deleteByQuery(cmd.query);
      }
    }
    
    dlist = null;
    submit(ureq, shards);
    return true;
  }
  
  // TODO: this is brittle
  private DeleteUpdateCommand clone(DeleteUpdateCommand cmd) {
    DeleteUpdateCommand c = new DeleteUpdateCommand(req);
    c.id = cmd.id;
    c.query = cmd.query;
    return c;
  }
  
  static class Request {
    // TODO: we may need to look at deep cloning this?
    List<String> shards;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    Exception exception;
  }
  
  void submit(UpdateRequestExt ureq, List<String> shards) {
    Request sreq = new Request();
    sreq.shards = shards;
    sreq.ureq = ureq;
    submit(sreq);
  }
  
  void submit(final Request sreq) {
    if (completionService == null) {
      completionService = new ExecutorCompletionService<Request>(commExecutor);
      pending = new HashSet<Future<Request>>();
    }

    for (final String shard : sreq.shards) {
      // TODO: when we break up shards, we might forward
      // to self again - makes things simple here, but we could
      // also have realized this before, done the req locally, and
      // removed self from this list.
      
      Callable<Request> task = new Callable<Request>() {
        @Override
        public Request call() throws Exception {
          Request clonedRequest = new Request();
          clonedRequest.shards = sreq.shards;
          clonedRequest.ureq = sreq.ureq;
          
          try {
            // TODO: what about https?
            String url;
            if (!shard.startsWith("http://")) {
              url = "http://" + shard;
            } else {
              url = shard;
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
  
  void checkResponses(boolean block) {
    
    int expectedResponses = pending == null ? 0 : pending.size();
    int failed = 0;
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
            failed++;
            // use the first exception encountered
            if (rsp.getException() == null) {
              Exception e = sreq.exception;
              String newMsg = "shard update error (" + sreq.shards + "):"
                  + e.getMessage();
              if (e instanceof SolrException) {
                SolrException se = (SolrException) e;
                e = new SolrException(ErrorCode.getErrorCode(se.code()),
                    newMsg, se.getCause());
              } else {
                e = new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "newMsg", e);
              }
              rsp.setException(e);
            }
            
            // TODO: we dont log which request url actually failed - we currently list them all
            SolrException.logOnce(SolrCore.log, "shard update error ("
                + sreq.shards + ")", sreq.exception);
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
    
    // nocommit: just for kicks at the moment...
    if (failed <= (expectedResponses / 2)) {
      // don't fail if half or more where fine
      rsp.setException(null);
    }
  }
}
