package org.apache.solr.update.processor;

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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;

// NOT mt-safe... create a new processor for each add thread
public class DistributedUpdateProcessor extends UpdateRequestProcessor {
  // TODO: shut this thing down
  static ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  
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
  private final UpdateRequestProcessor next;;
  private final SchemaField idField;
  
  private List<String> shards;
  private final List<AddUpdateCommand>[] adds;
  private final List<DeleteUpdateCommand>[] deletes;
  
  String selfStr;
  int self;
  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 100;
  
  private DistributedUpdateProcessorFactory factory;
  
  public DistributedUpdateProcessor(String shardStr, SolrQueryRequest req,
      SolrQueryResponse rsp, DistributedUpdateProcessorFactory factory,
      UpdateRequestProcessor next) {
    super(next);
    this.factory = factory;
    this.req = req;
    this.rsp = rsp;
    this.next = next;
    this.idField = req.getSchema().getUniqueKeyField();
    
    shards = factory.shards;
    
    String selfStr = req.getParams().get("self", factory.selfStr);

    if (shardStr != null) {
      shards = StrUtils.splitSmart(shardStr, ",", true);
    }
    
    self = -1;
    if (shards != null) {
      for (int i = 0; i < shards.size(); i++) {
        if (shards.get(i).equals(selfStr)) {
          self = i;
          break;
        }
      }
    }
    
    if (shards == null) {
      shards = new ArrayList<String>(1);
      shards.add("self");
      self = 0;
    }
    
    adds = new List[shards.size()];
    deletes = new List[shards.size()];
  }
  
  private int getSlot(String id) {
    return (id.hashCode() >>> 1) % shards.size();
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    checkResponses(false);
    
    SolrInputDocument doc = cmd.getSolrInputDocument();
    SolrInputField field = doc.getField(idField.getName());
    if (field == null) {
      if (next != null) next.processAdd(cmd);
      return;
    }
    String id = field.getFirstValue().toString();
    int slot = getSlot(id);
    if (slot == self) {
      if (next != null) next.processAdd(cmd);
      return;
    }
    
    // make sure any pending deletes are flushed
    flushDeletes(slot, 1, null);
    
    // TODO: this is brittle
    // need to make a clone since these commands may be reused
    AddUpdateCommand clone = new AddUpdateCommand(req);
    
    clone.solrDoc = cmd.solrDoc;
    clone.commitWithin = cmd.commitWithin;
    clone.overwrite = cmd.overwrite;
    
    // nocommit: review as far as SOLR-2685
    // clone.indexedId = cmd.indexedId;
    // clone.doc = cmd.doc;
    
    List<AddUpdateCommand> alist = adds[slot];
    if (alist == null) {
      alist = new ArrayList<AddUpdateCommand>(2);
      adds[slot] = alist;
    }
    alist.add(clone);
    
    flushAdds(slot, maxBufferedAddsPerServer, null);
  }
  
  // TODO: this is brittle
  private DeleteUpdateCommand clone(DeleteUpdateCommand cmd) {
    DeleteUpdateCommand c = new DeleteUpdateCommand(req);
    c.id = cmd.id;
    c.query = cmd.query;
    return c;
  }
  
  private void doDelete(int slot, DeleteUpdateCommand cmd) throws IOException {
    if (slot == self) {
      if (self >= 0) next.processDelete(cmd);
      return;
    }
    
    flushAdds(slot, 1, null);
    
    List<DeleteUpdateCommand> dlist = deletes[slot];
    if (dlist == null) {
      dlist = new ArrayList<DeleteUpdateCommand>(2);
      deletes[slot] = dlist;
    }
    dlist.add(clone(cmd));
    
    flushDeletes(slot, maxBufferedDeletesPerServer, null);
  }
  
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    checkResponses(false);
    
    if (cmd.id != null) {
      doDelete(getSlot(cmd.id), cmd);
    } else if (cmd.query != null) {
      // query must be broadcast to all
      for (int slot = 0; slot < deletes.length; slot++) {
        if (slot == self) continue;
        doDelete(slot, cmd);
      }
      doDelete(self, cmd);
    }
  }
  
  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    // Wait for all outstanding repsonses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true);
    
    for (int slot = 0; slot < shards.size(); slot++) {
      if (slot == self) continue;
      // piggyback on any outstanding adds or deletes if possible.
      if (flushAdds(slot, 1, cmd)) continue;
      if (flushDeletes(slot, 1, cmd)) continue;
      
      UpdateRequestExt ureq = new UpdateRequestExt();
      // pass on version
      if (ureq.getParams() == null) {
        ureq.setParams(new ModifiableSolrParams());
      }
      String seenLeader = req.getParams().get(DistributedUpdateProcessorFactory.SEEN_LEADER);
      if (seenLeader != null) {
        ureq.getParams().add(DistributedUpdateProcessorFactory.SEEN_LEADER, seenLeader);
      }
      
      // nocommit: we add the right update chain - we should add the current one?
      ureq.getParams().add("update.chain", "distrib-update-chain");
      addCommit(ureq, cmd);
      submit(slot, ureq);
    }
    if (next != null && self >= 0) next.processCommit(cmd);
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    // nocommit
    if (/* cmd.waitFlush || */cmd.waitSearcher) {
      checkResponses(true);
    }
  }
  
  @Override
  public void finish() throws IOException {
    for (int slot = 0; slot < shards.size(); slot++) {
      if (slot == self) continue;
      // piggyback on any outstanding adds or deletes if possible.
      flushAdds(slot, 1, null);
      flushDeletes(slot, 1, null);
    }
    checkResponses(true);
    if (next != null && self >= 0) next.finish();
  }
  
  void checkResponses(boolean block) {
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
            
            // use the first exception encountered
            if (rsp.getException() == null) {
              Exception e = sreq.exception;
              String newMsg = "shard update error (" + sreq.shard + "):"
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
            
            SolrException.logOnce(SolrCore.log, "shard update error ("
                + sreq.shard + ")", sreq.exception);
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
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    // nocommit
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher);
  }
  
  boolean flushAdds(int slot, int limit, CommitUpdateCommand ccmd) {
    // check for pending deletes
    List<AddUpdateCommand> alist = adds[slot];
    if (alist == null || alist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on version
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }
    if (req.getParams().get(DistributedUpdateProcessorFactory.SEEN_LEADER) != null) {
      ureq.getParams().add(DistributedUpdateProcessorFactory.SEEN_LEADER,
          req.getParams().get(DistributedUpdateProcessorFactory.SEEN_LEADER));
    }
    // nocommit: we add the right update chain - we should add the current one?
    ureq.getParams().add("update.chain", "distrib-update-chain");
    addCommit(ureq, ccmd);
    
    for (AddUpdateCommand cmd : alist) {
      ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
    }
    
    adds[slot] = null;
    submit(slot, ureq);
    return true;
  }
  
  boolean flushDeletes(int slot, int limit, CommitUpdateCommand ccmd) {
    // check for pending deletes
    List<DeleteUpdateCommand> dlist = deletes[slot];
    if (dlist == null || dlist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on version
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }

    String seenLeader = req.getParams().get(DistributedUpdateProcessorFactory.SEEN_LEADER);
    if (seenLeader != null) {
      ureq.getParams().add(DistributedUpdateProcessorFactory.SEEN_LEADER, seenLeader);
    }
    
    // nocommit: we add the right update chain - we should add the current one?
    ureq.getParams().add("update.chain", "distrib-update-chain");
    addCommit(ureq, ccmd);
    for (DeleteUpdateCommand cmd : dlist) {
      if (cmd.id != null) {
        ureq.deleteById(cmd.id);
      }
      if (cmd.query != null) {
        ureq.deleteByQuery(cmd.query);
      }
    }
    
    deletes[slot] = null;
    submit(slot, ureq);
    return true;
  }
  
  static class Request {
    String shard;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    Exception exception;
  }
  
  void submit(int slot, UpdateRequestExt ureq) {
    Request sreq = new Request();
    sreq.shard = shards.get(slot);
    sreq.ureq = ureq;
    submit(sreq);
  }
  
  void submit(final Request sreq) {
    if (completionService == null) {
      completionService = new ExecutorCompletionService<Request>(commExecutor);
      pending = new HashSet<Future<Request>>();
    }
    String[] shards;
    // look to see if we should send to multiple servers
    if (sreq.shard.contains("|")) {
      shards = sreq.shard.split("\\|");
    } else {
      shards = new String[1];
      shards[0] = sreq.shard;
    }
    for (final String shard : shards) {
      // TODO: when we break up shards, we might forward
      // to self again - makes things simple here, but we could
      // also have realized this before, done the req locally, and
      // removed self from this list.
      
      Callable<Request> task = new Callable<Request>() {
        @Override
        public Request call() throws Exception {
          
          try {
            // TODO: what about https?
            String url;
            if (!shard.startsWith("http://")) {
              url = "http://" + sreq.shard;
            } else {
              url = shard;
            }
            
            // TODO: allow shard syntax to use : to specify replicas
            SolrServer server = new CommonsHttpSolrServer(url, client);
            sreq.ursp = server.request(sreq.ureq);
          
            // currently no way to get the request body.
          } catch (Exception e) {
            sreq.exception = e;
            if (e instanceof SolrException) {
              sreq.rspCode = ((SolrException) e).code();
            } else {
              sreq.rspCode = -1;
            }
          }
          return sreq;
        }
      };
      
      pending.add(completionService.submit(task));
    }
  }
}
