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
import org.apache.commons.lang.NullArgumentException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequestExt;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;

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

  private static final String VERSION_FIELD = "_version_";
  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private final boolean returnVersions = true; // todo: default to false and make configurable

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private CharsRef scratch;
  private final boolean isLeader;
  private boolean forwardToLeader;
  
  public DistributedUpdateProcessor(String shardStr, SolrQueryRequest req,
      SolrQueryResponse rsp, DistributedUpdateProcessorFactory factory,
      boolean isLeader, boolean forwardToLeader, UpdateRequestProcessor next) {
    super(next);
    this.factory = factory;
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
    
    // version init
    this.isLeader = isLeader;
    this.forwardToLeader = forwardToLeader;

    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;

  }

  private int getSlot(String id) {
    return (id.hashCode() >>> 1) % shards.size();
  }
  
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    
    versionAdd(cmd);
    
    distribAdd(cmd);
    
    if (returnVersions && rsp != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<String>();
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }

    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.
  }

  private void versionAdd(AddUpdateCommand cmd) throws IOException {
    if (vinfo == null) {
      super.processAdd(cmd);
      return;
    }

    boolean leaderForUpdate = isLeader;
    System.out.println("LeaderParam:"
        + req.getParams().get(DistributedUpdateProcessorFactory.SEEN_LEADER));
   // leaderForUpdate = req.getParams().getBool(
    //    VersionProcessorFactory.SEEN_LEADER, false); // TODO: we need a better
                                                    // indicator of when an
                                                    // update comes from a
                                                    // leader

    System.out.println("leader? " + leaderForUpdate);
    if (forwardToLeader) {
      // TODO: forward update to the leader
      System.out.println("forward to leader");
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    long versionOnUpdate = 0;
    SolrInputField versionField = cmd.getSolrInputDocument().getField(VersionInfo.VERSION_FIELD);
    if (versionField != null) {
      Object o = versionField.getValue();
      versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
    } else {
      // TODO: check for the version in the request params (this will be for user provided versions and optimistic concurrency only)
    }



    VersionBucket bucket = vinfo.bucket(hash(cmd));
    synchronized (bucket) {
      // we obtain the version when synchronized and then do the add so we can ensure that
      // if version1 < version2 then version1 is actually added before version2.

      // even if we don't store the version field, synchronizing on the bucket
      // will enable us to know what version happened first, and thus enable
      // realtime-get to work reliably.
      // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
      // there may be other reasons in the future for a version on the commands
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (leaderForUpdate) {
          long version = vinfo.getNewClock();
          cmd.setVersion(version);
          cmd.getSolrInputDocument().setField(VersionInfo.VERSION_FIELD, version);
          bucket.updateHighest(version);
          System.out.println("add version field to doc");
        } else {
          // The leader forwarded us this update.
          cmd.setVersion(versionOnUpdate);

          // if we aren't the leader, then we need to check that updates were not re-ordered
          if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
            // we're OK... this update has a version higher than anything we've seen
            // in this bucket so far, so we know that no reordering has yet occured.
            bucket.updateHighest(versionOnUpdate);
          } else {
            // there have been updates higher than the current update.  we need to check
            // the specific version for this id.
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
              // This update is a repeat, or was reordered.  We need to drop this update.
              // TODO: do we need to add anything to the response?
              // nocommit: we should avoid next step in distrib add
              return;
            }
          }
        }
      }

    }


  }

  private void distribAdd(AddUpdateCommand cmd) throws IOException {
    if (shards == null) {
      super.processAdd(cmd);
      return;
    }
    
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
    versionDelete(cmd);
    
    distribDelete(cmd);

    // cmd.getIndexId == null when delete by query
    if (returnVersions && rsp != null && cmd.getIndexedId() != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<String>();
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  private void versionDelete(DeleteUpdateCommand cmd) throws IOException {
    if (cmd == null) {
      throw new NullArgumentException("cmd is null");
    }
    
    if (vinfo == null) {
      return;
    }

    if (cmd.id == null) {
      // delete-by-query
      // TODO: forward to all nodes in distrib mode?  or just don't bother to support?
      return;
    }

    boolean leaderForUpdate = isLeader;
   // leaderForUpdate = req.getParams().getBool(
   //     VersionProcessorFactory.SEEN_LEADER, false); // TODO: we need a better
                                                    // indicator of when an
                                                    // update comes from a
                                                    // leader

    if (forwardToLeader) {
      // TODO: forward update to the leader

      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
    Long versionOnUpdate = versionOnUpdateS == null ? null : Long.parseLong(versionOnUpdateS);


    VersionBucket bucket = vinfo.bucket(hash(cmd));
    synchronized (bucket) {
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (leaderForUpdate) {
          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          bucket.updateHighest(version);
        } else {
          // The leader forwarded us this update.
          if (versionOnUpdate == null) {
            throw new RuntimeException("we expected to find versionOnUpdate but did not");
          }
          
          cmd.setVersion(versionOnUpdate);
          // if we aren't the leader, then we need to check that updates were not re-ordered
          if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
            // we're OK... this update has a version higher than anything we've seen
            // in this bucket so far, so we know that no reordering has yet occured.
            bucket.updateHighest(versionOnUpdate);
          } else {
            // there have been updates higher than the current update.  we need to check
            // the specific version for this id.
            Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
            if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
              // This update is a repeat, or was reordered.  We need to drop this update.
              // TODO: do we need to add anything to the response?
              // nocommit: we should skip distrib update?
              return;
            }
          }
        }
      }

      return;
    }

  }

  private void distribDelete(DeleteUpdateCommand cmd) throws IOException {
    if (shards == null) {
      super.processDelete(cmd);
      return;
    }
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
    distribCommit(cmd);
  }

  private void distribCommit(CommitUpdateCommand cmd) throws IOException {
    if (shards == null) {
      super.processCommit(cmd);
      return;
    }
    
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
    if (shards == null) {
      super.finish();
      return;
    }
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
    // pass on seen leader
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
  
  // TODO: move this to AddUpdateCommand/DeleteUpdateCommand and cache it? And make the hash pluggable of course.
  // The hash also needs to be pluggable
  private int hash(AddUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
  private int hash(DeleteUpdateCommand cmd) {
    BytesRef br = cmd.getIndexedId();
    return Hash.murmurhash3_x86_32(br.bytes, br.offset, br.length, 0);
  }
}
