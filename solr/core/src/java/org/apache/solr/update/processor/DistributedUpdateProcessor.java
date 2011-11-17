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
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
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
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
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
import org.apache.zookeeper.KeeperException;

// NOT mt-safe... create a new processor for each add thread
public class DistributedUpdateProcessor extends UpdateRequestProcessor {
  public static final String SEEN_LEADER = "leader";
  
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
  
  //private List<String> shards;

  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 100;

  private static final String VERSION_FIELD = "_version_";
  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private final boolean returnVersions = true; // todo: default to false and make configurable

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private CharsRef scratch;
  private boolean isLeader;
  private boolean forwardToLeader;
  private volatile String shardStr;

  private List<AddUpdateCommand> alist;

  private ArrayList<DeleteUpdateCommand> dlist;
  
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.next = next;
    this.idField = req.getSchema().getUniqueKeyField();
    
    // version init

    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;

  }

  private void setupRequest(int hash) {
    System.out.println("hash:" + hash);
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    
    
    String shardId = getShard(hash); // get the right shard based on the hash...
    
    
    // TODO: first thing we actually have to do here is get a hash so we can send to the right shard...
    // to do that, most of this likely has to move
    
    // if we are in zk mode...
    if (coreDesc.getCoreContainer().getZkController() != null) {
      // the leader is...
      // TODO: if there is no leader, wait and look again
      // TODO: we are reading the leader from zk every time - we should cache
      // this and watch for changes??
     
      String collection = coreDesc.getCloudDescriptor().getCollectionName();

      
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      
      String leaderNode = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
          + ZkStateReader.LEADER_ELECT_ZKNODE + "/" + shardId + "/leader";
      SolrZkClient zkClient = coreDesc.getCoreContainer().getZkController()
          .getZkClient();

      try {
        List<String> leaderChildren = zkClient.getChildren(leaderNode, null);
        if (leaderChildren.size() > 0) {
          String leader = leaderChildren.get(0);
          
          ZkNodeProps zkNodeProps = new ZkNodeProps();
          byte[] bytes = zkClient
              .getData(leaderNode + "/" + leader, null, null);
          zkNodeProps.load(bytes);
          
          String leaderUrl = zkNodeProps.get("url");
          
          String nodeName = req.getCore().getCoreDescriptor()
              .getCoreContainer().getZkController().getNodeName();
          String shardZkNodeName = nodeName + "_" + req.getCore().getName();

          System.out.println("params:" + params);
          if (params.getBool(SEEN_LEADER, false)) {
            // we got a version, just go local - add no shards param
            
            // still mark if i am the leader though
            if (shardZkNodeName.equals(leader)) {
              isLeader = true;
            }
            System.out.println(" go local");
          } else if (shardZkNodeName.equals(leader)) {
            isLeader = true;
            // that means I want to forward onto my replicas...
            // so get the replicas...
            shardStr = addReplicas(req, collection, shardId,
                shardZkNodeName);
            
            // mark that this req has been to the leader
            params.set(SEEN_LEADER, true);
            System.out.println("mark leader seen");
          } else {
            // I need to forward onto the leader...
            // first I must hash...
            shardStr = leaderUrl;
            forwardToLeader  = true;
          }
          System.out.println("set params on req:" + params);
          req.setParams(params);
        }
      } catch (KeeperException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      } catch (IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
  }
  
  private String getShard(int hash) {
    if (hash < -715827884) {
      return "shard1";
    } else if (hash < 715827881) {
      return "shard2";
    } else {
      return "shard3";
    }
      
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    int hash = hash(cmd);
    
    setupRequest(hash);
    versionAdd(cmd, hash);
    
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

  private void versionAdd(AddUpdateCommand cmd, int hash) throws IOException {
    if (vinfo == null) {
      super.processAdd(cmd);
      return;
    }

    System.out.println("LeaderParam:"
        + req.getParams().get(SEEN_LEADER));


    System.out.println("leader? " + isLeader);
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



    VersionBucket bucket = vinfo.bucket(hash);
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

        if (isLeader) {
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
    if (shardStr == null) {
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
    
    // make sure any pending deletes are flushed
    flushDeletes(1, null);
    
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
    
    flushAdds(maxBufferedAddsPerServer, null);
  }
  
  // TODO: this is brittle
  private DeleteUpdateCommand clone(DeleteUpdateCommand cmd) {
    DeleteUpdateCommand c = new DeleteUpdateCommand(req);
    c.id = cmd.id;
    c.query = cmd.query;
    return c;
  }
  
  private void doDelete(DeleteUpdateCommand cmd) throws IOException {
    
    flushAdds(1, null);
    
    if (dlist == null) {
      dlist = new ArrayList<DeleteUpdateCommand>(2);
    }
    dlist.add(clone(cmd));
    
    flushDeletes(maxBufferedDeletesPerServer, null);
  }
  
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    int hash = 0;
    if (cmd.getIndexedId() == null) {
      // delete by query...
    } else {
      hash = hash(cmd);
    }
    
    setupRequest(hash);
    
    versionDelete(cmd, hash);
    
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

  private void versionDelete(DeleteUpdateCommand cmd, int hash) throws IOException {
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


    if (forwardToLeader) {
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
    Long versionOnUpdate = versionOnUpdateS == null ? null : Long.parseLong(versionOnUpdateS);


    VersionBucket bucket = vinfo.bucket(hash);
    synchronized (bucket) {
      if (versionsStored) {
        long bucketVersion = bucket.highest;

        if (isLeader) {
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
    if (shardStr == null) {
      super.processDelete(cmd);
      return;
    }
    checkResponses(false);
    
    if (cmd.id != null) {
      doDelete(cmd);
    } else if (cmd.query != null) {
      // TODO: query must be broadcast to all ??
      doDelete(cmd);
    }
  }
  
  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    String shardStr = null;
    // nocommit: make everyone commit?
    distribCommit(cmd, shardStr);
  }

  private void distribCommit(CommitUpdateCommand cmd, String shardStr) throws IOException {
    if (shardStr == null) {
      super.processCommit(cmd);
      return;
    }
    
    // Wait for all outstanding repsonses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true);
    
    for (int slot = 0; slot < 1; slot++) {
      // piggyback on any outstanding adds or deletes if possible.
      if (flushAdds(1, cmd)) continue;
      if (flushDeletes( 1, cmd)) continue;
      
      UpdateRequestExt ureq = new UpdateRequestExt();
      // pass on version
      if (ureq.getParams() == null) {
        ureq.setParams(new ModifiableSolrParams());
      }
      String seenLeader = req.getParams().get(SEEN_LEADER);
      if (seenLeader != null) {
        ureq.getParams().add(SEEN_LEADER, seenLeader);
      }
      
      // nocommit: we add the right update chain - we should add the current one?
      ureq.getParams().add("update.chain", "distrib-update-chain");
      addCommit(ureq, cmd);
      submit(ureq);
    }
    //if (next != null && shardStr == null) next.processCommit(cmd);
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    // nocommit
    if (/* cmd.waitFlush || */cmd.waitSearcher) {
      checkResponses(true);
    }
  }
  
  @Override
  public void finish() throws IOException {

    // piggyback on any outstanding adds or deletes if possible.
    flushAdds(1, null);
    flushDeletes(1, null);

    checkResponses(true);
    if (next != null && shardStr == null) next.finish();
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
          System.out.println("RSP:" + sreq.rspCode);
          if (sreq.rspCode != 0) {
            // error during request
            failed++;
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
    
    System.out.println("check failed rate:" + failed + " " + expectedResponses / 2);
    if (failed <= (expectedResponses / 2)) {
      // don't fail if half or more where fine
      rsp.setException(null);
    }
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    // nocommit
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher);
  }
  
  boolean flushAdds(int limit, CommitUpdateCommand ccmd) {
    // check for pending deletes
    if (alist == null || alist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on seen leader
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }
    String seenLeader = req.getParams().get(SEEN_LEADER);
    if (seenLeader != null) {
      ureq.getParams().add(SEEN_LEADER, seenLeader);
    }
    // nocommit: we add the right update chain - we should add the current one?
    ureq.getParams().add("update.chain", "distrib-update-chain");
    addCommit(ureq, ccmd);
    
    for (AddUpdateCommand cmd : alist) {
      ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
    }
    
    alist = null;
    submit(ureq);
    return true;
  }
  
  boolean flushDeletes(int limit, CommitUpdateCommand ccmd) {
    // check for pending deletes
    if (dlist == null || dlist.size() < limit) return false;
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    // pass on version
    if (ureq.getParams() == null) {
      ureq.setParams(new ModifiableSolrParams());
    }

    String seenLeader = req.getParams().get(SEEN_LEADER);
    if (seenLeader != null) {
      ureq.getParams().add(SEEN_LEADER, seenLeader);
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
    
    dlist = null;
    submit(ureq);
    return true;
  }
  
  static class Request {
    // TODO: we may need to look at deep cloning this?
    String shard;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    Exception exception;
  }
  
  void submit(UpdateRequestExt ureq) {
    Request sreq = new Request();
    sreq.shard = shardStr;
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
          Request clonedRequest = new Request();
          clonedRequest.shard = sreq.shard;
          clonedRequest.ureq = sreq.ureq;

          try {
            // TODO: what about https?
            String url;
            if (!shard.startsWith("http://")) {
              url = "http://" + shard;
            } else {
              url = shard;
            }
            System.out.println("URL:" + url);
            SolrServer server = new CommonsHttpSolrServer(url, client);
            clonedRequest.ursp = server.request(clonedRequest.ureq);
            
            // currently no way to get the request body.
          } catch (Exception e) {
            e.printStackTrace(System.out);
            clonedRequest.exception = e;
            if (e instanceof SolrException) {
              clonedRequest.rspCode = ((SolrException) e).code();
            } else {
              clonedRequest.rspCode = -1;
            }
          }
          System.out.println("RSPFirst:" + clonedRequest.rspCode);
          return clonedRequest;
        }
      };
      
      pending.add(completionService.submit(task));
    }
  }
  
  private String addReplicas(SolrQueryRequest req, String collection,
      String shardId, String shardZkNodeName) {
    CloudState cloudState = req.getCore().getCoreDescriptor()
        .getCoreContainer().getZkController().getCloudState();
    Slice replicas = cloudState.getSlices(collection).get(shardId);
    Map<String,ZkNodeProps> shardMap = replicas.getShards();
    //String self = null;
    StringBuilder replicasUrl = new StringBuilder();
    for (Entry<String,ZkNodeProps> entry : shardMap.entrySet()) {
      if (replicasUrl.length() > 0) {
        replicasUrl.append("|");
      }
      String replicaUrl = entry.getValue().get("url");
      replicasUrl.append(replicaUrl);
    }

    // we don't currently use self - it does not yet work with the | notation anyhow
    //params.add("self", self);
    return replicasUrl.toString();
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
