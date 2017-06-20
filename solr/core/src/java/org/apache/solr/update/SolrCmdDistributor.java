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
package org.apache.solr.update;

import org.apache.http.HttpResponse;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient; // jdoc
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.RequestReplicationTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Used for distributing commands from a shard leader to its replicas.
 */
public class SolrCmdDistributor implements Closeable {
  private static final int MAX_RETRIES_ON_FORWARD = 25;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private StreamingSolrClients clients;
  private boolean finished = false; // see finish()

  private int retryPause = 500;
  private int maxRetriesOnForward = MAX_RETRIES_ON_FORWARD;
  
  private final List<Error> allErrors = new ArrayList<>();
  private final List<Error> errors = Collections.synchronizedList(new ArrayList<Error>());
  private final ExecutorService updateExecutor;
  
  private final CompletionService<Object> completionService;
  private final Set<Future<Object>> pending = new HashSet<>();
  
  public static interface AbortCheck {
    public boolean abortCheck();
  }
  
  public SolrCmdDistributor(UpdateShardHandler updateShardHandler) {
    this.clients = new StreamingSolrClients(updateShardHandler);
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    this.completionService = new ExecutorCompletionService<>(updateExecutor);
  }
  
  public SolrCmdDistributor(StreamingSolrClients clients, int maxRetriesOnForward, int retryPause) {
    this.clients = clients;
    this.maxRetriesOnForward = maxRetriesOnForward;
    this.retryPause = retryPause;
    this.updateExecutor = clients.getUpdateExecutor();
    completionService = new ExecutorCompletionService<>(updateExecutor);
  }
  
  public void finish() {    
    try {
      assert ! finished : "lifecycle sanity check";
      finished = true;
      
      blockAndDoRetries();
    } finally {
      clients.shutdown();
    }
  }
  
  public void close() {
    clients.shutdown();
  }

  private void doRetriesIfNeeded() {
    // NOTE: retries will be forwards to a single url
    
    List<Error> errors = new ArrayList<>(this.errors);
    errors.addAll(clients.getErrors());
    List<Error> resubmitList = new ArrayList<>();

    for (Error err : errors) {
      try {
        String oldNodeUrl = err.req.node.getUrl();
        
        // if there is a retry url, we want to retry...
        boolean isRetry = err.req.node.checkRetry();
        
        boolean doRetry = false;
        int rspCode = err.statusCode;
        
        if (testing_errorHook != null) Diagnostics.call(testing_errorHook,
            err.e);
        
        // this can happen in certain situations such as close
        if (isRetry) {
          if (rspCode == 404 || rspCode == 403 || rspCode == 503) {
            doRetry = true;
          }
          
          // if it's a connect exception, lets try again
          if (err.e instanceof SolrServerException) {
            if (((SolrServerException) err.e).getRootCause() instanceof ConnectException) {
              doRetry = true;
            }
          }
          
          if (err.e instanceof ConnectException) {
            doRetry = true;
          }
          
          if (err.req.retries < maxRetriesOnForward && doRetry) {
            err.req.retries++;
            
            SolrException.log(SolrCmdDistributor.log, "forwarding update to "
                + oldNodeUrl + " failed - retrying ... retries: "
                + err.req.retries + " " + err.req.cmd.toString() + " params:"
                + err.req.uReq.getParams() + " rsp:" + rspCode, err.e);
            try {
              Thread.sleep(retryPause);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              log.warn(null, e);
            }
            
            resubmitList.add(err);
          } else {
            allErrors.add(err);
          }
        } else {
          allErrors.add(err);
        }
      } catch (Exception e) {
        // continue on
        log.error("Unexpected Error while doing request retries", e);
      }
    }
    
    clients.clearErrors();
    this.errors.clear();
    for (Error err : resubmitList) {
      submit(err.req, false);
    }
    
    if (resubmitList.size() > 0) {
      blockAndDoRetries();
    }
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribDelete(cmd, nodes, params, false);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean sync) throws IOException {
    
    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      uReq.setCommitWithin(cmd.commitWithin);
      if (cmd.isDeleteById()) {
        uReq.deleteById(cmd.getId(), cmd.getRoute(), cmd.getVersion());
      } else {
        uReq.deleteByQuery(cmd.query);
      }
      
      submit(new Req(cmd, node, uReq, sync), false);
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribAdd(cmd, nodes, params, false, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous) throws IOException {
    distribAdd(cmd, nodes, params, synchronous, null);
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous, RequestReplicationTracker rrt) throws IOException {  
    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      if (cmd.isLastDocInBatch)
        uReq.lastDocInBatch();
      uReq.setParams(params);
      uReq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      if (cmd.isInPlaceUpdate()) {
        params.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));
      }
      submit(new Req(cmd, node, uReq, synchronous, rrt), false);
    }
    
  }

  public void distribCommit(CommitUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) throws IOException {
    
    // we need to do any retries before commit...
    blockAndDoRetries();
    
    UpdateRequest uReq = new UpdateRequest();
    uReq.setParams(params);
    
    addCommit(uReq, cmd);
    
    log.debug("Distrib commit to: {} params: {}", nodes, params);
    
    for (Node node : nodes) {
      submit(new Req(cmd, node, uReq, false), true);
    }
    
  }

  public void blockAndDoRetries() {
    clients.blockUntilFinished();
    
    // wait for any async commits to complete
    while (pending != null && pending.size() > 0) {
      Future<Object> future = null;
      try {
        future = completionService.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("blockAndDoRetries interrupted", e);
      }
      if (future == null) break;
      pending.remove(future);
    }
    doRetriesIfNeeded();

  }
  
  void addCommit(UpdateRequest ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes, cmd.openSearcher);
  }

  private void submit(final Req req, boolean isCommit) {
    if (req.synchronous) {
      blockAndDoRetries();

      try (HttpSolrClient client = new HttpSolrClient.Builder(req.node.getUrl()).withHttpClient(clients.getHttpClient()).build()) {
        client.request(req.uReq);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed synchronous update on shard " + req.node + " update: " + req.uReq , e);
      }
      
      return;
    }
    
    if (log.isDebugEnabled()) {
      log.debug("sending update to "
          + req.node.getUrl() + " retry:"
          + req.retries + " " + req.cmd + " params:" + req.uReq.getParams());
    }
    
    if (isCommit) {
      // a commit using ConncurrentUpdateSolrServer is not async,
      // so we make it async to prevent commits from happening
      // serially across multiple nodes
      pending.add(completionService.submit(() -> {
        doRequest(req);
        return null;
      }));
    } else {
      doRequest(req);
    }
  }
  
  private void doRequest(final Req req) {
    try {
      SolrClient solrClient = clients.getSolrClient(req);
      solrClient.request(req.uReq);
    } catch (Exception e) {
      SolrException.log(log, e);
      Error error = new Error();
      error.e = e;
      error.req = req;
      if (e instanceof SolrException) {
        error.statusCode = ((SolrException) e).code();
      }
      errors.add(error);
    }
  }
  
  public static class Req {
    public Node node;
    public UpdateRequest uReq;
    public int retries;
    public boolean synchronous;
    public UpdateCommand cmd;
    public RequestReplicationTracker rfTracker;

    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous) {
      this(cmd, node, uReq, synchronous, null);
    }
    
    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous, RequestReplicationTracker rfTracker) {
      this.node = node;
      this.uReq = uReq;
      this.synchronous = synchronous;
      this.cmd = cmd;
      this.rfTracker = rfTracker;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SolrCmdDistributor$Req: cmd=").append(cmd.toString());
      sb.append("; node=").append(String.valueOf(node));
      return sb.toString();
    }
    
    public void trackRequestResult(HttpResponse resp, boolean success) {      
      if (rfTracker != null) {
        Integer rf = null;
        if (resp != null) {
          // need to parse out the rf from requests that were forwards to another leader
          InputStream inputStream = null;
          try {
            inputStream = resp.getEntity().getContent();
            BinaryResponseParser brp = new BinaryResponseParser();
            NamedList<Object> nl= brp.processResponse(inputStream, null);
            Object hdr = nl.get("responseHeader");
            if (hdr != null && hdr instanceof NamedList) {
              NamedList<Object> hdrList = (NamedList<Object>)hdr;
              Object rfObj = hdrList.get(UpdateRequest.REPFACT);
              if (rfObj != null && rfObj instanceof Integer) {
                rf = (Integer)rfObj;
              }
            }
          } catch (Exception e) {
            log.warn("Failed to parse response from "+node+" during replication factor accounting due to: "+e);
          } finally {
            if (inputStream != null) {
              try {
                inputStream.close();
              } catch (Exception ignore){}
            }
          }
        }
        rfTracker.trackRequestResult(node, success, rf);
      }
    }
  }
    

  public static Diagnostics.Callable testing_errorHook;  // called on error when forwarding request.  Currently data=[this, Request]

  
  public static class Response {
    public List<Error> errors = new ArrayList<>();
  }
  
  public static class Error {
    public Exception e;
    public int statusCode = -1;

    /**
     * NOTE: This is the request that happened to be executed when this error was <b>triggered</b> the error, 
     * but because of how {@link StreamingSolrClients} uses {@link ConcurrentUpdateSolrClient} it might not 
     * actaully be the request that <b>caused</b> the error -- multiple requests are merged &amp; processed as 
     * a sequential batch.
     */
    public Req req;
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SolrCmdDistributor$Error: statusCode=").append(statusCode);
      sb.append("; exception=").append(String.valueOf(e));
      sb.append("; req=").append(String.valueOf(req));
      return sb.toString();
    }
  }
  
  public static abstract class Node {
    public abstract String getUrl();
    public abstract boolean checkRetry();
    public abstract String getCoreName();
    public abstract String getBaseUrl();
    public abstract ZkCoreNodeProps getNodeProps();
    public abstract String getCollection();
    public abstract String getShardId();
  }

  public static class StdNode extends Node {
    protected ZkCoreNodeProps nodeProps;
    protected String collection;
    protected String shardId;

    public StdNode(ZkCoreNodeProps nodeProps) {
      this(nodeProps, null, null);
    }
    
    public StdNode(ZkCoreNodeProps nodeProps, String collection, String shardId) {    
      this.nodeProps = nodeProps;
      this.collection = collection;
      this.shardId = shardId;
    }
    
    public String getCollection() {
      return collection;
    }
    
    public String getShardId() {
      return shardId;
    }
        
    @Override
    public String getUrl() {
      return nodeProps.getCoreUrl();
    }
    
    @Override
    public String toString() {
      return this.getClass().getSimpleName() + ": " + nodeProps.getCoreUrl();
    }

    @Override
    public boolean checkRetry() {
      return false;
    }

    @Override
    public String getBaseUrl() {
      return nodeProps.getBaseUrl();
    }

    @Override
    public String getCoreName() {
      return nodeProps.getCoreName();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      String baseUrl = nodeProps.getBaseUrl();
      String coreName = nodeProps.getCoreName();
      String url = nodeProps.getCoreUrl();
      result = prime * result + ((baseUrl == null) ? 0 : baseUrl.hashCode());
      result = prime * result + ((coreName == null) ? 0 : coreName.hashCode());
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      StdNode other = (StdNode) obj;
      String baseUrl = nodeProps.getBaseUrl();
      String coreName = nodeProps.getCoreName();
      String url = nodeProps.getCoreUrl();
      if (baseUrl == null) {
        if (other.nodeProps.getBaseUrl() != null) return false;
      } else if (!baseUrl.equals(other.nodeProps.getBaseUrl())) return false;
      if (coreName == null) {
        if (other.nodeProps.getCoreName() != null) return false;
      } else if (!coreName.equals(other.nodeProps.getCoreName())) return false;
      if (url == null) {
        if (other.nodeProps.getCoreUrl() != null) return false;
      } else if (!url.equals(other.nodeProps.getCoreUrl())) return false;
      return true;
    }

    @Override
    public ZkCoreNodeProps getNodeProps() {
      return nodeProps;
    }
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class RetryNode extends StdNode {
    
    private ZkStateReader zkStateReader;
    
    public RetryNode(ZkCoreNodeProps nodeProps, ZkStateReader zkStateReader, String collection, String shardId) {
      super(nodeProps, collection, shardId);
      this.zkStateReader = zkStateReader;
      this.collection = collection;
      this.shardId = shardId;
    }

    @Override
    public boolean checkRetry() {
      ZkCoreNodeProps leaderProps;
      try {
        leaderProps = new ZkCoreNodeProps(zkStateReader.getLeaderRetry(
            collection, shardId));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (Exception e) {
        // we retry with same info
        log.warn(null, e);
        return true;
      }
     
      this.nodeProps = leaderProps;
      
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      RetryNode other = (RetryNode) obj;
      if (nodeProps.getCoreUrl() == null) {
        if (other.nodeProps.getCoreUrl() != null) return false;
      } else if (!nodeProps.getCoreUrl().equals(other.nodeProps.getCoreUrl())) return false;

      return true;
    }
  }

  public List<Error> getErrors() {
    return allErrors;
  }
}

