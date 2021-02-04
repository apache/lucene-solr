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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.LeaderRequestReplicationTracker;
import org.apache.solr.update.processor.DistributedUpdateProcessor.RollupRequestReplicationTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for distributing commands from a shard leader to its replicas.
 */
public class SolrCmdDistributor implements Closeable {
  private static final int MAX_RETRIES_ON_FORWARD = 2;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ConnectionManager.IsClosed isClosed;
  private final ZkStateReader zkStateReader;

  private volatile boolean finished = false; // see finish()

  private int maxRetries = MAX_RETRIES_ON_FORWARD;

  private final Map<UpdateCommand, Error> allErrors = new ConcurrentHashMap<>();
  
  private final Http2SolrClient solrClient;
  private volatile boolean closed;

  private volatile Throwable cancelExeption;

  public SolrCmdDistributor(ZkStateReader zkStateReader, UpdateShardHandler updateShardHandler) {
    assert ObjectReleaseTracker.track(this);
    this.zkStateReader = zkStateReader;
    this.solrClient = new Http2SolrClient.Builder().withHttpClient(updateShardHandler.getTheSharedHttpClient()).markInternalRequest().build();
    isClosed = null;
  }

  public SolrCmdDistributor(ZkStateReader zkStateReader, UpdateShardHandler updateShardHandler, ConnectionManager.IsClosed isClosed) {
    //assert ObjectReleaseTracker.track(this);
    this.zkStateReader = zkStateReader;
    this.solrClient = new Http2SolrClient.Builder().withHttpClient(updateShardHandler.getTheSharedHttpClient()).markInternalRequest().build();
    this.isClosed = isClosed;
  }

  public void finish() {
    assert !finished : "lifecycle sanity check";

//    if (cancelExeption != null) {
//      Throwable exp = cancelExeption;
//      cancelExeption = null;
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
//    }

    if (isClosed == null || isClosed != null && !isClosed.isClosed()) {
      solrClient.waitForOutstandingRequests();
    } else {
      //cancels.forEach(cancellable -> cancellable.cancel());
      Error error = new Error();
      error.t = new AlreadyClosedException();
      AlreadyClosedUpdateCmd cmd = new AlreadyClosedUpdateCmd(null);
      allErrors.put(cmd, error);
      solrClient.waitForOutstandingRequests();
    }
    finished = true;
  }
  
  public void close() {
    this.closed = true;
    solrClient.close();
    assert ObjectReleaseTracker.release(this);
  }

  public boolean checkRetry(Error err) {
    String oldNodeUrl = err.req.node.getUrl();

    // if there is a retry url, we want to retry...
    boolean isRetry = err.req.node.checkRetry();

    boolean doRetry = false;
    int rspCode = err.statusCode;

    if (testing_errorHook != null) Diagnostics.call(testing_errorHook,
            err.t);

    // this can happen in certain situations such as close
    if (isRetry) {
      if (rspCode == 403 || rspCode == 503) {
        doRetry = true;
      }

      // if it's a io exception exception, lets try again
      if (err.t instanceof SolrServerException) {
        if (((SolrServerException) err.t).getRootCause() instanceof IOException  && !(((SolrServerException) err.t).getRootCause() instanceof ClosedChannelException)) {
          doRetry = true;
        }
      }

      if (err.t instanceof IOException && !(err.t instanceof ClosedChannelException)) {
        doRetry = true;
      }

      if (err.req.retries < maxRetries && doRetry) {
        err.req.retries++;

        SolrException.log(SolrCmdDistributor.log, "sending update to "
                + oldNodeUrl + " failed - retrying ... retries: "
                + err.req.retries + " " + err.req.cmd.toString() + " params:"
                + err.req.uReq.getParams() + " rsp:" + rspCode, err.t);
        if (log.isDebugEnabled()) log.debug("check retry true");
        return true;
      } else {
        log.info("max retries exhausted or not a retryable error {} {}", err.req.retries, rspCode);
        return false;
      }
    } else {
      log.info("not a retry request, retry false");
      return false;
    }

  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribDelete(cmd, nodes, params, false, null, null);
  }

  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean sync,
                            RollupRequestReplicationTracker rollupTracker,
                            LeaderRequestReplicationTracker leaderTracker) throws IOException {
//    if (!cmd.isDeleteById()) {
//      blockAndDoRetries();
//    }
    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      uReq.setCommitWithin(cmd.commitWithin);
      if (cmd.isDeleteById()) {
        uReq.deleteById(cmd.getId(), cmd.getRoute(), cmd.getVersion());
      } else {
        uReq.deleteByQuery(cmd.query);
      }
      submit(new Req(cmd, node, uReq, sync, rollupTracker, leaderTracker));
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribAdd(cmd, nodes, params, false, null, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous) throws IOException {
    distribAdd(cmd, nodes, params, synchronous, null, null);
  }

  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous,
                         RollupRequestReplicationTracker rollupTracker,
                         LeaderRequestReplicationTracker leaderTracker) throws IOException {
    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      if (cmd.isLastDocInBatch)
        uReq.lastDocInBatch();
      uReq.setParams(params);
      uReq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      if (cmd.isInPlaceUpdate()) {
        params.set(DistributedUpdateProcessor.DISTRIB_INPLACE_PREVVERSION, String.valueOf(cmd.prevVersion));
      }
      submit(new Req(cmd, node, uReq, synchronous, rollupTracker, leaderTracker));
    }
    
  }

  public void distribCommit(CommitUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) {
    // we need to do any retries before commit...
    //blockAndDoRetries();
    if (log.isDebugEnabled()) log.debug("Distrib commit to: {} params: {}", nodes, params);

    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);

      addCommit(uReq, cmd);
      submit(new Req(cmd, node, uReq, false));
    }
  }

  public void blockAndDoRetries() {
    solrClient.waitForOutstandingRequests();
  }
  
  void addCommit(UpdateRequest ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes, cmd.openSearcher);
  }

  private void submit(final Req req) {

    if (cancelExeption != null) {
      Throwable exp = cancelExeption;
      cancelExeption = null;
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, exp);
    }

    if (log.isDebugEnabled()) {
      log.debug("sending update to " + req.node.getUrl() + " retry:" + req.retries + " " + req.cmd + " params:" + req.uReq.getParams());
    }

    if (isClosed != null && isClosed.isClosed()) {
      log.warn("Already closed, skippping update");
      throw new AlreadyClosedException();
    }

    req.uReq.setBasePath(req.node.getUrl());

    if (req.synchronous) {
      blockAndDoRetries();

      try {
        solrClient.request(req.uReq);
      } catch (Exception e) {
        log.error("Exception sending synchronous dist update", e);
        Error error = new Error();
        error.t = e;
        error.req = req;
        if (e instanceof SolrException) {
          error.statusCode = ((SolrException) e).code();
        }
        allErrors.put(req.cmd, error);
      }

      return;
    }

    try {
      Http2SolrClient client;

      client = solrClient;


      client.asyncRequest(req.uReq, null, new AsyncListener<>() {
        @Override
        public void onSuccess(NamedList result) {
          if (log.isTraceEnabled()) log.trace("Success for distrib update {}", result);
        }

        @Override
        public void onFailure(Throwable t, int code) {
          log.error("Exception sending dist update {} {}", code, t);

          // nocommit - we want to prevent any more from this request
          // to go just to this node rather than stop the whole request
          if (code == 404) {
            cancelExeption = t;
            return;
          }

          Error error = new Error();
          error.t = t;
          error.req = req;
          if (t instanceof SolrException) {
            error.statusCode = ((SolrException) t).code();
          }

          boolean retry = false;
          if (checkRetry(error)) {
            retry = true;
          }

          if (retry) {
            log.info("Retrying distrib update on error: {}", t.getMessage());
            try {
              submit(req);
            } catch (AlreadyClosedException e) {
              
            }
            return;
          } else {
            allErrors.put(req.cmd, error);
          }
        }
      });
    } catch (Exception e) {
      log.error("Exception sending dist update", e);
      Error error = new Error();
      error.t = e;
      error.req = req;
      if (e instanceof SolrException) {
        error.statusCode = ((SolrException) e).code();
      }
      if (checkRetry(error)) {
        log.info("Retrying distrib update on error: {}", e.getMessage());
        submit(req);
      } else {
        allErrors.put(req.cmd, error);
      }
    }
  }

  public static class Req {
    public Node node;
    public UpdateRequest uReq;
    public int retries;
    public boolean synchronous;
    public UpdateCommand cmd;
    final private RollupRequestReplicationTracker rollupTracker;
    final private LeaderRequestReplicationTracker leaderTracker;

    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous) {
      this(cmd, node, uReq, synchronous, null, null);
    }

    public Req(UpdateCommand cmd, Node node, UpdateRequest uReq, boolean synchronous,
               RollupRequestReplicationTracker rollupTracker,
               LeaderRequestReplicationTracker leaderTracker) {
      this.node = node;
      this.uReq = uReq;
      this.synchronous = synchronous;
      this.cmd = cmd;
      this.rollupTracker = rollupTracker;
      this.leaderTracker = leaderTracker;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SolrCmdDistributor$Req: cmd=").append(cmd.toString());
      sb.append("; node=").append(String.valueOf(node));
      return sb.toString();
    }

    // Called whenever we get results back from a sub-request.
    // The only ambiguity is if I have _both_ a rollup tracker and a leader tracker. In that case we need to handle
    // both requests returning from leaders of other shards _and_ from my followers. This happens if a leader happens
    // to be the aggregator too.
    //
    // This isn't really a problem because only responses _from_ some leader will have the "rf" parameter, in which case
    // we need to add the data to the rollup tracker.
    //
    // In the case of a leaderTracker and rollupTracker both being present, then we need to take care when assembling
    // the final response to check both the rollup and leader trackers on the aggregator node.
    public void trackRequestResult(org.eclipse.jetty.client.api.Response resp, InputStream respBody, boolean success) {

      // Returning Integer.MAX_VALUE here means there was no "rf" on the response, therefore we just need to increment
      // our achieved rf if we are a leader, i.e. have a leaderTracker.
      int rfFromResp = getRfFromResponse(respBody);

      if (leaderTracker != null && rfFromResp == Integer.MAX_VALUE) {
        leaderTracker.trackRequestResult(node, success);
      }

      if (rollupTracker != null) {
        rollupTracker.testAndSetAchievedRf(rfFromResp);
      }
    }

    private int getRfFromResponse(InputStream inputStream) {
      if (inputStream != null) {
        try {
          BinaryResponseParser brp = new BinaryResponseParser();
          NamedList<Object> nl = brp.processResponse(inputStream, null);
          Object hdr = nl.get("responseHeader");
          if (hdr != null && hdr instanceof NamedList) {
            @SuppressWarnings({"unchecked"})
            NamedList<Object> hdrList = (NamedList<Object>) hdr;
            Object rfObj = hdrList.get(UpdateRequest.REPFACT);
            if (rfObj != null && rfObj instanceof Integer) {
              return (Integer) rfObj;
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          log.warn("Failed to parse response from {} during replication factor accounting", node, e);
        }
      }
      return Integer.MAX_VALUE;
    }
  }

  public static volatile Diagnostics.Callable testing_errorHook;  // called on error when forwarding request.  Currently data=[this, Request]

  public static class Error {
    public volatile Throwable t;
    public volatile int statusCode = -1;

    public volatile Req req;
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("SolrCmdDistributor$Error: statusCode=").append(statusCode);
      sb.append("; throwable=").append(String.valueOf(t));
      sb.append("; req=").append(String.valueOf(req));
      return sb.toString();
    }
  }
  
  public static abstract class Node {
    public abstract String getUrl();
    public abstract boolean checkRetry();
    public abstract String getCoreName();
    public abstract String getBaseUrl();
    public abstract Replica getNodeProps();
    public abstract String getCollection();
    public abstract String getShardId();
    public abstract int getMaxRetries();
  }

  public static class StdNode extends Node {
    protected final ZkStateReader zkStateReader;
    protected Replica nodeProps;
    protected String collection;
    protected String shardId;
    private final boolean retry;
    private final int maxRetries;

    public StdNode(ZkStateReader zkStateReader, Replica nodeProps) {
      this(zkStateReader, nodeProps, null, null, 0);
    }
    
    public StdNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId) {
      this(zkStateReader, nodeProps, collection, shardId, 0);
    }
    
    public StdNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId, int maxRetries) {
      this.zkStateReader = zkStateReader;
      this.nodeProps = nodeProps;
      this.collection = collection;
      this.shardId = shardId;
      this.retry = maxRetries > 0;
      this.maxRetries = maxRetries;
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
      return true;
    }

    @Override
    public String getBaseUrl() {
      return nodeProps.getBaseUrl();
    }

    @Override
    public String getCoreName() {
      return nodeProps.getName();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      String baseUrl = nodeProps.getBaseUrl();
      String coreName = nodeProps.getName();
      String url = nodeProps.getCoreUrl();
      result = prime * result + ((baseUrl == null) ? 0 : baseUrl.hashCode());
      result = prime * result + ((coreName == null) ? 0 : coreName.hashCode());
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      result = prime * result + Boolean.hashCode(retry);
      result = prime * result + Integer.hashCode(maxRetries);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      StdNode other = (StdNode) obj;
      if (this.retry != other.retry) return false;
      if (this.maxRetries != other.maxRetries) return false;
      String baseUrl = nodeProps.getBaseUrl();
      String coreName = nodeProps.getName();
      String url = nodeProps.getCoreUrl();
      if (baseUrl == null) {
        if (other.nodeProps.getBaseUrl() != null) return false;
      } else if (!baseUrl.equals(other.nodeProps.getBaseUrl())) return false;
      if (coreName == null) {
        if (other.nodeProps.getName() != null) return false;
      } else if (!coreName.equals(other.nodeProps.getName())) return false;
      if (url == null) {
        if (other.nodeProps.getCoreUrl() != null) return false;
      } else if (!url.equals(other.nodeProps.getCoreUrl())) return false;
      return true;
    }

    @Override
    public Replica getNodeProps() {
      return nodeProps;
    }

    @Override
    public int getMaxRetries() {
      return this.maxRetries;
    }
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class ForwardNode extends StdNode {
    
    public ForwardNode(ZkStateReader zkStateReader, Replica nodeProps, String collection, String shardId) {
      super(zkStateReader, nodeProps, collection, shardId);
      this.collection = collection;
      this.shardId = shardId;
    }

    @Override
    public boolean checkRetry() {
      Replica leaderProps;
      try {
        leaderProps = zkStateReader.getLeaderRetry(
            collection, shardId);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
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
      ForwardNode other = (ForwardNode) obj;
      if (nodeProps.getCoreUrl() == null) {
        if (other.nodeProps.getCoreUrl() != null) return false;
      } else if (!nodeProps.getCoreUrl().equals(other.nodeProps.getCoreUrl())) return false;

      return true;
    }
  }

  public Map<UpdateCommand, Error> getErrors() {
    return allErrors;
  }

  private static class AlreadyClosedUpdateCmd extends UpdateCommand {
    public AlreadyClosedUpdateCmd(SolrQueryRequest req) {
      super(req);
    }

    @Override
    public String name() {
      return "AlreadyClosedException";
    }
  }
}

