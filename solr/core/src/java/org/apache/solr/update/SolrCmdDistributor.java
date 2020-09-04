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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.LeaderRequestReplicationTracker;
import org.apache.solr.update.processor.DistributedUpdateProcessor.RollupRequestReplicationTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

/**
 * Used for distributing commands from a shard leader to its replicas.
 */
public class SolrCmdDistributor implements Closeable {
  private static final int MAX_RETRIES_ON_FORWARD = 3;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean finished = false; // see finish()

  private int maxRetriesOnForward = MAX_RETRIES_ON_FORWARD;

  private final Set<Error> allErrors = ConcurrentHashMap.newKeySet();
  
  public static interface AbortCheck {
    public boolean abortCheck();
  }
  
  private final Http2SolrClient solrClient;

  private final Phaser phaser = new RequestPhaser();

  public SolrCmdDistributor(UpdateShardHandler updateShardHandler) {
    assert ObjectReleaseTracker.track(this);
    this.solrClient = new Http2SolrClient.Builder().markInternalRequest().withHttpClient(updateShardHandler.getTheSharedHttpClient()).idleTimeout(60000).build();
  }

  public void finish() {
    assert !finished : "lifecycle sanity check";

    solrClient.waitForOutstandingRequests();
    finished = true;
  }
  
  public void close() {
    IOUtils.closeQuietly(solrClient);
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
      if (rspCode == 404 || rspCode == 403 || rspCode == 503) {
        doRetry = true;
      }

      // if it's a connect exception, lets try again
      if (err.t instanceof SolrServerException) {
        if (((SolrServerException) err.t).getRootCause() instanceof ConnectException) {
          doRetry = true;
        }
      }

      if (err.t instanceof ConnectException) {
        doRetry = true;
      }

      if (err.req.retries < maxRetriesOnForward && doRetry) {
        err.req.retries++;

        SolrException.log(SolrCmdDistributor.log, "forwarding update to "
                + oldNodeUrl + " failed - retrying ... retries: "
                + err.req.retries + " " + err.req.cmd.toString() + " params:"
                + err.req.uReq.getParams() + " rsp:" + rspCode, err.t);
        log.info("check retry true");
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

    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      uReq.setCommitWithin(cmd.commitWithin);
      if (cmd.isDeleteById()) {
        uReq.deleteById(cmd.getId(), cmd.getRoute(), cmd.getVersion());
      } else {
        solrClient.waitForOutstandingRequests();

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
    Set<CountDownLatch> latches = new HashSet<>(nodes.size());

    // we need to do any retries before commit...
    blockAndDoRetries();
    if (log.isDebugEnabled()) log.debug("Distrib commit to: {} params: {}", nodes, params);

    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);

      addCommit(uReq, cmd);
      submit(new Req(cmd, node, uReq, false));
    }

  }

  public void blockAndDoRetries() {
    if (phaser.getUnarrivedParties() <= 1) {
      return;
    }
    phaser.arriveAndAwaitAdvance();
  }
  
  void addCommit(UpdateRequest ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes, cmd.openSearcher);
  }

  private void submit(final Req req) {

    if (log.isDebugEnabled()) {
      log.debug("sending update to "
          + req.node.getUrl() + " retry:"
          + req.retries + " " + req.cmd + " params:" + req.uReq.getParams());
    }
    try {
      req.uReq.setBasePath(req.node.getUrl());

      if (req.synchronous) {
        blockAndDoRetries();

        try {
          req.uReq.setBasePath(req.node.getUrl());
          solrClient.request(req.uReq);
        } catch (Exception e) {
          log.error("Exception sending synchronous dist update", e);
          Error error = new Error();
          error.t = e;
          error.req = req;
          if (e instanceof SolrException) {
            error.statusCode = ((SolrException) e).code();
          }
          allErrors.add(error);
        }

        return;
      }

      if (req.cmd instanceof CommitUpdateCommand) {
        // commit
      } else {
        phaser.register();
      }

      solrClient.request(req.uReq, null, new Http2SolrClient.OnComplete() {

        @Override
        public void onSuccess(NamedList result) {
          if (log.isDebugEnabled()) log.debug("Success for distrib update {}", result);
          arrive(req);
        }

        @Override
        public void onFailure(Throwable t) {
          log.error("Exception sending dist update", t);
          arrive(req);

          Error error = new Error();
          error.t = t;
          error.req = req;
          if (t instanceof SolrException) {
            error.statusCode = ((SolrException) t).code();
          }

          if (checkRetry(error)) {
            log.info("Retrying distrib update on error: {}", t.getMessage());
            submit(req);
          } else {
            allErrors.add(error);
          }

        }});
    } catch (Exception e) {
      log.error("Exception sending dist update", e);
      arrive(req);
      Error error = new Error();
      error.t = e;
      error.req = req;
      if (e instanceof SolrException) {
        error.statusCode = ((SolrException) e).code();
      }
      if (checkRetry(error)) {
        submit(req);
      } else {
        allErrors.add(error);
      }
    }
  }

  private void arrive(Req req) {
    if (req.cmd instanceof  CommitUpdateCommand) {
      // commit or delete by query
    } else {
      phaser.arriveAndDeregister();
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
          ParWork.propegateInterrupt(e);
          log.warn("Failed to parse response from {} during replication factor accounting", node, e);
        }
      }
      return Integer.MAX_VALUE;
    }
  }

  public static volatile Diagnostics.Callable testing_errorHook;  // called on error when forwarding request.  Currently data=[this, Request]

  public static class Error {
    public Throwable t;
    public int statusCode = -1;

    public Req req;
    
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
    public abstract ZkCoreNodeProps getNodeProps();
    public abstract String getCollection();
    public abstract String getShardId();
    public abstract int getMaxRetries();
  }

  public static class StdNode extends Node {
    protected ZkCoreNodeProps nodeProps;
    protected String collection;
    protected String shardId;
    private final boolean retry;
    private final int maxRetries;

    public StdNode(ZkCoreNodeProps nodeProps) {
      this(nodeProps, null, null, 0);
    }
    
    public StdNode(ZkCoreNodeProps nodeProps, String collection, String shardId) {
      this(nodeProps, collection, shardId, 0);
    }
    
    public StdNode(ZkCoreNodeProps nodeProps, String collection, String shardId, int maxRetries) {
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

    @Override
    public int getMaxRetries() {
      return this.maxRetries;
    }
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class ForwardNode extends StdNode {
    
    private ZkStateReader zkStateReader;
    
    public ForwardNode(ZkCoreNodeProps nodeProps, ZkStateReader zkStateReader, String collection, String shardId) {
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
        ParWork.propegateInterrupt(e);
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

  public Set<Error> getErrors() {
    return allErrors;
  }

  private static class RequestPhaser extends Phaser {
    public RequestPhaser() {
      super(1);
    }

    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  }
}

