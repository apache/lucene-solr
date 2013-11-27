package org.apache.solr.update;

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

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Diagnostics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrCmdDistributor {
  private static final int MAX_RETRIES_ON_FORWARD = 25;
  public static Logger log = LoggerFactory.getLogger(SolrCmdDistributor.class);
  
  private StreamingSolrServers servers;
  
  private int retryPause = 500;
  private int maxRetriesOnForward = MAX_RETRIES_ON_FORWARD;
  
  private List<Error> allErrors = new ArrayList<Error>();
  private List<Error> errors = new ArrayList<Error>();
  
  public static interface AbortCheck {
    public boolean abortCheck();
  }
  
  public SolrCmdDistributor(UpdateShardHandler updateShardHandler) {
    servers = new StreamingSolrServers(updateShardHandler);
  }
  
  public SolrCmdDistributor(StreamingSolrServers servers, int maxRetriesOnForward, int retryPause) {
    this.servers = servers;
    this.maxRetriesOnForward = maxRetriesOnForward;
    this.retryPause = retryPause;
  }
  
  public void finish() {
    try {
      servers.blockUntilFinished();
      doRetriesIfNeeded();
    } finally {
      servers.shutdown();
    }
  }

  private void doRetriesIfNeeded() {
    // NOTE: retries will be forwards to a single url
    
    List<Error> errors = new ArrayList<Error>(this.errors);
    errors.addAll(servers.getErrors());
    List<Error> resubmitList = new ArrayList<Error>();

    for (Error err : errors) {
      try {
        String oldNodeUrl = err.req.node.getUrl();
        
        // if there is a retry url, we want to retry...
        boolean isRetry = err.req.node.checkRetry();
        
        boolean doRetry = false;
        int rspCode = err.statusCode;
        
        if (testing_errorHook != null) Diagnostics.call(testing_errorHook,
            err.e);
        
        // this can happen in certain situations such as shutdown
        if (isRetry) {
          if (rspCode == 404 || rspCode == 403 || rspCode == 503) {
            doRetry = true;
          }
          
          // if its a connect exception, lets try again
          if (err.e instanceof ConnectException) {
            doRetry = true;
          } else if (err.e instanceof SolrServerException) {
            if (((SolrServerException) err.e).getRootCause() instanceof ConnectException) {
              doRetry = true;
            }
          } else if (err.e instanceof RemoteSolrException) {
            Exception cause = (RemoteSolrException) err.e.getCause();
            if (cause != null && cause instanceof ConnectException) {
              doRetry = true;
            }
          }
          if (err.req.retries < maxRetriesOnForward && doRetry) {
            err.req.retries++;
            
            SolrException.log(SolrCmdDistributor.log, "forwarding update to "
                + oldNodeUrl + " failed - retrying ... retries: "
                + err.req.retries + " " + err.req.cmdString, err.e);
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
    
    servers.clearErrors();
    this.errors.clear();
    for (Error err : resubmitList) {
      submit(err.req);
    }
    
    if (resubmitList.size() > 0) {
      servers.blockUntilFinished();
      doRetriesIfNeeded();
    }
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribDelete(cmd, nodes, params, false);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean sync) throws IOException {
    
    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      if (cmd.isDeleteById()) {
        uReq.deleteById(cmd.getId(), cmd.getVersion());
      } else {
        uReq.deleteByQuery(cmd.query);
      }
      
      submit(new Req(cmd.toString(), node, uReq, sync));
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    distribAdd(cmd, nodes, params, false);
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params, boolean synchronous) throws IOException {

    for (Node node : nodes) {
      UpdateRequest uReq = new UpdateRequest();
      uReq.setParams(params);
      uReq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      submit(new Req(cmd.toString(), node, uReq, synchronous));
    }
    
  }

  public void distribCommit(CommitUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) throws IOException {
    
    // we need to do any retries before commit...
    servers.blockUntilFinished();
    doRetriesIfNeeded();
    
    UpdateRequest uReq = new UpdateRequest();
    uReq.setParams(params);
    
    addCommit(uReq, cmd);
    
    log.debug("Distrib commit to:" + nodes + " params:" + params);
    
    for (Node node : nodes) {
      submit(new Req(cmd.toString(), node, uReq, false));
    }
    
  }
  
  void addCommit(UpdateRequest ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes);
  }

  private void submit(Req req) {
    if (req.synchronous) {
      servers.blockUntilFinished();
      doRetriesIfNeeded();
      
      HttpSolrServer server = new HttpSolrServer(req.node.getUrl(),
          servers.getHttpClient());
      try {
        server.request(req.uReq);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed synchronous update on shard " + req.node + " update: " + req.uReq , e);
      } finally {
        server.shutdown();
      }
      
      return;
    }
    
    try {
      SolrServer solrServer = servers.getSolrServer(req);
      NamedList<Object> rsp = solrServer.request(req.uReq);
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
    public String cmdString;
    
    public Req(String cmdString, Node node, UpdateRequest uReq, boolean synchronous) {
      this.node = node;
      this.uReq = uReq;
      this.synchronous = synchronous;
      this.cmdString = cmdString;
    }
  }
    

  public static Diagnostics.Callable testing_errorHook;  // called on error when forwarding request.  Currently data=[this, Request]

  
  public static class Response {
    public List<Error> errors = new ArrayList<Error>();
  }
  
  public static class Error {
    public Exception e;
    public int statusCode = -1;
    public Req req;
  }
  
  public static abstract class Node {
    public abstract String getUrl();
    public abstract boolean checkRetry();
    public abstract String getCoreName();
    public abstract String getBaseUrl();
    public abstract ZkCoreNodeProps getNodeProps();
  }

  public static class StdNode extends Node {
    protected ZkCoreNodeProps nodeProps;

    public StdNode(ZkCoreNodeProps nodeProps) {
      this.nodeProps = nodeProps;
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
    private String collection;
    private String shardId;
    
    public RetryNode(ZkCoreNodeProps nodeProps, ZkStateReader zkStateReader, String collection, String shardId) {
      super(nodeProps);
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


