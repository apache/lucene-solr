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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequestExt;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AdjustableSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolrCmdDistributor {
  private static final int MAX_RETRIES_ON_FORWARD = 10;
  public static Logger log = LoggerFactory.getLogger(SolrCmdDistributor.class);

  static final HttpClient client;
  static AdjustableSemaphore semaphore = new AdjustableSemaphore(8);
  
  static {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 500);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 16);
    client = HttpClientUtil.createClient(params);
  }
  
  CompletionService<Request> completionService;
  Set<Future<Request>> pending;
  
  int maxBufferedAddsPerServer = 10;
  int maxBufferedDeletesPerServer = 10;

  private Response response = new Response();
  
  private final Map<Node,List<AddRequest>> adds = new HashMap<Node,List<AddRequest>>();
  private final Map<Node,List<DeleteRequest>> deletes = new HashMap<Node,List<DeleteRequest>>();
  
  class AddRequest {
    AddUpdateCommand cmd;
    ModifiableSolrParams params;
  }
  
  class DeleteRequest {
    DeleteUpdateCommand cmd;
    ModifiableSolrParams params;
  }
  
  public static interface AbortCheck {
    public boolean abortCheck();
  }
  
  public SolrCmdDistributor(int numHosts, ThreadPoolExecutor executor) {
    int maxPermits = Math.max(16, numHosts * 16);
    // limits how many tasks can actually execute at once
    if (maxPermits != semaphore.getMaxPermits()) {
      semaphore.setMaxPermits(maxPermits);
    }

    completionService = new ExecutorCompletionService<Request>(executor);
    pending = new HashSet<Future<Request>>();
  }
  
  public void finish() {

    flushAdds(1);
    flushDeletes(1);

    checkResponses(true);
  }
  
  public void distribDelete(DeleteUpdateCommand cmd, List<Node> urls, ModifiableSolrParams params) throws IOException {
    checkResponses(false);
    
    if (cmd.isDeleteById()) {
      doDelete(cmd, urls, params);
    } else {
      doDelete(cmd, urls, params);
    }
  }
  
  public void distribAdd(AddUpdateCommand cmd, List<Node> nodes, ModifiableSolrParams params) throws IOException {
    checkResponses(false);
    
    // make sure any pending deletes are flushed
    flushDeletes(1);
    
    // TODO: this is brittle
    // need to make a clone since these commands may be reused
    AddUpdateCommand clone = new AddUpdateCommand(null);
    
    clone.solrDoc = cmd.solrDoc;
    clone.commitWithin = cmd.commitWithin;
    clone.overwrite = cmd.overwrite;
    clone.setVersion(cmd.getVersion());
    AddRequest addRequest = new AddRequest();
    addRequest.cmd = clone;
    addRequest.params = params;

    for (Node node : nodes) {
      List<AddRequest> alist = adds.get(node);
      if (alist == null) {
        alist = new ArrayList<AddRequest>(2);
        adds.put(node, alist);
      }
      alist.add(addRequest);
    }
    
    flushAdds(maxBufferedAddsPerServer);
  }
  
  public void distribCommit(CommitUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) throws IOException {
    
    // make sure we are ordered
    flushAdds(1);
    flushDeletes(1);

    
    // Wait for all outstanding responses to make sure that a commit
    // can't sneak in ahead of adds or deletes we already sent.
    // We could do this on a per-server basis, but it's more complex
    // and this solution will lead to commits happening closer together.
    checkResponses(true);
    
    // currently, we dont try to piggy back on outstanding adds or deletes
    
    UpdateRequestExt ureq = new UpdateRequestExt();
    ureq.setParams(params);
    
    addCommit(ureq, cmd);
    
    log.info("Distrib commit to:" + nodes + " params:" + params);
    
    for (Node node : nodes) {
      submit(ureq, node);
    }
    
    // if the command wanted to block until everything was committed,
    // then do that here.
    
    if (cmd.waitSearcher) {
      checkResponses(true);
    }
  }
  
  private void doDelete(DeleteUpdateCommand cmd, List<Node> nodes,
      ModifiableSolrParams params) {
    
    flushAdds(1);
    
    DeleteUpdateCommand clonedCmd = clone(cmd);
    DeleteRequest deleteRequest = new DeleteRequest();
    deleteRequest.cmd = clonedCmd;
    deleteRequest.params = params;
    for (Node node : nodes) {
      List<DeleteRequest> dlist = deletes.get(node);
      
      if (dlist == null) {
        dlist = new ArrayList<DeleteRequest>(2);
        deletes.put(node, dlist);
      }
      dlist.add(deleteRequest);
    }
    
    flushDeletes(maxBufferedDeletesPerServer);
  }
  
  void addCommit(UpdateRequestExt ureq, CommitUpdateCommand cmd) {
    if (cmd == null) return;
    ureq.setAction(cmd.optimize ? AbstractUpdateRequest.ACTION.OPTIMIZE
        : AbstractUpdateRequest.ACTION.COMMIT, false, cmd.waitSearcher, cmd.maxOptimizeSegments, cmd.softCommit, cmd.expungeDeletes);
  }
  
  boolean flushAdds(int limit) {
    // check for pending deletes
  
    Set<Node> removeNodes = new HashSet<Node>();
    Set<Node> nodes = adds.keySet();
 
    for (Node node : nodes) {
      List<AddRequest> alist = adds.get(node);
      if (alist == null || alist.size() < limit) continue;
  
      UpdateRequestExt ureq = new UpdateRequestExt();
      
      ModifiableSolrParams combinedParams = new ModifiableSolrParams();
      
      for (AddRequest aReq : alist) {
        AddUpdateCommand cmd = aReq.cmd;
        combinedParams.add(aReq.params);
       
        ureq.add(cmd.solrDoc, cmd.commitWithin, cmd.overwrite);
      }
      
      if (ureq.getParams() == null) ureq.setParams(new ModifiableSolrParams());
      ureq.getParams().add(combinedParams);

      removeNodes.add(node);
      
      submit(ureq, node);
    }
    
    for (Node node : removeNodes) {
      adds.remove(node);
    }
    
    return true;
  }
  
  boolean flushDeletes(int limit) {
    // check for pending deletes
 
    Set<Node> removeNodes = new HashSet<Node>();
    Set<Node> nodes = deletes.keySet();
    for (Node node : nodes) {
      List<DeleteRequest> dlist = deletes.get(node);
      if (dlist == null || dlist.size() < limit) continue;
      UpdateRequestExt ureq = new UpdateRequestExt();
      
      ModifiableSolrParams combinedParams = new ModifiableSolrParams();
      
      for (DeleteRequest dReq : dlist) {
        DeleteUpdateCommand cmd = dReq.cmd;
        combinedParams.add(dReq.params);
        if (cmd.isDeleteById()) {
          ureq.deleteById(cmd.getId(), cmd.getVersion());
        } else {
          ureq.deleteByQuery(cmd.query);
        }
        
        if (ureq.getParams() == null) ureq
            .setParams(new ModifiableSolrParams());
        ureq.getParams().add(combinedParams);
      }
      
      removeNodes.add(node);
      submit(ureq, node);
    }
    
    for (Node node : removeNodes) {
      deletes.remove(node);
    }
    
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
    public Node node;
    UpdateRequestExt ureq;
    NamedList<Object> ursp;
    int rspCode;
    public Exception exception;
    int retries;
  }
  
  void submit(UpdateRequestExt ureq, Node node) {
    Request sreq = new Request();
    sreq.node = node;
    sreq.ureq = ureq;
    submit(sreq);
  }
  
  public void submit(final Request sreq) {

    final String url = sreq.node.getUrl();

    Callable<Request> task = new Callable<Request>() {
      @Override
      public Request call() throws Exception {
        Request clonedRequest = null;
        try {
          clonedRequest = new Request();
          clonedRequest.node = sreq.node;
          clonedRequest.ureq = sreq.ureq;
          clonedRequest.retries = sreq.retries;
          
          String fullUrl;
          if (!url.startsWith("http://") && !url.startsWith("https://")) {
            fullUrl = "http://" + url;
          } else {
            fullUrl = url;
          }
  
          HttpSolrServer server = new HttpSolrServer(fullUrl,
              client);
          
          if (Thread.currentThread().isInterrupted()) {
            clonedRequest.rspCode = 503;
            clonedRequest.exception = new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Shutting down.");
            return clonedRequest;
          }
          
          clonedRequest.ursp = server.request(clonedRequest.ureq);
          
          // currently no way to get the request body.
        } catch (Exception e) {
          clonedRequest.exception = e;
          if (e instanceof SolrException) {
            clonedRequest.rspCode = ((SolrException) e).code();
          } else {
            clonedRequest.rspCode = -1;
          }
        } finally {
          semaphore.release();
        }
        return clonedRequest;
      }
    };
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Update thread interrupted", e);
    }
    try {
      pending.add(completionService.submit(task));
    } catch (RejectedExecutionException e) {
      semaphore.release();
      throw e;
    }
    
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
            
            // if there is a retry url, we want to retry...
            boolean isRetry = sreq.node.checkRetry();
            boolean doRetry = false;
            int rspCode = sreq.rspCode;
            
            // this can happen in certain situations such as shutdown
            if (isRetry) {
              if (rspCode == 404 || rspCode == 403 || rspCode == 503
                  || rspCode == 500) {
                doRetry = true;
              }
              
              // if its an ioexception, lets try again
              if (sreq.exception instanceof IOException) {
                doRetry = true;
              } else if (sreq.exception instanceof SolrServerException) {
                if (((SolrServerException) sreq.exception).getRootCause() instanceof IOException) {
                  doRetry = true;
                }
              }
            }
            
            if (isRetry && sreq.retries < MAX_RETRIES_ON_FORWARD && doRetry) {
              sreq.retries++;
              sreq.rspCode = 0;
              sreq.exception = null;
              SolrException.log(SolrCmdDistributor.log, "forwarding update to " + sreq.node.getUrl() + " failed - retrying ... ");
              Thread.sleep(500);
              submit(sreq);
              checkResponses(block);
            } else {
              Exception e = sreq.exception;
              Error error = new Error();
              error.e = e;
              error.node = sreq.node;
              response.errors.add(error);
              response.sreq = sreq;
              SolrException.log(SolrCmdDistributor.log, "shard update error "
                  + sreq.node, sreq.exception);
            }
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
  
  public static class Response {
    public Request sreq;
    public List<Error> errors = new ArrayList<Error>();
  }
  
  public static class Error {
    public Node node;
    public Exception e;
  }

  public Response getResponse() {
    return response;
  }
  
  public static abstract class Node {
    public abstract String getUrl();
    public abstract boolean checkRetry();
    public abstract String getCoreName();
    public abstract String getBaseUrl();
    public abstract ZkCoreNodeProps getNodeProps();
  }

  public static class StdNode extends Node {
    protected String url;
    protected String baseUrl;
    protected String coreName;
    private ZkCoreNodeProps nodeProps;

    public StdNode(ZkCoreNodeProps nodeProps) {
      this.url = nodeProps.getCoreUrl();
      this.baseUrl = nodeProps.getBaseUrl();
      this.coreName = nodeProps.getCoreName();
      this.nodeProps = nodeProps;
    }
    
    @Override
    public String getUrl() {
      return url;
    }
    
    @Override
    public String toString() {
      return this.getClass().getSimpleName() + ": " + url;
    }

    @Override
    public boolean checkRetry() {
      return false;
    }

    @Override
    public String getBaseUrl() {
      return baseUrl;
    }

    @Override
    public String getCoreName() {
      return coreName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
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
      if (baseUrl == null) {
        if (other.baseUrl != null) return false;
      } else if (!baseUrl.equals(other.baseUrl)) return false;
      if (coreName == null) {
        if (other.coreName != null) return false;
      } else if (!coreName.equals(other.coreName)) return false;
      if (url == null) {
        if (other.url != null) return false;
      } else if (!url.equals(other.url)) return false;
      return true;
    }

    public ZkCoreNodeProps getNodeProps() {
      return nodeProps;
    }
  }
  
}


