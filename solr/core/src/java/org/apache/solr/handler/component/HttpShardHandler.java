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
package org.apache.solr.handler.component;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;

import java.util.*;
import java.util.concurrent.*;

public class HttpShardHandler extends ShardHandler {

  private HttpShardHandlerFactory httpShardHandlerFactory;
  private CompletionService<ShardResponse> completionService;
  private Set<Future<ShardResponse>> pending;
  private Map<String, List<String>> shardToURLs;

  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory) {
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    completionService = new ExecutorCompletionService<ShardResponse>(httpShardHandlerFactory.commExecutor);
    pending = new HashSet<Future<ShardResponse>>();

    // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
    // This is primarily to keep track of what order we should use to query the replicas of a shard
    // so that we use the same replica for all phases of a distributed request.
    shardToURLs = new HashMap<String, List<String>>();
  }

  private static class SimpleSolrResponse extends SolrResponse {
    long elapsedTime;
    NamedList<Object> nl;

    @Override
    public long getElapsedTime() {
      return elapsedTime;
    }

    @Override
    public NamedList<Object> getResponse() {
      return nl;
    }

    @Override
    public void setResponse(NamedList<Object> rsp) {
      nl = rsp;
    }
  }

  // Not thread safe... don't use in Callable.
  // Don't modify the returned URL list.
  private List<String> getURLs(String shard) {
    List<String> urls = shardToURLs.get(shard);
    if (urls == null) {
      urls = StrUtils.splitSmart(shard, "|", true);

      // convert shard to URL
      for (int i = 0; i < urls.size(); i++) {
        urls.set(i, httpShardHandlerFactory.scheme + urls.get(i));
      }

      //
      // Shuffle the list instead of use round-robin by default.
      // This prevents accidental synchronization where multiple shards could get in sync
      // and query the same replica at the same time.
      //
      if (urls.size() > 1)
        Collections.shuffle(urls, httpShardHandlerFactory.r);
      shardToURLs.put(shard, urls);
    }
    return urls;
  }

  public void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
    // do this outside of the callable for thread safety reasons
    final List<String> urls = getURLs(shard);

    Callable<ShardResponse> task = new Callable<ShardResponse>() {
      public ShardResponse call() throws Exception {
        ShardResponse srsp = new ShardResponse();
        srsp.setShardRequest(sreq);
        srsp.setShard(shard);
        SimpleSolrResponse ssr = new SimpleSolrResponse();
        srsp.setSolrResponse(ssr);
        long startTime = System.currentTimeMillis();

        try {
          // String url = "http://" + shard + "/select";
          String url = "http://" + shard;

          params.remove(CommonParams.WT); // use default (currently javabin)
          params.remove(CommonParams.VERSION);

          SolrServer server = new CommonsHttpSolrServer(url, httpShardHandlerFactory.client);
          // SolrRequest req = new QueryRequest(SolrRequest.METHOD.POST, "/select");
          // use generic request to avoid extra processing of queries
          QueryRequest req = new QueryRequest(params);
          req.setMethod(SolrRequest.METHOD.POST);

          // no need to set the response parser as binary is the default
          // req.setResponseParser(new BinaryResponseParser());
          // srsp.rsp = server.request(req);
          // srsp.rsp = server.query(sreq.params);

          ssr.nl = server.request(req);
        } catch (Throwable th) {
          srsp.setException(th);
          if (th instanceof SolrException) {
            srsp.setResponseCode(((SolrException) th).code());
          } else {
            srsp.setResponseCode(-1);
          }
        }

        ssr.elapsedTime = System.currentTimeMillis() - startTime;

        return srsp;
      }
    };
    pending.add(completionService.submit(task));
  }

  /**
   * returns a ShardResponse of the last response correlated with a ShardRequest,
   * or immediately returns a ShardResponse if there was an error detected
   */
  public ShardResponse takeCompletedOrError() {
    while (pending.size() > 0) {
      try {
        Future<ShardResponse> future = completionService.take();
        pending.remove(future);
        ShardResponse rsp = future.get();
        if (rsp.getException() != null) return rsp; // if exception, return immediately
        // add response to the response list... we do this after the take() and
        // not after the completion of "call" so we know when the last response
        // for a request was received.  Otherwise we might return the same
        // request more than once.
        rsp.getShardRequest().responses.add(rsp);
        if (rsp.getShardRequest().responses.size() == rsp.getShardRequest().actualShards.length) {
          return rsp;
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (ExecutionException e) {
        // should be impossible... the problem with catching the exception
        // at this level is we don't know what ShardRequest it applied to
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Impossible Exception", e);
      }
    }
    return null;
  }

  public void cancelAll() {
    for (Future<ShardResponse> future : pending) {
      // TODO: any issues with interrupting?  shouldn't be if
      // there are finally blocks to release connections.
      future.cancel(true);
    }
  }

  public void checkDistributed(ResponseBuilder rb) {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    String shards_rows = params.get(ShardParams.SHARDS_ROWS);
    if (shards_rows != null) {
      rb.shards_rows = Integer.parseInt(shards_rows);
    }
    String shards_start = params.get(ShardParams.SHARDS_START);
    if (shards_start != null) {
      rb.shards_start = Integer.parseInt(shards_start);
    }
  }

}

