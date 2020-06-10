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
package org.apache.solr.handler.component;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.SolrRequestCarrier;
import org.slf4j.MDC;

class ShardRequestor implements Callable<ShardResponse> {
  private final ShardRequest sreq;
  private final String shard;
  private final ModifiableSolrParams params;
  private final Tracer tracer;
  private final Span span;
  private final List<String> urls;
  private final HttpShardHandler httpShardHandler;

  // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
  // This is primarily to keep track of what order we should use to query the replicas of a shard
  // so that we use the same replica for all phases of a distributed request.
  private Map<String, List<String>> shardToURLs = new HashMap<>();

  public ShardRequestor(ShardRequest sreq, String shard, ModifiableSolrParams params, HttpShardHandler httpShardHandler) {
    this.sreq = sreq;
    this.shard = shard;
    this.params = params;
    this.httpShardHandler = httpShardHandler;
    // do this before call() for thread safety reasons
    this.urls = getURLs(shard);
    tracer = GlobalTracer.getTracer();
    span = tracer != null ? tracer.activeSpan() : null;
  }


  // Not thread safe... don't use in Callable.
  // Don't modify the returned URL list.
  private List<String> getURLs(String shard) {
    List<String> urls = shardToURLs.get(shard);
    if (urls == null) {
      urls = httpShardHandler.httpShardHandlerFactory.buildURLList(shard);
      shardToURLs.put(shard, urls);
    }
    return urls;
  }

  void init() {
    if (shard != null) {
      MDC.put("ShardRequest.shards", shard);
    }
    if (urls != null && !urls.isEmpty()) {
      MDC.put("ShardRequest.urlList", urls.toString());
    }
  }

  void end() {
    MDC.remove("ShardRequest.shards");
    MDC.remove("ShardRequest.urlList");
  }

  @Override
  public ShardResponse call() throws Exception {

    ShardResponse srsp = new ShardResponse();
    if (sreq.nodeName != null) {
      srsp.setNodeName(sreq.nodeName);
    }
    srsp.setShardRequest(sreq);
    srsp.setShard(shard);
    SimpleSolrResponse ssr = new SimpleSolrResponse();
    srsp.setSolrResponse(ssr);
    long startTime = System.nanoTime();

    try {
      params.remove(CommonParams.WT); // use default (currently javabin)
      params.remove(CommonParams.VERSION);

      QueryRequest req = httpShardHandler.makeQueryRequest(sreq, params, shard);
      if (tracer != null && span != null) {
        tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new SolrRequestCarrier(req));
      }
      req.setMethod(SolrRequest.METHOD.POST);
      SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
      if (requestInfo != null) req.setUserPrincipal(requestInfo.getReq().getUserPrincipal());

      // no need to set the response parser as binary is the defaultJab
      // req.setResponseParser(new BinaryResponseParser());

      // if there are no shards available for a slice, urls.size()==0
      if (urls.size() == 0) {
        // TODO: what's the right error code here? We should use the same thing when
        // all of the servers for a shard are down.
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "no servers hosting shard: " + shard);
      }

      if (urls.size() <= 1) {
        String url = urls.get(0);
        srsp.setShardAddress(url);
        ssr.nl = httpShardHandler.request(url, req);
      } else {
        LBSolrClient.Rsp rsp = httpShardHandler.httpShardHandlerFactory.makeLoadBalancedRequest(req, urls);
        ssr.nl = rsp.getResponse();
        srsp.setShardAddress(rsp.getServer());
      }
    } catch (ConnectException cex) {
      srsp.setException(cex); //????
    } catch (Exception th) {
      srsp.setException(th);
      if (th instanceof SolrException) {
        srsp.setResponseCode(((SolrException) th).code());
      } else {
        srsp.setResponseCode(-1);
      }
    }

    ssr.elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    return httpShardHandler.transfomResponse(sreq, srsp, shard);
  }

  static class SimpleSolrResponse extends SolrResponse {

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

    @Override
    public void setElapsedTime(long elapsedTime) {
      this.elapsedTime = elapsedTime;
    }
  }
}
