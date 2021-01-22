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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.SolrSingleThreaded;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.tracing.GlobalTracer;
import org.apache.solr.util.tracing.SolrRequestCarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@SolrSingleThreaded
public class HttpShardHandler extends ShardHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String[] TS = new String[0];
  public static final String[] TS1 = new String[0];
  /**
   * If the request context map has an entry with this key and Boolean.TRUE as value,
   * {@link #prepDistributed(ResponseBuilder)} will only include {@link org.apache.solr.common.cloud.Replica.Type#NRT} replicas as possible
   * destination of the distributed request (or a leader replica of type {@link org.apache.solr.common.cloud.Replica.Type#TLOG}). This is used
   * by the RealtimeGet handler, since other types of replicas shouldn't respond to RTG requests
   */
  public static String ONLY_NRT_REPLICAS = "distribOnlyRealtime";

  private final HttpShardHandlerFactory httpShardHandlerFactory;
  private final Map<ShardResponse,Cancellable> responseCancellableMap;
  private final BlockingQueue<ShardResponse> responses;
  private final AtomicInteger pending;
  private final Map<String, List<String>> shardToURLs;
  private final LBHttp2SolrClient lbClient;

  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory) {
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    this.lbClient = httpShardHandlerFactory.loadbalancer;
    this.pending = new AtomicInteger(0);
    this.responses = new LinkedBlockingQueue<>();
    this.responseCancellableMap = new ConcurrentHashMap<>(32);

    // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
    // This is primarily to keep track of what order we should use to query the replicas of a shard
    // so that we use the same replica for all phases of a distributed request.
    shardToURLs = new ConcurrentHashMap<>();
  }

  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory, LBHttp2SolrClient lb) {
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    this.lbClient = lb;
    this.pending = new AtomicInteger(0);
    this.responses = new LinkedBlockingQueue<>();
    this.responseCancellableMap = new ConcurrentHashMap<>();

    // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
    // This is primarily to keep track of what order we should use to query the replicas of a shard
    // so that we use the same replica for all phases of a distributed request.
    shardToURLs = new ConcurrentHashMap<>();
  }


  private static class SimpleSolrResponse extends SolrResponse {

    volatile long elapsedTime;

    volatile NamedList<Object> nl;

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

  // Not thread safe... don't use in Callable.
  // Don't modify the returned URL list.
  private List<String> getURLs(String shard) {
    if (shard == null) {
      return Collections.emptyList();
    }
    List<String> urls = shardToURLs.get(shard);
    if (urls == null) {
      urls = httpShardHandlerFactory.buildURLList(shard);
      shardToURLs.put(shard, urls);
    }
    return urls;
  }

  @Override
  public void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
    // do this outside of the callable for thread safety reasons
    final List<String> urls = getURLs(shard);
    final Tracer tracer = GlobalTracer.getTracer();
    final Span span = tracer != null ? tracer.activeSpan() : null;

    params.remove(CommonParams.WT); // use default (currently javabin)
    params.remove(CommonParams.VERSION);
    QueryRequest req = makeQueryRequest(sreq, params, shard);
    req.setMethod(SolrRequest.METHOD.POST);

    LBSolrClient.Req lbReq = httpShardHandlerFactory.newLBHttpSolrClientReq(req, urls);

    ShardResponse srsp = new ShardResponse();
    if (sreq.nodeName != null) {
      srsp.setNodeName(sreq.nodeName);
    }
    srsp.setShardRequest(sreq);
    srsp.setShard(shard);
    SimpleSolrResponse ssr = new SimpleSolrResponse();
    srsp.setSolrResponse(ssr);

    pending.incrementAndGet();
    // if there are no shards available for a slice, urls.size()==0 asyncTracker.register();
    if (urls.size() == 0 && shard != null && !"".equals(shard)) {
      // TODO: what's the right error code here? We should use the same thing when
      // all of the servers for a shard are down.
      SolrException exception = new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "no servers hosting shard: " + shard);
      srsp.setException(exception);
      srsp.setResponseCode(exception.code());
      responses.add(srsp);
      return;
    }

    // all variables that set inside this listener must be at least volatile
    responseCancellableMap.put(srsp, this.lbClient.asyncReq(lbReq, new AsyncListener<>() {
      volatile long startTime = System.nanoTime();

      @Override
      public void onStart() {
        if (tracer != null && span != null) {
          tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS, new SolrRequestCarrier(req));
        }
        SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
        if (requestInfo != null) req.setUserPrincipal(requestInfo.getReq().getUserPrincipal());
      }

      @Override
      public void onSuccess(LBSolrClient.Rsp rsp) {
        ssr.nl = rsp.getResponse();
        srsp.setShardAddress(rsp.getServer());
        ssr.elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        responses.add(srsp);
      }

      public void onFailure(Throwable throwable, int code) {
        ssr.elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        srsp.setException(throwable);
        if (throwable instanceof SolrException) {
          srsp.setResponseCode(((SolrException) throwable).code());
        }
        responses.add(srsp);
      }
    }));
  }

  /**
   * Subclasses could modify the request based on the shard
   */
  protected QueryRequest makeQueryRequest(final ShardRequest sreq, ModifiableSolrParams params, String shard) {
    // use generic request to avoid extra processing of queries
    return new QueryRequest(params);
  }

  /**
   * Subclasses could modify the Response based on the shard
   */
  protected ShardResponse transfomResponse(final ShardRequest sreq, ShardResponse rsp, String shard) {
    return rsp;
  }

  /**
   * returns a ShardResponse of the last response correlated with a ShardRequest.  This won't
   * return early if it runs into an error.
   **/
  @Override
  public ShardResponse takeCompletedIncludingErrors() {
    return take(false);
  }


  /**
   * returns a ShardResponse of the last response correlated with a ShardRequest,
   * or immediately returns a ShardResponse if there was an error detected
   */
  @Override
  public ShardResponse takeCompletedOrError() {
    return take(true);
  }

  private ShardResponse take(boolean bailOnError) {
    try {
      while (pending.get() > 0) {
        //log.info("loop ing in httpshardhandler pending {}", pending.get());


//        ShardResponse rsp = responses.poll(3, TimeUnit.SECONDS);
//
//        if (rsp == null) {
////          if (pending.get() > 0 && httpShardHandlerFactory.isClosed()) {
////            throw new AlreadyClosedException();
////          }
//          continue;
//        }
        ShardResponse rsp = responses.take();
        responseCancellableMap.remove(rsp);

        pending.decrementAndGet();

        // add response to the response list... we do this after the take() and
        // not after the completion of "call" so we know when the last response
        // for a request was received.  Otherwise we might return the same
        // request more than once.
        rsp.getShardRequest().responses.add(rsp);

        if (bailOnError && rsp.getException() != null) {
          return rsp; // if exception, return immediately
        }

        if (rsp.getShardRequest().responses.size() == rsp.getShardRequest().actualShards.length) {
          return rsp;
        }
      }
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return null;
  }


  @Override
  public void cancelAll() {
    responseCancellableMap.values().forEach(cancellable -> {
      cancellable.cancel();
      pending.decrementAndGet();
    });

    responseCancellableMap.clear();
  }

  @Override
  public void prepDistributed(ResponseBuilder rb) {
    final SolrQueryRequest req = rb.req;
    final SolrParams params = req.getParams();
    final String shards = params.get(ShardParams.SHARDS);

    CoreDescriptor coreDescriptor = req.getCore().getCoreDescriptor();
    CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
    ZkController zkController = req.getCore().getCoreContainer().getZkController();

    final ReplicaListTransformer replicaListTransformer = httpShardHandlerFactory.getReplicaListTransformer(req);

    HttpShardHandlerFactory.WhitelistHostChecker hostChecker = httpShardHandlerFactory.getWhitelistHostChecker();
    if (shards != null && zkController == null && hostChecker.isWhitelistHostCheckingEnabled() && !hostChecker.hasExplicitWhitelist()) {
      throw new SolrException(SolrException.ErrorCode.FORBIDDEN, "HttpShardHandlerFactory " + HttpShardHandlerFactory.INIT_SHARDS_WHITELIST
          + " not configured but required (in lieu of ZkController and ClusterState) when using the '" + ShardParams.SHARDS + "' parameter."
          + HttpShardHandlerFactory.SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE);
    }

    ReplicaSource replicaSource;
    if (zkController != null) {
      boolean onlyNrt = Boolean.TRUE == req.getContext().get(ONLY_NRT_REPLICAS);

      replicaSource = new CloudReplicaSource.Builder()
          .params(params)
          .zkStateReader(zkController.getZkStateReader())
          .whitelistHostChecker(hostChecker)
          .replicaListTransformer(replicaListTransformer)
          .collection(cloudDescriptor.getCollectionName())
          .onlyNrt(onlyNrt)
          .build();
      rb.slices = replicaSource.getSliceNames().toArray(new String[replicaSource.getSliceCount()]);

      if (canShortCircuit(rb.slices, onlyNrt, params, cloudDescriptor)) {
        rb.isDistrib = false;
        rb.shortCircuitedURL = Replica.getCoreUrl(zkController.getBaseUrl(), coreDescriptor.getName());
        return;
        // We shouldn't need to do anything to handle "shard.rows" since it was previously meant to be an optimization?
      }

//      for (int i = 0; i < rb.slices.length; i++) {
//        boolean noReplicas = replicaSource.getReplicasBySlice(i).isEmpty();
//        if (!ShardParams.getShardsTolerantAsBool(params) && noReplicas) {
//          // stop the check when there are no replicas available for a shard
//          // todo fix use of slices[i] which can be null if user specified urls in shards param
//          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
//              "no servers hosting shard: " + rb.slices[i]);
//        }
//      }
    } else {
      replicaSource = new StandaloneReplicaSource.Builder()
          .whitelistHostChecker(hostChecker)
          .shards(shards)
          .build();
      rb.slices = new String[replicaSource.getSliceCount()];
    }

    rb.shards = new String[rb.slices.length];
    for (int i = 0; i < rb.slices.length; i++) {
      rb.shards[i] = createSliceShardsStr(replicaSource.getReplicasBySlice(i));
    }

    String shards_rows = params.get(ShardParams.SHARDS_ROWS);
    if (shards_rows != null) {
      rb.shards_rows = Integer.parseInt(shards_rows);
    }
    String shards_start = params.get(ShardParams.SHARDS_START);
    if (shards_start != null) {
      rb.shards_start = Integer.parseInt(shards_start);
    }
  }

  private static String createSliceShardsStr(final List<String> shardUrls) {
    final StringBuilder sliceShardsStr = new StringBuilder();
    boolean first = true;
    for (String shardUrl : shardUrls) {
      if (first) {
        first = false;
      } else {
        sliceShardsStr.append('|');
      }
      sliceShardsStr.append(shardUrl);
    }
    return sliceShardsStr.toString();
  }

  private boolean canShortCircuit(String[] slices, boolean onlyNrtReplicas, SolrParams params, CloudDescriptor cloudDescriptor) {
    // Are we hosting the shard that this request is for, and are we active? If so, then handle it ourselves
    // and make it a non-distributed request.
    String ourSlice = cloudDescriptor.getShardId();
    String ourCollection = cloudDescriptor.getCollectionName();
    // Some requests may only be fulfilled by replicas of type Replica.Type.NRT
    if (slices.length == 1 && slices[0] != null
        && (slices[0].equals(ourSlice) || slices[0].equals(ourCollection + "_" + ourSlice))  // handle the <collection>_<slice> format
        && cloudDescriptor.getLastPublished() == Replica.State.ACTIVE
        && (!onlyNrtReplicas || cloudDescriptor.getReplicaType() == Replica.Type.NRT)) {
      boolean shortCircuit = params.getBool("shortCircuit", true);       // currently just a debugging parameter to check distrib search on a single node

      String targetHandler = params.get(ShardParams.SHARDS_QT);
      shortCircuit = shortCircuit && targetHandler == null;             // if a different handler is specified, don't short-circuit

      return shortCircuit;
    }
    return false;
  }

  public ShardHandlerFactory getShardHandlerFactory() {
    return httpShardHandlerFactory;
  }

}