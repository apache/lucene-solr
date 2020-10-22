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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.SolrThreadUnsafe;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;

@SolrThreadUnsafe
public class HttpShardHandler extends ShardHandler {
  /**
   * If the request context map has an entry with this key and Boolean.TRUE as value,
   * {@link #prepDistributed(ResponseBuilder)} will only include {@link org.apache.solr.common.cloud.Replica.Type#NRT} replicas as possible
   * destination of the distributed request (or a leader replica of type {@link org.apache.solr.common.cloud.Replica.Type#TLOG}). This is used
   * by the RealtimeGet handler, since other types of replicas shouldn't respond to RTG requests
   */
  public static String ONLY_NRT_REPLICAS = "distribOnlyRealtime";

  final HttpShardHandlerFactory httpShardHandlerFactory;
  private CompletionService<ShardResponse> completionService;
  private Set<Future<ShardResponse>> pending;
  private Http2SolrClient httpClient;

  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory, Http2SolrClient httpClient) {
    this.httpClient = httpClient;
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    completionService = httpShardHandlerFactory.newCompletionService();
    pending = new HashSet<>();
  }


  @Override
  public void submit(final ShardRequest sreq, final String shard, final ModifiableSolrParams params) {
    ShardRequestor shardRequestor = new ShardRequestor(sreq, shard, params, this);
    try {
      shardRequestor.init();
      pending.add(completionService.submit(shardRequestor));
    } finally {
      shardRequestor.end();
    }
  }

  protected NamedList<Object> request(String url, @SuppressWarnings({"rawtypes"})SolrRequest req) throws IOException, SolrServerException {
    req.setBasePath(url);
    return httpClient.request(req);
  }

  /**
   * Subclasses could modify the request based on the shard
   */
  protected QueryRequest makeQueryRequest(final ShardRequest sreq, ModifiableSolrParams params, String shard) {
    // use generic request to avoid extra processing of queries
    return new QueryRequest(params);
  }

  /**
   * Subclasses could modify the Response based on the the shard
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

    while (pending.size() > 0) {
      try {
        Future<ShardResponse> future = completionService.take();
        pending.remove(future);
        ShardResponse rsp = future.get();
        if (bailOnError && rsp.getException() != null) return rsp; // if exception, return immediately
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


  @Override
  public void cancelAll() {
    for (Future<ShardResponse> future : pending) {
      future.cancel(false);
    }
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
        rb.shortCircuitedURL = ZkCoreNodeProps.getCoreUrl(zkController.getBaseUrl(), coreDescriptor.getName());
        return;
        // We shouldn't need to do anything to handle "shard.rows" since it was previously meant to be an optimization?
      }

      for (int i = 0; i < rb.slices.length; i++) {
        if (!ShardParams.getShardsTolerantAsBool(params) && replicaSource.getReplicasBySlice(i).isEmpty()) {
          // stop the check when there are no replicas available for a shard
          // todo fix use of slices[i] which can be null if user specified urls in shards param
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "no servers hosting shard: " + rb.slices[i]);
        }
      }
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
