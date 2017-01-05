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

import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class HttpShardHandler extends ShardHandler {

  private HttpShardHandlerFactory httpShardHandlerFactory;
  private CompletionService<ShardResponse> completionService;
  private Set<Future<ShardResponse>> pending;
  private Map<String,List<String>> shardToURLs;
  private HttpClient httpClient;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory, HttpClient httpClient) {
    this.httpClient = httpClient;
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    completionService = httpShardHandlerFactory.newCompletionService();
    pending = new HashSet<>();

    // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
    // This is primarily to keep track of what order we should use to query the replicas of a shard
    // so that we use the same replica for all phases of a distributed request.
    shardToURLs = new HashMap<>();

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

    @Override
    public void setElapsedTime(long elapsedTime) {
      this.elapsedTime = elapsedTime;
    }
  }


  // Not thread safe... don't use in Callable.
  // Don't modify the returned URL list.
  private List<String> getURLs(String shard) {
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

    Callable<ShardResponse> task = () -> {

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

        QueryRequest req = makeQueryRequest(sreq, params, shard);
        req.setMethod(SolrRequest.METHOD.POST);

        // no need to set the response parser as binary is the default
        // req.setResponseParser(new BinaryResponseParser());

        // if there are no shards available for a slice, urls.size()==0
        if (urls.size()==0) {
          // TODO: what's the right error code here? We should use the same thing when
          // all of the servers for a shard are down.
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "no servers hosting shard: " + shard);
        }

        if (urls.size() <= 1) {
          String url = urls.get(0);
          srsp.setShardAddress(url);
          try (SolrClient client = new Builder(url).withHttpClient(httpClient).build()) {
            ssr.nl = client.request(req);
          }
        } else {
          LBHttpSolrClient.Rsp rsp = httpShardHandlerFactory.makeLoadBalancedRequest(req, urls);
          ssr.nl = rsp.getResponse();
          srsp.setShardAddress(rsp.getServer());
        }
      }
      catch( ConnectException cex ) {
        srsp.setException(cex); //????
      } catch (Exception th) {
        srsp.setException(th);
        if (th instanceof SolrException) {
          srsp.setResponseCode(((SolrException)th).code());
        } else {
          srsp.setResponseCode(-1);
        }
      }

      ssr.elapsedTime = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

      return transfomResponse(sreq, srsp, shard);
    };

    try {
      if (shard != null)  {
        MDC.put("ShardRequest.shards", shard);
      }
      if (urls != null && !urls.isEmpty())  {
        MDC.put("ShardRequest.urlList", urls.toString());
      }
      pending.add( completionService.submit(task) );
    } finally {
      MDC.remove("ShardRequest.shards");
      MDC.remove("ShardRequest.urlList");
    }
  }
  
  /**
   * Subclasses could modify the request based on the shard
   */
  protected QueryRequest makeQueryRequest(final ShardRequest sreq, ModifiableSolrParams params, String shard)
  {
    // use generic request to avoid extra processing of queries
    return new QueryRequest(params);
  }
  
  /**
   * Subclasses could modify the Response based on the the shard
   */
  protected ShardResponse transfomResponse(final ShardRequest sreq, ShardResponse rsp, String shard)
  {
    return rsp;
  }

  /** returns a ShardResponse of the last response correlated with a ShardRequest.  This won't 
   * return early if it runs into an error.  
   **/
  @Override
  public ShardResponse takeCompletedIncludingErrors() {
    return take(false);
  }


  /** returns a ShardResponse of the last response correlated with a ShardRequest,
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
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Impossible Exception",e);
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

    // since the cost of grabbing cloud state is still up in the air, we grab it only
    // if we need it.
    ClusterState clusterState = null;
    Map<String,Slice> slices = null;
    CoreDescriptor coreDescriptor = req.getCore().getCoreDescriptor();
    CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
    ZkController zkController = coreDescriptor.getCoreContainer().getZkController();

    final ReplicaListTransformer replicaListTransformer = httpShardHandlerFactory.getReplicaListTransformer(req);

    if (shards != null) {
      List<String> lst = StrUtils.splitSmart(shards, ",", true);
      rb.shards = lst.toArray(new String[lst.size()]);
      rb.slices = new String[rb.shards.length];

      if (zkController != null) {
        // figure out which shards are slices
        for (int i=0; i<rb.shards.length; i++) {
          if (rb.shards[i].indexOf('/') < 0) {
            // this is a logical shard
            rb.slices[i] = rb.shards[i];
            rb.shards[i] = null;
          }
        }
      }
    } else if (zkController != null) {
      // we weren't provided with an explicit list of slices to query via "shards", so use the cluster state

      clusterState =  zkController.getClusterState();
      String shardKeys =  params.get(ShardParams._ROUTE_);

      // This will be the complete list of slices we need to query for this request.
      slices = new HashMap<>();

      // we need to find out what collections this request is for.

      // A comma-separated list of specified collections.
      // Eg: "collection1,collection2,collection3"
      String collections = params.get("collection");
      if (collections != null) {
        // If there were one or more collections specified in the query, split
        // each parameter and store as a separate member of a List.
        List<String> collectionList = StrUtils.splitSmart(collections, ",",
            true);
        // In turn, retrieve the slices that cover each collection from the
        // cloud state and add them to the Map 'slices'.
        for (String collectionName : collectionList) {
          // The original code produced <collection-name>_<shard-name> when the collections
          // parameter was specified (see ClientUtils.appendMap)
          // Is this necessary if ony one collection is specified?
          // i.e. should we change multiCollection to collectionList.size() > 1?
          addSlices(slices, clusterState, params, collectionName,  shardKeys, true);
        }
      } else {
        // just this collection
        String collectionName = cloudDescriptor.getCollectionName();
        addSlices(slices, clusterState, params, collectionName,  shardKeys, false);
      }


      // Store the logical slices in the ResponseBuilder and create a new
      // String array to hold the physical shards (which will be mapped
      // later).
      rb.slices = slices.keySet().toArray(new String[slices.size()]);
      rb.shards = new String[rb.slices.length];
    }

    //
    // Map slices to shards
    //
    if (zkController != null) {

      // Are we hosting the shard that this request is for, and are we active? If so, then handle it ourselves
      // and make it a non-distributed request.
      String ourSlice = cloudDescriptor.getShardId();
      String ourCollection = cloudDescriptor.getCollectionName();
      if (rb.slices.length == 1 && rb.slices[0] != null
          && ( rb.slices[0].equals(ourSlice) || rb.slices[0].equals(ourCollection + "_" + ourSlice) )  // handle the <collection>_<slice> format
          && cloudDescriptor.getLastPublished() == Replica.State.ACTIVE) {
        boolean shortCircuit = params.getBool("shortCircuit", true);       // currently just a debugging parameter to check distrib search on a single node

        String targetHandler = params.get(ShardParams.SHARDS_QT);
        shortCircuit = shortCircuit && targetHandler == null;             // if a different handler is specified, don't short-circuit

        if (shortCircuit) {
          rb.isDistrib = false;
          rb.shortCircuitedURL = ZkCoreNodeProps.getCoreUrl(zkController.getBaseUrl(), coreDescriptor.getName());
          return;
        }
        // We shouldn't need to do anything to handle "shard.rows" since it was previously meant to be an optimization?
      }


      for (int i=0; i<rb.shards.length; i++) {
        final List<String> shardUrls;
        if (rb.shards[i] != null) {
          shardUrls = StrUtils.splitSmart(rb.shards[i], "|", true);
          replicaListTransformer.transform(shardUrls);
        } else {
          if (clusterState == null) {
            clusterState =  zkController.getClusterState();
            slices = clusterState.getSlicesMap(cloudDescriptor.getCollectionName());
          }
          String sliceName = rb.slices[i];

          Slice slice = slices.get(sliceName);

          if (slice==null) {
            // Treat this the same as "all servers down" for a slice, and let things continue
            // if partial results are acceptable
            rb.shards[i] = "";
            continue;
            // throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no such shard: " + sliceName);
          }

          final Collection<Replica> allSliceReplicas = slice.getReplicasMap().values();
          final List<Replica> eligibleSliceReplicas = new ArrayList<>(allSliceReplicas.size());
          for (Replica replica : allSliceReplicas) {
            if (!clusterState.liveNodesContain(replica.getNodeName())
                || replica.getState() != Replica.State.ACTIVE) {
              continue;
            }
            eligibleSliceReplicas.add(replica);
          }

          replicaListTransformer.transform(eligibleSliceReplicas);

          shardUrls = new ArrayList<>(eligibleSliceReplicas.size());
          for (Replica replica : eligibleSliceReplicas) {
            String url = ZkCoreNodeProps.getCoreUrl(replica);
            shardUrls.add(url);
          }

          if (shardUrls.isEmpty()) {
            boolean tolerant = rb.req.getParams().getBool(ShardParams.SHARDS_TOLERANT, false);
            if (!tolerant) {
              // stop the check when there are no replicas available for a shard
              throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                  "no servers hosting shard: " + rb.slices[i]);
            }
          }
        }
        // And now recreate the | delimited list of equivalent servers
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
        rb.shards[i] = sliceShardsStr.toString();
      }
    }
    String shards_rows = params.get(ShardParams.SHARDS_ROWS);
    if(shards_rows != null) {
      rb.shards_rows = Integer.parseInt(shards_rows);
    }
    String shards_start = params.get(ShardParams.SHARDS_START);
    if(shards_start != null) {
      rb.shards_start = Integer.parseInt(shards_start);
    }
  }


  private void addSlices(Map<String,Slice> target, ClusterState state, SolrParams params, String collectionName, String shardKeys, boolean multiCollection) {
    DocCollection coll = state.getCollection(collectionName);
    Collection<Slice> slices = coll.getRouter().getSearchSlices(shardKeys, params , coll);
    ClientUtils.addSlices(target, collectionName, slices, multiCollection);
  }

  public ShardHandlerFactory getShardHandlerFactory(){
    return httpShardHandlerFactory;
  }



}
