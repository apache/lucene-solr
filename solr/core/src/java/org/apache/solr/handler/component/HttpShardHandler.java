package org.apache.solr.handler.component;
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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;

import java.util.*;
import java.util.concurrent.*;

public class HttpShardHandler extends ShardHandler {

  private HttpShardHandlerFactory httpShardHandlerFactory;
  private CompletionService<ShardResponse> completionService;
  private     Set<Future<ShardResponse>> pending;
  private Map<String,List<String>> shardToURLs;



  public HttpShardHandler(HttpShardHandlerFactory httpShardHandlerFactory) {
    this.httpShardHandlerFactory = httpShardHandlerFactory;
    completionService = new ExecutorCompletionService<ShardResponse>(httpShardHandlerFactory.commExecutor);
    pending = new HashSet<Future<ShardResponse>>();

    // maps "localhost:8983|localhost:7574" to a shuffled List("http://localhost:8983","http://localhost:7574")
      // This is primarily to keep track of what order we should use to query the replicas of a shard
      // so that we use the same replica for all phases of a distributed request.
    shardToURLs = new HashMap<String,List<String>>();

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
    if (urls==null) {
      urls = StrUtils.splitSmart(shard, "|", true);

      // convert shard to URL
      for (int i=0; i<urls.size(); i++) {
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
          params.remove(CommonParams.WT); // use default (currently javabin)
          params.remove(CommonParams.VERSION);

          // SolrRequest req = new QueryRequest(SolrRequest.METHOD.POST, "/select");
          // use generic request to avoid extra processing of queries
          QueryRequest req = new QueryRequest(params);
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
            SolrServer server = new CommonsHttpSolrServer(url, httpShardHandlerFactory.client);
            ssr.nl = server.request(req);
          } else {
            LBHttpSolrServer.Rsp rsp = httpShardHandlerFactory.loadbalancer.request(new LBHttpSolrServer.Req(req, urls));
            ssr.nl = rsp.getResponse();
            srsp.setShardAddress(rsp.getServer());
          }
        } catch (Throwable th) {
          srsp.setException(th);
          if (th instanceof SolrException) {
            srsp.setResponseCode(((SolrException)th).code());
          } else {
            srsp.setResponseCode(-1);
          }
        }

        ssr.elapsedTime = System.currentTimeMillis() - startTime;

        return srsp;
      }
    };

    pending.add( completionService.submit(task) );
  }

  /** returns a ShardResponse of the last response correlated with a ShardRequest */
  ShardResponse take() {
    while (pending.size() > 0) {
      try {
        Future<ShardResponse> future = completionService.take();
        pending.remove(future);
        ShardResponse rsp = future.get();
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


  /** returns a ShardResponse of the last response correlated with a ShardRequest,
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
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Impossible Exception",e);
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

    rb.isDistrib = params.getBool("distrib", req.getCore().getCoreDescriptor()
        .getCoreContainer().isZooKeeperAware());
    String shards = params.get(ShardParams.SHARDS);

    // for back compat, a shards param with URLs like localhost:8983/solr will mean that this
    // search is distributed.
    boolean hasShardURL = shards != null && shards.indexOf('/') > 0;
    rb.isDistrib = hasShardURL | rb.isDistrib;

    if (rb.isDistrib) {
      // since the cost of grabbing cloud state is still up in the air, we grab it only
      // if we need it.
      CloudState cloudState = null;
      Map<String,Slice> slices = null;
      CoreDescriptor coreDescriptor = req.getCore().getCoreDescriptor();
      CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
      ZkController zkController = coreDescriptor.getCoreContainer().getZkController();


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
        // we weren't provided with a list of slices to query, so find the list that will cover the complete index

        cloudState =  zkController.getCloudState();

        // This can be more efficient... we only record the name, even though we
        // have the shard info we need in the next step of mapping slice->shards
        
        // Stores the comma-separated list of specified collections.
        // Eg: "collection1,collection2,collection3"
        String collections = params.get("collection");
        if (collections != null) {
          // If there were one or more collections specified in the query, split
          // each parameter and store as a seperate member of a List.
          List<String> collectionList = StrUtils.splitSmart(collections, ",",
              true);
          
          // First create an empty HashMap to add the slice info to.
          slices = new HashMap<String,Slice>();
          
          // In turn, retrieve the slices that cover each collection from the
          // cloud state and add them to the Map 'slices'.
          for (int i = 0; i < collectionList.size(); i++) {
            String collection = collectionList.get(i);
            ClientUtils.appendMap(collection, slices, cloudState.getSlices(collection));
          }
        } else {
          // If no collections were specified, default to the collection for
          // this core.
          slices = cloudState.getSlices(cloudDescriptor.getCollectionName());
        }
        
        // Store the logical slices in the ResponseBuilder and create a new
        // String array to hold the physical shards (which will be mapped
        // later).
        rb.slices = slices.keySet().toArray(new String[slices.size()]);
        rb.shards = new String[rb.slices.length];

        /***
         rb.slices = new String[slices.size()];
         for (int i=0; i<rb.slices.length; i++) {
         rb.slices[i] = slices.get(i).getName();
         }
         ***/
      }

      //
      // Map slices to shards
      //
      if (zkController != null) {
        for (int i=0; i<rb.shards.length; i++) {
          if (rb.shards[i] == null) {
            if (cloudState == null) {
              cloudState =  zkController.getCloudState();
              slices = cloudState.getSlices(cloudDescriptor.getCollectionName());
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

            Map<String, ZkNodeProps> sliceShards = slice.getShards();

            // For now, recreate the | delimited list of equivalent servers
            Set<String> liveNodes = cloudState.getLiveNodes();
            StringBuilder sliceShardsStr = new StringBuilder();
            boolean first = true;
            for (ZkNodeProps nodeProps : sliceShards.values()) {
              ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
              if (!liveNodes.contains(coreNodeProps.getNodeName())
                  || !coreNodeProps.getState().equals(
                      ZkStateReader.ACTIVE)) continue;
              if (first) {
                first = false;
              } else {
                sliceShardsStr.append('|');
              }
              String url = coreNodeProps.getCoreUrl();
              if (url.startsWith("http://"))
                url = url.substring(7);
              sliceShardsStr.append(url);
            }

            rb.shards[i] = sliceShardsStr.toString();
          }
        }
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

}

