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
package org.apache.solr.analytics.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.analytics.AnalyticsRequestManager;
import org.apache.solr.analytics.AnalyticsRequestParser;
import org.apache.solr.analytics.TimeExceededStubException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.AnalyticsHandler;
import org.apache.solr.handler.component.AnalyticsComponent;
import org.apache.solr.response.AnalyticsShardResponseWriter;

/**
 * This class manages the requesting of shard responses from all shards in the queried collection.
 *
 * <p>
 * Shard Requests are sent to the {@link AnalyticsHandler} instead of the {@link AnalyticsComponent},
 * which is the entrance to the analytics component for all client requests.
 */
public class AnalyticsShardRequestManager {
  private final SolrParams params;
  protected transient CloudSolrClient cloudSolrClient;
  protected transient List<String> replicaUrls;

  /**
   * All shards responses, which are received in parallel, are funneled into the manager.
   * So the manager must be transient.
   */
  private transient final AnalyticsRequestManager manager;

  public AnalyticsShardRequestManager(SolrParams params, AnalyticsRequestManager manager) {
    this.manager = manager;
    this.params = loadParams(params, manager.analyticsRequest);
  }

  /**
   * Send out shard requests to each shard in the given collection.
   *
   * @param collection that is being queried
   * @param zkHost of the solr cloud hosting the collection
   * @throws IOException if an exception occurs while picking shards or sending requests
   */
  public void sendRequests(String collection, String zkHost) throws IOException {
    this.replicaUrls = new ArrayList<>();
    this.cloudSolrClient = new Builder(Collections.singletonList(zkHost), Optional.empty()).build();
    try {
      this.cloudSolrClient.connect();
      pickShards(collection);
      streamFromShards();
    } finally {
      cloudSolrClient.close();
    }
  }

  /**
   * Pick one replica from each shard to send the shard requests to.
   *
   * @param collection that is being queried
   * @throws IOException if an exception occurs while finding replicas
   */
  protected void pickShards(String collection) throws IOException {
    try {

      ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      Set<String> liveNodes = clusterState.getLiveNodes();

      Slice[] slices = clusterState.getCollection(collection).getActiveSlicesArr();

      for(Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        List<Replica> shuffler = new ArrayList<>();
        for(Replica replica : replicas) {
          if(replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName()))
          shuffler.add(replica);
        }

        Collections.shuffle(shuffler, new Random());
        Replica rep = shuffler.get(0);
        ZkCoreNodeProps zkProps = new ZkCoreNodeProps(rep);
        String url = zkProps.getCoreUrl();
        replicaUrls.add(url);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Send a shard request to each chosen replica, streaming
   * the responses back to the {@link AnalyticsRequestManager}
   * through the {@link AnalyticsShardResponseParser}.
   * <p>
   * A thread pool is used to send the requests simultaneously,
   * and therefore importing the results is also done in parallel.
   * However the manager can only import one shard response at a time,
   * so the {@link AnalyticsShardResponseParser} is blocked until each import is finished.
   *
   * @throws IOException if an exception occurs while sending requests.
   */
  private void streamFromShards() throws IOException {
    ExecutorService service = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("SolrAnalyticsStream"));
    List<Future<SolrException>> futures = new ArrayList<>();
    List<AnalyticsShardRequester> openers = new ArrayList<>();
    for (String replicaUrl : replicaUrls) {
      AnalyticsShardRequester opener = new AnalyticsShardRequester(replicaUrl);
      openers.add(opener);
      Future<SolrException> future = service.submit(opener);
      futures.add(future);
    }
    try {
      for (Future<SolrException> f : futures) {
        SolrException e = f.get();
        if (e != null) {
          if (TimeExceededStubException.isIt(e)) {
            manager.setPartialResults(true);
          } else {
            throw e;
          }
        }
      }
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    } catch (ExecutionException e1) {
      throw new RuntimeException(e1);
    } finally {
      service.shutdown();
      for (AnalyticsShardRequester opener : openers) {
        opener.close();
      }
    }
  }

  /**
   * Create a {@link SolrParams} for shard requests. The only parameters that are copied over from
   * the original search request are "q" and "fq".
   *
   * <p>
   * The request is sent to the {@link AnalyticsHandler} and the output will be encoded in the analytics bit-stream
   * format generated by the {@link AnalyticsShardResponseWriter}.
   *
   * @param paramsIn of the original solr request
   * @param analyticsRequest string representation
   * @return shard request SolrParams
   */
  private static SolrParams loadParams(SolrParams paramsIn, String analyticsRequest) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();

    solrParams.add(CommonParams.QT, AnalyticsHandler.NAME);
    solrParams.add(CommonParams.WT, AnalyticsShardResponseWriter.NAME);
    solrParams.add(CommonParams.Q, paramsIn.get(CommonParams.Q));
    solrParams.add(CommonParams.FQ, paramsIn.getParams(CommonParams.FQ));
    solrParams.add(CommonParams.TIME_ALLOWED, paramsIn.get(CommonParams.TIME_ALLOWED,"-1"));
    solrParams.add(AnalyticsRequestParser.analyticsParamName, analyticsRequest);

    return solrParams;
  }

  /**
   * A class that opens a connection to a given solr instance, a selected replica of the queried collection,
   * and sends a analytics request to the {@link AnalyticsHandler}. The results are processed by an
   * {@link AnalyticsShardResponseParser} constructed with the {@link AnalyticsRequestManager} passed
   * to the parent {@link AnalyticsShardRequestManager}.
   */
  protected class AnalyticsShardRequester implements Callable<SolrException> {
    private String baseUrl;
    HttpSolrClient client;

    /**
     * Create a requester for analytics shard data.
     *
     * @param baseUrl of the replica to send the request to
     */
    public AnalyticsShardRequester(String baseUrl) {
      this.baseUrl = baseUrl;
      this.client = null;
    }

    /**
     * Send the analytics request to the shard.
     */
    @Override
    public SolrException call() throws Exception {
      client = new HttpSolrClient.Builder(baseUrl).build();
      QueryRequest query = new QueryRequest( params );
      query.setPath(AnalyticsHandler.NAME);
      query.setResponseParser(new AnalyticsShardResponseParser(manager));
      query.setMethod(SolrRequest.METHOD.POST);
      NamedList<Object> exception = client.request(query);
      if (exception.size() > 0) {
        return (SolrException)exception.getVal(0);
      }
      return null;
    }

    /**
     * Close the connection to the solr instance.
     *
     * @throws IOException if an error occurs while closing the connection
     */
    public void close() throws IOException {
      if (client != null) {
        client.close();
      }
    }
  }
}