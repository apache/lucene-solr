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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;

/**
 * A ShardHandlerFactory that extends HttpShardHandlerFactory and
 * tracks requests made to nodes/shards such that interested parties
 * can watch such requests and make assertions inside tests
 * <p>
 * This is a test helper only and should *not* be used for production.
 */
public class TrackingShardHandlerFactory extends HttpShardHandlerFactory {

  private Queue<ShardRequestAndParams> queue;

  /**
   * Set the tracking queue for this factory. All the ShardHandler instances
   * created from this factory will share the queue and call {@link java.util.Queue#offer(Object)}
   * with a {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
   * instance whenever
   * {@link org.apache.solr.handler.component.ShardHandler#submit(ShardRequest, String, org.apache.solr.common.params.ModifiableSolrParams)}
   * is called before the request is actually submitted to the
   * wrapped {@link org.apache.solr.handler.component.HttpShardHandlerFactory} instance.
   * <p>
   * If a tracking queue is already set then this call will overwrite and replace the
   * previous queue with this one.
   *
   * @param queue the {@link java.util.Queue} to be used for tracking shard requests
   */
  public synchronized void setTrackingQueue(Queue<ShardRequestAndParams> queue) {
    this.queue = queue;
  }

  /**
   * @return the {@link java.util.Queue} being used for tracking, null if none
   * has been set
   */
  public synchronized Queue<ShardRequestAndParams> getTrackingQueue() {
    return queue;
  }

  /**
   * @return true if a tracking queue has been set through
   * {@link #setTrackingQueue(java.util.List, java.util.Queue)}, false otherwise
   */
  public synchronized boolean isTracking() {
    return queue != null;
  }

  @Override
  public ShardHandler getShardHandler() {
    final ShardHandlerFactory factory = this;
    final ShardHandler wrapped = super.getShardHandler();
    return new ShardHandler() {
      @Override
      public void prepDistributed(ResponseBuilder rb) {
        wrapped.prepDistributed(rb);
      }

      @Override
      public void submit(ShardRequest sreq, String shard, ModifiableSolrParams params) {
        synchronized (TrackingShardHandlerFactory.this) {
          if (isTracking()) {
            queue.offer(new ShardRequestAndParams(sreq, shard, params));
          }
        }
        wrapped.submit(sreq, shard, params);
      }

      @Override
      public ShardResponse takeCompletedIncludingErrors() {
        return wrapped.takeCompletedIncludingErrors();
      }

      @Override
      public ShardResponse takeCompletedOrError() {
        return wrapped.takeCompletedOrError();
      }

      @Override
      public void cancelAll() {
        wrapped.cancelAll();
      }

      @Override
      public ShardHandlerFactory getShardHandlerFactory() {
        return factory;
      }
    };
  }

  @Override
  public void close() {
    super.close();
  }

  /**
   * Sets the tracking queue for all nodes participating in this cluster. Once this method returns,
   * all search and core admin requests distributed to shards will be submitted to the given queue.
   * <p>
   * This is equivalent to calling:
   * <code>TrackingShardHandlerFactory.setTrackingQueue(cluster.getJettySolrRunners(), queue)</code>
   *
   * @see org.apache.solr.handler.component.TrackingShardHandlerFactory#setTrackingQueue(java.util.List, java.util.Queue)
   */
  public static void setTrackingQueue(MiniSolrCloudCluster cluster, Queue<ShardRequestAndParams> queue) {
    setTrackingQueue(cluster.getJettySolrRunners(), queue);
  }

  /**
   * Sets the tracking queue for all nodes participating in this cluster. Once this method returns,
   * all search and core admin requests distributed to shards will be submitted to the given queue.
   *
   * @param runners a list of {@link org.apache.solr.client.solrj.embedded.JettySolrRunner} nodes
   * @param queue   an implementation of {@link java.util.Queue} which
   *                accepts {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
   *                instances
   */
  public static void setTrackingQueue(List<JettySolrRunner> runners, Queue<ShardRequestAndParams> queue) {
    for (JettySolrRunner runner : runners) {
      CoreContainer container = runner.getCoreContainer();
      ShardHandlerFactory factory = container.getShardHandlerFactory();
      assert factory instanceof TrackingShardHandlerFactory : "not a TrackingShardHandlerFactory: " + factory.getClass();
      TrackingShardHandlerFactory trackingShardHandlerFactory = (TrackingShardHandlerFactory) factory;
      trackingShardHandlerFactory.setTrackingQueue(queue);
    }
  }

  public static class ShardRequestAndParams {
    public String shard;
    public ShardRequest sreq;
    public ModifiableSolrParams params;

    public ShardRequestAndParams(ShardRequest sreq, String shard, ModifiableSolrParams params) {
      this.sreq = sreq;
      this.params = params;
      this.shard = shard;
    }

    @Override
    public String toString() {
      return "ShardRequestAndParams{" +
          "shard='" + shard + '\'' +
          ", sreq=" + sreq +
          ", params=" + params +
          '}';
    }
  }

  /**
   * A queue having helper methods to select requests by shard and purpose.
   *
   * @see org.apache.solr.handler.component.TrackingShardHandlerFactory#setTrackingQueue(java.util.List, java.util.Queue)
   */
  public static class RequestTrackingQueue extends LinkedList<ShardRequestAndParams> {
    private final Map<String, List<ShardRequestAndParams>> requests = new ConcurrentHashMap<>();

    @Override
    public boolean offer(ShardRequestAndParams shardRequestAndParams) {
      List<ShardRequestAndParams> list = requests.get(shardRequestAndParams.shard);
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(shardRequestAndParams);
      requests.put(shardRequestAndParams.shard, list);
      return super.offer(shardRequestAndParams);
    }

    @Override
    public void clear() {
      requests.clear();
    }

    /**
     * Retrieve request recorded by this queue which were sent to given collection, shard and purpose
     *
     * @param zkStateReader  the {@link org.apache.solr.common.cloud.ZkStateReader} from which cluster state is read
     * @param collectionName the given collection name for which requests have to be extracted
     * @param shardId        the given shard name for which requests have to be extracted
     * @param purpose        the shard purpose
     * @return instance of {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
     * or null if none is found
     * @throws java.lang.RuntimeException if more than one request is found to the same shard with the same purpose
     */
    public ShardRequestAndParams getShardRequestByPurpose(ZkStateReader zkStateReader, String collectionName, String shardId, int purpose) throws RuntimeException {
      List<TrackingShardHandlerFactory.ShardRequestAndParams> shardRequests = getShardRequests(zkStateReader, collectionName, shardId);
      List<TrackingShardHandlerFactory.ShardRequestAndParams> result = new ArrayList<>(1);
      for (TrackingShardHandlerFactory.ShardRequestAndParams request : shardRequests) {
        if ((request.sreq.purpose & purpose) != 0) {
          result.add(request);
        }
      }
      if (result.size() > 1) {
        throw new RuntimeException("Multiple requests to the same shard with the same purpose were found. Requests: " + result);
      }
      return result.isEmpty() ? null : result.get(0);
    }

    /**
     * Retrieve all requests recorded by this queue which were sent to given collection and shard
     *
     * @param zkStateReader  the {@link org.apache.solr.common.cloud.ZkStateReader} from which cluster state is read
     * @param collectionName the given collection name for which requests have to be extracted
     * @param shardId        the given shard name for which requests have to be extracted
     * @return a list of {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
     * or empty list if none are found
     */
    public List<ShardRequestAndParams> getShardRequests(ZkStateReader zkStateReader, String collectionName, String shardId) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      assert collection != null;
      Slice slice = collection.getSlice(shardId);
      assert slice != null;

      for (Map.Entry<String, List<ShardRequestAndParams>> entry : requests.entrySet()) {
        // multiple shard addresses may be present separated by '|'
        List<String> list = StrUtils.splitSmart(entry.getKey(), '|');
        for (Map.Entry<String, Replica> replica : slice.getReplicasMap().entrySet()) {
          String coreUrl = new ZkCoreNodeProps(replica.getValue()).getCoreUrl();
          if (list.contains(coreUrl)) {
            return new ArrayList<>(entry.getValue());
          }
        }
      }
      return Collections.emptyList();
    }

    /**
     * Retrieves all core admin requests distributed to nodes by Collection API commands
     *
     * @return a list of {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
     * or empty if none found
     */
    public List<ShardRequestAndParams> getCoreAdminRequests() {
      List<ShardRequestAndParams> results = new ArrayList<>();
      Map<String, List<ShardRequestAndParams>> map = getAllRequests();
      for (Map.Entry<String, List<ShardRequestAndParams>> entry : map.entrySet()) {
        for (ShardRequestAndParams shardRequestAndParams : entry.getValue()) {
          if (shardRequestAndParams.sreq.purpose == ShardRequest.PURPOSE_PRIVATE) {
            results.add(shardRequestAndParams);
          }
        }
      }
      return results;
    }

    /**
     * Retrieves all requests recorded by this collection as a Map of shard address (string url)
     * to a list of {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams}
     *
     * @return a {@link java.util.concurrent.ConcurrentHashMap} of url strings to {@link org.apache.solr.handler.component.TrackingShardHandlerFactory.ShardRequestAndParams} objects
     * or empty map if none have been recorded
     */
    public Map<String, List<ShardRequestAndParams>> getAllRequests() {
      return requests;
    }
  }
}
