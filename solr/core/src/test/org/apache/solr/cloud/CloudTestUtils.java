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

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.RequestWriter.StringPayloadContentWriter;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.SolrResponseBase;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.TimeOut;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import static org.apache.solr.common.params.CommonParams.JSON_MIME;


/**
 * Some useful methods for SolrCloud tests.
 */
public class CloudTestUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int DEFAULT_TIMEOUT = 90;

  /**
   * Wait for a particular collection state to appear.
   *
   * This is a convenience method using the {@link #DEFAULT_TIMEOUT}
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param message     a message to report on failure
   * @param collection  the collection to watch
   * @param predicate   a predicate to match against the collection state
   */
  public static long waitForState(final SolrCloudManager cloudManager,
                                  final String message,
                                  final String collection,
                                  final CollectionStatePredicate predicate) {
    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      return waitForState(cloudManager, collection, DEFAULT_TIMEOUT, TimeUnit.SECONDS, (n, c) -> {
        state.set(c);
        liveNodesLastSeen.set(n);
        return predicate.matches(n, c);
      });
    } catch (Exception e) {
      throw new AssertionError(message + "\n" + "Live Nodes: " + liveNodesLastSeen.get() + "\nLast available state: " + state.get(), e);
    }
  }

  /**
   * Wait for a particular collection state to appear.
   *
   * This is a convenience method using the {@link #DEFAULT_TIMEOUT}
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param collection  the collection to watch
   * @param wait timeout value
   * @param unit timeout unit
   * @param predicate   a predicate to match against the collection state
   */
  public static long waitForState(final SolrCloudManager cloudManager,
                                  final String collection,
                                  long wait,
                                  final TimeUnit unit,
                                  final CollectionStatePredicate predicate) throws InterruptedException, TimeoutException, IOException {
    TimeOut timeout = new TimeOut(wait, unit, cloudManager.getTimeSource());
    long timeWarn = timeout.timeLeft(TimeUnit.MILLISECONDS) / 4;
    ClusterState state = null;
    DocCollection coll = null;
    while (!timeout.hasTimedOut()) {
      state = cloudManager.getClusterStateProvider().getClusterState();
      coll = state.getCollectionOrNull(collection);
      // due to the way we manage collections in SimClusterStateProvider a null here
      // can mean that a collection is still being created but has no replicas
      if (coll == null) { // does not yet exist?
        timeout.sleep(100);
        continue;
      }
      if (predicate.matches(state.getLiveNodes(), coll)) {
        log.trace("-- predicate matched with state {}", state);
        return timeout.timeElapsed(TimeUnit.MILLISECONDS);
      }
      timeout.sleep(100);
      if (timeout.timeLeft(TimeUnit.MILLISECONDS) < timeWarn) {
        log.trace("-- still not matching predicate: {}", state);
      }
    }
    throw new TimeoutException("last ClusterState: " + state + ", last coll state: " + coll);
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of shards and replicas
   */
  public static CollectionStatePredicate clusterShape(int expectedShards, int expectedReplicas) {
    return clusterShape(expectedShards, expectedReplicas, false, false);
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of shards and replicas.
   * <p>Note: for shards marked as inactive the current Solr behavior is that replicas remain active.
   * {@link org.apache.solr.cloud.autoscaling.sim.SimCloudManager} follows this behavior.</p>
   * @param expectedShards expected number of shards
   * @param expectedReplicas expected number of active replicas
   * @param withInactive if true then count also inactive shards
   * @param requireLeaders if true then require that each shard has a leader
   */
  public static CollectionStatePredicate clusterShape(int expectedShards, int expectedReplicas, boolean withInactive,
                                                      boolean requireLeaders) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null) {
        log.info("-- null collection");
        return false;
      }
      Collection<Slice> slices = withInactive ? collectionState.getSlices() : collectionState.getActiveSlices();
      if (slices.size() != expectedShards) {
        log.info("-- wrong number of slices for collection {}, expected={}, found={}: {}", collectionState.getName(), expectedShards, collectionState.getSlices().size(), collectionState.getSlices());
        return false;
      }
      Set<String> leaderless = new HashSet<>();
      for (Slice slice : slices) {
        int activeReplicas = 0;
        if (requireLeaders && slice.getState() != Slice.State.INACTIVE && slice.getLeader() == null) {
          leaderless.add(slice.getName());
          continue;
        }
        // skip other checks, we're going to fail anyway
        if (!leaderless.isEmpty()) {
          continue;
        }
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            activeReplicas++;
        }
        if (activeReplicas != expectedReplicas) {
          log.info("-- wrong number of active replicas for collection {} in slice {}, expected={}, found={}", collectionState.getName(), slice.getName(), expectedReplicas, activeReplicas);
          return false;
        }
      }
      if (leaderless.isEmpty()) {
        return true;
      } else {
        log.info("-- shards without leaders: {}", leaderless);
        return false;
      }
    };
  }
  
  /**
   * Wait for a particular named trigger to be scheduled.
   * <p>
   * This is a convenience method that polls the autoscaling API looking for a trigger with the 
   * specified name using the {@link #DEFAULT_TIMEOUT}.  It is particularly useful for tests 
   * that want to know when the Overseer has finished scheduling the automatic triggers on startup.
   * </p>
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param triggerName the name of the trigger we need to see sheduled in order to return successfully
   * @see #suspendTrigger
   */
  public static long waitForTriggerToBeScheduled(final SolrCloudManager cloudManager,
                                                 final String triggerName)
    throws InterruptedException, TimeoutException, IOException {

    TimeOut timeout = new TimeOut(DEFAULT_TIMEOUT, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (!timeout.hasTimedOut()) {
      final SolrResponse response = cloudManager.request(AutoScalingRequest.create(SolrRequest.METHOD.GET, null));
      final Map<String,?> triggers = (Map<String,?>) response.getResponse().get("triggers");
      Assert.assertNotNull("null triggers in response from autoscaling request", triggers);
      
      if ( triggers.containsKey(triggerName) ) {
        return timeout.timeElapsed(TimeUnit.MILLISECONDS);
      }
      timeout.sleep(100);
    }
    throw new TimeoutException("Never saw trigger with name: " + triggerName);
  }

  /**
   * Suspends the trigger with the specified name
   * <p>
   * This is a convenience method that sends a <code>suspend-trigger</code> command to the autoscaling
   * API for the specified trigger.  It is particularly useful for tests that may need to disable automatic
   * triggers such as <code>.scheduled_maintenance</code> in order to test their own
   * triggers.
   * </p>
   *
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param triggerName the name of the trigger to suspend.  This must already be scheduled.
   * @see #assertAutoScalingRequest
   * @see #waitForTriggerToBeScheduled
   */
  public static void suspendTrigger(final SolrCloudManager cloudManager,
                                    final String triggerName) throws IOException {
    assertAutoScalingRequest(cloudManager, "{'suspend-trigger' : {'name' : '"+triggerName+"'} }");
  }

  /**
   * Creates &amp; executes an autoscaling request against the current cluster, asserting that 
   * the result is a success.
   * 
   * @param cloudManager current instance of {@link SolrCloudManager}
   * @param json The request to POST to the AutoScaling Handler
   * @see AutoScalingRequest#create
   */
  public static void assertAutoScalingRequest(final SolrCloudManager cloudManager,
                                              final String json) throws IOException {
    // TODO: a lot of code that directly uses AutoScalingRequest.create should use this method
    
    final SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, json);
    final SolrResponse rsp = cloudManager.request(req);
    final String result = rsp.getResponse().get("result").toString();
    Assert.assertEquals("Unexpected result from auto-scaling command: " + json + " -> " + rsp,
                        "success", result);
  }

  
  /**
   * Helper class for sending (JSON) autoscaling requests that can randomize between V1 and V2 requests
   */
  public static class AutoScalingRequest extends SolrRequest {

    /**
     * Creates a request using a randomized root path (V1 vs V2)
     *
     * @param m HTTP Method to use
     * @aram message JSON payload, may be null
     */
    public static SolrRequest create(SolrRequest.METHOD m, String message) {
      return create(m, null, message);
    }
    /**
     * Creates a request using a randomized root path (V1 vs V2)
     *
     * @param m HTTP Method to use
     * @param subPath optional sub-path under <code>"$ROOT/autoscaling"</code>. may be null, 
     *        otherwise must start with "/"
     * @param message JSON payload, may be null
     */
    public static SolrRequest create(SolrRequest.METHOD m, String subPath, String message) {
      final boolean useV1 = LuceneTestCase.random().nextBoolean();
      String path = useV1 ? "/admin/autoscaling" : "/cluster/autoscaling";
      if (null != subPath) {
        assert subPath.startsWith("/");
        path += subPath;
      }
      return useV1
        ? new AutoScalingRequest(m, path, message)
        : new V2Request.Builder(path).withMethod(m).withPayload(message).build();
    }
    
    protected final String message;

    /**
     * Simple request
     * @param m HTTP Method to use
     * @param path path to send request to
     * @param message JSON payload, may be null
     */
    private AutoScalingRequest(METHOD m, String path, String message) {
      super(m, path);
      this.message = message;
    }

    @Override
    public SolrParams getParams() {
      return null;
    }

    @Override
    public RequestWriter.ContentWriter getContentWriter(String expectedType) {
      return message == null ? null : new StringPayloadContentWriter(message, JSON_MIME);
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return new SolrResponseBase();
    }
  }
}
