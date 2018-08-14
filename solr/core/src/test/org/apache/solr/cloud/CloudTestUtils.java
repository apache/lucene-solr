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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        timeout.sleep(50);
        continue;
      }
      if (predicate.matches(state.getLiveNodes(), coll)) {
        log.trace("-- predicate matched with state {}", state);
        return timeout.timeElapsed(TimeUnit.MILLISECONDS);
      }
      timeout.sleep(50);
      if (timeout.timeLeft(TimeUnit.MILLISECONDS) < timeWarn) {
        log.trace("-- still not matching predicate: {}", state);
      }
    }
    throw new TimeoutException("last state: " + coll);
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
        log.trace("-- null collection");
        return false;
      }
      Collection<Slice> slices = withInactive ? collectionState.getSlices() : collectionState.getActiveSlices();
      if (slices.size() != expectedShards) {
        log.trace("-- wrong number of active slices, expected={}, found={}", expectedShards, collectionState.getSlices().size());
        return false;
      }
      Set<String> leaderless = new HashSet<>();
      for (Slice slice : slices) {
        int activeReplicas = 0;
        if (requireLeaders && slice.getLeader() == null) {
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
          log.trace("-- wrong number of active replicas in slice {}, expected={}, found={}", slice.getName(), expectedReplicas, activeReplicas);
          return false;
        }
      }
      if (leaderless.isEmpty()) {
        return true;
      } else {
        log.trace("-- shards without leaders: {}", leaderless);
        return false;
      }
    };
  }
}
