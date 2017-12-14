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
package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Base class for simulated test cases. Tests that use this class should configure the simulated cluster
 * in <code>@BeforeClass</code> like this:
 * <pre>
 *   @BeforeClass
 *   public static void setupCluster() throws Exception {
 *     cluster = configureCluster(5, TimeSource.get("simTime:50"));
 *   }
 * </pre>
 */
public class SimSolrCloudTestCase extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int DEFAULT_TIMEOUT = 90;

  /** The cluster. */
  protected static SimCloudManager cluster;

  protected static void configureCluster(int nodeCount, TimeSource timeSource) throws Exception {
    cluster = SimCloudManager.createCluster(nodeCount, timeSource);
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    cluster = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (cluster != null) {
      // clear any persisted auto scaling configuration
      cluster.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), -1);
      // clean any persisted trigger state or events
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
      cluster.getSimClusterStateProvider().simDeleteAllCollections();
      cluster.simClearSystemCollection();
      cluster.getSimClusterStateProvider().simResetLeaderThrottle();
      cluster.simRestartOverseer(null);
    }
  }

  @Before
  public void checkClusterConfiguration() {
    if (cluster == null)
      throw new RuntimeException("SimCloudManager not configured - have you called configureCluster()?");
  }

  protected void removeChildren(String path) throws Exception {
    if (!cluster.getDistribStateManager().hasData(path)) {
      return;
    }
    List<String> children = cluster.getDistribStateManager().listData(path);
    for (String c : children) {
      if (cluster.getDistribStateManager().hasData(path + "/" + c)) {
        try {
          cluster.getDistribStateManager().removeData(path + "/" + c, -1);
        } catch (NoSuchElementException e) {
          // ignore
        }
      }
    }
  }

  /* Cluster helper methods ************************************/

  /**
   * Get the collection state for a particular collection
   */
  protected DocCollection getCollectionState(String collectionName) throws IOException {
    return cluster.getClusterStateProvider().getClusterState().getCollection(collectionName);
  }

  /**
   * Wait for a particular collection state to appear in the cluster client's state reader
   *
   * This is a convenience method using the {@link #DEFAULT_TIMEOUT}
   *
   * @param message     a message to report on failure
   * @param collection  the collection to watch
   * @param predicate   a predicate to match against the collection state
   */
  protected long waitForState(String message, String collection, CollectionStatePredicate predicate) {
    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      return waitForState(collection, DEFAULT_TIMEOUT, TimeUnit.SECONDS, (n, c) -> {
        state.set(c);
        liveNodesLastSeen.set(n);
        return predicate.matches(n, c);
      });
    } catch (Exception e) {
      throw new AssertionError(message + "\n" + "Live Nodes: " + liveNodesLastSeen.get() + "\nLast available state: " + state.get(), e);
    }
  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @return number of milliseconds elapsed
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   * @throws IOException on watcher register / unregister error
   */
  public long waitForState(final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException, IOException {
    TimeOut timeout = new TimeOut(wait, unit, cluster.getTimeSource());
    long timeWarn = timeout.timeLeft(TimeUnit.MILLISECONDS) / 4;
    while (!timeout.hasTimedOut()) {
      ClusterState state = cluster.getClusterStateProvider().getClusterState();
      DocCollection coll = state.getCollectionOrNull(collection);
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
    throw new TimeoutException();
  }

  /**
   * Return a {@link CollectionStatePredicate} that returns true if a collection has the expected
   * number of shards and replicas
   */
  public static CollectionStatePredicate clusterShape(int expectedShards, int expectedReplicas) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      if (collectionState.getSlices().size() != expectedShards)
        return false;
      for (Slice slice : collectionState) {
        int activeReplicas = 0;
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            activeReplicas++;
        }
        if (activeReplicas != expectedReplicas)
          return false;
      }
      return true;
    };
  }

  /**
   * Get a (reproducibly) random shard from a {@link DocCollection}
   */
  protected static Slice getRandomShard(DocCollection collection) {
    List<Slice> shards = new ArrayList<>(collection.getActiveSlices());
    if (shards.size() == 0)
      fail("Couldn't get random shard for collection as it has no shards!\n" + collection.toString());
    Collections.shuffle(shards, random());
    return shards.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice}
   */
  protected static Replica getRandomReplica(Slice slice) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice} matching a predicate
   */
  protected static Replica getRandomReplica(Slice slice, Predicate<Replica> matchPredicate) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    for (Replica replica : replicas) {
      if (matchPredicate.test(replica))
        return replica;
    }
    fail("Couldn't get random replica that matched conditions\n" + slice.toString());
    return null;  // just to keep the compiler happy - fail will always throw an Exception
  }
}
