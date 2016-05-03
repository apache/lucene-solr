package org.apache.solr.common.cloud;

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

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class TestCollectionStateWatchers extends SolrCloudTestCase {

  private static final int CLUSTER_SIZE = 4;

  private static final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new SolrjNamedThreadFactory("backgroundWatchers")
  );

  private static final int MAX_WAIT_TIMEOUT = 30;

  @BeforeClass
  public static void startCluster() throws Exception {
    configureCluster(CLUSTER_SIZE)
        .addConfig("config", getFile("solrj/solr/collection1/conf").toPath())
        .configure();
  }

  @AfterClass
  public static void shutdownBackgroundExecutors() {
    executor.shutdown();
  }

  @Before
  public void prepareCluster() throws Exception {
    int missingServers = CLUSTER_SIZE - cluster.getJettySolrRunners().size();
    for (int i = 0; i < missingServers; i++) {
      cluster.startJettySolrRunner();
    }
    cluster.waitForAllNodes(30);
  }

  private static Future<Boolean> waitInBackground(String collection, long timeout, TimeUnit unit,
                                                  CollectionStatePredicate predicate) {
    return executor.submit(() -> {
      try {
        cluster.getSolrClient().waitForState(collection, timeout, unit, predicate);
      } catch (InterruptedException | TimeoutException e) {
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    });
  }


  @Test
  public void testSimpleCollectionWatch() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    cluster.createCollection("testcollection", CLUSTER_SIZE, 1, "config", new HashMap<>());

    client.waitForState("testcollection", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, DocCollection::isFullyActive);

    // shutdown a node and check that we get notified about the change
    final AtomicInteger nodeCount = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);
    client.registerCollectionStateWatcher("testcollection", (liveNodes, collectionState) -> {
      // we can't just count liveNodes here, because that's updated by a separate watcher,
      // and it may be the case that we're triggered by a node setting itself to DOWN before
      // the liveNodes watcher is called
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            nodeCount.incrementAndGet();
        }
      }
      latch.countDown();
    });

    cluster.stopJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));
    assertTrue("CollectionStateWatcher was never notified of cluster change", latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));

    assertThat(nodeCount.intValue(), is(3));

  }

  @Test
  public void testWaitForStateChecksCurrentState() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    cluster.createCollection("waitforstate", 1, 1, "config", new HashMap<>());

    client.waitForState("waitforstate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, DocCollection::isFullyActive);

    // several goes, to check that we're not getting delayed state changes
    for (int i = 0; i < 10; i++) {
      try {
        client.waitForState("waitforstate", 1, TimeUnit.SECONDS, DocCollection::isFullyActive);
      }
      catch (TimeoutException e) {
        fail("waitForState should return immediately if the predicate is already satisfied");
      }
    }

  }

  @Test
  public void testCanWatchForNonexistantCollection() throws Exception {

    Future<Boolean> future = waitInBackground("delayed", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, DocCollection::isFullyActive);
    cluster.createCollection("delayed", 1, 1, "config", new HashMap<>());
    assertTrue("waitForState was not triggered by collection creation", future.get());

  }

  @Test
  public void testPredicateFailureTimesOut() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    expectThrows(TimeoutException.class, () -> {
      client.waitForState("nosuchcollection", 1, TimeUnit.SECONDS, ((liveNodes, collectionState) -> false));
    });
    Set<CollectionStateWatcher> watchers = client.getZkStateReader().getStateWatchers("nosuchcollection");
    assertTrue("Watchers for collection should be removed after timeout",
        watchers == null || watchers.size() == 0);

  }

  @Test
  public void testWaitForStateWatcherIsRetainedOnPredicateFailure() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    cluster.createCollection("falsepredicate", 4, 1, "config", new HashMap<>());
    client.waitForState("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, DocCollection::isFullyActive);

    final CountDownLatch firstCall = new CountDownLatch(1);

    // stop a node, then add a watch waiting for all nodes to be back up
    JettySolrRunner node1 = cluster.stopJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));

    Future<Boolean> future = waitInBackground("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
          firstCall.countDown();
          return DocCollection.isFullyActive(liveNodes, collectionState);
        });

    // first, stop another node; the watch should not be fired after this!
    JettySolrRunner node2 = cluster.stopJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));

    // now start them both back up
    cluster.startJettySolrRunner(node1);
    assertTrue("CollectionStateWatcher not called after 30 seconds", firstCall.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));
    cluster.startJettySolrRunner(node2);

    Boolean result = future.get();
    assertTrue("Did not see a fully active cluster after 30 seconds", result);

  }

  @Test
  public void testWatcherIsRemovedAfterTimeout() {
    CloudSolrClient client = cluster.getSolrClient();
    assertTrue("There should be no watchers for a non-existent collection!",
        client.getZkStateReader().getStateWatchers("no-such-collection") == null);

    expectThrows(TimeoutException.class, () -> {
      client.waitForState("no-such-collection", 10, TimeUnit.MILLISECONDS, DocCollection::isFullyActive);
    });

    Set<CollectionStateWatcher> watchers = client.getZkStateReader().getStateWatchers("no-such-collection");
    assertTrue("Watchers for collection should be removed after timeout",
        watchers == null || watchers.size() == 0);

  }

  @Test
  public void testDeletionsTriggerWatches() throws Exception {
    cluster.createCollection("tobedeleted", 1, 1, "config", new HashMap<>());
    Future<Boolean> future = waitInBackground("tobedeleted", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, (l, c) -> c == null);

    CollectionAdminRequest.deleteCollection("tobedeleted").process(cluster.getSolrClient());

    assertTrue("CollectionStateWatcher not notified of delete call after 30 seconds", future.get());
  }

  @Test
  public void testWatchesWorkForStateFormat1() throws Exception {

    final CloudSolrClient client = cluster.getSolrClient();

    Future<Boolean> future
        = waitInBackground("stateformat1", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, DocCollection::isFullyActive);

    CollectionAdminRequest.createCollection("stateformat1", "config", 1, 1).setStateFormat(1)
        .processAndWait(client, MAX_WAIT_TIMEOUT);
    assertTrue("CollectionStateWatcher not notified of stateformat=1 collection creation", future.get());

    Future<Boolean> migrated
        = waitInBackground("stateformat1", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
              (n, c) -> c != null && c.getStateFormat() == 2);

    CollectionAdminRequest.migrateCollectionFormat("stateformat1").processAndWait(client, MAX_WAIT_TIMEOUT);
    assertTrue("CollectionStateWatcher did not persist over state format migration", migrated.get());

  }

}
