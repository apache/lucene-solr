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

package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCollectionStateWatchers extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int CLUSTER_SIZE = 4;

  private static final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("backgroundWatchers");

  private static final int MAX_WAIT_TIMEOUT = 30;

  @BeforeClass
  public static void startCluster() throws Exception {

  }

  @AfterClass
  public static void shutdownBackgroundExecutors() {
    executor.shutdown();
  }

  @Before
  public void prepareCluster() throws Exception {
    configureCluster(CLUSTER_SIZE)
    .addConfig("config", getFile("solrj/solr/collection1/conf").toPath())
    .configure();
  }
  
  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
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

  private static void waitFor(String message, long timeout, TimeUnit unit, Callable<Boolean> predicate)
      throws InterruptedException, ExecutionException {
    Future<Boolean> future = executor.submit(() -> {
      try {
        while (true) {
          if (predicate.call())
            return true;
          TimeUnit.MILLISECONDS.sleep(10);
        }
      }
      catch (InterruptedException e) {
        return false;
      }
    });
    try {
      if (future.get(timeout, unit) == true) {
        return;
      }
    }
    catch (TimeoutException e) {
      // pass failure message on
    }
    future.cancel(true);
    fail(message);
  }

  @Test
  //Commented 14-Oct-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testSimpleCollectionWatch() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("testcollection", "config", 4, 1)
        .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("testcollection", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 4, 1));

    // shutdown a node and check that we get notified about the change
    final CountDownLatch latch = new CountDownLatch(1);
    client.registerCollectionStateWatcher("testcollection", (liveNodes, collectionState) -> {
      int nodeCount = 0;
      log.info("State changed: {}", collectionState);
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            nodeCount++;
        }
      }
      if (nodeCount == 3) {
        latch.countDown();
        return true;
      }
      return false;
    });

    JettySolrRunner j = cluster.stopJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));
    cluster.waitForJettyToStop(j);
    assertTrue("CollectionStateWatcher was never notified of cluster change", latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));

    waitFor("CollectionStateWatcher wasn't cleared after completion", 1, TimeUnit.SECONDS,
        () -> client.getZkStateReader().getStateWatchers("testcollection").isEmpty());

  }

  @Test
  // commented 20-July-2018  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testStateWatcherChecksCurrentStateOnRegister() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("currentstate", "config", 1, 1)
        .processAndWait(client, MAX_WAIT_TIMEOUT);

    final CountDownLatch latch = new CountDownLatch(1);
    client.registerCollectionStateWatcher("currentstate", (n, c) -> {
      latch.countDown();
      return false;
    });

    assertTrue("CollectionStateWatcher isn't called on new registration", latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));
    assertEquals("CollectionStateWatcher should be retained",
        1, client.getZkStateReader().getStateWatchers("currentstate").size());

    final CountDownLatch latch2 = new CountDownLatch(1);
    client.registerCollectionStateWatcher("currentstate", (n, c) -> {
      latch2.countDown();
      return true;
    });

    assertTrue("CollectionStateWatcher isn't called when registering for already-watched collection",
        latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));
    waitFor("CollectionStateWatcher should be removed", 1, TimeUnit.SECONDS,
        () -> client.getZkStateReader().getStateWatchers("currentstate").size() == 1);
  }

  @Test
  public void testWaitForStateChecksCurrentState() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("waitforstate", "config", 1, 1)
        .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("waitforstate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    // several goes, to check that we're not getting delayed state changes
    for (int i = 0; i < 10; i++) {
      try {
        client.waitForState("waitforstate", 1, TimeUnit.SECONDS, (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));
      }
      catch (TimeoutException e) {
        fail("waitForState should return immediately if the predicate is already satisfied");
      }
    }

  }

  @Test
  // commented 20-July-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testCanWaitForNonexistantCollection() throws Exception {

    Future<Boolean> future = waitInBackground("delayed", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    CollectionAdminRequest.createCollection("delayed", "config", 1, 1)
        .processAndWait(cluster.getSolrClient(), MAX_WAIT_TIMEOUT);

    assertTrue("waitForState was not triggered by collection creation", future.get());

  }

  @Test
  // commented 20-July-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testPredicateFailureTimesOut() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    expectThrows(TimeoutException.class, () -> {
      client.waitForState("nosuchcollection", 1, TimeUnit.SECONDS, ((liveNodes, collectionState) -> false));
    });
    waitFor("Watchers for collection should be removed after timeout", 1, TimeUnit.SECONDS,
        () -> client.getZkStateReader().getStateWatchers("nosuchcollection").isEmpty());

  }

  @Test
  //Commented 14-Oct-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testWaitForStateWatcherIsRetainedOnPredicateFailure() throws Exception {

    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("falsepredicate", "config", 4, 1)
        .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 4, 1));

    final CountDownLatch firstCall = new CountDownLatch(1);

    // stop a node, then add a watch waiting for all nodes to be back up
    JettySolrRunner node1 = cluster.stopJettySolrRunner(random().nextInt(cluster.getJettySolrRunners().size()));
    
    cluster.waitForJettyToStop(node1);

    Future<Boolean> future = waitInBackground("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
          firstCall.countDown();
          return DocCollection.isFullyActive(liveNodes, collectionState, 4, 1);
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
  // commented 20-July-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testWatcherIsRemovedAfterTimeout() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    assertTrue("There should be no watchers for a non-existent collection!",
        client.getZkStateReader().getStateWatchers("no-such-collection").isEmpty());

    expectThrows(TimeoutException.class, () -> {
      client.waitForState("no-such-collection", 10, TimeUnit.MILLISECONDS, (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));
    });

    waitFor("Watchers for collection should be removed after timeout", 1, TimeUnit.SECONDS,
        () -> client.getZkStateReader().getStateWatchers("no-such-collection").isEmpty());

  }

  @Test
  // commented 20-July-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void testDeletionsTriggerWatches() throws Exception {
    CollectionAdminRequest.createCollection("tobedeleted", "config", 1, 1)
        .process(cluster.getSolrClient());
    Future<Boolean> future = waitInBackground("tobedeleted", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, (l, c) -> c == null);

    CollectionAdminRequest.deleteCollection("tobedeleted").process(cluster.getSolrClient());

    assertTrue("CollectionStateWatcher not notified of delete call after 30 seconds", future.get());
  }

  @Test
  //Commented 14-Oct-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void testWatchesWorkForStateFormat1() throws Exception {

    final CloudSolrClient client = cluster.getSolrClient();

    Future<Boolean> future = waitInBackground("stateformat1", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

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
