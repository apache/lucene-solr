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
package org.apache.solr.cloud.overseer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CommonTestInjection;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateReaderTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long TIMEOUT = 30;

  private static class TestFixture implements Closeable {
    private final ZkTestServer server;
    private final SolrZkClient zkClient;
    private final ZkStateReader reader;
    private final ZkStateWriter writer;

    private TestFixture(
        ZkTestServer server, SolrZkClient zkClient, ZkStateReader reader, ZkStateWriter writer) {
      this.server = server;
      this.zkClient = zkClient;
      this.reader = reader;
      this.writer = writer;
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(reader, zkClient);
      try {
        server.shutdown();
      } catch (InterruptedException e) {
        // ok. Shutting down anyway
      }
    }
  }

  private TestFixture fixture = null;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    fixture = setupTestFixture(getTestName());
  }

  @After
  public void tearDown() throws Exception {
    if (fixture != null) {
      fixture.close();
    }
    super.tearDown();
  }

  private static TestFixture setupTestFixture(String testPrefix) throws Exception {
    Path zkDir = createTempDir(testPrefix);
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    SolrZkClient zkClient =
        new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
    ZkController.createClusterZkNodes(zkClient);

    ZkStateReader reader = new ZkStateReader(zkClient);
    reader.createClusterStateWatchersAndUpdate();

    ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), -1);

    return new TestFixture(server, zkClient, reader, writer);
  }

  /** Uses explicit refresh to ensure latest changes are visible. */
  public void testStateFormatUpdateWithExplicitRefresh() throws Exception {
    testStateFormatUpdate(true, true, false);
  }

  /** Uses explicit refresh to ensure latest changes are visible. */
  public void testStateFormatUpdateWithExplicitRefreshLazy() throws Exception {
    testStateFormatUpdate(true, false, false);
  }

  /** ZkStateReader should automatically pick up changes based on ZK watches. */
  public void testStateFormatUpdateWithTimeDelay() throws Exception {
    testStateFormatUpdate(false, true, false);
  }

  /** ZkStateReader should automatically pick up changes based on ZK watches. */
  public void testStateFormatUpdateWithTimeDelayLazy() throws Exception {
    testStateFormatUpdate(false, false, false);
  }

  public void testStateFormatUpdateWithExplicitRefreshCompressed() throws Exception {
    testStateFormatUpdate(true, true, true);
  }

  public void testStateFormatUpdate(boolean explicitRefresh, boolean isInteresting, boolean compressedState) throws Exception {
    ZkStateReader reader = fixture.reader;
    ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), compressedState ? 0 : -1);

    if (isInteresting) {
      reader.registerCore("c1");
    }

    SolrZkClient zkClient = fixture.zkClient;
    zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    {
      // create new collection with stateFormat = 1
      DocCollection stateV1 = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE);
      ZkWriteCommand c1 = new ZkWriteCommand("c1", stateV1);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
      writer.writePendingUpdates();

      Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
      assertNotNull(map.get("c1"));
      boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
      assertFalse(exists);

      if (explicitRefresh) {
        reader.forceUpdateCollection("c1");
      } else {
        reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
      }

      DocCollection collection = reader.getClusterState().getCollection("c1");
      assertEquals(1, collection.getStateFormat());
    }


    {
      // Now update the collection to stateFormat = 2
      DocCollection stateV2 = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json");
      ZkWriteCommand c2 = new ZkWriteCommand("c1", stateV2);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c2), null);
      writer.writePendingUpdates();

      Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
      assertNull(map.get("c1"));
      boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
      assertTrue(exists);

      if (explicitRefresh) {
        reader.forceUpdateCollection("c1");
      } else {
        reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS,
            (n, c) -> c != null && c.getStateFormat() == 2);
      }

      DocCollection collection = reader.getClusterState().getCollection("c1");
      assertEquals(2, collection.getStateFormat());
    }
  }

  public void testExternalCollectionWatchedNotWatched() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    // create new collection with stateFormat = 2
    ZkWriteCommand c1 = new ZkWriteCommand("c1",
        new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json"));

    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
    writer.writePendingUpdates();
    reader.forceUpdateCollection("c1");

    assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
    reader.registerCore("c1");
    assertFalse(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
    reader.unregisterCore("c1");
    assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
  }

  public void testCollectionStateWatcherCaching() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    DocCollection state = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();
    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
    reader.waitForState("c1", 1, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState != null);

    state = new DocCollection("c1", new HashMap<>(), Collections.singletonMap("x", "y"), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    boolean found = false;
    TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut())  {
      DocCollection c1 = reader.getClusterState().getCollection("c1");
      if ("y".equals(c1.getStr("x"))) {
        found = true;
        break;
      }
    }
    assertTrue("Could not find updated property in collection c1 even after 5 seconds", found);
  }

  public void testWatchedCollectionCreation() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1");

    // Initially there should be no c1 collection.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
    reader.forceUpdateCollection("c1");

    // Still no c1 collection, despite a collection path.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    // create new collection with stateFormat = 2
    DocCollection state = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

    // reader.forceUpdateCollection("c1");
    reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(2, ref.get().getStateFormat());
  }

  /**
   * Verifies that znode and child versions are correct and version changes trigger cluster state
   * updates
   */
  public void testNodeVersion() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    ClusterState clusterState = reader.getClusterState();
    // create new collection
    DocCollection state =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();

    // have to register it here after the updates, otherwise the child node watch will not be
    // inserted
    reader.registerCore("c1");

    TimeOut timeOut = new TimeOut(5000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "Timeout on waiting for c1 to show up in cluster state",
        () -> reader.getClusterState().getCollectionOrNull("c1") != null);

    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());
    assertEquals(0, ref.get().getChildNodesVersion()); //default is 0 in solr 8 (current version), but in Solr 9+ it's -1

    DocCollection collection = ref.get();
    PerReplicaStates prs =
        PerReplicaStates.fetch(
            collection.getZNode(), fixture.zkClient, collection.getPerReplicaStates());
    PerReplicaStatesOps.addReplica("r1", Replica.State.DOWN, false, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1",
        () -> {
          DocCollection c = reader.getCollection("c1");
          return c.getPerReplicaStates() != null
              && c.getPerReplicaStates().get("r1") != null
              && c.getPerReplicaStates().get("r1").state == Replica.State.DOWN;
        });

    ref = reader.getClusterState().getCollectionRef("c1");
    assertEquals(0, ref.get().getZNodeVersion()); // no change in Znode version
    assertEquals(1, ref.get().getChildNodesVersion()); // but child version should be 1 now

    prs = ref.get().getPerReplicaStates();
    PerReplicaStatesOps.flipState("r1", Replica.State.ACTIVE, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1 marked as DOWN",
        () ->
            reader.getCollection("c1").getPerReplicaStates().get("r1").state
                == Replica.State.ACTIVE);

    ref = reader.getClusterState().getCollectionRef("c1");
    assertEquals(0, ref.get().getZNodeVersion()); // no change in Znode version
    // but child version should be 3 now (1 del + 1 add)
    assertEquals(3, ref.get().getChildNodesVersion());

    // now delete the collection
    wc = new ZkWriteCommand("c1", null);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();
    timeOut.waitFor(
        "Timeout on waiting for c1 to be removed from cluster state",
        () -> reader.getClusterState().getCollectionOrNull("c1") == null);

    reader.unregisterCore("c1");
    // re-add the same collection
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
    clusterState = writer.writePendingUpdates();
    // re-register, otherwise the child watch would be missing from collection deletion
    reader.registerCore("c1");

    // reader.forceUpdateCollection("c1");
    timeOut.waitFor(
        "Timeout on waiting for c1 to show up in cluster state again",
        () -> reader.getClusterState().getCollectionOrNull("c1") != null);
    ref = reader.getClusterState().getCollectionRef("c1");
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());
    assertEquals(0, ref.get().getChildNodesVersion()); // child node version is reset

    // re-add PRS
    collection = ref.get();
    prs =
        PerReplicaStates.fetch(
            collection.getZNode(), fixture.zkClient, collection.getPerReplicaStates());
    PerReplicaStatesOps.addReplica("r1", Replica.State.DOWN, false, prs)
        .persist(collection.getZNode(), fixture.zkClient);
    timeOut.waitFor(
        "Timeout on waiting for c1 updated to have PRS state r1",
        () -> {
          DocCollection c = reader.getCollection("c1");
          return c.getPerReplicaStates() != null
              && c.getPerReplicaStates().get("r1") != null
              && c.getPerReplicaStates().get("r1").state == Replica.State.DOWN;
        });

    ref = reader.getClusterState().getCollectionRef("c1");

    // child version should be reset since the state.json node was deleted and re-created
    assertEquals(1, ref.get().getChildNodesVersion());
  }

  public void testForciblyRefreshAllClusterState() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1"); // watching c1, so it should get non lazy reference
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

    reader.forciblyRefreshAllClusterStateSlow();
    // Initially there should be no c1 collection.
    assertNull(reader.getClusterState().getCollectionRef("c1"));

    // create new collection
    DocCollection state =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    ZkWriteCommand wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    assertTrue(fixture.zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

    reader.forciblyRefreshAllClusterStateSlow();
    ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(0, ref.get().getZNodeVersion());

    // update the collection
    state =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            ref.get().getZNodeVersion(),
            ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    wc = new ZkWriteCommand("c1", state);
    writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNotNull(ref);
    assertFalse(ref.isLazilyLoaded());
    assertEquals(1, ref.get().getZNodeVersion());

    // delete the collection c1, add a collection c2 that is NOT watched
    ZkWriteCommand wc1 = new ZkWriteCommand("c1", null);

    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
    state =
        new DocCollection(
            "c2",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            ZkStateReader.CLUSTER_STATE + "/c2/state.json");
    ZkWriteCommand wc2 = new ZkWriteCommand("c2", state);

    writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
    writer.writePendingUpdates();

    reader.forciblyRefreshAllClusterStateSlow();
    ref = reader.getClusterState().getCollectionRef("c1");
    assertNull(ref);

    ref = reader.getClusterState().getCollectionRef("c2");
    assertNotNull(ref);
    assertTrue(
        "c2 should have been lazily loaded but is not!",
        ref.isLazilyLoaded()); // c2 should be lazily loaded as it's not watched
    assertEquals(0, ref.get().getZNodeVersion());
  }

  public void testGetCurrentCollections() throws Exception {
    ZkStateWriter writer = fixture.writer;
    ZkStateReader reader = fixture.reader;

    reader.registerCore("c1"); // listen to c1. not yet exist
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
    reader.forceUpdateCollection("c1");
    Set<String> currentCollections = reader.getCurrentCollections();
    assertEquals(0, currentCollections.size()); // no active collections yet

    // now create both c1 (watched) and c2 (not watched)
    DocCollection state1 =
        new DocCollection(
            "c1",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            ZkStateReader.CLUSTER_STATE + "/c1/state.json");
    ZkWriteCommand wc1 = new ZkWriteCommand("c1", state1);
    DocCollection state2 =
        new DocCollection(
            "c2",
            new HashMap<>(),
            Collections.singletonMap(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
            DocRouter.DEFAULT,
            0,
            ZkStateReader.CLUSTER_STATE + "/c2/state.json");

    // do not listen to c2
    fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
    ZkWriteCommand wc2 = new ZkWriteCommand("c2", state2);

    writer.enqueueUpdate(reader.getClusterState(), Arrays.asList(wc1, wc2), null);
    writer.writePendingUpdates();

    reader.forceUpdateCollection("c1");
    reader.forceUpdateCollection("c2");

    // should detect both collections (c1 watched, c2 lazy loaded)
    currentCollections = reader.getCurrentCollections();
    assertEquals(2, currentCollections.size());
  }

  /**
   * Simulates race condition that might arise when state updates triggered by watch notification
   * contend with removal of collection watches.
   *
   * <p>Such race condition should no longer exist with the new code that uses a single map for both
   * "collection watches" and "latest state of watched collection"
   */
  public void testWatchRaceCondition() throws Exception {
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory("zkStateReaderTest"));
    CommonTestInjection.setDelay(1000);
    final AtomicBoolean stopMutatingThread = new AtomicBoolean(false);
    try {
      ZkStateWriter writer = fixture.writer;
      final ZkStateReader reader = fixture.reader;
      fixture.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // start another thread to constantly updating the state
      final AtomicReference<Exception> updateException = new AtomicReference<>();
      executorService.submit(
          () -> {
            try {
              ClusterState clusterState = reader.getClusterState();
              while (!stopMutatingThread.get()) {
                DocCollection collection = clusterState.getCollectionOrNull("c1");
                int currentVersion = collection != null ? collection.getZNodeVersion() : 0;
                // create new collection
                DocCollection state =
                    new DocCollection(
                        "c1",
                        new HashMap<>(),
                        Collections.singletonMap(
                            ZkStateReader.CONFIGNAME_PROP,
                            ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                        DocRouter.DEFAULT,
                        currentVersion,
                        ZkStateReader.CLUSTER_STATE + "/c1/state.json");
                ZkWriteCommand wc = new ZkWriteCommand("c1", state);
                writer.enqueueUpdate(clusterState, Collections.singletonList(wc), null);
                clusterState = writer.writePendingUpdates();
                TimeUnit.MILLISECONDS.sleep(100);
              }
            } catch (Exception e) {
              updateException.set(e);
            }
            return null;
          });
      executorService.shutdown();

      reader.waitForState(
          "c1",
          10,
          TimeUnit.SECONDS,
          slices -> slices != null); // wait for the state to become available

      final CountDownLatch latch = new CountDownLatch(2);

      // remove itself on 2nd trigger
      DocCollectionWatcher dummyWatcher =
          collection -> {
            latch.countDown();
            return latch.getCount() == 0;
          };
      reader.registerDocCollectionWatcher("c1", dummyWatcher);
      assertTrue(
          "Missing expected collection updates after the wait", latch.await(10, TimeUnit.SECONDS));
      reader.removeDocCollectionWatcher("c1", dummyWatcher);

      // cluster state might not be updated right the way from the removeDocCollectionWatcher call
      // above as org.apache.solr.common.cloud.ZkStateReader.Notification might remove the watcher
      // as well and might still be in the middle of updating the cluster state.
      TimeOut timeOut = new TimeOut(2000, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      timeOut.waitFor(
          "The ref is not lazily loaded after waiting",
          () -> reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());

      if (updateException.get() != null) {
        throw (updateException.get());
      }
    } finally {
      stopMutatingThread.set(true);
      CommonTestInjection.reset();
      ExecutorUtil.awaitTermination(executorService);
    }
  }
}