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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateWriterTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final ZkStateWriter.ZkWriteCallback FAIL_ON_WRITE = () -> fail("Got unexpected flush");

  @BeforeClass
  public static void setup() {
    System.setProperty("solr.OverseerStateUpdateDelay", "1000");
    System.setProperty("solr.OverseerStateUpdateBatchSize", "10");
  }

  @AfterClass
  public static void cleanup() {
    System.clearProperty("solr.OverseerStateUpdateDelay");
    System.clearProperty("solr.OverseerStateUpdateBatchSize");
  }

  public void testZkStateWriterBatching() throws Exception {
    Path zkDir = createTempDir("testZkStateWriterBatching");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      try (ZkStateReader reader = new ZkStateReader(zkClient)) {
        reader.createClusterStateWatchersAndUpdate();

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c3", true);

        ZkWriteCommand c1 = new ZkWriteCommand("c1",
            new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1"));
        ZkWriteCommand c2 = new ZkWriteCommand("c2",
            new DocCollection("c2", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c2"));
        ZkWriteCommand c3 = new ZkWriteCommand("c3",
            new DocCollection("c3", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c3"));
        ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

        // First write is flushed immediately
        ClusterState clusterState = writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
        clusterState = writer.enqueueUpdate(clusterState, Collections.singletonList(c1), FAIL_ON_WRITE);
        clusterState = writer.enqueueUpdate(clusterState, Collections.singletonList(c2), FAIL_ON_WRITE);

        Thread.sleep(Overseer.STATE_UPDATE_DELAY + 100);
        AtomicBoolean didWrite = new AtomicBoolean(false);
        clusterState = writer.enqueueUpdate(clusterState, Collections.singletonList(c3), () -> didWrite.set(true));
        assertTrue("Exceed the update delay, should be flushed", didWrite.get());

        for (int i = 0; i <= Overseer.STATE_UPDATE_BATCH_SIZE; i++) {
          clusterState = writer.enqueueUpdate(clusterState, Collections.singletonList(c3), () -> didWrite.set(true));
        }
        assertTrue("Exceed the update batch size, should be flushed", didWrite.get());
      }

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }

  public void testSingleLegacyCollection() throws Exception {
    Path zkDir = createTempDir("testSingleLegacyCollection");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      try (ZkStateReader reader = new ZkStateReader(zkClient)) {
        reader.createClusterStateWatchersAndUpdate();

        ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

        // create new collection with stateFormat = 1
        ZkWriteCommand c1 = new ZkWriteCommand("c1",
            new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));

        writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
        writer.writePendingUpdates();

        Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
        assertNotNull(map.get("c1"));
        boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
        assertFalse(exists);
      }

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();

    }
  }

  public void testSingleExternalCollection() throws Exception {
    Path zkDir = createTempDir("testSingleExternalCollection");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      try (ZkStateReader reader = new ZkStateReader(zkClient)) {
        reader.createClusterStateWatchersAndUpdate();

        ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

        // create new collection with stateFormat = 2
        ZkWriteCommand c1 = new ZkWriteCommand("c1",
            new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json"));

        writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
        writer.writePendingUpdates();

        Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
        assertNull(map.get("c1"));
        map = (Map) Utils.fromJSON(zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", null, null, true));
        assertNotNull(map.get("c1"));
      }

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();

    }


  }

  public void testExternalModificationToSharedClusterState() throws Exception {
    Path zkDir = createTempDir("testExternalModification");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      try (ZkStateReader reader = new ZkStateReader(zkClient)) {
        reader.createClusterStateWatchersAndUpdate();

        ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);

        // create collection 1 with stateFormat = 1
        ZkWriteCommand c1 = new ZkWriteCommand("c1",
            new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));
        writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
        writer.writePendingUpdates();

        reader.forceUpdateCollection("c1");
        reader.forceUpdateCollection("c2");
        ClusterState clusterState = reader.getClusterState(); // keep a reference to the current cluster state object
        assertTrue(clusterState.hasCollection("c1"));
        assertFalse(clusterState.hasCollection("c2"));

        // Simulate an external modification to /clusterstate.json
        byte[] data = zkClient.getData("/clusterstate.json", null, null, true);
        zkClient.setData("/clusterstate.json", data, true);

        // enqueue another c1 so that ZkStateWriter has pending updates
        writer.enqueueUpdate(clusterState, Collections.singletonList(c1), null);
        assertTrue(writer.hasPendingUpdates());

        // Will trigger flush
        Thread.sleep(Overseer.STATE_UPDATE_DELAY + 100);
        ZkWriteCommand c2 = new ZkWriteCommand("c2",
            new DocCollection("c2", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.getCollectionPath("c2")));

        try {
          writer.enqueueUpdate(clusterState, Collections.singletonList(c2), null); // we are sending in the old cluster state object
          fail("Enqueue should not have succeeded");
        } catch (KeeperException.BadVersionException bve) {
          // expected
        }

        try {
          writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c2), null);
          fail("enqueueUpdate after BadVersionException should not have succeeded");
        } catch (IllegalStateException e) {
          // expected
        }

        try {
          writer.writePendingUpdates();
          fail("writePendingUpdates after BadVersionException should not have succeeded");
        } catch (IllegalStateException e) {
          // expected
        }
      }
    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }

  }

  public void testExternalModificationToStateFormat2() throws Exception {
    Path zkDir = createTempDir("testExternalModificationToStateFormat2");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      try (ZkStateReader reader = new ZkStateReader(zkClient)) {
        reader.createClusterStateWatchersAndUpdate();

        ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);

        ClusterState state = reader.getClusterState();

        // create collection 2 with stateFormat = 2
        ZkWriteCommand c2 = new ZkWriteCommand("c2",
            new DocCollection("c2", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.getCollectionPath("c2")));
        state = writer.enqueueUpdate(state, Collections.singletonList(c2), null);
        assertFalse(writer.hasPendingUpdates()); // first write is flushed immediately

        int sharedClusterStateVersion = state.getZkClusterStateVersion();
        int stateFormat2Version = state.getCollection("c2").getZNodeVersion();

        // Simulate an external modification to /collections/c2/state.json
        byte[] data = zkClient.getData(ZkStateReader.getCollectionPath("c2"), null, null, true);
        zkClient.setData(ZkStateReader.getCollectionPath("c2"), data, true);

        // get the most up-to-date state
        reader.forceUpdateCollection("c2");
        state = reader.getClusterState();
        log.info("Cluster state: {}", state);
        assertTrue(state.hasCollection("c2"));
        assertEquals(sharedClusterStateVersion, (int) state.getZkClusterStateVersion());
        assertEquals(stateFormat2Version + 1, state.getCollection("c2").getZNodeVersion());

        writer.enqueueUpdate(state, Collections.singletonList(c2), null);
        assertTrue(writer.hasPendingUpdates());

        // get the most up-to-date state
        reader.forceUpdateCollection("c2");
        state = reader.getClusterState();

        // Will trigger flush
        Thread.sleep(Overseer.STATE_UPDATE_DELAY+100);
        ZkWriteCommand c1 = new ZkWriteCommand("c1",
            new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));

        try {
          writer.enqueueUpdate(state, Collections.singletonList(c1), null);
          fail("Enqueue should not have succeeded");
        } catch (KeeperException.BadVersionException bve) {
          // expected
        }

        try {
          writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c2), null);
          fail("enqueueUpdate after BadVersionException should not have succeeded");
        } catch (IllegalStateException e) {
          // expected
        }

        try {
          writer.writePendingUpdates();
          fail("writePendingUpdates after BadVersionException should not have succeeded");
        } catch (IllegalStateException e) {
          // expected
        }
      }
    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }
}
