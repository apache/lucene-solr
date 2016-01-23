package org.apache.solr.cloud.overseer;

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

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerTest;
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

import java.util.HashMap;
import java.util.Map;

public class ZkStateWriterTest extends SolrTestCaseJ4 {

  public void testZkStateWriterBatching() throws Exception {
    String zkDir = createTempDir("testZkStateWriterBatching").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      assertFalse("Deletes can always be batched", writer.maybeFlushBefore(new ZkWriteCommand("xyz", null)));
      assertFalse("Deletes can always be batched", writer.maybeFlushAfter(new ZkWriteCommand("xyz", null)));

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);

      // create new collection with stateFormat = 2
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1"));
      assertFalse("First requests can always be batched", writer.maybeFlushBefore(c1));

      ClusterState clusterState = writer.enqueueUpdate(reader.getClusterState(), c1, null);

      ZkWriteCommand c2 = new ZkWriteCommand("c2",
          new DocCollection("c2", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c2"));
      assertTrue("Different (new) collection create cannot be batched together with another create", writer.maybeFlushBefore(c2));

      // simulate three state changes on same collection, all should be batched together before
      assertFalse(writer.maybeFlushBefore(c1));
      assertFalse(writer.maybeFlushBefore(c1));
      assertFalse(writer.maybeFlushBefore(c1));
      // and after too
      assertFalse(writer.maybeFlushAfter(c1));
      assertFalse(writer.maybeFlushAfter(c1));
      assertFalse(writer.maybeFlushAfter(c1));

      // simulate three state changes on two different collections with stateFormat=2, none should be batched
      assertFalse(writer.maybeFlushBefore(c1));
      // flushAfter has to be called as it updates the internal batching related info
      assertFalse(writer.maybeFlushAfter(c1));
      assertTrue(writer.maybeFlushBefore(c2));
      assertFalse(writer.maybeFlushAfter(c2));
      assertTrue(writer.maybeFlushBefore(c1));
      assertFalse(writer.maybeFlushAfter(c1));

      // create a collection in stateFormat = 1 i.e. inside the main cluster state
      ZkWriteCommand c3 = new ZkWriteCommand("c3",
          new DocCollection("c3", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));
      clusterState = writer.enqueueUpdate(clusterState, c3, null);

      // simulate three state changes in c3, all should be batched
      for (int i=0; i<3; i++) {
        assertFalse(writer.maybeFlushBefore(c3));
        assertFalse(writer.maybeFlushAfter(c3));
      }

      // simulate state change in c3 (stateFormat=1) interleaved with state changes from c1,c2 (stateFormat=2)
      // none should be batched together
      assertFalse(writer.maybeFlushBefore(c3));
      assertFalse(writer.maybeFlushAfter(c3));
      assertTrue("different stateFormat, should be flushed", writer.maybeFlushBefore(c1));
      assertFalse(writer.maybeFlushAfter(c1));
      assertTrue("different stateFormat, should be flushed", writer.maybeFlushBefore(c3));
      assertFalse(writer.maybeFlushAfter(c3));
      assertTrue("different stateFormat, should be flushed", writer.maybeFlushBefore(c2));
      assertFalse(writer.maybeFlushAfter(c2));

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }

  public void testSingleLegacyCollection() throws Exception {
    String zkDir = createTempDir("testSingleLegacyCollection").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // create new collection with stateFormat = 1
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));

      ClusterState clusterState = writer.enqueueUpdate(reader.getClusterState(), c1, null);
      writer.writePendingUpdates();

      Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
      assertNotNull(map.get("c1"));
      boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
      assertFalse(exists);

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();

    }
  }

  public void testSingleExternalCollection() throws Exception{
    String zkDir = createTempDir("testSingleExternalCollection").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // create new collection with stateFormat = 2
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json"));

      ClusterState clusterState = writer.enqueueUpdate(reader.getClusterState(), c1, null);
      writer.writePendingUpdates();

      Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
      assertNull(map.get("c1"));
      map = (Map) Utils.fromJSON(zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", null, null, true));
      assertNotNull(map.get("c1"));

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();

    }


  }

  public void testExternalModificationToSharedClusterState() throws Exception {
    String zkDir = createTempDir("testExternalModification").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);

      // create collection 1 with stateFormat = 1
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));
      writer.enqueueUpdate(reader.getClusterState(), c1, null);
      writer.writePendingUpdates();

      reader.updateClusterState();
      ClusterState clusterState = reader.getClusterState(); // keep a reference to the current cluster state object
      assertTrue(clusterState.hasCollection("c1"));
      assertFalse(clusterState.hasCollection("c2"));

      // Simulate an external modification to /clusterstate.json
      byte[] data = zkClient.getData("/clusterstate.json", null, null, true);
      zkClient.setData("/clusterstate.json", data, true);

      // enqueue another c1 so that ZkStateWriter has pending updates
      writer.enqueueUpdate(clusterState, c1, null);
      assertTrue(writer.hasPendingUpdates());

      // create collection 2 with stateFormat = 1
      ZkWriteCommand c2 = new ZkWriteCommand("c2",
          new DocCollection("c2", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.getCollectionPath("c2")));

      try {
        writer.enqueueUpdate(clusterState, c2, null); // we are sending in the old cluster state object
        fail("Enqueue should not have succeeded");
      } catch (KeeperException.BadVersionException bve) {
        // expected
      }

      reader.updateClusterState();
      try {
        writer.enqueueUpdate(reader.getClusterState(), c2, null);
        fail("enqueueUpdate after BadVersionException should not have suceeded");
      } catch (IllegalStateException e) {
        // expected
      }

      try {
        writer.writePendingUpdates();
        fail("writePendingUpdates after BadVersionException should not have suceeded");
      } catch (IllegalStateException e) {
        // expected
      }

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }

  public void testExternalModificationToStateFormat2() throws Exception {
    String zkDir = createTempDir("testExternalModificationToStateFormat2").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c2", true);

      ClusterState state = reader.getClusterState();

      // create collection 2 with stateFormat = 2
      ZkWriteCommand c2 = new ZkWriteCommand("c2",
          new DocCollection("c2", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.getCollectionPath("c2")));
      state = writer.enqueueUpdate(reader.getClusterState(), c2, null);
      assertFalse(writer.hasPendingUpdates()); // first write is flushed immediately

      int sharedClusterStateVersion = state.getZkClusterStateVersion();
      int stateFormat2Version = state.getCollection("c2").getZNodeVersion();

      // Simulate an external modification to /collections/c2/state.json
      byte[] data = zkClient.getData(ZkStateReader.getCollectionPath("c2"), null, null, true);
      zkClient.setData(ZkStateReader.getCollectionPath("c2"), data, true);

      // get the most up-to-date state
      reader.updateClusterState();
      state = reader.getClusterState();
      assertTrue(state.hasCollection("c2"));
      assertEquals(sharedClusterStateVersion, (int) state.getZkClusterStateVersion());
      assertEquals(stateFormat2Version + 1, state.getCollection("c2").getZNodeVersion());

      // enqueue an update to stateFormat2 collection such that update is pending
      state = writer.enqueueUpdate(state, c2, null);
      assertTrue(writer.hasPendingUpdates());

      // get the most up-to-date state
      reader.updateClusterState();
      state = reader.getClusterState();

      // enqueue a stateFormat=1 collection which should cause a flush
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE));

      try {
        state = writer.enqueueUpdate(state, c1, null);
        fail("Enqueue should not have succeeded");
      } catch (KeeperException.BadVersionException bve) {
        // expected
      }

      try {
        writer.enqueueUpdate(reader.getClusterState(), c2, null);
        fail("enqueueUpdate after BadVersionException should not have suceeded");
      } catch (IllegalStateException e) {
        // expected
      }

      try {
        writer.writePendingUpdates();
        fail("writePendingUpdates after BadVersionException should not have suceeded");
      } catch (IllegalStateException e) {
        // expected
      }
    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }
}
