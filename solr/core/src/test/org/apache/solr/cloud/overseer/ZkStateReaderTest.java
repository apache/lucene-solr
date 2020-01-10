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

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;

public class ZkStateReaderTest extends SolrTestCaseJ4 {

  private static final long TIMEOUT = 30;

  /** Uses explicit refresh to ensure latest changes are visible. */
  public void testStateFormatUpdateWithExplicitRefresh() throws Exception {
    testStateFormatUpdate(true, true);
  }

  /** Uses explicit refresh to ensure latest changes are visible. */
  public void testStateFormatUpdateWithExplicitRefreshLazy() throws Exception {
    testStateFormatUpdate(true, false);
  }

  /** ZkStateReader should automatically pick up changes based on ZK watches. */
  public void testStateFormatUpdateWithTimeDelay() throws Exception {
    testStateFormatUpdate(false, true);
  }

  /** ZkStateReader should automatically pick up changes based on ZK watches. */
  public void testStateFormatUpdateWithTimeDelayLazy() throws Exception {
    testStateFormatUpdate(false, false);
  }

  public void testStateFormatUpdate(boolean explicitRefresh, boolean isInteresting) throws Exception {
    Path zkDir = createTempDir("testStateFormatUpdate");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      if (isInteresting) {
        reader.registerCore("c1");
      }

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

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
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();

    }
  }

  public void testExternalCollectionWatchedNotWatched() throws Exception{
    Path zkDir = createTempDir("testExternalCollectionWatchedNotWatched");
    ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

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

    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testCollectionStateWatcherCaching() throws Exception  {
    Path zkDir = createTempDir("testCollectionStateWatcherCaching");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());
      DocCollection state = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE + "/c1/state.json");
      ZkWriteCommand wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();
      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
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
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testWatchedCollectionCreation() throws Exception {
    Path zkDir = createTempDir("testWatchedCollectionCreation");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      reader.registerCore("c1");

      // Initially there should be no c1 collection.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      reader.forceUpdateCollection("c1");

      // Still no c1 collection, despite a collection path.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());


      // create new collection with stateFormat = 2
      DocCollection state = new DocCollection("c1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE + "/c1/state.json");
      ZkWriteCommand wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();

      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

      //reader.forceUpdateCollection("c1");
      reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
      ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
      assertNotNull(ref);
      assertFalse(ref.isLazilyLoaded());
      assertEquals(2, ref.get().getStateFormat());
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();

    }
  }
}
