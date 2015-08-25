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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;

public class ZkStateReaderTest extends SolrTestCaseJ4 {

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
    String zkDir = createTempDir("testStateFormatUpdate").toFile().getAbsolutePath();

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
      if (isInteresting) {
        reader.addCollectionWatch("c1");
      }

      ZkStateWriter writer = new ZkStateWriter(reader, new Overseer.Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      {
        // create new collection with stateFormat = 1
        DocCollection stateV1 = new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.CLUSTER_STATE);
        ZkWriteCommand c1 = new ZkWriteCommand("c1", stateV1);
        writer.enqueueUpdate(reader.getClusterState(), c1, null);
        writer.writePendingUpdates();

        Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
        assertNotNull(map.get("c1"));
        boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
        assertFalse(exists);

        if (explicitRefresh) {
          reader.updateClusterState();
        } else {
          for (int i = 0; i < 100; ++i) {
            if (reader.getClusterState().hasCollection("c1")) {
              break;
            }
            Thread.sleep(50);
          }
        }

        DocCollection collection = reader.getClusterState().getCollection("c1");
        assertEquals(1, collection.getStateFormat());
      }


      {
        // Now update the collection to stateFormat = 2
        DocCollection stateV2 = new DocCollection("c1", new HashMap<String, Slice>(), new HashMap<String, Object>(), DocRouter.DEFAULT, 0, ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json");
        ZkWriteCommand c2 = new ZkWriteCommand("c1", stateV2);
        writer.enqueueUpdate(reader.getClusterState(), c2, null);
        writer.writePendingUpdates();

        Map map = (Map) Utils.fromJSON(zkClient.getData("/clusterstate.json", null, null, true));
        assertNull(map.get("c1"));
        boolean exists = zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true);
        assertTrue(exists);

        if (explicitRefresh) {
          reader.updateClusterState();
        } else {
          for (int i = 0; i < 100; ++i) {
            if (reader.getClusterState().getCollection("c1").getStateFormat() == 2) {
              break;
            }
            Thread.sleep(50);
          }
        }

        DocCollection collection = reader.getClusterState().getCollection("c1");
        assertEquals(2, collection.getStateFormat());
      }
    } finally {
      IOUtils.close(zkClient);
      server.shutdown();

    }
  }

  public void testExternalCollectionWatchedNotWatched() throws Exception{
    String zkDir = createTempDir("testExternalCollectionWatchedNotWatched").toFile().getAbsolutePath();
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
      writer.enqueueUpdate(reader.getClusterState(), c1, null);
      writer.writePendingUpdates();
      reader.updateClusterState();

      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.addCollectionWatch("c1");
      assertFalse(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.removeZKWatch("c1");
      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());

    } finally {
      IOUtils.close(zkClient);
      server.shutdown();
    }
  }
}
