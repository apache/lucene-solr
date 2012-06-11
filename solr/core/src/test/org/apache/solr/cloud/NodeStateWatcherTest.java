package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.NodeStateWatcher.NodeStateChangeListener;
import org.apache.solr.cloud.OverseerTest.MockZKController;
import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import java.util.Collection;

public class NodeStateWatcherTest extends SolrTestCaseJ4 {

  private int TIMEOUT = 10000;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  public void testCoreAddDelete() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    SolrZkClient overseerClient = null;
    MockZKController controller = null;

    try {
      final String NODE_NAME = "node1";
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      zkClient.makePath("/live_nodes", true);

      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "2");

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      controller = new MockZKController(server.getZkAddress(), NODE_NAME, "collection1");

      final String path = Overseer.STATES_NODE + "/" + NODE_NAME;
      
      final AtomicInteger callCounter = new AtomicInteger();
      NodeStateWatcher watcher = new NodeStateWatcher(zkClient, NODE_NAME, path, new NodeStateChangeListener() {
        
        @Override
        public void coreChanged(String nodeName, Set<CoreState> states)
            throws KeeperException, InterruptedException {
          callCounter.incrementAndGet();
        }

        @Override
        public void coreDeleted(String nodeName, Collection<CoreState> states)
            throws KeeperException, InterruptedException {
          callCounter.incrementAndGet();
        }
      });

      controller.publishState("core1", "state1", 2);
      waitForCall(1, callCounter);
      assertEquals(1, watcher.getCurrentState().size());
      controller.publishState("core2", "state1", 2);
      waitForCall(2, callCounter);
      assertEquals(2, watcher.getCurrentState().size());
      controller.publishState("core1", null, 2);
      waitForCall(3, callCounter);
      assertEquals(1, watcher.getCurrentState().size());
      controller.publishState("core2", null, 2);
      waitForCall(4, callCounter);
      assertEquals(0, watcher.getCurrentState().size());
    } finally {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
      if (zkClient != null) {
        zkClient.close();
      }
      if (controller != null) {
        controller.close();
      }
      if (overseerClient != null) {
        overseerClient.close();
      }
      if (reader != null) {
        reader.close();
      }

    }

  }

  private void waitForCall(int i, AtomicInteger callCounter) throws InterruptedException {
    while (i > callCounter.get()) {
      Thread.sleep(10);
    }
  }
}
