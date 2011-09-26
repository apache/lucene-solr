package org.apache.solr.cloud;

/**
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
import java.io.IOException;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import org.junit.BeforeClass;
import org.junit.Test;

public class ZkControllerTest extends SolrTestCaseJ4 {

  private static final String TEST_NODE_NAME = "test_node_name";

  private static final String URL3 = "http://localhost:3133/solr/core1";

  private static final String URL2 = "http://localhost:3123/solr/core1";

  private static final String SHARD3 = "localhost:3123_solr_core3";

  private static final String SHARD2 = "localhost:3123_solr_core2";

  private static final String SHARD1 = "localhost:3123_solr_core1";

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 10000;

  private static final String URL1 = "http://localhost:3133/solr/core0";

  private static final boolean DEBUG = false;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  @Test
  public void testReadShards() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    ZkController zkController = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String shardsPath = "/collections/collection1/shards/shardid1";
      zkClient.makePath(shardsPath);

      addShardToZk(zkClient, shardsPath, SHARD1, URL1);
      addShardToZk(zkClient, shardsPath, SHARD2, URL2);
      addShardToZk(zkClient, shardsPath, SHARD3, URL3);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }

      zkController = new ZkController(server.getZkAddress(),
          TIMEOUT, 1000, "localhost", "8983", "solr");
 
      zkController.getZkStateReader().updateCloudState(true);
      CloudState cloudInfo = zkController.getCloudState();
      Map<String,Slice> slices = cloudInfo.getSlices("collection1");
      assertNotNull(slices);

      for (Slice slice : slices.values()) {
        Map<String,ZkNodeProps> shards = slice.getShards();
        if (DEBUG) {
          for (String shardName : shards.keySet()) {
            ZkNodeProps props = shards.get(shardName);
            System.out.println("shard:" + shardName);
            System.out.println("props:" + props.toString());
          }
        }
        assertNotNull(shards.get(SHARD1));
        assertNotNull(shards.get(SHARD2));
        assertNotNull(shards.get(SHARD3));

        ZkNodeProps props = shards.get(SHARD1);
        assertEquals(URL1, props.get(ZkStateReader.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkStateReader.NODE_NAME));

        props = shards.get(SHARD2);
        assertEquals(URL2, props.get(ZkStateReader.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkStateReader.NODE_NAME));

        props = shards.get(SHARD3);
        assertEquals(URL3, props.get(ZkStateReader.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkStateReader.NODE_NAME));

      }

    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      if (zkController != null) {
        zkController.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }

  @Test
  public void testReadConfigName() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + actualConfigName);
      
      ZkNodeProps props = new ZkNodeProps();
      props.put("configName", actualConfigName);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION_NAME , props.store(), CreateMode.PERSISTENT);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      ZkController zkController = new ZkController(server.getZkAddress(), TIMEOUT, TIMEOUT,
          "localhost", "8983", "/solr");
      try {
        String configName = zkController.readConfigName(COLLECTION_NAME);
        assertEquals(configName, actualConfigName);
      } finally {
        zkController.close();
      }
    } finally {

      server.shutdown();
    }

  }

  @Test
  public void testUploadToCloud() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    ZkController zkController = null;
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkController = new ZkController(server.getZkAddress(),
          TIMEOUT, 10000, "localhost", "8983", "/solr");

      zkController.uploadToZK(getFile("solr/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");

      if (DEBUG) {
        zkController.printLayoutToStdOut();
      }

    } finally {
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }

  }

  private void addShardToZk(SolrZkClient zkClient, String shardsPath,
      String zkNodeName, String url) throws IOException,
      KeeperException, InterruptedException {

    ZkNodeProps props = new ZkNodeProps();
    props.put(ZkStateReader.URL_PROP, url);
    props.put(ZkStateReader.NODE_NAME, TEST_NODE_NAME);
    byte[] bytes = props.store();

    zkClient
        .create(shardsPath + "/" + zkNodeName, bytes, CreateMode.PERSISTENT);
  }
  
  @Override
  public void tearDown() throws Exception {
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }
}
