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

import junit.framework.TestCase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class ZkControllerTest extends TestCase {

  private static final String TEST_NODE_NAME = "test_node_name";

  private static final String URL3 = "http://localhost:3133/solr/core1";

  private static final String URL2 = "http://localhost:3123/solr/core1";

  private static final String SHARD3 = "localhost:3123_solr_core3";

  private static final String SHARD2 = "localhost:3123_solr_core2";

  private static final String SHARD1 = "localhost:3123_solr_core1";

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 10000;

  private static final String URL1 = "http://localhost:3133/solr/core0";

  private static final boolean DEBUG = true;

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  public void testReadShards() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    ZkController zkController = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, TIMEOUT);
      String shardsPath = "/collections/collection1/shards/shardid1";
      zkClient.makePath(shardsPath);

      zkClient.makePath("collections/collection1/config=collection1");

      addShardToZk(zkClient, shardsPath, SHARD1, URL1, "slave");
      addShardToZk(zkClient, shardsPath, SHARD2, URL2, "master");
      addShardToZk(zkClient, shardsPath, SHARD3, URL3, "slave");

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }

      zkController = new ZkController(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, TIMEOUT, "localhost",
          "8983", "/solr", null);
      zkController.updateCloudState(true);
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
        assertEquals("slave", props.get(ZkController.ROLE_PROP));
        assertEquals(URL1, props.get(ZkController.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkController.NODE_NAME));

        props = shards.get(SHARD2);
        assertEquals("master", props.get(ZkController.ROLE_PROP));
        assertEquals(URL2, props.get(ZkController.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkController.NODE_NAME));

        props = shards.get(SHARD3);
        assertEquals("slave", props.get(ZkController.ROLE_PROP));
        assertEquals(URL3, props.get(ZkController.URL_PROP));
        assertEquals(TEST_NODE_NAME, props.get(ZkController.NODE_NAME));

      }

      // nocommit : check properties

    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
      if (zkController != null) {
        zkController.close();
      }
    }
  }

  public void testReadConfigName() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      SolrZkClient zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, TIMEOUT);
      String actualConfigName = "firstConfig";

      String shardsPath = "/collections/" + COLLECTION_NAME + "/config="
          + actualConfigName;
      zkClient.makePath(shardsPath);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      ZkController zkController = new ZkController(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, TIMEOUT,
          "localhost", "8983", "/solr", null);
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

  public void testUploadToCloud() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    ZkController zkController = null;
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkController = new ZkController(AbstractZkTestCase.ZOO_KEEPER_ADDRESS, TIMEOUT, "localhost",
          "8983", "/solr", null);

      zkController.uploadToZK(new File("solr/conf"),
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
      String zkNodeName, String url, String role) throws IOException,
      KeeperException, InterruptedException {

    ZkNodeProps props = new ZkNodeProps();
    props.put(ZkController.URL_PROP, url);
    props.put(ZkController.ROLE_PROP, role);
    props.put(ZkController.NODE_NAME, TEST_NODE_NAME);
    byte[] bytes = props.store();

    System.out.println("shards path:" + shardsPath);
    zkClient
        .create(shardsPath + "/" + zkNodeName, bytes, CreateMode.PERSISTENT);
  }

}
