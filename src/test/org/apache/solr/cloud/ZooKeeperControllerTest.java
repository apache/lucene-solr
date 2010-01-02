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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class ZooKeeperControllerTest extends TestCase {

  private static final String COLLECTION_NAME = "collection1";

  private static final String SHARD2 = "shard2";

  private static final String SHARD1 = "shard1";

  static final String ZOO_KEEPER_ADDRESS = "localhost:2181/solr";
  static final String ZOO_KEEPER_HOST = "localhost:2181";

  static final int TIMEOUT = 10000;

  private static final String URL1 = "http://localhost:3133/solr/core0";

  private static final boolean DEBUG = true;

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  public void testReadShards() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZooKeeperTestServer server = null;
      SolrZkClient zkClient = null;
    try {
    server = new ZooKeeperTestServer(zkDir);
    server.run();

    makeSolrZkNode();
    
    zkClient = new SolrZkClient(ZOO_KEEPER_ADDRESS, TIMEOUT);
    String shardsPath = "/collections/collection1/shards";
    zkClient.makePath(shardsPath);
    
    zkClient.makePath("collections/collection1/config=collection1");

    addShardToZk(zkClient, shardsPath, URL1, SHARD1 + "," + SHARD2);
    addShardToZk(zkClient, shardsPath, "http://localhost:3123/solr/core1", SHARD1);
    addShardToZk(zkClient, shardsPath, "http://localhost:3133/solr/core1", SHARD1);


    if (DEBUG) {
      zkClient.printLayoutToStdOut();
    }

    ZooKeeperController zkController = new ZooKeeperController(ZOO_KEEPER_ADDRESS, "collection1", "localhost", "8983", "/solr", TIMEOUT);
    Map<String,ShardInfoList> shardInfoMap = zkController.readShardInfo(shardsPath);
    assertTrue(shardInfoMap.size() > 0);
    
    Set<Entry<String,ShardInfoList>> entries = shardInfoMap.entrySet();

    if (DEBUG) {
      for (Entry<String,ShardInfoList> entry : entries) {
        System.out.println("shard:" + entry.getKey() + " value:"
            + entry.getValue().toString());
      }
    }

    Set<String> keys = shardInfoMap.keySet();

    assertTrue(keys.size() == 2);

    assertTrue(keys.contains(SHARD1));
    assertTrue(keys.contains(SHARD2));

    ShardInfoList shardInfoList = shardInfoMap.get(SHARD1);

    assertEquals(3, shardInfoList.getShards().size());

    shardInfoList = shardInfoMap.get(SHARD2);

    assertEquals(1, shardInfoList.getShards().size());

    assertEquals(URL1, shardInfoList.getShards().get(0).getUrl());
    } finally {
      if(zkClient != null) {
        zkClient.close();
      }
      if(server != null) {
        server.shutdown();
      }
    }
  }

  public void testReadConfigName() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZooKeeperTestServer server = new ZooKeeperTestServer(zkDir);
    server.run();

    makeSolrZkNode();

    SolrZkClient zkClient = new SolrZkClient(ZOO_KEEPER_ADDRESS, TIMEOUT);
    String actualConfigName = "firstConfig";
      
    String shardsPath = "/collections/" + COLLECTION_NAME + "/config=" + actualConfigName;
    zkClient.makePath(shardsPath);

    if (DEBUG) {
      zkClient.printLayoutToStdOut();
    }
    
    ZooKeeperController zkController = new ZooKeeperController(ZOO_KEEPER_ADDRESS, "collection1", "localhost", "8983", "/solr", TIMEOUT);
    String configName = zkController.readConfigName(COLLECTION_NAME);
    assertEquals(configName, actualConfigName);
    
    zkClient.close();
    server.shutdown();
    
  }

  private void addShardToZk(SolrZkClient zkClient, String shardsPath,
      String url, String shardList) throws IOException, KeeperException,
      InterruptedException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // nocommit: could do xml
    Properties props = new Properties();
    props.put(CollectionInfo.URL_PROP, url);
    props.put(CollectionInfo.SHARD_LIST_PROP, shardList);
    props.store(baos, ZooKeeperController.PROPS_DESC);

    zkClient.create(shardsPath
        + ZooKeeperController.NODE_ZKPREFIX, baos.toByteArray(), CreateMode.EPHEMERAL_SEQUENTIAL, null);
  }

  private void makeSolrZkNode() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(ZOO_KEEPER_HOST, TIMEOUT);
    zkClient.makePath("/solr");
    zkClient.close();
  }
}
