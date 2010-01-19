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
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.zookeeper.KeeperException;

public class ZkControllerTest extends TestCase {

  private static final String COLLECTION_NAME = "collection1";

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
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    ZkController zkController = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(ZOO_KEEPER_ADDRESS, TIMEOUT);
      String shardsPath = "/collections/collection1/shards/shardid1";
      zkClient.makePath(shardsPath);

      zkClient.makePath("collections/collection1/config=collection1");

      addShardToZk(zkClient, shardsPath, URL1, "slave");
      addShardToZk(zkClient, shardsPath, "http://localhost:3123/solr/core1",
          "master");
      addShardToZk(zkClient, shardsPath, "http://localhost:3133/solr/core1",
          "slave");

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }

      zkController = new ZkController(ZOO_KEEPER_ADDRESS, TIMEOUT,
          "localhost", "8983", "/solr");
      zkController.updateCloudState();
      CloudState cloudInfo = zkController.getCloudState();
      Slices slices = cloudInfo.getSlices("collection1");
      assertNotNull(slices);


      if (DEBUG) {
        for(Slice slice : slices) {
          for (String shard : slice.getShards().keySet()) {
            System.out.println("shard:" + shard);
          }
        }
      }

      // nocommit : check properties 
      
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
      if(zkController != null) {
        zkController.close();
      }
    }
  }

  public void testReadConfigName() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    AbstractZkTestCase.makeSolrZkNode();

    SolrZkClient zkClient = new SolrZkClient(ZOO_KEEPER_ADDRESS, TIMEOUT);
    String actualConfigName = "firstConfig";

    String shardsPath = "/collections/" + COLLECTION_NAME + "/config="
        + actualConfigName;
    zkClient.makePath(shardsPath);

    if (DEBUG) {
      zkClient.printLayoutToStdOut();
    }

    ZkController zkController = new ZkController(ZOO_KEEPER_ADDRESS, TIMEOUT,
        "localhost", "8983", "/solr");
    String configName = zkController.readConfigName(COLLECTION_NAME);
    assertEquals(configName, actualConfigName);


    // nocommit : close in finally
    zkController.close();
    zkClient.close();
    server.shutdown();

  }
  
  public void testUploadToCloud() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();

    AbstractZkTestCase.makeSolrZkNode();

    ZkController zkController = new ZkController(ZOO_KEEPER_ADDRESS, TIMEOUT,
        "localhost", "8983", "/solr");


    zkController.uploadDirToCloud(new File("solr/conf"), ZkController.CONFIGS_ZKNODE + "/config1");
    
    if (DEBUG) {
      zkController.printLayoutToStdOut();
    }
    
    // nocommit close in finally
    zkController.close();
    server.shutdown();

  }

  private void addShardToZk(SolrZkClient zkClient, String shardsPath,
      String url, String role) throws IOException, KeeperException,
      InterruptedException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    ZkNodeProps props = new ZkNodeProps();
    props.put(ZkController.URL_PROP, url);
    props.put(ZkController.ROLE_PROP, role);
    props.store(new DataOutputStream(baos));

    //nocommit : fix
//    zkClient.create(shardsPath + ZkController.CORE_ZKPREFIX,
//        baos.toByteArray(), CreateMode.EPHEMERAL_SEQUENTIAL);
  }


}
