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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkControllerTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 1000;

  private static final boolean DEBUG = false;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  @Test
  public void testReadConfigName() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + actualConfigName, true);
      
      Map<String,String> props = new HashMap<String,String>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/"
          + COLLECTION_NAME, ZkStateReader.toJSON(zkProps),
          CreateMode.PERSISTENT, true);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      ZkController zkController = new ZkController(null, server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "8983", "solr", new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });
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
    boolean testFinished = false;
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkController = new ZkController(null, server.getZkAddress(),
          TIMEOUT, 10000, "localhost", "8983", "solr", new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      zkController.uploadToZK(getFile("solr/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");
      
      // uploading again should overwrite, not error...
      zkController.uploadToZK(getFile("solr/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");

      if (DEBUG) {
        zkController.printLayoutToStdOut();
      }
      testFinished = true;
    } finally {
      if (!testFinished) {
        zkController.getZkClient().printLayoutToStdOut();
      }
      
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }

  }
  
  @Test
  public void testCoreUnload() throws Exception {
    
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    ZkController zkController = null;
    SolrZkClient zkClient = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      zkClient.makePath(ZkStateReader.LIVE_NODES_ZKNODE, true);
      
      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      
      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "1");
      System.setProperty("solrcloud.skip.autorecovery", "true");
      
      zkController = new ZkController(null, server.getZkAddress(), TIMEOUT,
          10000, "localhost", "8983", "solr",
          new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });
      
      System.setProperty("bootstrap_confdir", getFile("solr/conf")
          .getAbsolutePath());
      
      final int numShards = 2;
      final String[] ids = new String[numShards];
      
      for (int i = 0; i < numShards; i++) {
        CloudDescriptor collection1Desc = new CloudDescriptor();
        collection1Desc.setCollectionName("collection1");
        CoreDescriptor desc1 = new CoreDescriptor(null, "core" + (i + 1), "");
        desc1.setCloudDescriptor(collection1Desc);
        zkController.preRegister(desc1);
        ids[i] = zkController.register("core" + (i + 1), desc1);
      }
      
      assertEquals("shard1", ids[0]);
      assertEquals("shard1", ids[1]);
      
      assertNotNull(reader.getLeaderUrl("collection1", "shard1", 15000));
      
      assertEquals("Shard(s) missing from cloudstate", 2, zkController.getZkStateReader().getCloudState().getSlice("collection1", "shard1").getShards().size());
      
      // unregister current leader
      final ZkNodeProps shard1LeaderProps = reader.getLeaderProps(
          "collection1", "shard1");
      final String leaderUrl = reader.getLeaderUrl("collection1", "shard1",
          15000);
      
      final CloudDescriptor collection1Desc = new CloudDescriptor();
      collection1Desc.setCollectionName("collection1");
      final CoreDescriptor desc1 = new CoreDescriptor(null,
          shard1LeaderProps.get(ZkStateReader.CORE_NAME_PROP), "");
      desc1.setCloudDescriptor(collection1Desc);
      zkController.unregister(
          shard1LeaderProps.get(ZkStateReader.CORE_NAME_PROP), collection1Desc);
      assertNotSame(
          "New leader was not promoted after unregistering the current leader.",
          leaderUrl, reader.getLeaderUrl("collection1", "shard1", 15000));
      assertNotNull("New leader was null.",
          reader.getLeaderUrl("collection1", "shard1", 15000));

      for(int i=0;i<30;i++) {
        if(zkController.getZkStateReader().getCloudState().getSlice("collection1", "shard1").getShards().size()==1) break; 
        Thread.sleep(500);
      }
      assertEquals("shard was not unregistered", 1, zkController.getZkStateReader().getCloudState().getSlice("collection1", "shard1").getShards().size());
    } finally {
      System.clearProperty("solrcloud.skip.autorecovery");
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
      System.clearProperty("bootstrap_confdir");
      if (DEBUG) {
        if (zkController != null) {
          zkClient.printLayoutToStdOut();
        }
      }
      if (zkClient != null) {
        zkClient.close();
      }
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
