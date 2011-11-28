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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
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

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + actualConfigName);
      
      Map<String,String> props = new HashMap<String,String>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + COLLECTION_NAME , zkProps.store(), CreateMode.PERSISTENT);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      ZkController zkController = new ZkController(server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "8983", "/solr", 3, new CurrentCoreDescriptorProvider() {
            
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
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkController = new ZkController(server.getZkAddress(),
          TIMEOUT, 10000, "localhost", "8983", "/solr", 3, new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

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
  
  @Test
  public void testAutoShard() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);

    
    ZkController zkController = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkController = new ZkController(server.getZkAddress(),
          TIMEOUT, 10000, "localhost", "8983", "solr", 3, new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      System.setProperty("bootstrap_confdir", getFile("solr/conf").getAbsolutePath());
      
      // ensure our shards node for the collection exists - other tests can mess with this
      
      CloudDescriptor cloudDesc = new CloudDescriptor();
      cloudDesc.setCollectionName("collection1");
      
      
      zkController.createCollectionZkNode(cloudDesc);
     
      CoreDescriptor desc = new CoreDescriptor(null, "core1", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard1 = zkController.register("core1", desc);
      cloudDesc.setShardId(null);
      desc = new CoreDescriptor(null, "core2", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard2 = zkController.register("core2", desc);
      cloudDesc.setShardId(null);
      desc = new CoreDescriptor(null, "core3", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard3 = zkController.register("core3", desc);
      cloudDesc.setShardId(null);
      desc = new CoreDescriptor(null, "core4", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard4 = zkController.register("core4", desc);
      cloudDesc.setShardId(null);
      desc = new CoreDescriptor(null, "core5", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard5 = zkController.register("core5", desc);
      cloudDesc.setShardId(null);
      desc = new CoreDescriptor(null, "core6", "");
      desc.setCloudDescriptor(cloudDesc);
      String shard6 = zkController.register("core6", desc);
      cloudDesc.setShardId(null);

      assertEquals("shard1", shard1);
      assertEquals("shard2", shard2);
      assertEquals("shard3", shard3);
      assertEquals("shard1", shard4);
      assertEquals("shard2", shard5);
      assertEquals("shard3", shard6);

    } finally {
      if (DEBUG) {
        if (zkController != null) {
          zkController.printLayoutToStdOut();
        }
      }
      
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }

  }
  
  @Override
  public void tearDown() throws Exception {
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
