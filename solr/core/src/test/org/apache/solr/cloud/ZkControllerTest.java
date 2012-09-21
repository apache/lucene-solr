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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
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
    CoreContainer cc = null;

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + actualConfigName, true);
      
      Map<String,Object> props = new HashMap<String,Object>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/"
          + COLLECTION_NAME, ZkStateReader.toJSON(zkProps),
          CreateMode.PERSISTENT, true);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      
      cc = getCoreContainer();
      
      ZkController zkController = new ZkController(cc, server.getZkAddress(), TIMEOUT, 10000,
          "127.0.0.1", "8983", "solr", new CurrentCoreDescriptorProvider() {
            
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
      if (cc != null) {
        cc.shutdown();
      }
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
    CoreContainer cc = null;
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      cc = getCoreContainer();
      
      zkController = new ZkController(cc, server.getZkAddress(),
          TIMEOUT, 10000, "127.0.0.1", "8983", "solr", new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      zkController.uploadToZK(new File(ExternalPaths.EXAMPLE_HOME + "/collection1/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");
      
      // uploading again should overwrite, not error...
      zkController.uploadToZK(new File(ExternalPaths.EXAMPLE_HOME + "/collection1/conf"),
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
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }

  }

  private CoreContainer getCoreContainer() {
    return new CoreContainer(TEMP_DIR.getAbsolutePath());
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
