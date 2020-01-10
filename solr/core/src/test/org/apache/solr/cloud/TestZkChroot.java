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
package org.apache.solr.cloud;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.CoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZkChroot extends SolrTestCaseJ4 {
  protected CoreContainer cores = null;
  private Path home;
  
  protected ZkTestServer zkServer;
  protected Path zkDir;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    zkDir = createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    home = Paths.get(SolrJettyTestBase.legacyExampleCollection1SolrHome());
    
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("zkHost");
    
    if (cores != null) {
      cores.shutdown();
      cores = null;
    }
    
    if (null != zkServer) {
      zkServer.shutdown();
      zkServer = null;
    }
    zkDir = null;
    
    super.tearDown();
  }
  
  @Test
  public void testChrootBootstrap() throws Exception {
    String chroot = "/foo/bar";
    
    System.setProperty("bootstrap_conf", "true");
    System.setProperty("zkHost", zkServer.getZkHost() + chroot);
    SolrZkClient zkClient = null;
    SolrZkClient zkClient2 = null;
    
    try {
      cores = CoreContainer.createAndLoad(home);
      zkClient = cores.getZkController().getZkClient();
      
      assertTrue(zkClient.exists("/clusterstate.json", true));
      assertFalse(zkClient.exists(chroot + "/clusterstate.json", true));
      
      zkClient2 = new SolrZkClient(zkServer.getZkHost(),
          AbstractZkTestCase.TIMEOUT);
      assertTrue(zkClient2.exists(chroot + "/clusterstate.json", true));
      assertFalse(zkClient2.exists("/clusterstate.json", true));
    } finally {
      if (cores != null) cores.shutdown();
      if (zkClient != null) zkClient.close();
      if (zkClient2 != null) zkClient2.close();
    }
  }
  
  @Test
  public void testNoBootstrapConf() throws Exception {
    String chroot = "/foo/bar2";
    
    System.setProperty("bootstrap_conf", "false");
    System.setProperty("zkHost", zkServer.getZkHost() + chroot);

    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT)) {
      expectThrows(ZooKeeperException.class,
          "did not get a top level exception when more then 4 updates failed",
          () -> {
        assertFalse("Path '" + chroot + "' should not exist before the test",
            zkClient.exists(chroot, true));
        cores = CoreContainer.createAndLoad(home);
      });
      assertFalse("Path shouldn't have been created",
          zkClient.exists(chroot, true));// check the path was not created
    } finally {
      if (cores != null) cores.shutdown();
    }
  }
  
  @Test
  public void testWithUploadDir() throws Exception {
    String chroot = "/foo/bar3";
    String configName = "testWithUploadDir";
    
    System.setProperty("bootstrap_conf", "false");
    System.setProperty("bootstrap_confdir", home + "/collection1/conf");
    System.setProperty("collection.configName", configName);
    System.setProperty("zkHost", zkServer.getZkHost() + chroot);
    SolrZkClient zkClient = null;
    
    try {
      zkClient = new SolrZkClient(zkServer.getZkHost(),
          AbstractZkTestCase.TIMEOUT);
      assertFalse("Path '" + chroot + "' should not exist before the test",
          zkClient.exists(chroot, true));
      cores = CoreContainer.createAndLoad(home);
      assertTrue(
          "solrconfig.xml should have been uploaded to zk to the correct config directory",
          zkClient.exists(chroot + ZkConfigManager.CONFIGS_ZKNODE + "/"
              + configName + "/solrconfig.xml", true));
    } finally {
      if (cores != null) cores.shutdown();
      if (zkClient != null) zkClient.close();
    }
  }
  
  @Test
  public void testInitPathExists() throws Exception {
    String chroot = "/foo/bar4";
    
    System.setProperty("bootstrap_conf", "true");
    System.setProperty("zkHost", zkServer.getZkHost() + chroot);
    SolrZkClient zkClient = null;
    
    try {
      zkClient = new SolrZkClient(zkServer.getZkHost(),
          AbstractZkTestCase.TIMEOUT);
      zkClient.makePath("/foo/bar4", true);
      assertTrue(zkClient.exists(chroot, true));
      assertFalse(zkClient.exists(chroot + "/clusterstate.json", true));
      
      cores = CoreContainer.createAndLoad(home);
      assertTrue(zkClient.exists(chroot + "/clusterstate.json", true));
    } finally {
      if (cores != null) cores.shutdown();
      if (zkClient != null) zkClient.close();
    }
  }
}
