package org.apache.solr.cloud;

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

import java.io.File;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: This test would be a lot faster if it used a solrhome with fewer config
// files - there are a lot of them to upload
public class ZkCLITest extends SolrTestCaseJ4 {
  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);
  
  private static final boolean VERBOSE = false;
  
  protected ZkTestServer zkServer;
  
  protected String zkDir;

  private SolrZkClient zkClient;
  
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    log.info("####SETUP_START " + getTestName());
    createTempDir();
    
    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    
    this.zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);
    
    log.info("####SETUP_END " + getTestName());
  }
  
  @Test
  public void testBootstrap() throws Exception {
    // test bootstrap_conf
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "bootstrap", "-solrhome", ExternalPaths.EXAMPLE_HOME};
    ZkCLI.main(args);
    
    assertTrue(zkClient.exists(ZkController.CONFIGS_ZKNODE + "/collection1", true));
    
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "bootstrap", "-solrhome", ExternalPaths.EXAMPLE_MULTICORE_HOME};
    ZkCLI.main(args);
    
    assertTrue(zkClient.exists(ZkController.CONFIGS_ZKNODE + "/core0", true));
    assertTrue(zkClient.exists(ZkController.CONFIGS_ZKNODE + "/core1", true));
  }
  
  @Test
  public void testBootstrapWithChroot() throws Exception {
    String chroot = "/foo/bar";
    assertFalse(zkClient.exists(chroot, true));
    
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress() + chroot,
        "-cmd", "bootstrap", "-solrhome", ExternalPaths.EXAMPLE_HOME};
    
    ZkCLI.main(args);
    
    assertTrue(zkClient.exists(chroot + ZkController.CONFIGS_ZKNODE
        + "/collection1", true));
  }

  @Test
  public void testMakePath() throws Exception {
    // test bootstrap_conf
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "makepath", "/path/mynewpath"};
    ZkCLI.main(args);


    assertTrue(zkClient.exists("/path/mynewpath", true));
  }
  
  @Test
  public void testList() throws Exception {
    zkClient.makePath("/test", true);
    String[] args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "list"};
    ZkCLI.main(args);
  }
  
  @Test
  public void testUpConfigLinkConfigClearZk() throws Exception {
    // test upconfig
    String confsetname = "confsetone";
    String[] args = new String[] {
        "-zkhost",
        zkServer.getZkAddress(),
        "-cmd",
        "upconfig",
        "-confdir",
        ExternalPaths.EXAMPLE_HOME + File.separator + "collection1"
            + File.separator + "conf", "-confname", confsetname};
    ZkCLI.main(args);
    
    assertTrue(zkClient.exists(ZkController.CONFIGS_ZKNODE + "/" + confsetname, true));

    // print help
    // ZkCLI.main(new String[0]);
    
    // test linkconfig
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "linkconfig", "-collection", "collection1", "-confname", confsetname};
    ZkCLI.main(args);
    
    ZkNodeProps collectionProps = ZkNodeProps.load(zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/collection1", null, null, true));
    assertTrue(collectionProps.containsKey("configName"));
    assertEquals(confsetname, collectionProps.getStr("configName"));
    
    // test down config
    File confDir = new File(TEMP_DIR,
        "solrtest-confdropspot-" + this.getClass().getName() + "-" + System.currentTimeMillis());
    
    assertFalse(confDir.exists());
    
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "downconfig", "-confdir", confDir.getAbsolutePath(), "-confname", confsetname};
    ZkCLI.main(args);
    
    File[] files = confDir.listFiles();
    List<String> zkFiles = zkClient.getChildren(ZkController.CONFIGS_ZKNODE + "/" + confsetname, null, true);
    assertEquals(files.length, zkFiles.size());
    
    // test reset zk
    args = new String[] {"-zkhost", zkServer.getZkAddress(), "-cmd",
        "clear", "/"};
    ZkCLI.main(args);

    assertEquals(0, zkClient.getChildren("/", null, true).size());
  }
  
  @Override
  public void tearDown() throws Exception {
    if (VERBOSE) {
      printLayout(zkServer.getZkHost());
    }
    zkClient.close();
    zkServer.shutdown();
    super.tearDown();
  }
  
  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
