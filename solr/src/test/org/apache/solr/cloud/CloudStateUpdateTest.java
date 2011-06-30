package org.apache.solr.cloud;

/**
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
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreContainer.Initializer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: look at hostPort used below
 */
public class CloudStateUpdateTest extends SolrTestCaseJ4  {
  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);

  private static final boolean VERBOSE = false;

  protected ZkTestServer zkServer;

  protected String zkDir;

  private CoreContainer container1;

  private CoreContainer container2;

  private CoreContainer container3;

  private File dataDir1;

  private File dataDir2;

  private File dataDir3;
  
  private File dataDir4;

  private Initializer init2;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTempDir();
    System.setProperty("zkClientTimeout", "3000");

    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(), zkServer
        .getZkAddress(), "solrconfig.xml", "schema.xml");
    
    log.info("####SETUP_START " + getName());
    dataDir1 = new File(dataDir + File.separator + "data1");
    dataDir1.mkdirs();
    
    dataDir2 = new File(dataDir + File.separator + "data2");
    dataDir2.mkdirs();
    
    dataDir3 = new File(dataDir + File.separator + "data3");
    dataDir3.mkdirs();
    
    dataDir4 = new File(dataDir + File.separator + "data4");
    dataDir4.mkdirs();
    
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    System.setProperty("hostPort", "1661");
    CoreContainer.Initializer init1 = new CoreContainer.Initializer();
    System.setProperty("solr.data.dir", CloudStateUpdateTest.this.dataDir1.getAbsolutePath());
    container1 = init1.initialize();
    System.clearProperty("hostPort");
    
    System.setProperty("hostPort", "1662");
    init2 = new CoreContainer.Initializer();
    System.setProperty("solr.data.dir", CloudStateUpdateTest.this.dataDir2.getAbsolutePath());
    container2 = init2.initialize();
    System.clearProperty("hostPort");
    
    System.setProperty("hostPort", "1663");
    CoreContainer.Initializer init3 = new CoreContainer.Initializer();
   
    System.setProperty("solr.data.dir", CloudStateUpdateTest.this.dataDir3.getAbsolutePath());
    container3 = init3.initialize();
    System.clearProperty("hostPort");
    
    log.info("####SETUP_END " + getName());
    
  }

  @Test
  public void testCoreRegistration() throws Exception {
    System.setProperty("CLOUD_UPDATE_DELAY", "1");
    
    ZkNodeProps props2 = new ZkNodeProps();
    props2.put("configName", "conf1");
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/collections/testcore", props2.store(), CreateMode.PERSISTENT);
    zkClient.makePath("/collections/testcore/shards", CreateMode.PERSISTENT);
    zkClient.close();
    
    CoreDescriptor dcore = new CoreDescriptor(container1, "testcore",
        "testcore");
    
    dcore.setDataDir(dataDir4.getAbsolutePath());

    SolrCore core = container1.create(dcore);
    
    container1.register(core, false);
    
    ZkController zkController2 = container2.getZkController();

    String host = zkController2.getHostName();
    
    // slight pause - TODO: takes an oddly long amount of time to schedule tasks
    // with almost no delay ...
    CloudState cloudState2 = null;
    Map<String,Slice> slices = null;
    for (int i = 75; i > 0; i--) {
      cloudState2 = zkController2.getCloudState();
      slices = cloudState2.getSlices("testcore");
      
      if (slices != null && slices.containsKey(host + ":1661_solr_testcore")) {
        break;
      }
      Thread.sleep(500);
    }

    assertNotNull(slices);
    assertTrue(slices.containsKey(host + ":1661_solr_testcore"));

    Slice slice = slices.get(host + ":1661_solr_testcore");
    assertEquals(host + ":1661_solr_testcore", slice.getName());

    Map<String,ZkNodeProps> shards = slice.getShards();

    assertEquals(1, shards.size());

    ZkNodeProps zkProps = shards.get(host + ":1661_solr_testcore");

    assertNotNull(zkProps);

    assertEquals(host + ":1661_solr", zkProps.get("node_name"));

    assertEquals("http://" + host + ":1661/solr/testcore", zkProps.get("url"));

    Set<String> liveNodes = cloudState2.getLiveNodes();
    assertNotNull(liveNodes);
    assertEquals(3, liveNodes.size());

    container3.shutdown();

    // slight pause (15s timeout) for watch to trigger
    for(int i = 0; i < (5 * 15); i++) {
      if(zkController2.getCloudState().getLiveNodes().size() == 2) {
        break;
      }
      Thread.sleep(200);
    }

    assertEquals(2, zkController2.getCloudState().getLiveNodes().size());

    // quickly kill / start client

    container2.getZkController().getZkClient().getSolrZooKeeper().getConnection()
        .disconnect();
    container2.shutdown();

    container2 = init2.initialize();
    
    // pause for watch to trigger
    for(int i = 0; i < 200; i++) {
      if (container1.getZkController().getCloudState().liveNodesContain(
          container2.getZkController().getNodeName())) {
        break;
      }
      Thread.sleep(100);
    }

    assertTrue(container1.getZkController().getCloudState().liveNodesContain(
        container2.getZkController().getNodeName()));
    core.close();
  }

  @Override
  public void tearDown() throws Exception {
    if (VERBOSE) {
      printLayout(zkServer.getZkHost());
    }
    container1.shutdown();
    container2.shutdown();
    container3.shutdown();
    zkServer.shutdown();
    super.tearDown();
    System.clearProperty("zkClientTimeout");
    System.clearProperty("zkHost");
    System.clearProperty("hostPort");
    System.clearProperty("CLOUD_UPDATE_DELAY");
    SolrConfig.severeErrors.clear();
  }

  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(
        zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
