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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slow
public class ClusterStateUpdateTest extends SolrTestCaseJ4  {
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
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("genericCoreNodeNames", "false");
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
    System.clearProperty("genericCoreNodeNames");
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
    
    log.info("####SETUP_START " + getTestName());
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
    
    System.setProperty("solr.solr.home", TEST_HOME());
    System.setProperty("hostPort", "1661");
    System.setProperty("solr.data.dir", ClusterStateUpdateTest.this.dataDir1.getAbsolutePath());
    container1 = new CoreContainer();
    container1.load();
    System.clearProperty("hostPort");
    
    System.setProperty("hostPort", "1662");
    System.setProperty("solr.data.dir", ClusterStateUpdateTest.this.dataDir2.getAbsolutePath());
    container2 = new CoreContainer();
    container2.load();
    System.clearProperty("hostPort");
    
    System.setProperty("hostPort", "1663");
    System.setProperty("solr.data.dir", ClusterStateUpdateTest.this.dataDir3.getAbsolutePath());
    container3 = new CoreContainer();
    container3.load();
    System.clearProperty("hostPort");
    System.clearProperty("solr.solr.home");
    
    log.info("####SETUP_END " + getTestName());
    
  }

  
  @Test
  public void testCoreRegistration() throws Exception {
    System.setProperty("solrcloud.update.delay", "1");
    
   
    Map<String,Object> props2 = new HashMap<String,Object>();
    props2.put("configName", "conf1");
    ZkNodeProps zkProps2 = new ZkNodeProps(props2);
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);
    zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/testcore",
        ZkStateReader.toJSON(zkProps2), CreateMode.PERSISTENT, true);
    zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/testcore/shards",
        CreateMode.PERSISTENT, true);
    zkClient.close();

    CoreDescriptor dcore = buildCoreDescriptor(container1, "testcore", "testcore")
                              .withDataDir(dataDir4.getAbsolutePath()).build();

    if (container1.getZkController() != null) {
      container1.preRegisterInZk(dcore);
    }
    
    SolrCore core = container1.create(dcore);
    
    container1.register(core, false);
    
    ZkController zkController2 = container2.getZkController();

    String host = zkController2.getHostName();
    
    // slight pause - TODO: takes an oddly long amount of time to schedule tasks
    // with almost no delay ...
    ClusterState clusterState2 = null;
    Map<String,Slice> slices = null;
    for (int i = 75; i > 0; i--) {
      clusterState2 = zkController2.getClusterState();
      slices = clusterState2.getSlicesMap("testcore");
      
      if (slices != null && slices.containsKey("shard1")
          && slices.get("shard1").getReplicasMap().size() > 0) {
        break;
      }
      Thread.sleep(500);
    }

    assertNotNull(slices);
    assertTrue(slices.containsKey("shard1"));

    Slice slice = slices.get("shard1");
    assertEquals("shard1", slice.getName());

    Map<String,Replica> shards = slice.getReplicasMap();

    assertEquals(1, shards.size());

    Replica zkProps = shards.get(host + ":1661_solr_testcore");

    assertNotNull(zkProps);

    assertEquals(host + ":1661_solr", zkProps.getStr(ZkStateReader.NODE_NAME_PROP));

    assertEquals("http://" + host + ":1661/solr", zkProps.getStr(ZkStateReader.BASE_URL_PROP));

    Set<String> liveNodes = clusterState2.getLiveNodes();
    assertNotNull(liveNodes);
    assertEquals(3, liveNodes.size());

    container3.shutdown();

    // slight pause (15s timeout) for watch to trigger
    for(int i = 0; i < (5 * 15); i++) {
      if(zkController2.getClusterState().getLiveNodes().size() == 2) {
        break;
      }
      Thread.sleep(200);
    }

    assertEquals(2, zkController2.getClusterState().getLiveNodes().size());

    // quickly kill / start client

    container2.getZkController().getZkClient().getSolrZooKeeper().getConnection()
        .disconnect();
    container2.shutdown();

    System.setProperty("hostPort", "1662");
    System.setProperty("solr.data.dir", ClusterStateUpdateTest.this.dataDir2.getAbsolutePath());
    container2 = new CoreContainer();
    container2.load();
    System.clearProperty("hostPort");
    
    // pause for watch to trigger
    for(int i = 0; i < 200; i++) {
      if (container1.getZkController().getClusterState().liveNodesContain(
          container2.getZkController().getNodeName())) {
        break;
      }
      Thread.sleep(100);
    }

    assertTrue(container1.getZkController().getClusterState().liveNodesContain(
        container2.getZkController().getNodeName()));

    // core.close();  // don't close - this core is managed by container1 now
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
    System.clearProperty("solrcloud.update.delay");
  }
  
  static void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(
        zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
