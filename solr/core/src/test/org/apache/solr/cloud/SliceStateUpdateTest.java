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
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slow
public class SliceStateUpdateTest extends SolrTestCaseJ4 {
  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);

  private static final boolean VERBOSE = false;

  protected ZkTestServer zkServer;

  protected String zkDir;

  private CoreContainer container1;

  private CoreContainer container2;

  private File dataDir1;

  private File dataDir2;

  private File dataDir3;

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
    Map<String, Object> props2 = new HashMap<String, Object>();
    props2.put("configName", "conf1");

    ZkNodeProps zkProps2 = new ZkNodeProps(props2);

    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);
    zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/testcore",
        ZkStateReader.toJSON(zkProps2), CreateMode.PERSISTENT, true);
    zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/testcore/shards",
        CreateMode.PERSISTENT, true);
    zkClient.close();

    dataDir1 = new File(dataDir + File.separator + "data1");
    dataDir1.mkdirs();

    dataDir2 = new File(dataDir + File.separator + "data2");
    dataDir2.mkdirs();

    dataDir3 = new File(dataDir + File.separator + "data3");
    dataDir3.mkdirs();

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    System.setProperty("solr.solr.home", TEST_HOME());
    System.setProperty("hostPort", "1661");
    System.setProperty("solr.data.dir", SliceStateUpdateTest.this.dataDir1.getAbsolutePath());
    container1 = new CoreContainer();

    System.clearProperty("hostPort");

    System.setProperty("hostPort", "1662");
    System.setProperty("solr.data.dir", SliceStateUpdateTest.this.dataDir2.getAbsolutePath());
    container2 = new CoreContainer();
    System.clearProperty("hostPort");

    System.clearProperty("solr.solr.home");

    container1.load();
    container2.load();

    log.info("####SETUP_END " + getTestName());

  }


  @Test
  public void testSliceStateUpdate() throws Exception {
    System.setProperty("solrcloud.update.delay", "1");
    
    /* Get ClusterState, update slice state and publish it to Zookeeper */

    ClusterState clusterState = container1.getZkController().getClusterState();
    Map<String, DocCollection> collectionStates =
        new LinkedHashMap<String, DocCollection>(clusterState.getCollectionStates());

    Map<String, Slice> slicesMap = clusterState.getSlicesMap("collection1");
    Map<String, Object> props = new HashMap<String, Object>(1);
    Slice slice = slicesMap.get("shard1");
    Map<String, Object> prop = slice.getProperties();
    prop.put("state", "inactive");
    Slice newSlice = new Slice(slice.getName(), slice.getReplicasMap(), prop);
    slicesMap.put(newSlice.getName(), newSlice);
    props.put(DocCollection.DOC_ROUTER, ImplicitDocRouter.NAME);

    DocCollection coll = new DocCollection("collection1", slicesMap, props, DocRouter.DEFAULT);
    collectionStates.put("collection1", coll);
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT);

    ClusterState newState = new ClusterState(clusterState.getLiveNodes(), collectionStates);
    zkClient.setData(ZkStateReader.CLUSTER_STATE,
        ZkStateReader.toJSON(newState), true);
    zkClient.close();
    
    /* Read state from another container and confirm the change */
    ZkController zkController2 = container2.getZkController();
    ClusterState clusterState2 = null;
    Map<String, Slice> slices = null;
    for (int i = 75; i > 0; i--) {
      clusterState2 = zkController2.getClusterState();
      slices = clusterState2.getSlicesMap("collection1");
      if (slices != null && slices.containsKey("shard1")
          && slices.get("shard1").getState().equals("inactive")) {
        break;
      }
      Thread.sleep(500);
    }

    assertNotNull(slices);

    assertEquals("shard1", slices.get("shard1").getName());
    assertEquals("inactive", slices.get("shard1").getState());
  }

  @Override
  public void tearDown() throws Exception {
    if (VERBOSE) {
      ClusterStateUpdateTest.printLayout(zkServer.getZkHost());
    }
    container1.shutdown();
    container2.shutdown();

    zkServer.shutdown();
    super.tearDown();
    System.clearProperty("zkClientTimeout");
    System.clearProperty("zkHost");
    System.clearProperty("hostPort");
    System.clearProperty("solrcloud.update.delay");
  }
}
