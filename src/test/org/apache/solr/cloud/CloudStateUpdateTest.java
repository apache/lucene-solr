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

import junit.framework.TestCase;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.CoreContainer.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CloudStateUpdateTest extends TestCase {
  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  protected String getSolrConfigFilename() {
    return "solr.lowZkTimeout.xml";
  }

  private static final boolean VERBOSE = true;

  protected ZkTestServer zkServer;

  protected String zkDir;

  private CoreContainer container1;

  private CoreContainer container2;

  private CoreContainer container3;

  private File dataDir1;

  private File dataDir2;

  private File dataDir3;

  private Initializer init2;

  public void setUp() throws Exception {
    try {
      System.setProperty("zkClientTimeout", "3000");
      System.setProperty("zkHost", AbstractZkTestCase.ZOO_KEEPER_ADDRESS);
      zkDir = tmpDir.getAbsolutePath() + File.separator
          + "zookeeper/server1/data";
      zkServer = new ZkTestServer(zkDir);
      zkServer.run();

      AbstractZkTestCase.buildZooKeeper("solrconfig.xml", "schema.xml");

      log.info("####SETUP_START " + getName());
      dataDir1 = new File(tmpDir + File.separator + "data1");
      dataDir1.mkdirs();

      dataDir2 = new File(tmpDir + File.separator + "data2");
      dataDir2.mkdirs();

      dataDir3 = new File(tmpDir + File.separator + "data3");
      dataDir3.mkdirs();

      // set some system properties for use by tests
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      CoreContainer.Initializer init1 = new CoreContainer.Initializer() {
        {
          this.dataDir = CloudStateUpdateTest.this.dataDir1.getAbsolutePath();
          this.zkPortOverride = "8983";
        }
      };

      container1 = init1.initialize();

      init2 = new CoreContainer.Initializer() {
        {
          this.dataDir = CloudStateUpdateTest.this.dataDir2.getAbsolutePath();
          this.zkPortOverride = "8984";
        }
      };

      container2 = init2.initialize();

      CoreContainer.Initializer init3 = new CoreContainer.Initializer() {
        {
          this.dataDir = CloudStateUpdateTest.this.dataDir3.getAbsolutePath();
          this.zkPortOverride = "8985";
        }
      };

      container3 = init3.initialize();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    log.info("####SETUP_END " + getName());

  }

  public void testCoreRegistration() throws Exception {
    System.setProperty("CLOUD_UPDATE_DELAY", "1");
    CoreDescriptor dcore = new CoreDescriptor(container1, "testcore",
        "testcore");

    SolrCore core = container1.create(dcore);
    container1.register(core, false);

    // slight pause - TODO: takes an oddly long amount of time to schedule tasks
    // with almost no delay ...
    Thread.sleep(5000);

    ZkController zkController2 = container2.getZkController();

    String host = zkController2.getHostName();

    CloudState cloudState2 = zkController2.getCloudState();
    Map<String,Slice> slices = cloudState2.getSlices("testcore");

    assertNotNull(slices);
    assertTrue(slices.containsKey(host + ":8983_solr_testcore"));

    Slice slice = slices.get(host + ":8983_solr_testcore");
    assertEquals(host + ":8983_solr_testcore", slice.getName());

    Map<String,ZkNodeProps> shards = slice.getShards();

    assertEquals(1, shards.size());

    ZkNodeProps zkProps = shards.get(host + ":8983_solr_testcore");

    assertNotNull(zkProps);

    assertEquals(host + ":8983_solr", zkProps.get("node_name"));

    assertEquals("http://" + host + ":8983/solr/testcore", zkProps.get("url"));

    Set<String> liveNodes = cloudState2.getLiveNodes();
    assertNotNull(liveNodes);
    assertEquals(3, liveNodes.size());

    container3.shutdown();

    liveNodes = zkController2.getCloudState().getLiveNodes();

    // slight pause for watch to trigger
    Thread.sleep(500);

    assertEquals(2, liveNodes.size());

    // quickly kill / start client

    container2.getZkController().getZkClient().keeper.getConnection()
        .disconnect();
    container2.shutdown();

    container2 = init2.initialize();

    Thread.sleep(8000);

    assertTrue(container1.getZkController().getCloudState().liveNodesContain(
        container2.getZkController().getNodeName()));

  }

  public void tearDown() throws Exception {
    if (VERBOSE) {
      printLayout();
    }
    super.tearDown();
    zkServer.shutdown();
  }

  private void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(
        AbstractZkTestCase.ZOO_KEEPER_SERVER, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
