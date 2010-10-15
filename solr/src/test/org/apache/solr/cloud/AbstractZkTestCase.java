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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.util.TestHarness;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZkTestCase extends SolrTestCaseJ4 {

  static final int TIMEOUT = 10000;

  private static final boolean DEBUG = false;

  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);

  protected ZkTestServer zkServer;

  protected String zkDir;

  public AbstractZkTestCase() {

  }

  @BeforeClass
  public static void beforeClass() throws Exception {
  }
  
  @Override
  public void setUp() throws Exception {

    super.setUp();
    
    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    buildZooKeeper(zkServer.getZkHost(), zkServer.getZkAddress(),
        getSolrConfigFile(), getSchemaFile());
    
    log.info("####SETUP_START " + getName());

    dataDir.mkdirs();
    
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    CoreContainer.Initializer init = new CoreContainer.Initializer() {
      {
        this.dataDir = AbstractZkTestCase.dataDir.getAbsolutePath();
      }
    };
    
    h = new TestHarness("", init);
    lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");
    
    log.info("####SETUP_END " + getName());
    
  }

  // static to share with distrib test
  static void buildZooKeeper(String zkHost, String zkAddress, String config,
      String schema) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr");
    zkClient.close();

    zkClient = new SolrZkClient(zkAddress, AbstractZkTestCase.TIMEOUT);

    ZkNodeProps props = new ZkNodeProps();
    props.put("configName", "conf1");
    zkClient.makePath("/collections/collection1", props.store(), CreateMode.PERSISTENT);
    zkClient.makePath("/collections/collection1/shards", CreateMode.PERSISTENT);

    zkClient.makePath("/collections/control_collection", props.store(), CreateMode.PERSISTENT);
    zkClient.makePath("/collections/control_collection/shards", CreateMode.PERSISTENT);

    putConfig(zkClient, config);
    putConfig(zkClient, schema);
    putConfig(zkClient, "stopwords.txt");
    putConfig(zkClient, "protwords.txt");
    putConfig(zkClient, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, "old_synonyms.txt");
    putConfig(zkClient, "synonyms.txt");
    
    zkClient.close();
  }

  private static void putConfig(SolrZkClient zkConnection, String name)
      throws Exception {
    zkConnection.setData("/configs/conf1/" + name, new File("solr"
        + File.separator + "conf" + File.separator + name));
  }

  public void tearDown() throws Exception {
    if (DEBUG) {
      printLayout(zkServer.getZkHost());
    }
    zkServer.shutdown();
    System.clearProperty("zkHost");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }

  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }

  static void makeSolrZkNode(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, TIMEOUT);
    zkClient.makePath("/solr");
    zkClient.close();
  }
}
