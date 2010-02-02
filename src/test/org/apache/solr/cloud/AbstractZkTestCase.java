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

import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZkTestCase extends AbstractSolrTestCase {

  static final String ZOO_KEEPER_ADDRESS = "localhost:2323/solr";
  static final String ZOO_KEEPER_SERVER = "localhost:2323";
  static final int TIMEOUT = 10000;

  protected static Logger log = LoggerFactory
      .getLogger(AbstractZkTestCase.class);


  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  protected ZkTestServer zkServer;
  protected String zkDir;

  public AbstractZkTestCase() {

  }

  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void setUp() throws Exception {
    try {
      System.setProperty("zkHost", ZOO_KEEPER_ADDRESS);
      zkDir = tmpDir.getAbsolutePath() + File.separator
      + "zookeeper/server1/data";
      zkServer = new ZkTestServer(zkDir);
      zkServer.run();

      buildZooKeeper(getSolrConfigFile(), getSchemaFile());

      log.info("####SETUP_START " + getName());
      dataDir = tmpDir;
      dataDir.mkdirs();

      // set some system properties for use by tests
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      CoreContainer.Initializer init = new CoreContainer.Initializer() {
        {
          this.dataDir = AbstractZkTestCase.this.dataDir
              .getAbsolutePath();
        }
      };

      h = new TestHarness("", init);
      lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    log.info("####SETUP_END " + getName());

  }

  final static String JUST_HOST_NAME = AbstractZkTestCase.ZOO_KEEPER_ADDRESS.substring(0,
      AbstractZkTestCase.ZOO_KEEPER_ADDRESS.indexOf('/'));
  
  // static to share with distrib test
  static void buildZooKeeper(String config, String schema)
      throws Exception {
    SolrZkClient zkClient = new SolrZkClient(JUST_HOST_NAME, AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr");
    zkClient.close();

    zkClient = new SolrZkClient(ZOO_KEEPER_ADDRESS, AbstractZkTestCase.TIMEOUT);
    
    ZkNodeProps props1 = new ZkNodeProps();
    props1.put("configName", "conf1");
    zkClient.makePath("/collections/collection1", props1.store(), CreateMode.PERSISTENT);
    
    ZkNodeProps props2 = new ZkNodeProps();
    props2.put("configName", "conf1");
    zkClient.makePath("/collections/testcore", props2.store(), CreateMode.PERSISTENT);
    
    putConfig(zkClient, config);
    putConfig(zkClient, schema);
    putConfig(zkClient, "stopwords.txt");
    putConfig(zkClient, "protwords.txt");
    putConfig(zkClient, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, "old_synonyms.txt");
    
    zkClient.close();
  }

  private static void putConfig(SolrZkClient zkConnection, String name) throws Exception {
    zkConnection.setData("/configs/conf1/" + name, new File("solr"
        + File.separator + "conf" + File.separator + name));
  }

  public void tearDown() throws Exception {
    printLayout();
    super.tearDown();
    zkServer.shutdown();
  }

  private void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(
        AbstractZkTestCase.ZOO_KEEPER_SERVER,
        AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
  
  static void makeSolrZkNode() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(ZOO_KEEPER_SERVER, TIMEOUT);
    zkClient.makePath("/solr");
    zkClient.close();
  }
}
