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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZkTestCase extends SolrTestCaseJ4 {
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";
  
  public static final int TIMEOUT = 45000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  
  public static File SOLRHOME;
  static {
    try {
      SOLRHOME = new File(TEST_HOME());
    } catch (RuntimeException e) {
      log.warn("TEST_HOME() does not exist - solrj test?");
      // solrj tests not working with TEST_HOME()
      // must override getSolrHome
    }
  }
  
  protected static ZkTestServer zkServer;

  protected static String zkDir;


  @BeforeClass
  public static void azt_beforeClass() throws Exception {
    zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("jetty.port", "0000");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");
    
    buildZooKeeper(zkServer.getZkHost(), zkServer.getZkAddress(), SOLRHOME,
        "solrconfig.xml", "schema.xml");

    initCore("solrconfig.xml", "schema.xml");
  }

  static void buildZooKeeper(String zkHost, String zkAddress, String config,
      String schema) throws Exception {
    buildZooKeeper(zkHost, zkAddress, SOLRHOME, config, schema);
  }
  
  // static to share with distrib test
  public static void buildZooKeeper(String zkHost, String zkAddress, File solrhome, String config,
      String schema) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkAddress, AbstractZkTestCase.TIMEOUT);

    Map<String,Object> props = new HashMap<>();
    props.put("configName", "conf1");
    final ZkNodeProps zkProps = new ZkNodeProps(props);
    
    zkClient.makePath("/collections/collection1", Utils.toJSON(zkProps), CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/collection1/shards", CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/control_collection", Utils.toJSON(zkProps), CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/control_collection/shards", CreateMode.PERSISTENT, true);
    // this workaround is acceptable until we remove legacyCloud because we just init a single core here
    String defaultClusterProps = "{\""+ZkStateReader.LEGACY_CLOUD+"\":\"true\"}";
    zkClient.makePath(ZkStateReader.CLUSTER_PROPS, defaultClusterProps.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    // for now, always upload the config and schema to the canonical names
    putConfig("conf1", zkClient, solrhome, config, "solrconfig.xml");
    putConfig("conf1", zkClient, solrhome, schema, "schema.xml");

    putConfig("conf1", zkClient, solrhome, "solrconfig.snippet.randomindexconfig.xml");
    putConfig("conf1", zkClient, solrhome, "stopwords.txt");
    putConfig("conf1", zkClient, solrhome, "protwords.txt");
    putConfig("conf1", zkClient, solrhome, "currency.xml");
    putConfig("conf1", zkClient, solrhome, "enumsConfig.xml");
    putConfig("conf1", zkClient, solrhome, "open-exchange-rates.json");
    putConfig("conf1", zkClient, solrhome, "mapping-ISOLatin1Accent.txt");
    putConfig("conf1", zkClient, solrhome, "old_synonyms.txt");
    putConfig("conf1", zkClient, solrhome, "synonyms.txt");
    zkClient.close();
  }

  public static void putConfig(String confName, SolrZkClient zkClient, File solrhome, final String name)
      throws Exception {
    putConfig(confName, zkClient, solrhome, name, name);
  }

  public static void putConfig(String confName, SolrZkClient zkClient, File solrhome, final String srcName, String destName)
      throws Exception {
    File file = new File(solrhome, "collection1"
        + File.separator + "conf" + File.separator + srcName);
    if (!file.exists()) {
      log.info("skipping " + file.getAbsolutePath() + " because it doesn't exist");
      return;
    }

    String destPath = "/configs/" + confName + "/" + destName;
    log.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @AfterClass
  public static void azt_afterClass() throws Exception {
    deleteCore();

    System.clearProperty("zkHost");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    System.clearProperty("solrcloud.skip.autorecovery");
    System.clearProperty("jetty.port");
    System.clearProperty(ZOOKEEPER_FORCE_SYNC);

    if (zkServer != null) {
      zkServer.shutdown();
      zkServer = null;
    }
    zkDir = null;
  }

  protected void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }

  public static void makeSolrZkNode(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();
  }
  
  public static void tryCleanSolrZkNode(String zkHost) throws Exception {
    tryCleanPath(zkHost, "/solr");
  }
  
  static void tryCleanPath(String zkHost, String path) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, TIMEOUT);
    if (zkClient.exists(path, true)) {
      zkClient.clean(path);
    }
    zkClient.close();
  }
}
