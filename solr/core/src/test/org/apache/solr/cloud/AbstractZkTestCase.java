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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrConfig;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
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

  protected static ZkTestServer zkServer;

  protected static String zkDir;


  @BeforeClass
  public static void azt_beforeClass() throws Exception {
    createTempDir();
    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("hostPort", "0000");
    
    buildZooKeeper(zkServer.getZkHost(), zkServer.getZkAddress(),
        "solrconfig.xml", "schema.xml");
    
    initCore("solrconfig.xml", "schema.xml");
  }

  // static to share with distrib test
  static void buildZooKeeper(String zkHost, String zkAddress, String config,
      String schema) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkAddress, AbstractZkTestCase.TIMEOUT);

    Map<String,String> props = new HashMap<String,String>();
    props.put("configName", "conf1");
    final ZkNodeProps zkProps = new ZkNodeProps(props);
    
    zkClient.makePath("/collections/collection1", ZkStateReader.toJSON(zkProps), CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/collection1/shards", CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/control_collection", ZkStateReader.toJSON(zkProps), CreateMode.PERSISTENT, true);
    zkClient.makePath("/collections/control_collection/shards", CreateMode.PERSISTENT, true);

    putConfig(zkClient, config);
    putConfig(zkClient, schema);
    putConfig(zkClient, "solrconfig.xml");
    putConfig(zkClient, "stopwords.txt");
    putConfig(zkClient, "protwords.txt");
    putConfig(zkClient, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, "old_synonyms.txt");
    putConfig(zkClient, "synonyms.txt");
    
    zkClient.close();
  }

  private static void putConfig(SolrZkClient zkClient, final String name)
      throws Exception {
    zkClient.makePath("/configs/conf1/" + name, getFile("solr"
        + File.separator + "conf" + File.separator + name), false, true);  
  }

  @Override
  public void tearDown() throws Exception {
    if (DEBUG) {
      printLayout(zkServer.getZkHost());
    }

    super.tearDown();
  }
  
  @AfterClass
  public static void azt_afterClass() throws Exception {
    System.clearProperty("zkHost");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    System.clearProperty("solrcloud.skip.autorecovery");
    zkServer.shutdown();

    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
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
      List<String> children = zkClient.getChildren(path, null, true);
      for (String string : children) {
        tryCleanPath(zkHost, path+"/"+string);
      }
      zkClient.delete(path, -1, true);
    }
    zkClient.close();
  }
}
