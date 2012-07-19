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

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMultiCoreConfBootstrap extends SolrTestCaseJ4 {
  protected static Logger log = LoggerFactory.getLogger(TestMultiCoreConfBootstrap.class);
  protected CoreContainer cores = null;
  private String home;


  protected static ZkTestServer zkServer;
  protected static String zkDir;
  
  @BeforeClass
  public static void beforeClass() {
    createTempDir();
  }
  
  @AfterClass
  public static void afterClass() {

  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    home = ExternalPaths.EXAMPLE_MULTICORE_HOME;
    System.setProperty("solr.solr.home", home);
    
    zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();
    
    System.setProperty("zkHost", zkServer.getZkAddress());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("bootstrap_conf");
    System.clearProperty("zkHost");
    System.clearProperty("solr.solr.home");
    
    if (cores != null)
      cores.shutdown();
    
    zkServer.shutdown();
    
    File dataDir1 = new File(home + File.separator + "core0","data");
    File dataDir2 = new File(home + File.separator + "core1","data");

    String skip = System.getProperty("solr.test.leavedatadir");
    if (null != skip && 0 != skip.trim().length()) {
      log.info("NOTE: per solr.test.leavedatadir, dataDir will not be removed: " + dataDir.getAbsolutePath());
    } else {
      if (!AbstractSolrTestCase.recurseDelete(dataDir1)) {
        log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
      }
      if (!AbstractSolrTestCase.recurseDelete(dataDir2)) {
        log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
      }
    }

    super.tearDown();
  }


  @Test
  public void testMultiCoreConfBootstrap() throws Exception {
    System.setProperty("bootstrap_conf", "true");
    cores = new CoreContainer(home, new File(home, "solr.xml"));
    SolrZkClient zkclient = cores.getZkController().getZkClient();
    // zkclient.printLayoutToStdOut();
    
    assertTrue(zkclient.exists("/configs/core1/solrconfig.xml", true));
    assertTrue(zkclient.exists("/configs/core1/schema.xml", true));
    assertTrue(zkclient.exists("/configs/core0/solrconfig.xml", true));
    assertTrue(zkclient.exists("/configs/core1/schema.xml", true));
  }

}
