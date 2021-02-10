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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZkTestCase extends SolrTestCaseJ4 {
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";
  
  public static final int TIMEOUT = 15000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public File SOLRHOME;
  {
    try {
      SOLRHOME = new File(SolrTestUtil.TEST_HOME());
    } catch (RuntimeException e) {
      log.warn("TEST_HOME() does not exist - solrj test?");
      // solrj tests not working with TEST_HOME()
      // must override getSolrHome
    }
  }

  protected volatile static ZkTestServer zkServer;

  protected volatile static Path zkDir;


  @Before
  public void azt_before() throws Exception {
    zkDir = SolrTestUtil.createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    try {
      zkServer.run();
    } catch (Exception e) {
      log.error("Error starting Zk Test Server", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    
    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("jetty.port", "0000");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");
    
    zkServer.buildZooKeeper();

    initCore("solrconfig.xml", "schema.xml");
  }



  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    deleteCore();

    System.clearProperty("zkHost");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    System.clearProperty("solrcloud.skip.autorecovery");
    System.clearProperty("jetty.port");
    System.clearProperty(ZOOKEEPER_FORCE_SYNC);
    try {
      if (zkServer != null) {
        zkServer.shutdown();
      }
    } finally {
      zkDir = null;
      zkServer = null;
    }
  }

  protected void printLayout() throws Exception {
    zkServer.printLayout();
  }
}
