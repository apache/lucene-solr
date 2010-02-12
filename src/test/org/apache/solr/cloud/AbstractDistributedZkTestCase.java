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
import java.util.HashSet;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TestHarness;

public abstract class AbstractDistributedZkTestCase extends BaseDistributedSearchTestCase {
  private ZkTestServer zkServer;

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  boolean createEmbeddedCore;

  @Override
  public void setUp() throws Exception {
    // we don't call super.setUp
    log.info("####SETUP_START " + getName());
    portSeed = 13000;

    // TODO: HACK: inserting dead servers doesn't currently work with these tests
    deadServers = null;
    
    System.setProperty("zkHost", AbstractZkTestCase.ZOO_KEEPER_ADDRESS);
    String zkDir = tmpDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    
    AbstractZkTestCase.buildZooKeeper(getSolrConfigFile(), getSchemaFile());

    dataDir = tmpDir;
    dataDir.mkdirs();

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");


    if (createEmbeddedCore) {
    CoreContainer.Initializer init = new CoreContainer.Initializer() {
      {
        this.dataDir = AbstractDistributedZkTestCase.this.dataDir.getAbsolutePath();
        this.solrConfigFilename = AbstractDistributedZkTestCase.this.getSolrConfigFilename();
      }
    };
    h = new TestHarness("", init);
    lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");
    }
  
    log.info("####SETUP_END " + getName());

    testDir = new File(System.getProperty("java.io.tmpdir")
            + System.getProperty("file.separator")
            + getClass().getName() + "-" + System.currentTimeMillis());
    testDir.mkdirs();
    postSetUp();
  }
  
  protected String getSolrConfigFilename() {
    return null;
  }

  public void testDistribSearch() throws Exception {
    for (int nServers = 2; nServers < 3; nServers++) {
      createServers(nServers);
     
      RandVal.uniqueValues = new HashSet(); //reset random values
      doTest();
      printLayout();

      destroyServers();
    }
  }

  public void tearDown() throws Exception {
    printLayout();
    super.tearDown();
  }
  
  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(AbstractZkTestCase.JUST_HOST_NAME, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
