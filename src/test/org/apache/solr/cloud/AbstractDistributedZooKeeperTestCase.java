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
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TestHarness;

public abstract class AbstractDistributedZooKeeperTestCase extends BaseDistributedSearchTestCase {
  private ZooKeeperTestServer zkServer;

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());
  
  @Override
  public void setUp() throws Exception {
    // we don't call super.setUp
    log.info("####SETUP_START " + getName());
    portSeed = 13000;
    
    System.setProperty("zkHost", AbstractZooKeeperTestCase.ZOO_KEEPER_HOST);
    String zkDir = tmpDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    zkServer = new ZooKeeperTestServer(zkDir);
    zkServer.run();
    
    
    AbstractZooKeeperTestCase.buildZooKeeper(getSolrConfigFile(), getSchemaFile());

    dataDir = tmpDir;
    dataDir.mkdirs();

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    CoreContainer.Initializer init = new CoreContainer.Initializer() {
      {
        this.dataDir = AbstractDistributedZooKeeperTestCase.this.dataDir.getAbsolutePath();
        this.solrConfigFilename = AbstractDistributedZooKeeperTestCase.this.getSolrConfigFilename();
      }
    };

    h = new TestHarness("", init);
    lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");

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
      printeLayout();

      destroyServers();
    }
  }

  public void tearDown() throws Exception {
    printeLayout();
    super.tearDown();
  }
  
  private void printeLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(AbstractZooKeeperTestCase.JUST_HOST_NAME, AbstractZooKeeperTestCase.TIMEOUT);
    zkClient.connect();
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
