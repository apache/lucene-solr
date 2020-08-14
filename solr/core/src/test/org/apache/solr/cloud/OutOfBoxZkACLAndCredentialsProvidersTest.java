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

import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutOfBoxZkACLAndCredentialsProvidersTest extends SolrTestCaseJ4 {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Charset DATA_ENCODING = Charset.forName("UTF-8");
  
  protected ZkTestServer zkServer;

  protected Path zkDir;
  
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
    log.info("####SETUP_START " + getTestName());
    createTempDir();

    zkDir = createTempDir().resolve("zookeeper/server1/data");
    log.info("ZooKeeper dataDir:" + zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    System.setProperty("zkHost", zkServer.getZkAddress());
    
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.makePath("/solr", false, true);
    zkClient.close();

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    zkClient.create("/protectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/protectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.create("/unprotectedCreateNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.makePath("/unprotectedMakePathNode", "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.create(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, "content".getBytes(DATA_ENCODING), CreateMode.PERSISTENT, false);
    zkClient.close();

    log.info("####SETUP_END " + getTestName());
  }

  @Override
  public void tearDown() throws Exception {
    zkServer.shutdown();
    
    super.tearDown();
  }

  @Test
  public void testOutOfBoxSolrZkClient() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      VMParamsZkACLAndCredentialsProvidersTest.doTest(zkClient,
          true, true, true, true, true,
          true, true, true, true, true);
    } finally {
      zkClient.close();
    }
  }
  
  @Test
  public void testOpenACLUnsafeAllover() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    try {
      List<String> verifiedList = new ArrayList<String>();
      assertOpenACLUnsafeAllover(zkClient, "/", verifiedList);
      assertTrue(verifiedList.contains("/solr"));
      assertTrue(verifiedList.contains("/solr/unprotectedCreateNode"));
      assertTrue(verifiedList.contains("/solr/unprotectedMakePathNode"));
      assertTrue(verifiedList.contains("/solr/protectedMakePathNode"));
      assertTrue(verifiedList.contains("/solr/protectedCreateNode"));
      assertTrue(verifiedList.contains("/solr" + SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH));
    } finally {
      zkClient.close();
    }
  }

  
  protected void assertOpenACLUnsafeAllover(SolrZkClient zkClient, String path, List<String> verifiedList) throws Exception {
    List<ACL> acls = zkClient.getSolrZooKeeper().getACL(path, new Stat());
    if (log.isInfoEnabled()) {
      log.info("Verifying " + path);
    }
    if (ZooDefs.CONFIG_NODE.equals(path)) {
      // Treat this node specially, from the ZK docs:
      // The dynamic configuration is stored in a special znode ZooDefs.CONFIG_NODE = /zookeeper/config.
      // This node by default is read only for all users, except super user and
      // users that's explicitly configured for write access.
      assertEquals("Path " + path + " does not have READ_ACL_UNSAFE", ZooDefs.Ids.READ_ACL_UNSAFE, acls);
    } else {
      assertEquals("Path " + path + " does not have OPEN_ACL_UNSAFE", ZooDefs.Ids.OPEN_ACL_UNSAFE, acls);
    }
    verifiedList.add(path);
    List<String> children = zkClient.getChildren(path, null, false);
    for (String child : children) {
      assertOpenACLUnsafeAllover(zkClient, path + ((path.endsWith("/")) ? "" : "/") + child, verifiedList);
    }
  }
  
}
