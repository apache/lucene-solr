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
package org.apache.solr.common.cloud;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestZkMaintenanceUtils extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected ZkTestServer zkServer;
  private SolrZkClient defaultClient;
  private CloudSolrClient solrClient;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(1)
        .addConfig("_default", new File(ExternalPaths.DEFAULT_CONFIGSET).toPath())
        .configure();
    solrClient = getCloudSolrClient(cluster.getZkServer().getZkAddress());

    Path zkDir = createTempDir();
    log.info("ZooKeeper dataDir:{}", zkDir);
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    try (SolrZkClient client = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT)) {
      // Set up chroot
      client.makePath("/solr", false, true);
    }

    defaultClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);

  }

  @Override
  public void tearDown() throws Exception {
    defaultClient.close();
    zkServer.shutdown();
    solrClient.close();
    cluster.shutdown();
    super.tearDown();
  }

  /**
   * This test reproduces the issue of trying to delete zk-nodes that have the same length. (SOLR-14961). 
   * 
   * @throws InterruptedException when having troublke creating test nodes
   * @throws KeeperException error when talking to zookeeper
   * @throws SolrServerException when having trouble connecting to solr 
   * @throws UnsupportedEncodingException when getBytes() uses unknown encoding
   * 
   */
  @Test
  public void testClean() throws KeeperException, InterruptedException, SolrServerException, UnsupportedEncodingException {
    /* PREPARE */
    String path = "/myPath/isTheBest";
    String data1 = "myStringData1";
    String data2 = "myStringData2";
    String longData = "myLongStringData";
    // create zk nodes that have the same path length
    defaultClient.create("/myPath", null, CreateMode.PERSISTENT, true);
    defaultClient.create(path, null, CreateMode.PERSISTENT, true);
    defaultClient.create(path +"/file1.txt", data1.getBytes("UTF-8"), CreateMode.PERSISTENT, true);
    defaultClient.create(path +"/nothing.txt", null, CreateMode.PERSISTENT, true);
    defaultClient.create(path +"/file2.txt", data2.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    defaultClient.create(path +"/some_longer_file2.txt", longData.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
    
    String listZnode = defaultClient.listZnode(path, false);
    log.info(listZnode);
    
    /* RUN */
    // delete all nodes that contain "file"
    ZkMaintenanceUtils.clean(defaultClient,path, node -> node.contains("file"));
    
    /* CHECK */
    listZnode = defaultClient.listZnode(path, false);
    log.info(listZnode);
    // list of node must not contain file1, file2 or some_longer_file2 because they where deleted
    assertFalse(listZnode.contains("file1"));
    assertFalse(listZnode.contains("file2"));
    assertFalse(listZnode.contains("some_longer_file2"));
    assertTrue(listZnode.contains("nothing"));
  }
}
