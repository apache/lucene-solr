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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZkMaintenanceUtils extends SolrTestCaseJ4 {

  protected static ZkTestServer zkServer;
  private static Path zkDir;

  @BeforeClass
  public static void setUpClass() throws Exception {
    zkDir = createTempDir("TestZkMaintenanceUtils");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
  }

  @AfterClass
  public static void tearDownClass() throws IOException, InterruptedException {

    if (zkServer != null) {
      zkServer.shutdown();
      zkServer = null;
    }
    if (null != zkDir) {
      FileUtils.deleteDirectory(zkDir.toFile());
      zkDir = null;
    }
  }

  /**
   * This test reproduces the issue of trying to delete zk-nodes that have the same length. (SOLR-14961).
   *
   * @throws InterruptedException when having trouble creating test nodes
   * @throws KeeperException error when talking to zookeeper
   * @throws SolrServerException when having trouble connecting to solr
   * @throws UnsupportedEncodingException when getBytes() uses unknown encoding
   *
   */
  @Test
  public void testClean() throws KeeperException, InterruptedException, SolrServerException, UnsupportedEncodingException {
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)){
      /* PREPARE */
      String path = "/myPath/isTheBest";
      String data1 = "myStringData1";
      String data2 = "myStringData2";
      String longData = "myLongStringData";
      // create zk nodes that have the same path length
      zkClient.create("/myPath", null, CreateMode.PERSISTENT, true);
      zkClient.create(path, null, CreateMode.PERSISTENT, true);
      zkClient.create(path +"/file1.txt", data1.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
      zkClient.create(path +"/nothing.txt", null, CreateMode.PERSISTENT, true);
      zkClient.create(path +"/file2.txt", data2.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);
      zkClient.create(path +"/some_longer_file2.txt", longData.getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT, true);

      /* RUN */
      // delete all nodes that contain "file"
      ZkMaintenanceUtils.clean(zkClient,path, node -> node.contains("file"));

      /* CHECK */
      String listZnode = zkClient.listZnode(path, false);
      // list of node must not contain file1, file2 or some_longer_file2 because they where deleted
      assertFalse(listZnode.contains("file1"));
      assertFalse(listZnode.contains("file2"));
      assertFalse(listZnode.contains("some_longer_file2"));
      assertTrue(listZnode.contains("nothing"));
    }
  }

  @Test
  public void testPaths() {
    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent(null));

    assertEquals("Unexpected path construction"
        , "this/is/a"
        , ZkMaintenanceUtils.getZkParent("this/is/a/path"));

    assertEquals("Unexpected path construction"
        , "/root"
        , ZkMaintenanceUtils.getZkParent("/root/path/"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("/"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent(""));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("noslashesinstring"));

    assertEquals("Unexpected path construction"
        , ""
        , ZkMaintenanceUtils.getZkParent("/leadingslashonly"));

  }

  @Test
  public void testTraverseZkTree() throws Exception {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      zkClient.makePath("/testTraverseZkTree/1/1", true, true);
      zkClient.makePath("/testTraverseZkTree/1/2", false, true);
      zkClient.makePath("/testTraverseZkTree/2", false, true);
      assertEquals(Arrays.asList("/testTraverseZkTree", "/testTraverseZkTree/1", "/testTraverseZkTree/1/1", "/testTraverseZkTree/1/2", "/testTraverseZkTree/2"), getTraverseedZNodes(zkClient, "/testTraverseZkTree", ZkMaintenanceUtils.VISIT_ORDER.VISIT_PRE));
      assertEquals(Arrays.asList("/testTraverseZkTree/1/1", "/testTraverseZkTree/1/2", "/testTraverseZkTree/1", "/testTraverseZkTree/2", "/testTraverseZkTree"), getTraverseedZNodes(zkClient, "/testTraverseZkTree", ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST));

    }
  }

  // SOLR-14993
  @Test
  public void testOneByteFile() throws Exception {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      byte[] oneByte = new byte[1];
      oneByte[0] = 0x30;
      zkClient.makePath("/test1byte/one", oneByte, true);

      Path tmpDest = Paths.get(createTempDir().toFile().getAbsolutePath(), "MustBeOne");
      ZkMaintenanceUtils.downloadFromZK(zkClient, "/test1byte/one", tmpDest);

      try (FileInputStream fis = new FileInputStream(tmpDest.toFile())) {

        byte[] data = new byte[10];
        int read = fis.read(data);
        assertEquals("Should have downloaded a one-byte file", read,  1);
        assertEquals("contents of the one-byte file should be 0x30", 0x30, data[0]);
      }
    }
  }
  private List<String> getTraverseedZNodes(SolrZkClient zkClient, String path, ZkMaintenanceUtils.VISIT_ORDER visitOrder) throws KeeperException, InterruptedException {
    List<String> result = new ArrayList<>();
    ZkMaintenanceUtils.traverseZkTree(zkClient, path, visitOrder, new ZkMaintenanceUtils.ZkVisitor() {

      @Override
      public void visit(String path) throws InterruptedException, KeeperException {
        result.add(path);
      }
    });
    return result;
  }
}
