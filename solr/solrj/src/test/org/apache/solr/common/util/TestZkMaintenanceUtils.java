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
package org.apache.solr.common.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
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
