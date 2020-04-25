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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestDistributedMap extends SolrTestCaseJ4 {
  
  private static Path zkDir;
  
  protected static ZkTestServer zkServer;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    zkDir = createTempDir("TestDistributedMap");
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
  
  public void testPut() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertFalse(zkClient.exists(path + "/" + DistributedMap.PREFIX + "foo", true));
      map.put("foo", new byte[0]);
      assertTrue(zkClient.exists(path + "/" + DistributedMap.PREFIX + "foo", true));
    }
  }
  
  public void testGet() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      byte[] data = "data".getBytes(Charset.defaultCharset());
      zkClient.makePath(path + "/" + DistributedMap.PREFIX + "foo", data, CreateMode.PERSISTENT, null, false, true);
      DistributedMap map = createMap(zkClient, path);
      assertArrayEquals(data,  map.get("foo"));
    }
  }
  
  public void testContains() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertFalse(map.contains("foo"));
      zkClient.makePath(path + "/" + DistributedMap.PREFIX + "foo", new byte[0], CreateMode.PERSISTENT, null, false, true);
      assertTrue(map.contains("foo"));
    }
  }
  
  public void testRemove() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertFalse(map.remove("foo"));
      zkClient.makePath(path + "/" + DistributedMap.PREFIX + "foo", new byte[0], CreateMode.PERSISTENT, null, false, true);
      assertTrue(map.remove("foo"));
      assertFalse(map.contains("foo"));
      assertFalse(zkClient.exists(path + "/" + DistributedMap.PREFIX + "foo", true));
    }
  }
  
  public void testSize() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertEquals(0, map.size());
      map.remove("bar");
      assertEquals(0, map.size());
      map.put("foo", new byte[0]);
      assertEquals(1, map.size());
      map.put("foo2", new byte[0]);
      assertEquals(2, map.size());
      map.remove("foo");
      assertEquals(1, map.size());
    }
  }
  
  public void testPutIfAbsent() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertEquals(0, map.size());
      assertFalse(map.contains("foo"));
      assertTrue(map.putIfAbsent("foo", new byte[0]));
      assertEquals(1, map.size());
      assertTrue(map.contains("foo"));
      assertFalse(map.putIfAbsent("foo", new byte[0]));
      assertTrue(map.contains("foo"));
      assertEquals(1, map.size());
      map.remove("foo");
      assertFalse(map.contains("foo"));
      assertEquals(0, map.size());
      assertTrue(map.putIfAbsent("foo", new byte[0]));
      assertEquals(1, map.size());
      assertTrue(map.contains("foo"));
    }
    
  }
  
  public void testKeys() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      assertEquals(0, map.keys().size());
      map.put("foo", new byte[0]);
      assertTrue(map.keys().contains("foo"));
      assertEquals(1, map.keys().size());
      
      map.put("bar", new byte[0]);
      assertTrue(map.keys().contains("bar"));
      assertTrue(map.keys().contains("foo"));
      assertEquals(2, map.keys().size());
      
      map.remove("foo");
      assertTrue(map.keys().contains("bar"));
      assertEquals(1, map.keys().size());
    }
  }
  
  public void testClear() throws KeeperException, InterruptedException {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), 10000)) {
      String path = getAndMakeInitialPath(zkClient);
      DistributedMap map = createMap(zkClient, path);
      map.clear();
      assertEquals(0, map.size());
      map.put("foo", new byte[0]);
      map.put("bar", new byte[0]);
      assertEquals(2, map.size());
      map.clear();
      assertEquals(0, map.size());
    }
  }
  
  protected DistributedMap createMap(SolrZkClient zkClient, String path) {
    return new DistributedMap(zkClient, path);
  }
  
  protected String getAndMakeInitialPath(SolrZkClient zkClient) throws KeeperException, InterruptedException {
    String path = String.format(Locale.ROOT, "/%s/%s", getClass().getName(), getSaferTestName());
    zkClient.makePath(path, false, true);
    return path;
  }
  

}
