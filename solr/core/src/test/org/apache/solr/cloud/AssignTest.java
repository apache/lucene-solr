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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AssignTest extends SolrTestCaseJ4 {
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testAssignNode() throws Exception {
    SolrZkClient zkClient = mock(SolrZkClient.class);
    Map<String, byte[]> zkClientData = new HashMap<>();
    when(zkClient.setData(anyString(), any(), anyInt(), anyBoolean())).then(invocation -> {
        zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
        return null;
      }
    );
    when(zkClient.getData(anyString(), any(), any(), anyBoolean())).then(invocation ->
        zkClientData.get(invocation.getArgument(0)));
    String nodeName = Assign.assignNode(zkClient, new DocCollection("collection1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node1", nodeName);
    nodeName = Assign.assignNode(zkClient, new DocCollection("collection2", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node1", nodeName);
    nodeName = Assign.assignNode(zkClient, new DocCollection("collection1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node2", nodeName);
  }

  @Test
  public void testIdIsUnique() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = new ZkTestServer(zkDir);
    Object fixedValue = new Object();
    String[] collections = new String[]{"c1","c2","c3","c4","c5","c6","c7","c8","c9"};
    Map<String, ConcurrentHashMap<Integer, Object>> collectionUniqueIds = new HashMap<>();
    for (String c : collections) {
      collectionUniqueIds.put(c, new ConcurrentHashMap<>());
    }

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("threadpool");
    try {
      server.run();

      try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), 10000)) {
        assertTrue(zkClient.isConnected());
        zkClient.makePath("/", true);
        for (String c : collections) {
          zkClient.makePath("/collections/"+c, true);
        }
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
          futures.add(executor.submit(() -> {
            String collection = collections[random().nextInt(collections.length)];
            int id = Assign.incAndGetId(zkClient, collection, 0);
            Object val = collectionUniqueIds.get(collection).put(id, fixedValue);
            if (val != null) {
              fail("ZkController do not generate unique id for " + collection);
            }
          }));
        }
        for (Future<?> future : futures) {
          future.get();
        }
      }
      assertEquals(1000, (long) collectionUniqueIds.values().stream()
          .map(ConcurrentHashMap::size)
          .reduce((m1, m2) -> m1 + m2).get());
    } finally {
      server.shutdown();
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }


  @Test
  public void testBuildCoreName() throws IOException, InterruptedException, KeeperException {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), 10000)) {
      zkClient.makePath("/", true);
      Map<String, Slice> slices = new HashMap<>();
      slices.put("shard1", new Slice("shard1", new HashMap<>(), null));
      slices.put("shard2", new Slice("shard2", new HashMap<>(), null));

      DocCollection docCollection = new DocCollection("collection1", slices, null, DocRouter.DEFAULT);
      assertEquals("Core name pattern changed", "collection1_shard1_replica_n1", Assign.buildCoreName(zkClient, docCollection, "shard1", Replica.Type.NRT));
      assertEquals("Core name pattern changed", "collection1_shard2_replica_p2", Assign.buildCoreName(zkClient, docCollection, "shard2", Replica.Type.PULL));
    } finally {
      server.shutdown();
    }
  }
  
}
