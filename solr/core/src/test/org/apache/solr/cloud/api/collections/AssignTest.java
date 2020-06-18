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
package org.apache.solr.cloud.api.collections;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.ZkDistribStateManager;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Utils;
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
    assumeWorkingMockito();
    
    SolrZkClient zkClient = mock(SolrZkClient.class);
    Map<String, byte[]> zkClientData = new HashMap<>();
    when(zkClient.setData(anyString(), any(), anyInt(), anyBoolean())).then(invocation -> {
        zkClientData.put(invocation.getArgument(0), invocation.getArgument(1));
        return null;
      }
    );
    when(zkClient.getData(anyString(), any(), any(), anyBoolean())).then(invocation ->
        zkClientData.get(invocation.getArgument(0)));
    // TODO: fix this to be independent of ZK
    ZkDistribStateManager stateManager = new ZkDistribStateManager(zkClient);
    String nodeName = Assign.assignCoreNodeName(stateManager, new DocCollection("collection1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node1", nodeName);
    nodeName = Assign.assignCoreNodeName(stateManager, new DocCollection("collection2", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node1", nodeName);
    nodeName = Assign.assignCoreNodeName(stateManager, new DocCollection("collection1", new HashMap<>(), new HashMap<>(), DocRouter.DEFAULT));
    assertEquals("core_node2", nodeName);
  }

  @Test
  public void testIdIsUnique() throws Exception {
    Path zkDir = createTempDir("zkData");
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
        for (String c : collections) {
          zkClient.makePath("/collections/" + c, true);
        }
        // TODO: fix this to be independent of ZK
        ZkDistribStateManager stateManager = new ZkDistribStateManager(zkClient);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 73; i++) {
          futures.add(executor.submit(() -> {
            String collection = collections[random().nextInt(collections.length)];
            int id = Assign.incAndGetId(stateManager, collection, 0);
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
      assertEquals(73, (long) collectionUniqueIds.values().stream()
          .map(ConcurrentHashMap::size)
          .reduce((m1, m2) -> m1 + m2).get());
    } finally {
      server.shutdown();
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }


  @Test
  public void testBuildCoreName() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    try (SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), 10000)) {
      // TODO: fix this to be independent of ZK
      ZkDistribStateManager stateManager = new ZkDistribStateManager(zkClient);
      Map<String, Slice> slices = new HashMap<>();
      slices.put("shard1", new Slice("shard1", new HashMap<>(), null,"collection1"));
      slices.put("shard2", new Slice("shard2", new HashMap<>(), null,"collection1"));

      DocCollection docCollection = new DocCollection("collection1", slices, null, DocRouter.DEFAULT);
      assertEquals("Core name pattern changed", "collection1_shard1_replica_n1", Assign.buildSolrCoreName(stateManager, docCollection, "shard1", Replica.Type.NRT));
      assertEquals("Core name pattern changed", "collection1_shard2_replica_p2", Assign.buildSolrCoreName(stateManager, docCollection, "shard2", Replica.Type.PULL));
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testUseLegacyByDefault() throws Exception {
    assumeWorkingMockito();

    SolrCloudManager solrCloudManager = mock(SolrCloudManager.class);
    ClusterStateProvider clusterStateProvider = mock(ClusterStateProvider.class);
    when(solrCloudManager.getClusterStateProvider()).thenReturn(clusterStateProvider);
    DistribStateManager distribStateManager = mock(DistribStateManager.class);
    when(solrCloudManager.getDistribStateManager()).thenReturn(distribStateManager);
    when(distribStateManager.getAutoScalingConfig()).thenReturn(new AutoScalingConfig(Collections.emptyMap()));

    // first we don't set any cluster property and assert that legacy assignment is used
    when(clusterStateProvider.getClusterProperties()).thenReturn(Collections.emptyMap());
    // verify
    boolean usePolicyFramework = Assign.usePolicyFramework(solrCloudManager);
    assertFalse(usePolicyFramework);
    // another sanity check
    when(clusterStateProvider.getClusterProperties()).thenReturn(Utils.makeMap("defaults", Collections.emptyMap()));
    // verify
    usePolicyFramework = Assign.usePolicyFramework(solrCloudManager);
    assertFalse(usePolicyFramework);

    // first we set useLegacyReplicaAssignment=false, so autoscaling should always be used
    when(clusterStateProvider.getClusterProperties()).thenReturn(Utils.makeMap("defaults", Utils.makeMap("cluster", Utils.makeMap("useLegacyReplicaAssignment", false))));
    // verify
    usePolicyFramework = Assign.usePolicyFramework(solrCloudManager);
    assertTrue(usePolicyFramework);

    // now we set useLegacyReplicaAssignment=true, so autoscaling can only be used if an explicit policy or preference exists
    when(clusterStateProvider.getClusterProperties()).thenReturn(Utils.makeMap("defaults", Utils.makeMap("cluster", Utils.makeMap("useLegacyReplicaAssignment", true))));
    when(distribStateManager.getAutoScalingConfig()).thenReturn(new AutoScalingConfig(Collections.emptyMap()));
    assertFalse(Assign.usePolicyFramework(solrCloudManager));

    // lets provide a custom preference and assert that autoscaling is used even if useLegacyReplicaAssignment=false
    // our custom preferences are exactly the same as the default ones
    // but because we are providing them explicitly, they must cause autoscaling to turn on
    @SuppressWarnings({"rawtypes"})
    List<Map> customPreferences = Policy.DEFAULT_PREFERENCES
        .stream().map(preference -> preference.getOriginal()).collect(Collectors.toList());

    when(distribStateManager.getAutoScalingConfig()).thenReturn(new AutoScalingConfig(Utils.makeMap("cluster-preferences", customPreferences)));
    assertTrue(Assign.usePolicyFramework(solrCloudManager));
  }
}
