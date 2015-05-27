package org.apache.solr.cloud;

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

import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


@Nightly
@Slow
@SuppressSSL
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class SharedFSAutoReplicaFailoverTest extends AbstractFullDistribZkTestBase {
  
  private static final boolean DEBUG = true;
  private static MiniDFSCluster dfsCluster;

  ThreadPoolExecutor executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;

  
  @BeforeClass
  public static void hdfsFailoverBeforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }
  
  @AfterClass
  public static void hdfsFailoverAfterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useJettyDataDir = false;
    System.setProperty("solr.xml.persist", "true");
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  
  public SharedFSAutoReplicaFailoverTest() {
    sliceCount = 2;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    try {
      testBasics();
    } finally {
      if (DEBUG) {
        super.printLayout();
      }
    }
  }

  // very slow tests, especially since jetty is started and stopped
  // serially
  private void testBasics() throws Exception {
    String collection1 = "solrj_collection";
    Create createCollectionRequest = new Create()
            .setCollectionName(collection1)
            .setNumShards(2)
            .setReplicationFactor(2)
            .setMaxShardsPerNode(2)
            .setConfigName("conf1")
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    waitForRecoveriesToFinish(collection1, false);
    
    String collection2 = "solrj_collection2";
    createCollectionRequest = new Create()
            .setCollectionName(collection2)
            .setNumShards(2)
            .setReplicationFactor(2)
            .setMaxShardsPerNode(2)
            .setConfigName("conf1")
            .setRouterField("myOwnField")
            .setAutoAddReplicas(false);
    CollectionAdminResponse response2 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response2.getStatus());
    assertTrue(response2.isSuccess());
    
    waitForRecoveriesToFinish(collection2, false);
    
    String collection3 = "solrj_collection3";
    createCollectionRequest = new Create()
            .setCollectionName(collection3)
            .setNumShards(5)
            .setReplicationFactor(1)
            .setMaxShardsPerNode(1)
            .setConfigName("conf1")
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response3 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response3.getStatus());
    assertTrue(response3.isSuccess());
    
    waitForRecoveriesToFinish(collection3, false);

    ChaosMonkey.stop(jettys.get(1));
    ChaosMonkey.stop(jettys.get(2));

    Thread.sleep(5000);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), collection1, 120000));

    assertSliceAndReplicaCount(collection1);

    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection2) < 4);

    // collection3 has maxShardsPerNode=1, there are 4 standard jetties and one control jetty and 2 nodes stopped
    ClusterStateUtil.waitForLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection3, 3, 30000);
    
    // collection1 should still be at 4
    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));
    // and collection2 less than 4
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection2) < 4);
    
    ChaosMonkey.stop(jettys);
    ChaosMonkey.stop(controlJetty);

    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));

    ChaosMonkey.start(jettys);
    ChaosMonkey.start(controlJetty);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), collection1, 120000));

    assertSliceAndReplicaCount(collection1);
    assertSingleReplicationAndShardSize(collection3, 5);
    
    int jettyIndex = random().nextInt(jettys.size());
    ChaosMonkey.stop(jettys.get(jettyIndex));
    ChaosMonkey.start(jettys.get(jettyIndex));
    
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), collection1, 60000));
    
    assertSliceAndReplicaCount(collection1);
    
    assertSingleReplicationAndShardSize(collection3, 5);
    ClusterStateUtil.waitForLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection3, 5, 30000);
    //disable autoAddReplicas
    Map m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS,
        "val", "false");

    SolrRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);

    int currentCount = ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1);

    ChaosMonkey.stop(jettys.get(3));

    //solr-no-core.xml has defined workLoopDelay=10s and waitAfterExpiration=10s
    //Hence waiting for 30 seconds to be on the safe side.
    Thread.sleep(30000);
    //Ensures that autoAddReplicas has not kicked in.
    assertTrue(currentCount > ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));

    //enable autoAddReplicas
    m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS);

    request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), collection1, 60000));
    assertSliceAndReplicaCount(collection1);
  }
  
  private void assertSingleReplicationAndShardSize(String collection, int numSlices) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection);
    assertEquals(numSlices, slices.size());
    for (Slice slice : slices) {
      assertEquals(1, slice.getReplicas().size());
    }
  }

  private void assertSliceAndReplicaCount(String collection) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection);
    assertEquals(2, slices.size());
    for (Slice slice : slices) {
      assertEquals(2, slice.getReplicas().size());
    }
  }
  
  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("solr.xml.persist");
  }
}
