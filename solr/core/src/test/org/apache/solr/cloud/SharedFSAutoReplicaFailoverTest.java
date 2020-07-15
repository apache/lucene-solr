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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.LuceneTestCase.Slow;
import com.carrotsearch.randomizedtesting.annotations.Nightly;

import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Nightly
@Slow
@SuppressSSL
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.*=DEBUG")
@LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
public class SharedFSAutoReplicaFailoverTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final boolean DEBUG = true;
  private static MiniDFSCluster dfsCluster;

  ThreadPoolExecutor executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new SolrNamedThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  private final Map<String, String> collectionUlogDirMap = new HashMap<>();
  
  @BeforeClass
  public static void hdfsFailoverBeforeClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "512");
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
    System.setProperty("solr.hdfs.blockcache.global", "true"); // always use global cache, this test can create a lot of directories
    schemaString = "schema15.xml"; 
  }
  
  @AfterClass
  public static void hdfsFailoverAfterClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      System.clearProperty("solr.hdfs.blockcache.blocksperbank");
      dfsCluster = null;
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    collectionUlogDirMap.clear();
    if (random().nextBoolean()) {
      CollectionAdminRequest.setClusterProperty("legacyCloud", "false").process(cloudClient);
    } else {
      CollectionAdminRequest.setClusterProperty("legacyCloud", "true").process(cloudClient);
    }
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useJettyDataDir = false;
  }
  
  protected String getSolrXml() {
    return "solr.xml";
  }

  
  public SharedFSAutoReplicaFailoverTest() {
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
  }

  @Test
  @ShardsFixed(num = 4)
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  //commented 23-AUG-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
  public void test() throws Exception {
    try {
      // to keep uncommitted docs during failover
      TestInjection.skipIndexWriterCommitOnClose = true;
      testBasics();
    } finally {
      TestInjection.reset();
      if (DEBUG) {
        super.printLayout();
      }
    }
  }

  // very slow tests, especially since jetty is started and stopped
  // serially
  private void testBasics() throws Exception {
    String collection1 = "solrj_collection";
    Create createCollectionRequest = CollectionAdminRequest.createCollection(collection1,"conf1",2,2)
            .setMaxShardsPerNode(2)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    waitForRecoveriesToFinish(collection1, false);
    
    String collection2 = "solrj_collection2";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection2,"conf1",2,2)
            .setMaxShardsPerNode(2)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(false);
    CollectionAdminResponse response2 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response2.getStatus());
    assertTrue(response2.isSuccess());
    
    waitForRecoveriesToFinish(collection2, false);
    
    String collection3 = "solrj_collection3";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection3,"conf1",5,1)
            .setMaxShardsPerNode(1)
            .setRouterField("myOwnField")
            .setAutoAddReplicas(true);
    CollectionAdminResponse response3 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response3.getStatus());
    assertTrue(response3.isSuccess());
    
    waitForRecoveriesToFinish(collection3, false);

    // a collection has only 1 replica per a shard
    String collection4 = "solrj_collection4";
    createCollectionRequest = CollectionAdminRequest.createCollection(collection4,"conf1",5,1)
        .setMaxShardsPerNode(5)
        .setRouterField("text")
        .setAutoAddReplicas(true);
    CollectionAdminResponse response4 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response4.getStatus());
    assertTrue(response4.isSuccess());

    waitForRecoveriesToFinish(collection4, false);

    // all collections
    String[] collections = {collection1, collection2, collection3, collection4};

    // add some documents to collection4
    final int numDocs = 100;
    addDocs(collection4, numDocs, false);  // indexed but not committed

    // no result because not committed yet
    queryAndAssertResultSize(collection4, 0, 10000);

    assertUlogDir(collections);

    jettys.get(1).stop();
    jettys.get(2).stop();

    Thread.sleep(5000);

    assertSliceAndReplicaCount(collection1, 2, 2, 120000);

    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection2) < 4);

    // collection3 has maxShardsPerNode=1, there are 4 standard jetties and one control jetty and 2 nodes stopped
    ClusterStateUtil.waitForLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection3, 3, 30000);

    // collection4 has maxShardsPerNode=5 and setMaxShardsPerNode=5
    ClusterStateUtil.waitForLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection4, 5, 30000);

    // all docs should be queried after failover
    cloudClient.commit(); // to query all docs
    assertSliceAndReplicaCount(collection4, 5, 1, 120000);
    queryAndAssertResultSize(collection4, numDocs, 10000);

    // collection1 should still be at 4
    assertEquals(4, ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection1));
    // and collection2 less than 4
    assertTrue(ClusterStateUtil.getLiveAndActiveReplicaCount(cloudClient.getZkStateReader(), collection2) < 4);

    assertUlogDir(collections);

    boolean allowOverseerRestart = random().nextBoolean();
    List<JettySolrRunner> stoppedJetties = allowOverseerRestart
        ? jettys.stream().filter(jettySolrRunner -> random().nextBoolean()).collect(Collectors.toList()) : notOverseerJetties();
    ChaosMonkey.stop(stoppedJetties);
    controlJetty.stop();

    assertTrue("Timeout waiting for all not live", waitingForReplicasNotLive(cloudClient.getZkStateReader(), 45000, stoppedJetties));

    ChaosMonkey.start(stoppedJetties);
    controlJetty.start();

    assertSliceAndReplicaCount(collection1, 2, 2, 120000);
    assertSliceAndReplicaCount(collection3, 5, 1, 120000);

    // all docs should be queried
    assertSliceAndReplicaCount(collection4, 5, 1, 120000);
    queryAndAssertResultSize(collection4, numDocs, 10000);

    assertUlogDir(collections);

    int jettyIndex = random().nextInt(jettys.size());
    jettys.get(jettyIndex).stop();
    jettys.get(jettyIndex).start();

    assertSliceAndReplicaCount(collection1, 2, 2, 120000);

    assertUlogDir(collections);

    assertSliceAndReplicaCount(collection3, 5, 1, 120000);
    assertSliceAndReplicaCount(collection4, 5, 1, 120000);
  }

  private void queryAndAssertResultSize(String collection, int expectedResultSize, int timeoutMS)
      throws SolrServerException, IOException, InterruptedException {
    long startTimestamp = System.nanoTime();

    long actualResultSize = 0;
    while(true) {
      if (System.nanoTime() - startTimestamp > TimeUnit.MILLISECONDS.toNanos(timeoutMS) || actualResultSize > expectedResultSize) {
        fail("expected: " + expectedResultSize + ", actual: " + actualResultSize);
      }
      SolrParams queryAll = new SolrQuery("*:*");
      cloudClient.setDefaultCollection(collection);
      try {
        QueryResponse queryResponse = cloudClient.query(queryAll);
        actualResultSize = queryResponse.getResults().getNumFound();
        if(expectedResultSize == actualResultSize) {
          return;
        }
      } catch (SolrServerException | IOException e) {
        log.warn("Querying solr threw an exception. This can be expected to happen during restarts.", e);
      }

      Thread.sleep(1000);
    }
  }

  private void addDocs(String collection, int numDocs, boolean commit) throws SolrServerException, IOException {
    for (int docId = 1; docId <= numDocs; docId++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docId);
      doc.addField("text", "shard" + docId % 5);
      cloudClient.setDefaultCollection(collection);
      cloudClient.add(doc);
    }
    if (commit) {
      cloudClient.commit();
    }
  }

  /**
   * After failover, ulogDir should not be changed.
   */
  private void assertUlogDir(String... collections) {
    for (String collection : collections) {
      Collection<Slice> slices = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getSlices();
      for (Slice slice : slices) {
        for (Replica replica : slice.getReplicas()) {
          Map<String, Object> properties = replica.getProperties();
          String coreName = replica.getCoreName();
          String curUlogDir = (String) properties.get(CoreDescriptor.CORE_ULOGDIR);
          String prevUlogDir = collectionUlogDirMap.get(coreName);
          if (curUlogDir != null) {
            if (prevUlogDir == null) {
              collectionUlogDirMap.put(coreName, curUlogDir);
            } else {
              assertEquals(prevUlogDir, curUlogDir);
            }
          }
        }
      }
    }
  }

  // Overseer failover is not currently guaranteed with MoveReplica or Policy Framework
  private List<JettySolrRunner> notOverseerJetties() throws IOException, SolrServerException {
    CollectionAdminResponse response = CollectionAdminRequest.getOverseerStatus().process(cloudClient);
    String overseerNode = (String) response.getResponse().get("leader");
    return jettys.stream().filter(jetty -> !(jetty.getCoreContainer() != null && overseerNode.equals(jetty.getNodeName())))
    .collect(Collectors.toList());
  }

  private boolean waitingForReplicasNotLive(ZkStateReader zkStateReader, int timeoutInMs, List<JettySolrRunner> jetties) {
    Set<String> nodeNames = jetties.stream()
        .filter(jetty -> jetty.getCoreContainer() != null)
        .map(JettySolrRunner::getNodeName)
        .collect(Collectors.toSet());
    long timeout = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(timeoutInMs, TimeUnit.MILLISECONDS);
    boolean success = false;
    while (!success && System.nanoTime() < timeout) {
      success = true;
      ClusterState clusterState = zkStateReader.getClusterState();
      if (clusterState != null) {
        Map<String, DocCollection> collections = clusterState.getCollectionsMap();
        for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
          DocCollection docCollection = entry.getValue();
          Collection<Slice> slices = docCollection.getSlices();
          for (Slice slice : slices) {
            // only look at active shards
            if (slice.getState() == Slice.State.ACTIVE) {
              Collection<Replica> replicas = slice.getReplicas();
              for (Replica replica : replicas) {
                if (nodeNames.contains(replica.getNodeName())) {
                  boolean live = clusterState.liveNodesContain(replica
                      .getNodeName());
                  if (live) {
                    success = false;
                  }
                }
              }
            }
          }
        }
        if (!success) {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted");
          }
        }
      }
    }

    return success;
  }

  private void assertSliceAndReplicaCount(String collection, int numSlices, int numReplicas, int timeOutInMs) throws InterruptedException {
    TimeOut timeOut = new TimeOut(timeOutInMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      Collection<Slice> slices = clusterState.getCollection(collection).getActiveSlices();
      if (slices.size() == numSlices) {
        boolean isMatch = true;
        for (Slice slice : slices) {
          int count = 0;
          for (Replica replica : slice.getReplicas()) {
            if (replica.getState() == Replica.State.ACTIVE && clusterState.liveNodesContain(replica.getNodeName())) {
              count++;
            }
          }
          if (count < numReplicas) {
            isMatch = false;
          }
        }
        if (isMatch) return;
      }
      Thread.sleep(200);
    }
    fail("Expected numSlices=" + numSlices + " numReplicas=" + numReplicas + " but found " + cloudClient.getZkStateReader().getClusterState().getCollection(collection) + " with /live_nodes: " + cloudClient.getZkStateReader().getClusterState().getLiveNodes());
  }

}
