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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettyConfig.Builder;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the MiniSolrCloudCluster functionality. Keep in mind, 
 * MiniSolrCloudCluster is designed to be used outside of the Lucene test
 * hierarchy.
 */
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestMiniSolrCloudCluster extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected int NUM_SERVERS = 5;
  protected int NUM_SHARDS = 2;
  protected int REPLICATION_FACTOR = 2;

  public TestMiniSolrCloudCluster () {
    NUM_SERVERS = 5;
    NUM_SHARDS = 2;
    REPLICATION_FACTOR = 2;
  }
  
  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());
  
  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());
  
  private MiniSolrCloudCluster createMiniSolrCloudCluster() throws Exception {
    Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    return new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), jettyConfig.build());
  }
    
  private void createCollection(MiniSolrCloudCluster miniCluster, String collectionName, String createNodeSet, String asyncId,
      Boolean indexToPersist, Map<String,String> collectionProperties) throws Exception {
    String configName = "solrCloudCollectionConfig";
    miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf"), configName);

    final boolean persistIndex = (indexToPersist != null ? indexToPersist.booleanValue() : random().nextBoolean());
    if (collectionProperties == null) {
      collectionProperties = new HashMap<>();
    }
    collectionProperties.putIfAbsent(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
    collectionProperties.putIfAbsent("solr.tests.maxBufferedDocs", "100000");
    collectionProperties.putIfAbsent("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    if (random().nextBoolean()) {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY, TieredMergePolicy.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "false");
    } else {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, TieredMergePolicyFactory.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "false");
    }
    collectionProperties.putIfAbsent("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    collectionProperties.putIfAbsent("solr.directoryFactory", (persistIndex ? "solr.StandardDirectoryFactory" : "solr.RAMDirectoryFactory"));

    if (asyncId == null) {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .process(miniCluster.getSolrClient());
    }
    else {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .processAndWait(miniCluster.getSolrClient(), 30);
    }
  }

  @Test
  public void testCollectionCreateSearchDelete() throws Exception {

    final String collectionName = "testcollection";
    MiniSolrCloudCluster miniCluster = createMiniSolrCloudCluster();

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(NUM_SERVERS, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // shut down a server
      log.info("#### Stopping a server");
      JettySolrRunner stoppedServer = miniCluster.stopJettySolrRunner(0);
      assertTrue(stoppedServer.isStopped());
      assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());

      // create a server
      log.info("#### Starting a server");
      JettySolrRunner startedServer = miniCluster.startJettySolrRunner();
      assertTrue(startedServer.isRunning());
      assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

      // create collection
      log.info("#### Creating a collection");
      final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      createCollection(miniCluster, collectionName, null, asyncId, null, null);

      ZkStateReader zkStateReader = miniCluster.getSolrClient().getZkStateReader();
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

      // modify/query collection
      log.info("#### updating a querying collection");
      cloudSolrClient.setDefaultCollection(collectionName);
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "1");
      cloudSolrClient.add(doc);
      cloudSolrClient.commit();
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");
      QueryResponse rsp = cloudSolrClient.query(query);
      assertEquals(1, rsp.getResults().getNumFound());

      // remove a server not hosting any replicas
      zkStateReader.forceUpdateCollection(collectionName);
      ClusterState clusterState = zkStateReader.getClusterState();
      HashMap<String, JettySolrRunner> jettyMap = new HashMap<String, JettySolrRunner>();
      for (JettySolrRunner jetty : miniCluster.getJettySolrRunners()) {
        String key = jetty.getBaseUrl().toString().substring((jetty.getBaseUrl().getProtocol() + "://").length());
        jettyMap.put(key, jetty);
      }
      Collection<Slice> slices = clusterState.getSlices(collectionName);
      // track the servers not host repliacs
      for (Slice slice : slices) {
        jettyMap.remove(slice.getLeader().getNodeName().replace("_solr", "/solr"));
        for (Replica replica : slice.getReplicas()) {
          jettyMap.remove(replica.getNodeName().replace("_solr", "/solr"));
        }
      }
      assertTrue("Expected to find a node without a replica", jettyMap.size() > 0);
      log.info("#### Stopping a server");
      JettySolrRunner jettyToStop = jettyMap.entrySet().iterator().next().getValue();
      jettys = miniCluster.getJettySolrRunners();
      for (int i = 0; i < jettys.size(); ++i) {
        if (jettys.get(i).equals(jettyToStop)) {
          miniCluster.stopJettySolrRunner(i);
          assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());
        }
      }

      // re-create a server (to restore original NUM_SERVERS count)
      log.info("#### Starting a server");
      startedServer = miniCluster.startJettySolrRunner(jettyToStop);
      assertTrue(startedServer.isRunning());
      assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

      CollectionAdminRequest.deleteCollection(collectionName).process(miniCluster.getSolrClient());

      // create it again
      String asyncId2 = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      createCollection(miniCluster, collectionName, null, asyncId2, null, null);
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

      // check that there's no left-over state
      assertEquals(0, cloudSolrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
      cloudSolrClient.add(doc);
      cloudSolrClient.commit();
      assertEquals(1, cloudSolrClient.query(new SolrQuery("*:*")).getResults().getNumFound());

    }
    finally {
      miniCluster.shutdown();
    }
  }

  @Test
  public void testCollectionCreateWithoutCoresThenDelete() throws Exception {

    final String collectionName = "testSolrCloudCollectionWithoutCores";
    final MiniSolrCloudCluster miniCluster = createMiniSolrCloudCluster();
    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      assertFalse(miniCluster.getJettySolrRunners().isEmpty());

      // create collection
      final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      createCollection(miniCluster, collectionName, OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY, asyncId, null, null);

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
          ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
        zkStateReader.createClusterStateWatchersAndUpdate();

        // wait for collection to appear
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

        // check the collection's corelessness
        {
          int coreCount = 0; 
          for (Map.Entry<String,Slice> entry : zkStateReader.getClusterState().getSlicesMap(collectionName).entrySet()) {
            coreCount += entry.getValue().getReplicasMap().entrySet().size();
          }
          assertEquals(0, coreCount);
        }

      }
    }
    finally {
      miniCluster.shutdown();
    }
  }

  @Test
  public void testStopAllStartAll() throws Exception {

    final String collectionName = "testStopAllStartAllCollection";

    final MiniSolrCloudCluster miniCluster = createMiniSolrCloudCluster();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(NUM_SERVERS, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      createCollection(miniCluster, collectionName, null, null, Boolean.TRUE, null);
      final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();
      cloudSolrClient.setDefaultCollection(collectionName);
      final SolrQuery query = new SolrQuery("*:*");
      final SolrInputDocument doc = new SolrInputDocument();

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
          ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
        zkStateReader.createClusterStateWatchersAndUpdate();
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

        // modify collection
        final int numDocs = 1 + random().nextInt(10);
        for (int ii = 1; ii <= numDocs; ++ii) {
          doc.setField("id", ""+ii);
          cloudSolrClient.add(doc);
          if (ii*2 == numDocs) cloudSolrClient.commit();
        }
        cloudSolrClient.commit();
        // query collection
        {
          final QueryResponse rsp = cloudSolrClient.query(query);
          assertEquals(numDocs, rsp.getResults().getNumFound());
        }

        // the test itself
        zkStateReader.forceUpdateCollection(collectionName);
        final ClusterState clusterState = zkStateReader.getClusterState();

        final HashSet<Integer> leaderIndices = new HashSet<Integer>();
        final HashSet<Integer> followerIndices = new HashSet<Integer>();
        {
          final HashMap<String,Boolean> shardLeaderMap = new HashMap<String,Boolean>();
          for (final Slice slice : clusterState.getSlices(collectionName)) {
            for (final Replica replica : slice.getReplicas()) {
              shardLeaderMap.put(replica.getNodeName().replace("_solr", "/solr"), Boolean.FALSE);
            }
            shardLeaderMap.put(slice.getLeader().getNodeName().replace("_solr", "/solr"), Boolean.TRUE);
          }
          for (int ii = 0; ii < jettys.size(); ++ii) {
            final URL jettyBaseUrl = jettys.get(ii).getBaseUrl();
            final String jettyBaseUrlString = jettyBaseUrl.toString().substring((jettyBaseUrl.getProtocol() + "://").length());
            final Boolean isLeader = shardLeaderMap.get(jettyBaseUrlString);
            if (Boolean.TRUE.equals(isLeader)) {
              leaderIndices.add(new Integer(ii));
            } else if (Boolean.FALSE.equals(isLeader)) {
              followerIndices.add(new Integer(ii));
            } // else neither leader nor follower i.e. node without a replica (for our collection)
          }
        }
        final List<Integer> leaderIndicesList = new ArrayList<Integer>(leaderIndices);
        final List<Integer> followerIndicesList = new ArrayList<Integer>(followerIndices);

        // first stop the followers (in no particular order)
        Collections.shuffle(followerIndicesList, random());
        for (Integer ii : followerIndicesList) {
          if (!leaderIndices.contains(ii)) {
            miniCluster.stopJettySolrRunner(jettys.get(ii.intValue()));
          }
        }

        // then stop the leaders (again in no particular order)
        Collections.shuffle(leaderIndicesList, random());
        for (Integer ii : leaderIndicesList) {
          miniCluster.stopJettySolrRunner(jettys.get(ii.intValue()));
        }

        // calculate restart order
        final List<Integer> restartIndicesList = new ArrayList<Integer>();
        Collections.shuffle(leaderIndicesList, random());
        restartIndicesList.addAll(leaderIndicesList);
        Collections.shuffle(followerIndicesList, random());
        restartIndicesList.addAll(followerIndicesList);
        if (random().nextBoolean()) Collections.shuffle(restartIndicesList, random());

        // and then restart jettys in that order
        for (Integer ii : restartIndicesList) {
          final JettySolrRunner jetty = jettys.get(ii.intValue());
          if (!jetty.isRunning()) {
            miniCluster.startJettySolrRunner(jetty);
            assertTrue(jetty.isRunning());
          }
        }
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

        zkStateReader.forceUpdateCollection(collectionName);

        // re-query collection
        {
          final QueryResponse rsp = cloudSolrClient.query(query);
          assertEquals(numDocs, rsp.getResults().getNumFound());
        }

      }
    }
    finally {
      miniCluster.shutdown();
    }
  }

}
