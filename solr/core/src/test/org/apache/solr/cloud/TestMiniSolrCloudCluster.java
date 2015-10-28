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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettyConfig.Builder;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
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

  private static Logger log = LoggerFactory.getLogger(MiniSolrCloudCluster.class);
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
    
  private void createCollection(MiniSolrCloudCluster miniCluster, String collectionName, String createNodeSet, String asyncId, boolean persistIndex) throws Exception {
    String configName = "solrCloudCollectionConfig";
    File configDir = new File(SolrTestCaseJ4.TEST_HOME() + File.separator + "collection1" + File.separator + "conf");
    miniCluster.uploadConfigDir(configDir, configName);

    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
    collectionProperties.put("solr.tests.maxBufferedDocs", "100000");
    collectionProperties.put("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    collectionProperties.put("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
    collectionProperties.put("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    collectionProperties.put("solr.directoryFactory", (persistIndex ? "solr.StandardDirectoryFactory" : "solr.RAMDirectoryFactory"));
    
    miniCluster.createCollection(collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName, createNodeSet, asyncId, collectionProperties);
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
      createCollection(miniCluster, collectionName, null, asyncId, random().nextBoolean());
      if (asyncId != null) {
        assertEquals("did not see async createCollection completion", "completed", AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 330, cloudSolrClient));
      }

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
      zkStateReader.updateClusterState();
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


      // delete the collection we created earlier
      miniCluster.deleteCollection(collectionName);
      AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName, zkStateReader, true, true, 330);

      // create it again
      String asyncId2 = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      createCollection(miniCluster, collectionName, null, asyncId2, random().nextBoolean());
      if (asyncId2 != null) {
        assertEquals("did not see async createCollection completion", "completed", AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId2, 330, cloudSolrClient));
      }
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
  public void testErrorsInStartup() throws Exception {

    final AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = null;
    try {
      cluster = new MiniSolrCloudCluster(3, createTempDir(), JettyConfig.builder().build()) {
        @Override
        public JettySolrRunner startJettySolrRunner(String name, String context, JettyConfig config) throws Exception {
          if (jettyIndex.incrementAndGet() != 2)
            return super.startJettySolrRunner(name, context, config);
          throw new IOException("Fake exception on startup!");
        }
      };
      fail("Expected an exception to be thrown from MiniSolrCloudCluster");
    }
    catch (Exception e) {
      assertEquals("Error starting up MiniSolrCloudCluster", e.getMessage());
      assertEquals("Expected one suppressed exception", 1, e.getSuppressed().length);
      assertEquals("Fake exception on startup!", e.getSuppressed()[0].getMessage());
    }
    finally {
      if (cluster != null)
        cluster.shutdown();
    }
  }

  @Test
  public void testErrorsInShutdown() throws Exception {

    final AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(3, createTempDir(), JettyConfig.builder().build()) {
        @Override
        protected JettySolrRunner stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
          JettySolrRunner j = super.stopJettySolrRunner(jetty);
          if (jettyIndex.incrementAndGet() == 2)
            throw new IOException("Fake IOException on shutdown!");
          return j;
        }
      };

    try {
      cluster.shutdown();
      fail("Expected an exception to be thrown on MiniSolrCloudCluster shutdown");
    }
    catch (Exception e) {
      assertEquals("Error shutting down MiniSolrCloudCluster", e.getMessage());
      assertEquals("Expected one suppressed exception", 1, e.getSuppressed().length);
      assertEquals("Fake IOException on shutdown!", e.getSuppressed()[0].getMessage());
    }

  }

  @Test
  public void testExtraFilters() throws Exception {
    Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    jettyConfig.withFilter(JettySolrRunner.DebugFilter.class, "*");
    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), jettyConfig.build());
    cluster.shutdown();
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
      createCollection(miniCluster, collectionName, OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY, asyncId, random().nextBoolean());
      if (asyncId != null) {
        assertEquals("did not see async createCollection completion", "completed", AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 330, cloudSolrClient));
      }

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, 45000, null);
          ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
        
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
        
        // delete the collection we created earlier
        miniCluster.deleteCollection(collectionName);
        AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName, zkStateReader, true, true, 330);    
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

      createCollection(miniCluster, collectionName, null, null, true);
      final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();
      cloudSolrClient.setDefaultCollection(collectionName);
      final SolrQuery query = new SolrQuery("*:*");
      final SolrInputDocument doc = new SolrInputDocument();

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, 45000, null);
          ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
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
        zkStateReader.updateClusterState();
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

        zkStateReader.updateClusterState();

        // re-query collection
        {
          final QueryResponse rsp = cloudSolrClient.query(query);
          assertEquals(numDocs, rsp.getResults().getNumFound());
        }

        // delete the collection we created earlier
        miniCluster.deleteCollection(collectionName);
        AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName, zkStateReader, true, true, 330);
      }
    }
    finally {
      miniCluster.shutdown();
    }
  }

}
