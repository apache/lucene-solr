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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettyConfig.Builder;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
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

  @Test
  public void testBasics() throws Exception {
    testCollectionCreateSearchDelete();
    // sometimes run a second test e.g. to test collection create-delete-create scenario
    if (random().nextBoolean()) testCollectionCreateSearchDelete();
  }
    
  protected void testCollectionCreateSearchDelete() throws Exception {

    File solrXml = new File(SolrTestCaseJ4.TEST_HOME(), "solr-no-core.xml");
    Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    MiniSolrCloudCluster miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir().toFile(), solrXml, jettyConfig.build());

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(NUM_SERVERS, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // shut down a server
      JettySolrRunner stoppedServer = miniCluster.stopJettySolrRunner(0);
      assertTrue(stoppedServer.isStopped());
      assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());

      // create a server
      JettySolrRunner startedServer = miniCluster.startJettySolrRunner();
      assertTrue(startedServer.isRunning());
      assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

      // create collection
      String collectionName = "testSolrCloudCollection";
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
      collectionProperties.put("solr.directoryFactory", "solr.RAMDirectoryFactory");
      final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      miniCluster.createCollection(collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName, asyncId, collectionProperties);
      if (asyncId != null) {
        assertEquals("did not see async createCollection completion", "completed", AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 330, cloudSolrClient));
      }

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, 45000, null);
          ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
        AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

        // modify/query collection
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
        JettySolrRunner jettyToStop = jettyMap.entrySet().iterator().next().getValue();
        jettys = miniCluster.getJettySolrRunners();
        for (int i = 0; i < jettys.size(); ++i) {
          if (jettys.get(i).equals(jettyToStop)) {
            miniCluster.stopJettySolrRunner(i);
            assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());
          }
        }

        // now restore the original state so that this function could be called multiple times
        
        // re-create a server (to restore original NUM_SERVERS count)
        startedServer = miniCluster.startJettySolrRunner();
        assertTrue(startedServer.isRunning());
        assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());
        Thread.sleep(15000);
        try {
          cloudSolrClient.query(query);
          fail("Expected exception on query because collection should not be ready - we have turned on async core loading");
        } catch (SolrServerException e) {
          SolrException rc = (SolrException) e.getRootCause();
          assertTrue(rc.code() >= 500 && rc.code() < 600);
        } catch (SolrException e) {
          assertTrue(e.code() >= 500 && e.code() < 600);
        }

        doExtraTests(miniCluster, zkClient, zkStateReader,cloudSolrClient, collectionName);
        // delete the collection we created earlier
        miniCluster.deleteCollection(collectionName);
        AbstractDistribZkTestBase.waitForCollectionToDisappear(collectionName, zkStateReader, true, true, 330);
      }
    }
    finally {
      miniCluster.shutdown();
    }
  }

  protected void doExtraTests(MiniSolrCloudCluster miniCluster, SolrZkClient zkClient, ZkStateReader zkStateReader, CloudSolrClient cloudSolrClient,
                            String defaultCollName) throws Exception { /*do nothing*/ }

  @Test
  public void testErrorsInStartup() throws Exception {

    File solrXml = new File(SolrTestCaseJ4.TEST_HOME(), "solr-no-core.xml");
    final AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = null;
    try {
      cluster = new MiniSolrCloudCluster(3, createTempDir().toFile(), solrXml, JettyConfig.builder().build()) {
        @Override
        public JettySolrRunner startJettySolrRunner(JettyConfig config) throws Exception {
          if (jettyIndex.incrementAndGet() != 2)
            return super.startJettySolrRunner(config);
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

    File solrXml = new File(SolrTestCaseJ4.TEST_HOME(), "solr-no-core.xml");
    final AtomicInteger jettyIndex = new AtomicInteger();

    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(3, createTempDir().toFile(), solrXml, JettyConfig.builder().build()) {
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
  public void testExraFilters() throws Exception {
    File solrXml = new File(SolrTestCaseJ4.TEST_HOME(), "solr-no-core.xml");
    Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    jettyConfig.withFilter(JettySolrRunner.DebugFilter.class, "*");
    MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir().toFile(), solrXml, jettyConfig.build());
    cluster.shutdown();
  }

}
