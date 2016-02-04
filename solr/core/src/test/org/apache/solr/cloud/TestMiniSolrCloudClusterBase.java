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

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
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

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestMiniSolrCloudClusterBase extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected int NUM_SERVERS = 5;
  protected int NUM_SHARDS = 2;
  protected int REPLICATION_FACTOR = 2;

  public TestMiniSolrCloudClusterBase () {
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
    final String collectionName = "testSolrCloudCollection";
    testCollectionCreateSearchDelete(collectionName);
  }

  private MiniSolrCloudCluster createMiniSolrCloudCluster() throws Exception {
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    return new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), jettyConfig.build());
  }

  private void createCollection(MiniSolrCloudCluster miniCluster, String collectionName, String createNodeSet, String asyncId) throws Exception {
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

    miniCluster.createCollection(collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName, createNodeSet, asyncId, collectionProperties);
  }

  protected void testCollectionCreateSearchDelete(String collectionName) throws Exception {

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
      JettySolrRunner stoppedServer = miniCluster.stopJettySolrRunner(0);
      assertTrue(stoppedServer.isStopped());
      assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());

      // create a server
      JettySolrRunner startedServer = miniCluster.startJettySolrRunner();
      assertTrue(startedServer.isRunning());
      assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

      // create collection
      final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
      createCollection(miniCluster, collectionName, null, asyncId);
      if (asyncId != null) {
        final RequestStatusState state = AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(asyncId, 330,
            cloudSolrClient);
        assertSame("did not see async createCollection completion", RequestStatusState.COMPLETED, state);
      }

      try (SolrZkClient zkClient = new SolrZkClient
          (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
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

        doExtraTests(miniCluster, zkClient, zkStateReader,cloudSolrClient, collectionName);
      }
    }
    finally {
      miniCluster.shutdown();
    }
  }

  protected void doExtraTests(MiniSolrCloudCluster miniCluster, SolrZkClient zkClient, ZkStateReader zkStateReader, CloudSolrClient cloudSolrClient,
                              String defaultCollName) throws Exception { /*do nothing*/ }

}
