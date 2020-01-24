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

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the Collections API with the MiniSolrCloudCluster.
 */
@LuceneTestCase.Slow
public class TestCollectionsAPIViaSolrCloudCluster extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int numShards = 2;
  private static final int numReplicas = 2;
  private static final int maxShardsPerNode = 1;
  private static final int nodeCount = 5;
  private static final String configName = "solrCloudCollectionConfig";
  private static final Map<String,String> collectionProperties  // ensure indexes survive core shutdown
      = Collections.singletonMap("solr.directoryFactory", "solr.StandardDirectoryFactory");

  @Override
  public void setUp() throws Exception {
    configureCluster(nodeCount).addConfig(configName, configset("cloud-minimal")).configure();
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  private void createCollection(String collectionName, String createNodeSet) throws Exception {
    if (random().nextBoolean()) { // process asynchronously
      CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
          .setMaxShardsPerNode(maxShardsPerNode)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .processAndWait(cluster.getSolrClient(), 30);
    }
    else {
      CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
          .setMaxShardsPerNode(maxShardsPerNode)
          .setCreateNodeSet(createNodeSet)
          .setProperties(collectionProperties)
          .process(cluster.getSolrClient());

    }
    
    if (createNodeSet != null && createNodeSet.equals(OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY)) {
      cluster.waitForActiveCollection(collectionName, numShards, 0);
    } else {
      cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    }
  }

  @Test
  public void testCollectionCreateSearchDelete() throws Exception {
    final CloudSolrClient client = cluster.getSolrClient();
    final String collectionName = "testcollection";

    assertNotNull(cluster.getZkServer());
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    assertEquals(nodeCount, jettys.size());
    for (JettySolrRunner jetty : jettys) {
      assertTrue(jetty.isRunning());
    }

    // shut down a server
    JettySolrRunner stoppedServer = cluster.stopJettySolrRunner(0);
    
    cluster.waitForJettyToStop(stoppedServer);
    
    assertTrue(stoppedServer.isStopped());
    assertEquals(nodeCount - 1, cluster.getJettySolrRunners().size());

    // create a server
    JettySolrRunner startedServer = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue(startedServer.isRunning());
    assertEquals(nodeCount, cluster.getJettySolrRunners().size());

    // create collection
    createCollection(collectionName, null);

    // modify/query collection
    new UpdateRequest().add("id", "1").commit(client, collectionName);
    QueryResponse rsp = client.query(collectionName, new SolrQuery("*:*"));
    assertEquals(1, rsp.getResults().getNumFound());

    // remove a server not hosting any replicas
    ZkStateReader zkStateReader = client.getZkStateReader();
    zkStateReader.forceUpdateCollection(collectionName);
    ClusterState clusterState = zkStateReader.getClusterState();
    Map<String,JettySolrRunner> jettyMap = new HashMap<>();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String key = jetty.getBaseUrl().toString().substring((jetty.getBaseUrl().getProtocol() + "://").length());
      jettyMap.put(key, jetty);
    }
    Collection<Slice> slices = clusterState.getCollection(collectionName).getSlices();
    // track the servers not host replicas
    for (Slice slice : slices) {
      jettyMap.remove(slice.getLeader().getNodeName().replace("_solr", "/solr"));
      for (Replica replica : slice.getReplicas()) {
        jettyMap.remove(replica.getNodeName().replace("_solr", "/solr"));
      }
    }
    assertTrue("Expected to find a node without a replica", jettyMap.size() > 0);
    JettySolrRunner jettyToStop = jettyMap.entrySet().iterator().next().getValue();
    jettys = cluster.getJettySolrRunners();
    for (int i = 0; i < jettys.size(); ++i) {
      if (jettys.get(i).equals(jettyToStop)) {
        cluster.stopJettySolrRunner(i);
        assertEquals(nodeCount - 1, cluster.getJettySolrRunners().size());
      }
    }

    // re-create a server (to restore original nodeCount count)
    startedServer = cluster.startJettySolrRunner(jettyToStop);
    cluster.waitForAllNodes(30);
    assertTrue(startedServer.isRunning());
    assertEquals(nodeCount, cluster.getJettySolrRunners().size());

    CollectionAdminRequest.deleteCollection(collectionName).process(client);
    AbstractDistribZkTestBase.waitForCollectionToDisappear
        (collectionName, client.getZkStateReader(), true, true, 330);

    // create it again
    createCollection(collectionName, null);
    
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);

    // check that there's no left-over state
    assertEquals(0, client.query(collectionName, new SolrQuery("*:*")).getResults().getNumFound());

    // modify/query collection
    new UpdateRequest().add("id", "1").commit(client, collectionName);
    assertEquals(1, client.query(collectionName, new SolrQuery("*:*")).getResults().getNumFound());
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void testCollectionCreateWithoutCoresThenDelete() throws Exception {

    final String collectionName = "testSolrCloudCollectionWithoutCores";
    final CloudSolrClient client = cluster.getSolrClient();

    assertNotNull(cluster.getZkServer());
    assertFalse(cluster.getJettySolrRunners().isEmpty());

    // create collection
    createCollection(collectionName, OverseerCollectionMessageHandler.CREATE_NODE_SET_EMPTY);

    // check the collection's corelessness
    int coreCount = 0;
    DocCollection docCollection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    for (Map.Entry<String,Slice> entry : docCollection.getSlicesMap().entrySet()) {
      coreCount += entry.getValue().getReplicasMap().entrySet().size();
    }
    assertEquals(0, coreCount);

    // delete the collection
    CollectionAdminRequest.deleteCollection(collectionName).process(client);
    AbstractDistribZkTestBase.waitForCollectionToDisappear
        (collectionName, client.getZkStateReader(), true, true, 330);
  }

  @Test
  public void testStopAllStartAll() throws Exception {

    final String collectionName = "testStopAllStartAllCollection";
    final CloudSolrClient client = cluster.getSolrClient();

    assertNotNull(cluster.getZkServer());
    List<JettySolrRunner> jettys = new ArrayList<>(cluster.getJettySolrRunners()); // make a copy
    assertEquals(nodeCount, jettys.size());
    for (JettySolrRunner jetty : jettys) {
      assertTrue(jetty.isRunning());
    }

    final SolrQuery query = new SolrQuery("*:*");
    final SolrInputDocument doc = new SolrInputDocument();

    // create collection
    createCollection(collectionName, null);

    ZkStateReader zkStateReader = client.getZkStateReader();

    // modify collection
    final int numDocs = 1 + random().nextInt(10);
    for (int ii = 1; ii <= numDocs; ++ii) {
      doc.setField("id", ""+ii);
      client.add(collectionName, doc);
      if (ii*2 == numDocs) client.commit(collectionName);
    }
    client.commit(collectionName);

    // query collection
    assertEquals(numDocs, client.query(collectionName, query).getResults().getNumFound());

    // the test itself
    zkStateReader.forceUpdateCollection(collectionName);
    final ClusterState clusterState = zkStateReader.getClusterState();

    final Set<Integer> leaderIndices = new HashSet<>();
    final Set<Integer> followerIndices = new HashSet<>();
    {
      final Map<String,Boolean> shardLeaderMap = new HashMap<>();
      for (final Slice slice : clusterState.getCollection(collectionName).getSlices()) {
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
          leaderIndices.add(ii);
        } else if (Boolean.FALSE.equals(isLeader)) {
          followerIndices.add(ii);
        } // else neither leader nor follower i.e. node without a replica (for our collection)
      }
    }
    final List<Integer> leaderIndicesList = new ArrayList<>(leaderIndices);
    final List<Integer> followerIndicesList = new ArrayList<>(followerIndices);

    // first stop the followers (in no particular order)
    Collections.shuffle(followerIndicesList, random());
    for (Integer ii : followerIndicesList) {
      if (!leaderIndices.contains(ii)) {
        cluster.stopJettySolrRunner(jettys.get(ii));
      }
    }

    // then stop the leaders (again in no particular order)
    Collections.shuffle(leaderIndicesList, random());
    for (Integer ii : leaderIndicesList) {
      cluster.stopJettySolrRunner(jettys.get(ii));
    }

    // calculate restart order
    final List<Integer> restartIndicesList = new ArrayList<>();
    Collections.shuffle(leaderIndicesList, random());
    restartIndicesList.addAll(leaderIndicesList);
    Collections.shuffle(followerIndicesList, random());
    restartIndicesList.addAll(followerIndicesList);
    if (random().nextBoolean()) Collections.shuffle(restartIndicesList, random());

    // and then restart jettys in that order
    for (Integer ii : restartIndicesList) {
      final JettySolrRunner jetty = jettys.get(ii);
      if (!jetty.isRunning()) {
        cluster.startJettySolrRunner(jetty);
        assertTrue(jetty.isRunning());
      }
    }
    cluster.waitForAllNodes(30);
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);

    zkStateReader.forceUpdateCollection(collectionName);

    // re-query collection
    assertEquals(numDocs, client.query(collectionName, query).getResults().getNumFound());
  }
}
