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

package org.apache.solr.core;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrCoreProxyTest extends AbstractFullDistribZkTestBase {

  public SolrCoreProxyTest() {
    sliceCount = 1;
  }

  @Before
  public void setUp() throws Exception {
    System.setProperty(CoreContainer.SOLR_QUERY_AGGREGATOR, "true");
    super.setUp();
    createQueryNode();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty(CoreContainer.SOLR_QUERY_AGGREGATOR);
  }

  @Test
  @ShardsFixed(num = 1)
  public void testCreateProxyCore() throws Exception {
    String collectionName = "collection1";

    CoreContainer queryNodeContainer = getQueryNodeContainer();

    ClusterState clusterState = queryNodeContainer.getZkController().getClusterState();
    Set<String> queryNodes = clusterState.getLiveQueryNodes();
    Set<String> liveNodes = clusterState.getLiveNodes();

    assertTrue("Expected one but found." + queryNodes.size(), queryNodes.size() == 1);
    assertTrue("Query nodes should contain query node.", queryNodes.contains(queryNodeContainer.getZkController().getNodeName()));
    assertTrue("There must be some live nodes.", liveNodes.size() > 0);
    assertFalse("Query node should not register in live nodes.", liveNodes.contains(queryNodeContainer.getZkController().getNodeName()));
    assertTrue("Collection should have one slice.", clusterState.getCollection(collectionName).getSlices().size() == 1);

    SolrCore core = queryNodeContainer.getCore(collectionName);

    assertNotNull(core);
    assertTrue(core instanceof SolrCoreProxy);

    CoreDescriptor cd = core.getCoreDescriptor();
    assertEquals(collectionName, cd.getCollectionName());

    core.close();

    String queryNodeUrl = null;
    String ingestNodeUrl = null;
    for (JettySolrRunner jetty : jettys) {
      if (jetty.getCoreContainer().isQueryAggregator()) {
        queryNodeUrl = jetty.getBaseUrl().toString();
      } else {
        ingestNodeUrl = jetty.getBaseUrl().toString();
      }
    }
    assertNotNull(queryNodeUrl);
    assertNotNull(ingestNodeUrl);

    addDocs(ingestNodeUrl, collectionName, 10);
    //query through query node
    queryDocs(queryNodeUrl, collectionName, 10);

    try {
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");
      ClusterState preSplitClusterState = queryNodeContainer.getZkController().getClusterState();
      verifyCoreReloadAfterSchemaConfigUpdate(collectionName);
      verifyCollectionShardSplit(collectionName);
      verifyCollectionListenerInstalled(collectionName);
      verifyMetrics(collectionName);
      ClusterState postSplitClusterState = queryNodeContainer.getZkController().getClusterState();

      log.info("restarting zookeeper================================ ");
      WatchedEvent watchedEvent = new WatchedEvent(Watcher.Event.EventType.ChildWatchRemoved, Watcher.Event.KeeperState.Expired, "/solr/live_query_node");
      queryNodeContainer.getZkController().getZkClient().getConnectionManager().process(watchedEvent);
      log.info("restarted zookeeper ======================================================== ");

      ClusterState postZkReconnectClusterState = queryNodeContainer.getZkController().getClusterState();

      //collection state should be same
      assertTrue(postSplitClusterState.getCollection(collectionName).equals(postZkReconnectClusterState.getCollection(collectionName)));
      //collection object should be same
      DocCollection preC = postSplitClusterState.getCollection(collectionName);
      DocCollection postC = postZkReconnectClusterState.getCollection(collectionName);
      assertTrue( preC == postC);

      //add more docs after zk disconnect
      addDocs(ingestNodeUrl, collectionName, 10);
      //total results should be 20
      queryDocs(queryNodeUrl, collectionName, 20);

      verifyCollectionListenerInstalled(collectionName);

      //do again split and verify the data
      verifyCollectionShardSplitAfterZkDisconnect(collectionName);
      DocCollection postSplitC = queryNodeContainer.getZkController().getClusterState().getCollection(collectionName);
      assertTrue(!postC.equals(postSplitC));
      assertTrue(postC != postSplitC);
      //add more docs after split
      addDocs(ingestNodeUrl, collectionName, 10);
      //total results should be 30
      queryDocs(queryNodeUrl, collectionName, 30);

      verifyDeleteCollection(collectionName);
    } finally {
      System.clearProperty("solr.test.sys.prop1");
      System.clearProperty("solr.test.sys.prop2");
    }
  }

   @Test
  @ShardsFixed(num = 1)
  public void testQACollectionNotFoundIssue() throws Exception {
      String collectionName = "collection1";

      CoreContainer queryNodeContainer = getQueryNodeContainer();

      ClusterState clusterState = queryNodeContainer.getZkController().getClusterState();
      Set<String> queryNodes = clusterState.getLiveQueryNodes();
      Set<String> liveNodes = clusterState.getLiveNodes();

      assertTrue("Expected one but found." + queryNodes.size(), queryNodes.size() == 1);
      assertTrue("Query nodes should contain query node.", queryNodes.contains(queryNodeContainer.getZkController().getNodeName()));
      assertTrue("There must be some live nodes.", liveNodes.size() > 0);
      assertTrue("Collection should have one slice.", clusterState.getCollection(collectionName).getSlices().size() == 1);

      String queryNodeUrl = null;
      String ingestNodeUrl = null;
      for (JettySolrRunner jetty : jettys) {
        if (jetty.getCoreContainer().isQueryAggregator()) {
          queryNodeUrl = jetty.getBaseUrl().toString();
        } else {
          ingestNodeUrl = jetty.getBaseUrl().toString();
        }
      }
      assertNotNull(queryNodeUrl);
      assertNotNull(ingestNodeUrl);

      addDocs(ingestNodeUrl, collectionName, 10);
      //query through query node
      queryDocs(queryNodeUrl, collectionName, 10);

      try {
        System.setProperty("solr.test.sys.prop1", "propone");
        System.setProperty("solr.test.sys.prop2", "proptwo");

        //deleting the collection
        verifyDeleteCollection(collectionName);
        //create collection again with same name
        verifyCreateCollection(collectionName, 0);
        addDocs(ingestNodeUrl, collectionName, 10);
        //query through query node
        queryDocs(queryNodeUrl, collectionName, 10);

        //create new collection and verify
        String anotherCollection = "anothercollection";
        verifyCreateCollection(anotherCollection, 1);
        addDocs(ingestNodeUrl, anotherCollection, 10);
        //query through query node
        queryDocs(queryNodeUrl, anotherCollection, 10);
      } finally {
        System.clearProperty("solr.test.sys.prop1");
        System.clearProperty("solr.test.sys.prop2");
      }
    }

  private void verifyMetrics(String collectionName) {
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();
    SolrMetricManager smm = queryAggregatorContainer.getMetricManager();

    verifyOpMetrics(smm);
    verifyNoOpMetrics(collectionName, queryAggregatorContainer, smm);
  }

  private void verifyOpMetrics(SolrMetricManager smm) {
    SearchHandler sh = new SearchHandler();

    Meter noOpmeter = smm.meter(sh, "testregistry", "testmetric", "query");
    assertTrue(!(noOpmeter.getClass().toString().contains("NoOpMeter")));

    Histogram histogram = smm.histogram(sh, "testregistry", "testhitogram", "query");
    assertTrue(!(histogram.getClass().toString().contains("NoOpHistogram")));

    Timer timer = smm.timer(sh, "testregistry", "testtimer", "query");
    assertTrue(!(timer.getClass().toString().contains("NoOpTimer")));

    Counter counter = smm.counter(sh, "testregistry", "testcounter", "query");
    assertTrue(!(counter.getClass().toString().contains("NoOpCounter")));

    smm.registerGauge(sh, "testregistry", new Gauge<Long>() {

      @Override
      public Long getValue() {
        return (long)9;
      }
    }, "gauge", false,"testgauge", "query");

    Map<String, Metric> metricMap = smm.getMetrics("testregistry", MetricFilter.contains("testgauge"));
    assertTrue(metricMap.size() == 1);
    SolrMetricManager.GaugeWrapper<Long> gw = (SolrMetricManager.GaugeWrapper<Long> )metricMap.get("query.testgauge");
    assertTrue(!gw.getGauge().getClass().toString().contains("NoOpGauge"));
  }

  private void verifyNoOpMetrics(String collectionName, CoreContainer queryAggregatorContainer, SolrMetricManager smm) {
    SolrCore core = queryAggregatorContainer.getCore(collectionName);

    Meter noOpmeter = smm.meter(core, "testregistry", "testmetric", "core");
    assertTrue((noOpmeter.getClass().toString().contains("NoOpMeter")));

    Histogram histogram = smm.histogram(core, "testregistry", "testhitogram", "core");
    assertTrue((histogram.getClass().toString().contains("NoOpHistogram")));

    Timer timer = smm.timer(core, "testregistry", "testtimer", "core");
    assertTrue((timer.getClass().toString().contains("NoOpTimer")));

    Counter counter = smm.counter(core, "testregistry", "testcounter", "core");
    assertTrue((counter.getClass().toString().contains("NoOpCounter")));

    smm.registerGauge(core, "testregistry", new Gauge<Long>() {

      @Override
      public Long getValue() {
        return (long)9;
      }
    }, "gauge", false,"testgauge2", "core");

    Map<String, Metric> metricMap = smm.getMetrics("testregistry", MetricFilter.contains("testgauge2"));
    assertTrue(metricMap.size() == 1);
    SolrMetricManager.GaugeWrapper<Long> gw = (SolrMetricManager.GaugeWrapper<Long> )metricMap.get("core.testgauge2");
    assertTrue(gw.getGauge().getClass().toString().contains("NoOpGauge"));

    core.close();
  }


  private void verifyCoreReloadAfterSchemaConfigUpdate(final String collection) throws Exception {
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();
    SolrCore currentCore = queryAggregatorContainer.getCore(collection);
    currentCore.close();

    String update = "true";
    //update conf1 directory
    String zkConfigPath = "/configs/conf1";
    zkServer.getZkClient().setData(zkConfigPath, update.getBytes(), true);

    //now observe if unload happens.
    Thread.sleep(10000);
    SolrCore newCore = queryAggregatorContainer.getCore(collection);
    newCore.close();
    //core reference should be different
    assertTrue(currentCore == newCore);
  }

  private void verifyCollectionShardSplit(final String collection) throws Exception {
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();
    ClusterState clusterState = queryAggregatorContainer.getZkController().getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collection);

    assertTrue("Collection should have one slice", docCollection.getActiveSlices().size() == 1);

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collection);
    splitShard.setShardName("shard1");
    NamedList<Object> response = splitShard.process(cloudClient).getResponse();
    assertNotNull(response.get("success"));
    Thread.sleep(5000);
    clusterState = queryAggregatorContainer.getZkController().getZkStateReader().getClusterState();
    docCollection = clusterState.getCollection(collection);
    ClusterState.CollectionRef collectionRef = clusterState.getCollectionStates().get(collection);
    assertNotNull(collectionRef);
    assertFalse(collectionRef instanceof ZkStateReader.LazyCollectionRef);
    assertTrue("Collection now should have two slices", docCollection.getActiveSlices().size() == 2);
  }

  private void verifyCollectionShardSplitAfterZkDisconnect(final String collection) throws Exception {
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();
    ClusterState clusterState = queryAggregatorContainer.getZkController().getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collection);

    assertTrue("Collection should have one slice", docCollection.getActiveSlices().size() == 2);

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collection);
    splitShard.setShardName("shard1_0");
    NamedList<Object> response = splitShard.process(cloudClient).getResponse();
    assertNotNull(response.get("success"));
    Thread.sleep(5000);
    clusterState = queryAggregatorContainer.getZkController().getZkStateReader().getClusterState();
    docCollection = clusterState.getCollection(collection);
    ClusterState.CollectionRef collectionRef = clusterState.getCollectionStates().get(collection);
    assertNotNull(collectionRef);
    assertFalse(collectionRef instanceof ZkStateReader.LazyCollectionRef);
    assertTrue("Collection now should have two slices", docCollection.getActiveSlices().size() == 3);
  }

  private void verifyCollectionListenerInstalled(final String collection) throws Exception {
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();

    assertTrue("There should be one collection watcher.",
        queryAggregatorContainer.getZkController().getZkStateReader().getStateWatchers(collection).size() == 1);
  }

  private void verifyDeleteCollection(final String collection) throws Exception {
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collection);
    NamedList<Object> response = delete.process(cloudClient).getResponse();
    assertNotNull(response.get("success"));
    Thread.sleep(10000);
    CoreContainer queryAggregatorContainer = getQueryNodeContainer();
    String corenames = "";

    for (SolrCore core : queryAggregatorContainer.getCores()) {
      corenames += core.getName() + " , ";
    }
    assertTrue("There should not be any core. " + corenames, queryAggregatorContainer.getCores().isEmpty());
  }

    private void verifyCreateCollection(final String collection, int numCores) throws Exception {
      CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, 1, 1);
      NamedList<Object> response = create.process(cloudClient).getResponse();
      assertNotNull(response.get("success"));
      Thread.sleep(10000);
      CoreContainer queryAggregatorContainer = getQueryNodeContainer();

      assertTrue("There should not be any core. " , queryAggregatorContainer.getCores().size() == numCores);
    }

  private CoreContainer getQueryNodeContainer() {
    CoreContainer queryAggregatorContainer = null;
    for (JettySolrRunner jetty : jettys) {
      if (jetty.getCoreContainer().isQueryAggregator()) {
        queryAggregatorContainer = jetty.getCoreContainer();
      }
    }
    assertNotNull("There should be one query node container", queryAggregatorContainer);
    return queryAggregatorContainer;
  }

  private void addDocs(final String baseUrl, final String collection, int docs) throws Exception {
    Random rd = new Random();
    try (HttpSolrClient qclient = getHttpSolrClient(baseUrl)) {
      for (int i = 1; i <= docs; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", rd.nextInt());
        qclient.add(collection, doc);
        qclient.commit(collection);
      }
    }
  }

  private void queryDocs(final String baseUrl, final String collection, int docs) throws Exception {
    log.info("queryDocs ...");
    SolrQuery query = new SolrQuery("*:*");
    try (HttpSolrClient qclient = getHttpSolrClient(baseUrl)) {
      QueryResponse results = qclient.query(collection, query);
      assertEquals(docs, results.getResults().getNumFound());
    }
  }
}
