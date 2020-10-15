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
package org.apache.solr.cloud.autoscaling;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AtomicDouble;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkDistributedQueueFactory;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class SearchRateTriggerTest extends SolrCloudTestCase {
  private static final String PREFIX = SearchRateTriggerTest.class.getSimpleName() + "-";
  private static final String COLL1 = PREFIX + "collection1";
  private static final String COLL2 = PREFIX + "collection2";

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the listener to fire on first run!");
    return true;
  };

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");

  }

  @Before
  public void removeCollections() throws Exception {
    configureCluster(4)
    .addConfig("conf", configset("cloud-minimal"))
    .configure();
  }
  
  @After
  public void after() throws Exception {
    shutdownCluster();
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  @SuppressWarnings({"unchecked"})
  public void testTrigger() throws Exception {
    JettySolrRunner targetNode = cluster.getJettySolrRunner(0);
    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();
    SolrResourceLoader loader = targetNode.getCoreContainer().getResourceLoader();
    CoreContainer container = targetNode.getCoreContainer();
    SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), cluster.getSolrClient());

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 2, 2);
    CloudSolrClient solrClient = cluster.getSolrClient();
    create.setMaxShardsPerNode(1);
    create.process(solrClient);
    create = CollectionAdminRequest.createCollection(COLL2,
        "conf", 2, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS, clusterShape(2, 2));
    CloudUtil.waitForState(cloudManager, COLL2, 60, TimeUnit.SECONDS, clusterShape(2, 2));

    double rate = 1.0;
    URL baseUrl = targetNode.getBaseUrl();
    long waitForSeconds = 5 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(Arrays.asList(COLL1, COLL2), waitForSeconds, rate, -1);
    final List<TriggerEvent> events = new ArrayList<>();

    try (SearchRateTrigger trigger = new SearchRateTrigger("search_rate_trigger")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();
      trigger.setProcessor(event -> events.add(event));

      // generate replica traffic
      String coreName = container.getLoadedCoreNames().iterator().next();
      String url = baseUrl.toString() + "/" + coreName;
      try (HttpSolrClient simpleClient = new HttpSolrClient.Builder(url).build()) {
        SolrParams query = params(CommonParams.Q, "*:*", CommonParams.DISTRIB, "false");
        for (int i = 0; i < 130; i++) {
          simpleClient.query(query);
        }
        String registryCoreName = coreName.replaceFirst("_", ".").replaceFirst("_", ".");
        SolrMetricManager manager = targetNode.getCoreContainer().getMetricManager();
        MetricRegistry registry = manager.registry("solr.core."+registryCoreName);
        TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        // If we getting the rate too early, it will return 0
        timeOut.waitFor("Timeout waiting for rate is not zero",
            () -> registry.timer("QUERY./select.requestTimes").getOneMinuteRate()!=0.0);
        trigger.run();
        // waitFor delay
        assertEquals(0, events.size());
        Thread.sleep(waitForSeconds * 1000);
        // should generate replica event
        trigger.run();
        assertEquals(1, events.size());
        TriggerEvent event = events.get(0);
        assertEquals(TriggerEventType.SEARCHRATE, event.eventType);
        List<ReplicaInfo> infos = (List<ReplicaInfo>)event.getProperty(SearchRateTrigger.HOT_REPLICAS);
        assertEquals(1, infos.size());
        ReplicaInfo info = infos.get(0);
        assertEquals(coreName, info.getCore());
        assertTrue((Double)info.getVariable(AutoScalingParams.RATE) > rate);
      }
      // close that jetty to remove the violation - alternatively wait for 1 min...
      JettySolrRunner j = cluster.stopJettySolrRunner(1);
      cluster.waitForJettyToStop(j);
      events.clear();
      SolrParams query = params(CommonParams.Q, "*:*");
      for (int i = 0; i < 130; i++) {
        solrClient.query(COLL1, query);
      }
      Thread.sleep(waitForSeconds * 1000);
      trigger.run();
      // should generate collection event
      assertEquals(1, events.size());
      TriggerEvent event = events.get(0);
      Map<String, Double> hotCollections = (Map<String, Double>)event.getProperty(SearchRateTrigger.HOT_COLLECTIONS);
      assertEquals(1, hotCollections.size());
      Double Rate = hotCollections.get(COLL1);
      assertNotNull(Rate);
      assertTrue(Rate > rate);
      events.clear();

      for (int i = 0; i < 150; i++) {
        solrClient.query(COLL2, query);
        solrClient.query(COLL1, query);
      }
      Thread.sleep(waitForSeconds * 1000);
      trigger.run();
      // should generate collection event but not for COLL2 because of waitFor
      assertEquals(1, events.size());
      event = events.get(0);
      Map<String, Double> hotNodes = (Map<String, Double>)event.getProperty(SearchRateTrigger.HOT_NODES);
      assertTrue("hotNodes", hotNodes.isEmpty());
      hotNodes.forEach((n, r) -> assertTrue(n, r > rate));
      hotCollections = (Map<String, Double>)event.getProperty(SearchRateTrigger.HOT_COLLECTIONS);
      assertEquals(1, hotCollections.size());
      Rate = hotCollections.get(COLL1);
      assertNotNull(Rate);

      events.clear();
      // assert that waitFor prevents new events from being generated
      trigger.run();
      // should not generate any events
      assertEquals(0, events.size());

      Thread.sleep(waitForSeconds * 1000 * 2);
      trigger.run();
      // should generate collection event
      assertEquals(1, events.size());
      event = events.get(0);
      hotCollections = (Map<String, Double>)event.getProperty(SearchRateTrigger.HOT_COLLECTIONS);
      assertEquals(2, hotCollections.size());
      Rate = hotCollections.get(COLL1);
      assertNotNull(Rate);
      Rate = hotCollections.get(COLL2);
      assertNotNull(Rate);
      hotNodes = (Map<String, Double>)event.getProperty(SearchRateTrigger.HOT_NODES);
      assertTrue("hotNodes", hotNodes.isEmpty());
    }
  }

  private static final AtomicDouble mockRate = new AtomicDouble();

  @Test
  @SuppressWarnings({"unchecked"})
  public void testWaitForElapsed() throws Exception {
    SolrResourceLoader loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    CloudSolrClient solrClient = cluster.getSolrClient();
    SolrZkClient zkClient = solrClient.getZkStateReader().getZkClient();
    SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), solrClient) {
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new SolrClientNodeStateProvider(solrClient) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            Map<String, Object> values = super.getNodeValues(node, tags);
            values.keySet().forEach(k -> {
              values.replace(k, mockRate.get());
            });
            return values;
          }
        };
      }
    };
    TimeSource timeSource = cloudManager.getTimeSource();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 2, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS, clusterShape(2, 4));

    long waitForSeconds = 5 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(Arrays.asList(COLL1, COLL2), waitForSeconds, 1.0, 0.1);
    final List<TriggerEvent> events = new ArrayList<>();

    try (SearchRateTrigger trigger = new SearchRateTrigger("search_rate_trigger1")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();
      trigger.setProcessor(event -> events.add(event));

      // set mock rates
      mockRate.set(2.0);
      TimeOut timeOut = new TimeOut(waitForSeconds + 2, TimeUnit.SECONDS, timeSource);
      // simulate ScheduledTriggers
      while (!timeOut.hasTimedOut()) {
        trigger.run();
        timeSource.sleep(1000);
      }
      // violation persisted longer than waitFor - there should be events
      assertTrue(events.toString(), events.size() > 0);
      TriggerEvent event = events.get(0);
      assertEquals(event.toString(), TriggerEventType.SEARCHRATE, event.eventType);
      Map<String, Object> hotNodes, hotCollections, hotShards;
      List<ReplicaInfo> hotReplicas;
      hotNodes = (Map<String, Object>)event.properties.get(SearchRateTrigger.HOT_NODES);
      hotCollections = (Map<String, Object>)event.properties.get(SearchRateTrigger.HOT_COLLECTIONS);
      hotShards = (Map<String, Object>)event.properties.get(SearchRateTrigger.HOT_SHARDS);
      hotReplicas = (List<ReplicaInfo>)event.properties.get(SearchRateTrigger.HOT_REPLICAS);
      assertTrue("no hot nodes?", hotNodes.isEmpty());
      assertFalse("no hot collections?", hotCollections.isEmpty());
      assertFalse("no hot shards?", hotShards.isEmpty());
      assertFalse("no hot replicas?", hotReplicas.isEmpty());
    }

    mockRate.set(0.0);
    events.clear();

    try (SearchRateTrigger trigger = new SearchRateTrigger("search_rate_trigger2")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();
      trigger.setProcessor(event -> events.add(event));

      mockRate.set(2.0);
      trigger.run();
      // waitFor not elapsed
      assertTrue(events.toString(), events.isEmpty());
      Thread.sleep(1000);
      trigger.run();
      assertTrue(events.toString(), events.isEmpty());
      Thread.sleep(1000);
      mockRate.set(0.0);
      trigger.run();
      Thread.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds - 2, TimeUnit.SECONDS));
      trigger.run();

      // violations persisted shorter than waitFor - there should be no events
      assertTrue(events.toString(), events.isEmpty());

    }
  }

  @Test
  public void testDefaultsAndBackcompat() throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put("rate", 1.0);
    props.put("collection", "test");
    SolrResourceLoader loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();
    SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), cluster.getSolrClient());
    try (SearchRateTrigger trigger = new SearchRateTrigger("search_rate_trigger2")) {
      trigger.configure(loader, cloudManager, props);
      Map<String, Object> config = trigger.getConfig();
      @SuppressWarnings({"unchecked"})
      Set<String> collections = (Set<String>)config.get(SearchRateTrigger.COLLECTIONS_PROP);
      assertEquals(collections.toString(), 1, collections.size());
      assertEquals("test", collections.iterator().next());
      assertEquals("#ANY", config.get(AutoScalingParams.SHARD));
      assertEquals("#ANY", config.get(AutoScalingParams.NODE));
      assertEquals(1.0, config.get(SearchRateTrigger.ABOVE_RATE_PROP));
      assertEquals(-1.0, config.get(SearchRateTrigger.BELOW_RATE_PROP));
      assertEquals(SearchRateTrigger.DEFAULT_METRIC, config.get(SearchRateTrigger.METRIC_PROP));
      assertEquals(SearchRateTrigger.DEFAULT_MAX_OPS, config.get(SearchRateTrigger.MAX_OPS_PROP));
      assertNull(config.get(SearchRateTrigger.MIN_REPLICAS_PROP));
      assertEquals(CollectionParams.CollectionAction.ADDREPLICA, config.get(SearchRateTrigger.ABOVE_OP_PROP));
      assertEquals(CollectionParams.CollectionAction.MOVEREPLICA, config.get(SearchRateTrigger.ABOVE_NODE_OP_PROP));
      assertEquals(CollectionParams.CollectionAction.DELETEREPLICA, config.get(SearchRateTrigger.BELOW_OP_PROP));
      assertNull(config.get(SearchRateTrigger.BELOW_NODE_OP_PROP));
    }
  }

  private Map<String, Object> createTriggerProps(List<String> collections, long waitForSeconds, double aboveRate, double belowRate) {
    Map<String, Object> props = new HashMap<>();
    props.put("aboveRate", aboveRate);
    props.put("belowRate", belowRate);
    props.put("event", "searchRate");
    props.put("waitFor", waitForSeconds);
    props.put("enabled", true);
    if (collections != null && !collections.isEmpty()) {
      props.put("collections", String.join(",", collections));
    }
    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
