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

package org.apache.solr.cluster.events;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.events.impl.DefaultClusterEventProducer;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;

/**
 *
 */
@LogLevel("org.apache.solr.cluster.events=DEBUG")
public class ClusterEventProducerTest extends SolrCloudTestCase {

  private AllEventsListener eventsListener;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Before
  public void setUp() throws Exception  {
    System.setProperty("enable.packages", "true");
    super.setUp();
    cluster.deleteAllCollections();
    eventsListener = new AllEventsListener();
    cluster.getOpenOverseer().getCoreContainer().getClusterEventProducer().registerListener(eventsListener);
  }

  @After
  public void teardown() throws Exception {
    System.clearProperty("enable.packages");
    if (eventsListener != null) {
      cluster.getOpenOverseer().getCoreContainer().getClusterEventProducer().unregisterListener(eventsListener);
      eventsListener.events.clear();
    }
    V2Request readPluginState = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .withMethod(GET)
        .build();
    V2Response rsp = readPluginState.process(cluster.getSolrClient());
    if (rsp._getStr("/plugin/" + ClusterEventProducer.PLUGIN_NAME + "/class", null) != null) {
      V2Request req = new V2Request.Builder("/cluster/plugin")
          .withMethod(POST)
          .withPayload(Collections.singletonMap("remove", ClusterEventProducer.PLUGIN_NAME))
          .build();
      req.process(cluster.getSolrClient());
    }
  }

  @Test
  public void testEvents() throws Exception {
    PluginMeta plugin = new PluginMeta();
    plugin.klass = DefaultClusterEventProducer.class.getName();
    plugin.name = ClusterEventProducer.PLUGIN_NAME;
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .withMethod(POST)
        .withPayload(Collections.singletonMap("add", plugin))
        .build();
    V2Response rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());

    // NODES_DOWN

    eventsListener.setExpectedType(ClusterEvent.EventType.NODES_DOWN);

    // don't kill Overseer
    JettySolrRunner nonOverseerJetty = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (cluster.getOpenOverseer().getCoreContainer().getZkController().getNodeName().equals(jetty.getNodeName())) {
        continue;
      }
      nonOverseerJetty = jetty;
      break;
    }
    String nodeName = nonOverseerJetty.getNodeName();
    cluster.stopJettySolrRunner(nonOverseerJetty);
    cluster.waitForJettyToStop(nonOverseerJetty);
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be NODES_DOWN events", eventsListener.events.get(ClusterEvent.EventType.NODES_DOWN));
    List<ClusterEvent> events = eventsListener.events.get(ClusterEvent.EventType.NODES_DOWN);
    assertEquals("should be one NODES_DOWN event", 1, events.size());
    ClusterEvent event = events.get(0);
    assertEquals("should be NODES_DOWN event type", ClusterEvent.EventType.NODES_DOWN, event.getType());
    NodesDownEvent nodesDown = (NodesDownEvent) event;
    assertEquals("should be node " + nodeName, nodeName, nodesDown.getNodeNames().next());

    // NODES_UP
    eventsListener.setExpectedType(ClusterEvent.EventType.NODES_UP);
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForNode(newNode, 60);
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be NODES_UP events", eventsListener.events.get(ClusterEvent.EventType.NODES_UP));
    events = eventsListener.events.get(ClusterEvent.EventType.NODES_UP);
    assertEquals("should be one NODES_UP event", 1, events.size());
    event = events.get(0);
    assertEquals("should be NODES_UP event type", ClusterEvent.EventType.NODES_UP, event.getType());
    NodesUpEvent nodesUp = (NodesUpEvent) event;
    assertEquals("should be node " + newNode.getNodeName(), newNode.getNodeName(), nodesUp.getNodeNames().next());

    // COLLECTIONS_ADDED
    eventsListener.setExpectedType(ClusterEvent.EventType.COLLECTIONS_ADDED);
    String collection = "testNodesEvent_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf", 1, 1);
    cluster.getSolrClient().request(create);
    cluster.waitForActiveCollection(collection, 1, 1);
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be COLLECTIONS_ADDED events", eventsListener.events.get(ClusterEvent.EventType.COLLECTIONS_ADDED));
    events = eventsListener.events.get(ClusterEvent.EventType.COLLECTIONS_ADDED);
    assertEquals("should be one COLLECTIONS_ADDED event", 1, events.size());
    event = events.get(0);
    assertEquals("should be COLLECTIONS_ADDED event type", ClusterEvent.EventType.COLLECTIONS_ADDED, event.getType());
    CollectionsAddedEvent collectionsAdded = (CollectionsAddedEvent) event;
    assertEquals("should be collection " + collection, collection, collectionsAdded.getCollectionNames().next());

    // COLLECTIONS_REMOVED
    eventsListener.setExpectedType(ClusterEvent.EventType.COLLECTIONS_REMOVED);
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collection);
    cluster.getSolrClient().request(delete);
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be COLLECTIONS_REMOVED events", eventsListener.events.get(ClusterEvent.EventType.COLLECTIONS_REMOVED));
    events = eventsListener.events.get(ClusterEvent.EventType.COLLECTIONS_REMOVED);
    assertEquals("should be one COLLECTIONS_REMOVED event", 1, events.size());
    event = events.get(0);
    assertEquals("should be COLLECTIONS_REMOVED event type", ClusterEvent.EventType.COLLECTIONS_REMOVED, event.getType());
    CollectionsRemovedEvent collectionsRemoved = (CollectionsRemovedEvent) event;
    assertEquals("should be collection " + collection, collection, collectionsRemoved.getCollectionNames().next());

    // CLUSTER_CONFIG_CHANGED
    eventsListener.events.clear();
    eventsListener.setExpectedType(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED);
    ClusterProperties clusterProperties = new ClusterProperties(cluster.getZkClient());
    Map<String, Object> oldProps = new HashMap<>(clusterProperties.getClusterProperties());
    clusterProperties.setClusterProperty("ext.foo", "bar");
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be CLUSTER_CONFIG_CHANGED events", eventsListener.events.get(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED));
    events = eventsListener.events.get(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED);
    assertEquals("should be one CLUSTER_CONFIG_CHANGED event", 1, events.size());
    event = events.get(0);
    assertEquals("should be CLUSTER_CONFIG_CHANGED event type", ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED, event.getType());
    ClusterPropertiesChangedEvent propertiesChanged = (ClusterPropertiesChangedEvent) event;
    Map<String, Object> newProps = propertiesChanged.getNewClusterProperties();
    assertEquals("new properties wrong value of the 'ext.foo' property: " + newProps,
        "bar", newProps.get("ext.foo"));

    // unset the property
    eventsListener.events.clear();
    eventsListener.setExpectedType(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED);
    clusterProperties.setClusterProperty("ext.foo", null);
    eventsListener.waitForExpectedEvent(30);
    assertNotNull("should be CLUSTER_CONFIG_CHANGED events", eventsListener.events.get(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED));
    events = eventsListener.events.get(ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED);
    assertEquals("should be one CLUSTER_CONFIG_CHANGED event", 1, events.size());
    event = events.get(0);
    assertEquals("should be CLUSTER_CONFIG_CHANGED event type", ClusterEvent.EventType.CLUSTER_PROPERTIES_CHANGED, event.getType());
    propertiesChanged = (ClusterPropertiesChangedEvent) event;
    assertEquals("new properties should not have 'ext.foo' property: " + propertiesChanged.getNewClusterProperties(),
        null, propertiesChanged.getNewClusterProperties().get("ext.foo"));

  }

  private static CountDownLatch dummyEventLatch = new CountDownLatch(1);
  private static ClusterEvent lastEvent = null;

  public static class DummyEventListener implements ClusterEventListener, ClusterSingleton {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    State state = State.STOPPED;
    @Override
    public void onEvent(ClusterEvent event) {
      if (state != State.RUNNING) {
        if (log.isDebugEnabled()) {
          log.debug("skipped event, not running: {}", event);
        }
        return;
      }
      if (event.getType() == ClusterEvent.EventType.COLLECTIONS_ADDED ||
          event.getType() == ClusterEvent.EventType.COLLECTIONS_REMOVED) {
        if (log.isDebugEnabled()) {
          log.debug("recorded event {}", Utils.toJSONString(event));
        }
        lastEvent = event;
        dummyEventLatch.countDown();
      } else {
        if (log.isDebugEnabled()) {
          log.debug("skipped event, wrong type: {}", event.getType());
        }
      }
    }

    @Override
    public String getName() {
      return "dummy";
    }

    @Override
    public void start() throws Exception {
      if (log.isDebugEnabled()) {
        log.debug("starting {}", Integer.toHexString(hashCode()));
      }
      state = State.RUNNING;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public void stop() {
      if (log.isDebugEnabled()) {
        log.debug("stopping {}", Integer.toHexString(hashCode()));
      }
      state = State.STOPPED;
    }

    @Override
    public void close() throws IOException {
      if (log.isDebugEnabled()) {
        log.debug("closing {}", Integer.toHexString(hashCode()));
      }
    }
  }

  @Test
  public void testListenerPlugins() throws Exception {
    PluginMeta plugin = new PluginMeta();
    plugin.klass = DefaultClusterEventProducer.class.getName();
    plugin.name = ClusterEventProducer.PLUGIN_NAME;
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .withMethod(POST)
        .withPayload(Collections.singletonMap("add", plugin))
        .build();
    V2Response rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());

    plugin = new PluginMeta();
    plugin.name = "testplugin";
    plugin.klass = DummyEventListener.class.getName();
    req = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .withMethod(POST)
        .withPayload(singletonMap("add", plugin))
        .build();
    rsp = req.process(cluster.getSolrClient());
    //just check if the plugin is indeed registered
    V2Request readPluginState = new V2Request.Builder("/cluster/plugin")
        .forceV2(true)
        .withMethod(GET)
        .build();
    rsp = readPluginState.process(cluster.getSolrClient());
    assertEquals(DummyEventListener.class.getName(), rsp._getStr("/plugin/testplugin/class", null));

    String collection = "testListenerPlugins_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf", 1, 1);
    cluster.getSolrClient().request(create);
    cluster.waitForActiveCollection(collection, 1, 1);
    boolean await = dummyEventLatch.await(30, TimeUnit.SECONDS);
    if (!await) {
      fail("Timed out waiting for COLLECTIONS_ADDED event, " + collection);
    }
    assertNotNull("lastEvent should be COLLECTIONS_ADDED", lastEvent);
    assertEquals("lastEvent should be COLLECTIONS_ADDED", ClusterEvent.EventType.COLLECTIONS_ADDED, lastEvent.getType());
    // verify timestamp
    Instant now = Instant.now();
    assertTrue("timestamp of the event is in the future", now.isAfter(lastEvent.getTimestamp()));
    assertEquals(collection, ((CollectionsAddedEvent)lastEvent).getCollectionNames().next());

    dummyEventLatch = new CountDownLatch(1);
    lastEvent = null;

    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collection);
    cluster.getSolrClient().request(delete);
    await = dummyEventLatch.await(30, TimeUnit.SECONDS);
    if (!await) {
      fail("Timed out waiting for COLLECTIONS_REMOVED event, " + collection);
    }
    assertNotNull("lastEvent should be COLLECTIONS_REMOVED", lastEvent);
    assertEquals("lastEvent should be COLLECTIONS_REMOVED", ClusterEvent.EventType.COLLECTIONS_REMOVED, lastEvent.getType());
    // verify timestamp
    now = Instant.now();
    assertTrue("timestamp of the event is in the future", now.isAfter(lastEvent.getTimestamp()));
    assertEquals(collection, ((CollectionsRemovedEvent)lastEvent).getCollectionNames().next());

    // test changing the ClusterEventProducer plugin dynamically

    // remove the plugin (a NoOpProducer will be used instead)
    req = new V2Request.Builder("/cluster/plugin")
        .withMethod(POST)
        .withPayload(Collections.singletonMap("remove", ClusterEventProducer.PLUGIN_NAME))
        .build();
    req.process(cluster.getSolrClient());

    dummyEventLatch = new CountDownLatch(1);
    lastEvent = null;
    // should not receive any events now
    cluster.getSolrClient().request(create);
    cluster.waitForActiveCollection(collection, 1, 1);
    await = dummyEventLatch.await(5, TimeUnit.SECONDS);
    if (await) {
      fail("should not receive any events but got " + lastEvent);
    }
    // reinstall the plugin
    plugin = new PluginMeta();
    plugin.klass = DefaultClusterEventProducer.class.getName();
    plugin.name = ClusterEventProducer.PLUGIN_NAME;
    req = new V2Request.Builder("/cluster/plugin")
        .withMethod(POST)
        .withPayload(Collections.singletonMap("add", plugin))
        .build();
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());

    dummyEventLatch = new CountDownLatch(1);
    lastEvent = null;

    cluster.getSolrClient().request(delete);
    await = dummyEventLatch.await(30, TimeUnit.SECONDS);
    if (!await) {
      fail("Timed out waiting for COLLECTIONS_REMOVED event, " + collection);
    }
    assertNotNull("lastEvent should be COLLECTIONS_REMOVED", lastEvent);
    assertEquals("lastEvent should be COLLECTIONS_REMOVED", ClusterEvent.EventType.COLLECTIONS_REMOVED, lastEvent.getType());
  }
}
