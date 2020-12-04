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

package org.apache.solr.cluster.events.impl;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.events.AllEventsListener;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;

/**
 *
 */
@LogLevel("org.apache.solr.cluster.events=DEBUG")
public class CollectionsRepairEventListenerTest extends SolrCloudTestCase {

  public static class CollectionsRepairWrapperListener implements ClusterEventListener, ClusterSingleton {
    final CollectionsRepairEventListener delegate;

    CountDownLatch completed = new CountDownLatch(1);

    CollectionsRepairWrapperListener(CoreContainer cc, int waitFor) throws Exception {
      delegate = new CollectionsRepairEventListener(cc);
      delegate.setWaitForSecond(waitFor);
    }

    @Override
    public void onEvent(ClusterEvent event) {
      delegate.onEvent(event);
      completed.countDown();
    }

    @Override
    public String getName() {
      return "wrapperListener";
    }

    @Override
    public void start() throws Exception {
      delegate.start();
    }

    @Override
    public State getState() {
      return delegate.getState();
    }

    @Override
    public void stop() {
      delegate.stop();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

  private static AllEventsListener eventsListener = new AllEventsListener();
  private static CollectionsRepairWrapperListener repairListener;

  private static int NUM_NODES = 3;
  private static int waitFor;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    PluginMeta plugin = new PluginMeta();
    plugin.klass = DefaultClusterEventProducer.class.getName();
    plugin.name = ClusterEventProducer.PLUGIN_NAME;
    V2Request req = new V2Request.Builder("/cluster/plugin")
        .withMethod(POST)
        .withPayload(Collections.singletonMap("add", plugin))
        .build();
    V2Response rsp = req.process(cluster.getSolrClient());
    assertNotNull(rsp);

    waitFor = 1 + random().nextInt(9);

    CoreContainer cc = cluster.getOpenOverseer().getCoreContainer();
    cc.getClusterEventProducer()
        .registerListener(eventsListener, ClusterEvent.EventType.values());
    repairListener = new CollectionsRepairWrapperListener(cc, waitFor);
    cc.getClusterEventProducer()
        .registerListener(repairListener, ClusterEvent.EventType.NODES_DOWN);
    repairListener.start();
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @Test
  public void testCollectionRepair() throws Exception {
    eventsListener.setExpectedType(ClusterEvent.EventType.COLLECTIONS_ADDED);
    String collection = "testCollectionRepair_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf", 1, 3);
    cluster.getSolrClient().request(create);
    cluster.waitForActiveCollection(collection, 1, 3);
    eventsListener.waitForExpectedEvent(10);
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
    eventsListener.waitForExpectedEvent(10);
    cluster.waitForActiveCollection(collection, 1, 2);

    Thread.sleep(TimeUnit.MILLISECONDS.convert(waitFor, TimeUnit.SECONDS));

    // wait for completed processing in the repair listener
    boolean await = repairListener.completed.await(60, TimeUnit.SECONDS);
    if (!await) {
      fail("Timeout waiting for the processing to complete");
    }
    cluster.waitForActiveCollection(collection, 1, 3);
  }
}
