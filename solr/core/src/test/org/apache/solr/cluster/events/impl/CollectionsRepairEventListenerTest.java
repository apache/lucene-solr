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
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.events.AllEventsListener;
import org.apache.solr.cluster.events.ClusterEvent;
import org.apache.solr.cluster.events.ClusterEventListener;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CollectionsRepairEventListenerTest extends SolrCloudTestCase {

  public static class CollectionsRepairWrapperListener implements ClusterEventListener, ClusterSingleton {
    final CollectionsRepairEventListener delegate;

    CountDownLatch completed = new CountDownLatch(1);

    CollectionsRepairWrapperListener(CoreContainer cc) throws Exception {
      delegate = new CollectionsRepairEventListener(cc);
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
  }

  private static AllEventsListener eventsListener = new AllEventsListener();
  private static CollectionsRepairWrapperListener repairListener;

  private static int NUM_NODES = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    CoreContainer cc = cluster.getOpenOverseer().getCoreContainer();
    cc.getClusterEventProducer()
        .registerListener(eventsListener, ClusterEvent.EventType.values());
    repairListener = new CollectionsRepairWrapperListener(cc);
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

    // wait for completed processing in the repair listener
    boolean await = repairListener.completed.await(60, TimeUnit.SECONDS);
    if (!await) {
      fail("Timeout waiting for the processing to complete");
    }
    cluster.waitForActiveCollection(collection, 1, 3);
  }
}
