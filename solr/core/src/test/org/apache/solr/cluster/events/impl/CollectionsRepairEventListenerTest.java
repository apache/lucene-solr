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

import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.events.AllEventsListener;
import org.apache.solr.cluster.events.ClusterEvent;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class CollectionsRepairEventListenerTest extends SolrCloudTestCase {

  private static AllEventsListener eventsListener = new AllEventsListener();

  private static int NUM_NODES = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    cluster.getOpenOverseer().getCoreContainer().getClusterEventProducer()
        .registerListener(eventsListener, ClusterEvent.EventType.values());
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @Test
  public void testCollectionRepair() throws Exception {

  }
}
