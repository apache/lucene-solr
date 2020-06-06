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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.junit.Test;

/**
 * Test for {@link org.apache.solr.handler.component.TrackingShardHandlerFactory}
 * See SOLR-7147 for more information
 */
@SolrTestCaseJ4.SuppressSSL
public class TestTrackingShardHandlerFactory extends AbstractFullDistribZkTestBase {

  public TestTrackingShardHandlerFactory() {
    schemaString = "schema15.xml"; // we need a string id
  }

  @Override
  protected String getSolrXml() {
    return "solr-trackingshardhandler.xml";
  }

  @Test
  @BaseDistributedSearchTestCase.ShardsFixed(num = 2)
  public void testRequestTracking() throws Exception {
    String collectionName = "testTwoPhase";

    List<JettySolrRunner> runners = new ArrayList<>(jettys);
    runners.add(controlJetty);

    TrackingShardHandlerFactory.RequestTrackingQueue trackingQueue = new TrackingShardHandlerFactory.RequestTrackingQueue();
    TrackingShardHandlerFactory.setTrackingQueue(runners, trackingQueue);

    for (JettySolrRunner runner : runners) {
      CoreContainer container = runner.getCoreContainer();
      ShardHandlerFactory factory = container.getShardHandlerFactory();
      assert factory instanceof TrackingShardHandlerFactory;
      @SuppressWarnings("resource")
      TrackingShardHandlerFactory trackingShardHandlerFactory = (TrackingShardHandlerFactory) factory;
      assertSame(trackingQueue, trackingShardHandlerFactory.getTrackingQueue());
    }

    createCollection(collectionName, "conf1", 2, 1, 1);

    waitForRecoveriesToFinish(collectionName, true);

    List<TrackingShardHandlerFactory.ShardRequestAndParams> coreAdminRequests = trackingQueue.getCoreAdminRequests();
    assertNotNull(coreAdminRequests);
    assertEquals("Unexpected number of core admin requests were found", 2, coreAdminRequests.size());

    CloudSolrClient client = cloudClient;

    client.setDefaultCollection(collectionName);
        /*
        hash of b is 95de7e03 high bits=2 shard=shard1
        hash of e is 656c4367 high bits=1 shard=shard2
         */
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", (i % 2 == 0 ? "b!" : "e!") + i);
      doc.addField("a_i", i);
      doc.addField("a_t", "text_" + i);
      client.add(doc);
    }
    client.commit();

    client.query(new SolrQuery("*:*"));

    TrackingShardHandlerFactory.ShardRequestAndParams getTopIdsRequest = trackingQueue.getShardRequestByPurpose(client.getZkStateReader(), collectionName, "shard1", ShardRequest.PURPOSE_GET_TOP_IDS);
    assertNotNull(getTopIdsRequest);
    getTopIdsRequest = trackingQueue.getShardRequestByPurpose(client.getZkStateReader(), collectionName, "shard2", ShardRequest.PURPOSE_GET_TOP_IDS);
    assertNotNull(getTopIdsRequest);

    TrackingShardHandlerFactory.ShardRequestAndParams getFieldsRequest = trackingQueue.getShardRequestByPurpose(client.getZkStateReader(), collectionName, "shard1", ShardRequest.PURPOSE_GET_FIELDS);
    assertNotNull(getFieldsRequest);
    getFieldsRequest = trackingQueue.getShardRequestByPurpose(client.getZkStateReader(), collectionName, "shard2", ShardRequest.PURPOSE_GET_FIELDS);
    assertNotNull(getFieldsRequest);

    int numRequests = 0;
    Map<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> allRequests = trackingQueue.getAllRequests();
    for (Map.Entry<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> entry : allRequests.entrySet()) {
      numRequests += entry.getValue().size();
    }
    // 4 shard requests + 2 core admin requests (invoked by create collection API)
    assertEquals("Total number of requests do not match expected", 6, numRequests);

    // reset
    TrackingShardHandlerFactory.setTrackingQueue(runners, null);

    for (JettySolrRunner runner : runners) {
      CoreContainer container = runner.getCoreContainer();
      ShardHandlerFactory factory = container.getShardHandlerFactory();
      assert factory instanceof TrackingShardHandlerFactory;
      @SuppressWarnings("resource")
      TrackingShardHandlerFactory trackingShardHandlerFactory = (TrackingShardHandlerFactory) factory;
      assertFalse(trackingShardHandlerFactory.isTracking());
    }

    // make another request and verify
    client.query(new SolrQuery("*:*"));
    numRequests = 0;
    allRequests = trackingQueue.getAllRequests();
    for (Map.Entry<String, List<TrackingShardHandlerFactory.ShardRequestAndParams>> entry : allRequests.entrySet()) {
      numRequests += entry.getValue().size();
    }
    // should still be 6
    assertEquals("Total number of shard requests do not match expected", 6, numRequests);
  }
}
