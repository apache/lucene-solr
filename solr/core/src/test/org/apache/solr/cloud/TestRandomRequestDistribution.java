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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.TrackingShardHandlerFactory;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.Test;

/**
 * Asserts that requests aren't always sent to the same poor node. See SOLR-7493
 */
@SolrTestCaseJ4.SuppressSSL
public class TestRandomRequestDistribution extends AbstractFullDistribZkTestBase {

  @Test
  @BaseDistributedSearchTestCase.ShardsFixed(num = 3)
  public void testRequestTracking() throws Exception {
    waitForThingsToLevelOut(30);

    List<String> nodeNames = new ArrayList<>(3);
    for (CloudJettyRunner cloudJetty : cloudJettys) {
      nodeNames.add(cloudJetty.nodeName);
    }
    assertEquals(3, nodeNames.size());

    new CollectionAdminRequest.Create()
        .setCollectionName("a1x2")
        .setNumShards(1)
        .setReplicationFactor(2)
        .setCreateNodeSet(nodeNames.get(0) + ',' + nodeNames.get(1))
        .process(cloudClient);

    new CollectionAdminRequest.Create()
        .setCollectionName("b1x1")
        .setNumShards(1)
        .setReplicationFactor(1)
        .setCreateNodeSet(nodeNames.get(2))
        .process(cloudClient);

    waitForRecoveriesToFinish("a1x2", true);
    waitForRecoveriesToFinish("b1x1", true);

    cloudClient.getZkStateReader().updateClusterState(true);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection b1x1 = clusterState.getCollection("b1x1");
    Collection<Replica> replicas = b1x1.getSlice("shard1").getReplicas();
    assertEquals(1, replicas.size());
    String baseUrl = replicas.iterator().next().getStr(ZkStateReader.BASE_URL_PROP);
    if (!baseUrl.endsWith("/")) baseUrl += "/";
    HttpSolrClient client = new HttpSolrClient(baseUrl + "a1x2");
    client.setSoTimeout(5000);
    client.setConnectionTimeout(2000);

    log.info("Making requests to " + baseUrl + "a1x2");
    for (int i=0; i < 10; i++)  {
      client.query(new SolrQuery("*:*"));
    }

    Map<String, Integer> shardVsCount = new HashMap<>();
    for (JettySolrRunner runner : jettys) {
      CoreContainer container = ((SolrDispatchFilter) runner.getDispatchFilter().getFilter()).getCores();
      for (SolrCore core : container.getCores()) {
        SolrRequestHandler select = core.getRequestHandler("");
        long c = (long) select.getStatistics().get("requests");
        shardVsCount.put(core.getName(), (int) c);
      }
    }

    log.info("Shard count map = " + shardVsCount);

    for (Map.Entry<String, Integer> entry : shardVsCount.entrySet()) {
      assertTrue("Shard " + entry.getKey() + " received all 10 requests", entry.getValue() != 10);
    }
  }
}
