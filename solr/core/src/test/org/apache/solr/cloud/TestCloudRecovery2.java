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

import java.lang.invoke.MethodHandles;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCloudRecovery2 extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1,2)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
  }

  @Test
  public void test() throws Exception {
    JettySolrRunner node1 = cluster.getJettySolrRunner(0);
    JettySolrRunner node2 = cluster.getJettySolrRunner(1);
    try (HttpSolrClient client1 = getHttpSolrClient(node1.getBaseUrl().toString())) {

      node2.stop();
      waitForState("", COLLECTION, (liveNodes, collectionState) -> liveNodes.size() == 1);

      UpdateRequest req = new UpdateRequest();
      for (int i = 0; i < 100; i++) {
        req = req.add("id", i+"", "num", i+"");
      }
      req.commit(client1, COLLECTION);

      node2.start();
      waitForState("", COLLECTION, clusterShape(1, 2));

      try (HttpSolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        long numFound = client.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
        assertEquals(100, numFound);
      }
      long numFound = client1.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
      assertEquals(100, numFound);

      new UpdateRequest().add("id", "1", "num", "10")
          .commit(client1, COLLECTION);

      try (HttpSolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("10", v.toString());
      }
      Object v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("10", v.toString());

      //
      node2.stop();
      waitForState("", COLLECTION, (liveNodes, collectionState) -> liveNodes.size() == 1);

      new UpdateRequest().add("id", "1", "num", "20")
          .commit(client1, COLLECTION);
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("20", v.toString());

      node2.start();
      waitForState("", COLLECTION, clusterShape(1, 2));
      try (HttpSolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("20", v.toString());
      }

      node2.stop();
      waitForState("", COLLECTION, (liveNodes, collectionState) -> liveNodes.size() == 1);

      new UpdateRequest().add("id", "1", "num", "30")
          .commit(client1, COLLECTION);
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());

      node2.start();
      waitForState("", COLLECTION, clusterShape(1, 2));

      try (HttpSolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("30", v.toString());
      }
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }

    node1.stop();
    waitForState("", COLLECTION, (liveNodes, collectionState) -> {
      Replica leader = collectionState.getLeader("shard1");
      return leader != null && leader.getNodeName().equals(node2.getNodeName());
    });

    node1.start();
    waitForState("", COLLECTION, clusterShape(1, 2));
    try (HttpSolrClient client = getHttpSolrClient(node1.getBaseUrl().toString())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }
    try (HttpSolrClient client = getHttpSolrClient(node2.getBaseUrl().toString())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }

  }

}
