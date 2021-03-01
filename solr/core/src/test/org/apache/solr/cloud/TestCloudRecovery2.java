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
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.Replica;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.SuppressCodecs({"MockRandom", "Direct", "SimpleText"})
@LuceneTestCase.Nightly
public class TestCloudRecovery2 extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void beforeTestCloudRecovery2() throws Exception {
    useFactory(null);
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig("config", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 1,2)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());
  }

  @AfterClass
  public static void afterTestCloudRecovery2() throws Exception {
    shutdownCluster();
  }

  @Test
  public void test() throws Exception {
    JettySolrRunner node1 = cluster.getJettySolrRunner(0);
    JettySolrRunner node2 = cluster.getJettySolrRunner(1);
    try (Http2SolrClient client1 = SolrTestCaseJ4.getHttpSolrClient(node1.getBaseUrl().toString())) {

      node2.stop();

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 1);

      UpdateRequest req = new UpdateRequest();
      for (int i = 0; i < 100; i++) {
        req = req.add("id", i+"", "num", i+"");
      }
      req.commit(client1, COLLECTION);

      node2.start();

      cluster.getSolrClient().getZkStateReader().waitForLiveNodes(5, TimeUnit.SECONDS, (newLiveNodes) -> newLiveNodes.size() == 2);

      cluster.waitForActiveCollection(COLLECTION, 1, 2, true);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl().toString())) {
        long numFound = client.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
        assertEquals(100, numFound);
      }
      long numFound = client1.query(COLLECTION, new SolrQuery("q","*:*", "distrib", "false")).getResults().getNumFound();
      assertEquals(100, numFound);

      new UpdateRequest().add("id", "1", "num", "10")
          .commit(client1, COLLECTION);

      Object v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "true")).getResults().get(0).get("num");
      assertEquals("10", v.toString());


      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("10", v.toString());


      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl().toString())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "true")).getResults().get(0).get("num");
        assertEquals("10", v.toString());
      }

      //
      node2.stop();


      Thread.sleep(250);

      new UpdateRequest().add("id", "1", "num", "20")
          .commit(client1, COLLECTION);
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("20", v.toString());

      node2.start();


      Thread.sleep(250);

      cluster.waitForActiveCollection(COLLECTION, 1, 2);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl().toString())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("20", v.toString());
      }

      node2.stop();


      Thread.sleep(250);

      new UpdateRequest().add("id", "1", "num", "30")
          .commit(client1, COLLECTION);
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      SolrTestCaseJ4.      assertEquals("30", v.toString());

      node2.start();


      Thread.sleep(250);

      cluster.waitForActiveCollection(COLLECTION, 1, 2);

      try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl().toString())) {
        v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
        assertEquals("30", v.toString());
      }
      v = client1.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }

    node1.stop();


    Thread.sleep(250);
    waitForState("", COLLECTION, (liveNodes, collectionState) -> {
      Replica leader = collectionState.getLeader("s1");
      return leader != null && leader.getNodeName().equals(node2.getNodeName());
    });

    node1.start();


    Thread.sleep(250);

    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node1.getBaseUrl().toString())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(node2.getBaseUrl().toString())) {
      Object v = client.query(COLLECTION, new SolrQuery("q","id:1", "distrib", "false")).getResults().get(0).get("num");
      assertEquals("30", v.toString());
    }

  }

}
