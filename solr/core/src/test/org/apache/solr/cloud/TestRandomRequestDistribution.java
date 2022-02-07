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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.codahale.metrics.Counter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SolrTestCaseJ4.SuppressSSL
public class TestRandomRequestDistribution extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  List<String> nodeNames = new ArrayList<>(3);

  @Test
  @BaseDistributedSearchTestCase.ShardsFixed(num = 3)
  public void test() throws Exception {
    waitForThingsToLevelOut(30);

    for (CloudJettyRunner cloudJetty : cloudJettys) {
      nodeNames.add(cloudJetty.nodeName);
    }
    assertEquals(3, nodeNames.size());

    testRequestTracking();
    testQueryAgainstDownReplica();
  }

  /**
   * Asserts that requests aren't always sent to the same poor node. See SOLR-7493
   */
  private void testRequestTracking() throws Exception {

    CollectionAdminRequest.createCollection("a1x2", "conf1", 1, 2)
        .setCreateNodeSet(nodeNames.get(0) + ',' + nodeNames.get(1))
        .process(cloudClient);

    CollectionAdminRequest.createCollection("b1x1", "conf1", 1, 1)
        .setCreateNodeSet(nodeNames.get(2))
        .process(cloudClient);

    waitForRecoveriesToFinish("a1x2", true);
    waitForRecoveriesToFinish("b1x1", true);

    cloudClient.getZkStateReader().forceUpdateCollection("b1x1");

    // get direct access to the metrics counters for each core/replica we're interested to monitor them
    final Map<String,Counter> counters = new LinkedHashMap<>();
    for (JettySolrRunner runner : jettys) {
      CoreContainer container = runner.getCoreContainer();
      SolrMetricManager metricManager = container.getMetricManager();
      for (SolrCore core : container.getCores()) {
        if ("a1x2".equals(core.getCoreDescriptor().getCollectionName())) {
          String registry = core.getCoreMetricManager().getRegistryName();
          Counter cnt = metricManager.counter(null, registry, "requests", "QUERY./select");
          // sanity check
          assertEquals(core.getName() + " has already received some requests?",
                       0, cnt.getCount());
          counters.put(core.getName(), cnt);
        }
      }
    }
    assertEquals("Sanity Check: we know there should be 2 replicas", 2, counters.size());

    // send queries to the node that doesn't host any core/replica and see where it routes them
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection b1x1 = clusterState.getCollection("b1x1");
    Collection<Replica> replicas = b1x1.getSlice("shard1").getReplicas();
    assertEquals(1, replicas.size());
    String baseUrl = replicas.iterator().next().getBaseUrl();
    if (!baseUrl.endsWith("/")) baseUrl += "/";
    try (HttpSolrClient client = getHttpSolrClient(baseUrl + "a1x2", 2000, 5000)) {

      long expectedTotalRequests = 0;
      Set<String> uniqueCoreNames = new LinkedHashSet<>();
      
      log.info("Making requests to {} a1x2", baseUrl);
      while (uniqueCoreNames.size() < counters.keySet().size() && expectedTotalRequests < 1000L) {
        expectedTotalRequests++;
        client.query(new SolrQuery("*:*"));

        long actualTotalRequests = 0;
        for (Map.Entry<String,Counter> e : counters.entrySet()) {
          final long coreCount = e.getValue().getCount();
          actualTotalRequests += coreCount;
          if (0 < coreCount) {
            uniqueCoreNames.add(e.getKey());
          }
        }
        assertEquals("Sanity Check: Num Queries So Far Doesn't Match Total????",
                     expectedTotalRequests, actualTotalRequests);
      }
      log.info("Total requests: {}", expectedTotalRequests);
      assertEquals("either request randomization code is broken of this test seed is really unlucky, " +
                   "Gave up waiting for requests to hit every core at least once after " +
                   expectedTotalRequests + " requests",
                   uniqueCoreNames.size(), counters.size());
    }
  }

  /**
   * Asserts that requests against a collection are only served by a 'active' local replica
   */
  private void testQueryAgainstDownReplica() throws Exception {

    log.info("Creating collection 'football' with 1 shard and 2 replicas");
    CollectionAdminRequest.createCollection("football", "conf1", 1, 2)
        .setCreateNodeSet(nodeNames.get(0) + ',' + nodeNames.get(1))
        .process(cloudClient);

    waitForRecoveriesToFinish("football", true);

    cloudClient.getZkStateReader().forceUpdateCollection("football");

    Replica leader = null;
    Replica notLeader = null;

    Collection<Replica> replicas = cloudClient.getZkStateReader().getClusterState().getCollection("football").getSlice("shard1").getReplicas();
    for (Replica replica : replicas) {
      if (replica.getStr(ZkStateReader.LEADER_PROP) != null) {
        leader = replica;
      } else {
        notLeader = replica;
      }
    }

    //Simulate a replica being in down state.
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
        ZkStateReader.NODE_NAME_PROP, notLeader.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.BASE_URL_PROP, notLeader.getStr(ZkStateReader.BASE_URL_PROP),
        ZkStateReader.COLLECTION_PROP, "football",
        ZkStateReader.SHARD_ID_PROP, "shard1",
        ZkStateReader.CORE_NAME_PROP, notLeader.getStr(ZkStateReader.CORE_NAME_PROP),
        ZkStateReader.ROLES_PROP, "",
        ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());

    if (log.isInfoEnabled()) {
      log.info("Forcing {} to go into 'down' state", notLeader.getStr(ZkStateReader.CORE_NAME_PROP));
    }
    ZkDistributedQueue q = jettys.get(0).getCoreContainer().getZkController().getOverseer().getStateUpdateQueue();
    q.offer(Utils.toJSON(m));

    verifyReplicaStatus(cloudClient.getZkStateReader(), "football", "shard1", notLeader.getName(), Replica.State.DOWN);

    //Query against the node which hosts the down replica

    String baseUrl = notLeader.getBaseUrl();
    if (!baseUrl.endsWith("/")) baseUrl += "/";
    String path = baseUrl + "football";
    log.info("Firing queries against path={}", path);
    try (HttpSolrClient client = getHttpSolrClient(path, 2000, 5000)) {

      SolrCore leaderCore = null;
      for (JettySolrRunner jetty : jettys) {
        CoreContainer container = jetty.getCoreContainer();
        for (SolrCore core : container.getCores()) {
          if (core.getName().equals(leader.getStr(ZkStateReader.CORE_NAME_PROP))) {
            leaderCore = core;
            break;
          }
        }
      }
      assertNotNull(leaderCore);

      SolrMetricManager leaderMetricManager = leaderCore.getCoreContainer().getMetricManager();
      String leaderRegistry = leaderCore.getCoreMetricManager().getRegistryName();
      Counter cnt = leaderMetricManager.counter(null, leaderRegistry, "requests", "QUERY./select");

      // All queries should be served by the active replica
      // To make sure that's true we keep querying the down replica
      // If queries are getting processed by the down replica then the cluster state hasn't updated for that replica
      // locally
      // So we keep trying till it has updated and then verify if ALL queries go to the active replica
      long count = 0;
      while (true) {
        count++;
        client.query(new SolrQuery("*:*"));

        long c = cnt.getCount();

        if (c == 1) {
          break; // cluster state has got update locally
        } else {
          Thread.sleep(100);
        }

        if (count > 10000) {
          fail("After 10k queries we still see all requests being processed by the down replica");
        }
      }

      // Now we fire a few additional queries and make sure ALL of them
      // are served by the active replica
      int moreQueries = TestUtil.nextInt(random(), 4, 10);
      count = 1; // Since 1 query has already hit the leader
      for (int i = 0; i < moreQueries; i++) {
        client.query(new SolrQuery("*:*"));
        count++;

        long c = cnt.getCount();

        assertEquals("Query wasn't served by leader", count, c);
      }
    }
  }
}
