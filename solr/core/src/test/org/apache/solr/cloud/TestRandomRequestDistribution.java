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
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestHandler;
import org.junit.Test;


@SolrTestCaseJ4.SuppressSSL
public class TestRandomRequestDistribution extends AbstractFullDistribZkTestBase {

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

    cloudClient.getZkStateReader().updateClusterState();

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
      CoreContainer container = runner.getCoreContainer();
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

  /**
   * Asserts that requests against a collection are only served by a 'active' local replica
   */
  private void testQueryAgainstDownReplica() throws Exception {

    log.info("Creating collection 'football' with 1 shard and 2 replicas");
    new CollectionAdminRequest.Create()
        .setCollectionName("football")
        .setNumShards(1)
        .setReplicationFactor(2)
        .setCreateNodeSet(nodeNames.get(0) + ',' + nodeNames.get(1))
        .process(cloudClient);

    waitForRecoveriesToFinish("football", true);

    cloudClient.getZkStateReader().updateClusterState();

    Replica leader = null;
    Replica notLeader = null;

    Collection<Replica> replicas = cloudClient.getZkStateReader().getClusterState().getSlice("football", "shard1").getReplicas();
    for (Replica replica : replicas) {
      if (replica.getStr(ZkStateReader.LEADER_PROP) != null) {
        leader = replica;
      } else {
        notLeader = replica;
      }
    }

    //Simulate a replica being in down state.
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
        ZkStateReader.BASE_URL_PROP, notLeader.getStr(ZkStateReader.BASE_URL_PROP),
        ZkStateReader.NODE_NAME_PROP, notLeader.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, "football",
        ZkStateReader.SHARD_ID_PROP, "shard1",
        ZkStateReader.CORE_NAME_PROP, notLeader.getStr(ZkStateReader.CORE_NAME_PROP),
        ZkStateReader.ROLES_PROP, "",
        ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());

    log.info("Forcing {} to go into 'down' state", notLeader.getStr(ZkStateReader.CORE_NAME_PROP));
    DistributedQueue q = Overseer.getInQueue(cloudClient.getZkStateReader().getZkClient());
    q.offer(Utils.toJSON(m));

    verifyReplicaStatus(cloudClient.getZkStateReader(), "football", "shard1", notLeader.getName(), Replica.State.DOWN);

    //Query against the node which hosts the down replica

    String baseUrl = notLeader.getStr(ZkStateReader.BASE_URL_PROP);
    if (!baseUrl.endsWith("/")) baseUrl += "/";
    String path = baseUrl + "football";
    log.info("Firing query against path=" + path);
    HttpSolrClient client = new HttpSolrClient(path);
    client.setSoTimeout(5000);
    client.setConnectionTimeout(2000);

    client.query(new SolrQuery("*:*"));
    client.close();

    //Test to see if the query got forwarded to the active replica or not.
    for (JettySolrRunner jetty : jettys) {
      CoreContainer container = jetty.getCoreContainer();
      for (SolrCore core : container.getCores()) {
        if (core.getName().equals(leader.getStr(ZkStateReader.CORE_NAME_PROP))) {
          SolrRequestHandler select = core.getRequestHandler("");
          long c = (long) select.getStatistics().get("requests");
          assertEquals(core.getName() + " should have got 1 request", 1, c);
          break;
        }
      }
    }

  }
}
