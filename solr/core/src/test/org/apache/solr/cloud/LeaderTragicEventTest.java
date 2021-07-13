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

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.util.TestInjection;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;

public class LeaderTragicEventTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String collection;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    collection = getSaferTestName();
    // TODO Investigate why using same cluster for all tests in this class led to sporadic failure
    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    cluster.getSolrClient().setDefaultCollection(collection);
  }

  @After
  public void tearDown() throws Exception {
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    cluster.shutdown();
    super.tearDown();
  }

  @Test
  public void testLeaderFailsOver() throws Exception {
    CollectionAdminRequest
        .createCollection(collection, "config", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 2);

    UpdateResponse updateResponse = new UpdateRequest().add("id", "1").commit(cluster.getSolrClient(), null);
    assertEquals(0, updateResponse.getStatus());

    Replica oldLeader = corruptLeader(collection);

    waitForState("Now waiting for new replica to become leader", collection, (liveNodes, collectionState) -> {
      Slice slice = collectionState.getSlice("shard1");

      if (slice.getReplicas().size() != 2) return false;
      if (slice.getLeader() == null) return false;
      if (slice.getLeader().getName().equals(oldLeader.getName())) return false;

      return true;
    });
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), collection, 120000);
    Slice shard = getCollectionState(collection).getSlice("shard1");
    assertNotEquals("Old leader should not be leader again", oldLeader.getNodeName(), shard.getLeader().getNodeName());
    assertEquals("Old leader should be a follower", oldLeader.getNodeName(), getNonLeader(shard).getNodeName());

    // Check that we can continue indexing after this
    updateResponse = new UpdateRequest().add("id", "2").commit(cluster.getSolrClient(), null);
    assertEquals(0, updateResponse.getStatus());
    try (SolrClient followerClient = new HttpSolrClient.Builder(oldLeader.getCoreUrl()).build()) {
      QueryResponse queryResponse = new QueryRequest(new SolrQuery("*:*")).process(followerClient);
      assertEquals(queryResponse.getResults().toString(), 2, queryResponse.getResults().getNumFound());
    }
  }

  private Replica corruptLeader(String collection) throws IOException, SolrServerException {
    try {
      TestInjection.leaderTragedy = "true:100";

      DocCollection dc = getCollectionState(collection);
      Replica oldLeader = dc.getLeader("shard1");
      log.info("Will crash leader : {}", oldLeader);

      try (HttpSolrClient solrClient = new HttpSolrClient.Builder(dc.getLeader("shard1").getCoreUrl()).build()) {
        new UpdateRequest().add("id", "99").commit(solrClient, null);
        fail("Should have injected tragedy");
      } catch (RemoteSolrException e) {
        // solrClient.add would throw RemoteSolrException with code 500
        // or 404 if the bad replica has already been deleted
        MatcherAssert.assertThat(e.code(), anyOf(is(500), is(404)));
      } catch (AlreadyClosedException e) {
        // If giving up leadership, might be already closed/closing
      }

      return oldLeader;
    } finally {
      TestInjection.leaderTragedy = null;
    }
  }

  private Replica getNonLeader(Slice slice) {
    if (slice.getReplicas().size() <= 1) return null;
    return slice.getReplicas(rep -> !rep.getName().equals(slice.getLeader().getName())).get(0);
  }

  @Test
  public void testOtherReplicasAreNotActive() throws Exception {
    int numReplicas = random().nextInt(2) + 1;
    // won't do anything if leader is the only one active replica in the shard
    CollectionAdminRequest
        .createCollection(collection, "config", 1, numReplicas)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, numReplicas);

      JettySolrRunner otherReplicaJetty = null;
      if (numReplicas == 2) {
        Slice shard = getCollectionState(collection).getSlice("shard1");
        otherReplicaJetty = cluster.getReplicaJetty(getNonLeader(shard));
        if (log.isInfoEnabled()) {
          log.info("Stop jetty node : {} state:{}", otherReplicaJetty.getBaseUrl(), getCollectionState(collection));
        }
        otherReplicaJetty.stop();
        cluster.waitForJettyToStop(otherReplicaJetty);
        waitForState("Timeout waiting for replica get down", collection, (liveNodes, collectionState) -> getNonLeader(collectionState.getSlice("shard1")).getState() != Replica.State.ACTIVE);
      }

      Replica oldLeader = corruptLeader(collection);

      if (otherReplicaJetty != null) {
        otherReplicaJetty.start();
        cluster.waitForNode(otherReplicaJetty, 30);
      }

      Replica leader = getCollectionState(collection).getSlice("shard1").getLeader();
      assertEquals(leader.getName(), oldLeader.getName());
  }


}
