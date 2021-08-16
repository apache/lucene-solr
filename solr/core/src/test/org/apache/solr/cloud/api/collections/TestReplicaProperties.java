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
package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

@Slow
public class TestReplicaProperties extends ReplicaPropertiesBase {

  public static final String COLLECTION_NAME = "testcollection";

  public TestReplicaProperties() {
    schemaString = "schema15.xml";      // we need a string id
    sliceCount = 2;
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {

    try (CloudSolrClient client = createCloudClient(null)) {
      // Mix up a bunch of different combinations of shards and replicas in order to exercise boundary cases.
      // shards, replicationfactor, maxreplicaspernode
      int shards = random().nextInt(7);
      if (shards < 2) shards = 2;
      int rFactor = random().nextInt(4);
      if (rFactor < 2) rFactor = 2;
      createCollection(null, COLLECTION_NAME, shards, rFactor, shards * rFactor + 1, client, null, "conf1");
    }

    waitForCollection(cloudClient.getZkStateReader(), COLLECTION_NAME, 2);
    waitForRecoveriesToFinish(COLLECTION_NAME, false);

    listCollection();

    clusterAssignPropertyTest();
  }

  private void listCollection() throws IOException, SolrServerException {

    try (CloudSolrClient client = createCloudClient(null)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.LIST.toString());
      @SuppressWarnings({"rawtypes"})
      SolrRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      NamedList<Object> rsp = client.request(request);
      @SuppressWarnings({"unchecked"})
      List<String> collections = (List<String>) rsp.get("collections");
      assertTrue("control_collection was not found in list", collections.contains("control_collection"));
      assertTrue(DEFAULT_COLLECTION + " was not found in list", collections.contains(DEFAULT_COLLECTION));
      assertTrue(COLLECTION_NAME + " was not found in list", collections.contains(COLLECTION_NAME));
    }
  }


  private void clusterAssignPropertyTest() throws Exception {

    try (CloudSolrClient client = createCloudClient(null)) {
      client.connect();
      try {
        doPropertyAction(client,
            "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
            "property", "preferredLeader");
      } catch (SolrException se) {
        assertTrue("Should have seen missing required parameter 'collection' error",
            se.getMessage().contains("Missing required parameter: collection"));
      }

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
          "collection", COLLECTION_NAME,
          "property", "preferredLeader");

      verifyUniqueAcrossCollection(client, COLLECTION_NAME, "preferredleader");

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
          "collection", COLLECTION_NAME,
          "property", "property.newunique",
          "shardUnique", "true");
      verifyUniqueAcrossCollection(client, COLLECTION_NAME, "property.newunique");

      try {
        doPropertyAction(client,
            "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
            "collection", COLLECTION_NAME,
            "property", "whatever",
            "shardUnique", "false");
        fail("Should have thrown an exception here.");
      } catch (SolrException se) {
        assertTrue("Should have gotten a specific error message here",
            se.getMessage().contains("Balancing properties amongst replicas in a slice requires that the " +
                "property be pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'"));
      }
      // Should be able to set non-unique-per-slice values in several places.
      Map<String, Slice> slices = client.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlicesMap();
      List<String> sliceList = new ArrayList<>(slices.keySet());
      String c1_s1 = sliceList.get(0);
      List<String> replicasList = new ArrayList<>(slices.get(c1_s1).getReplicasMap().keySet());
      String c1_s1_r1 = replicasList.get(0);
      String c1_s1_r2 = replicasList.get(1);

      addProperty(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r1,
          "property", "bogus1",
          "property.value", "true");

      addProperty(client,
          "action", CollectionParams.CollectionAction.ADDREPLICAPROP.toString(),
          "collection", COLLECTION_NAME,
          "shard", c1_s1,
          "replica", c1_s1_r2,
          "property", "property.bogus1",
          "property.value", "whatever");

      try {
        doPropertyAction(client,
            "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
            "collection", COLLECTION_NAME,
            "property", "bogus1",
            "shardUnique", "false");
        fail("Should have thrown parameter error here");
      } catch (SolrException se) {
        assertTrue("Should have caught specific exception ",
            se.getMessage().contains("Balancing properties amongst replicas in a slice requires that the property be " +
                "pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'"));
      }

      // Should have no effect despite the "shardUnique" param being set.

      doPropertyAction(client,
          "action", CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toString(),
          "collection", COLLECTION_NAME,
          "property", "property.bogus1",
          "shardUnique", "true");

      verifyPropertyVal(client, COLLECTION_NAME,
          c1_s1_r1, "bogus1", "true");
      verifyPropertyVal(client, COLLECTION_NAME,
          c1_s1_r2, "property.bogus1", "whatever");

      // At this point we've assigned a preferred leader. Make it happen and check that all the nodes that are
      // leaders _also_ have the preferredLeader property set.


      NamedList<Object> res = doPropertyAction(client,
          "action", CollectionParams.CollectionAction.REBALANCELEADERS.toString(),
          "collection", COLLECTION_NAME);

      verifyLeaderAssignment(client, COLLECTION_NAME);

    }
  }

  private void verifyLeaderAssignment(CloudSolrClient client, String collectionName)
      throws InterruptedException, KeeperException {
    String lastFailMsg = "";
    for (int idx = 0; idx < 300; ++idx) { // Keep trying while Overseer writes the ZK state for up to 30 seconds.
      lastFailMsg = "";
      ClusterState clusterState = client.getZkStateReader().getClusterState();
      for (Slice slice : clusterState.getCollection(collectionName).getSlices()) {
        boolean foundLeader = false;
        boolean foundPreferred = false;
        for (Replica replica : slice.getReplicas()) {
          boolean isLeader = replica.getBool("leader", false);
          boolean isPreferred = replica.getBool("property.preferredleader", false);
          if (isLeader != isPreferred) {
            lastFailMsg = "Replica should NOT have preferredLeader != leader. Preferred: " + isPreferred +
                " leader is " + isLeader;
          }
          if (foundLeader && isLeader) {
            lastFailMsg = "There should only be a single leader in _any_ shard! Replica " + replica.getName() +
                " is the second leader in slice " + slice.getName();
          }
          if (foundPreferred && isPreferred) {
            lastFailMsg = "There should only be a single preferredLeader in _any_ shard! Replica " + replica.getName() +
                " is the second preferredLeader in slice " + slice.getName();
          }
          foundLeader = foundLeader ? foundLeader : isLeader;
          foundPreferred = foundPreferred ? foundPreferred : isPreferred;
        }
      }
      if (lastFailMsg.length() == 0) return;
      Thread.sleep(100);
    }
    fail(lastFailMsg);
  }

  private void addProperty(CloudSolrClient client, String... paramsIn) throws IOException, SolrServerException {
    assertTrue("paramsIn must be an even multiple of 2, it is: " + paramsIn.length, (paramsIn.length % 2) == 0);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (int idx = 0; idx < paramsIn.length; idx += 2) {
      params.set(paramsIn[idx], paramsIn[idx + 1]);
    }
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    client.request(request);

  }
}

