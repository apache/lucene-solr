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
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test which verifies requests on collection which has all replicas in down state (but nodes are among live nodes) fail
 * fast and throws a meaningful exception
 */
public class TestQueryOnInfectedCollection extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_NAME = "infected";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void searchingShouldNotMakeMutuallyRecursiveCalls() throws Exception {

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
        .process(cluster.getSolrClient());

    // add some dummy documents
    UpdateRequest update = new UpdateRequest();
    for (int i = 0; i < 100; i++) {
      update.add("id", Integer.toString(i));
    }
    update.commit(cluster.getSolrClient(), COLLECTION_NAME);

    // bring down replicas but keep nodes up, this can done by performing various combinations of collections api operations
    // to make it faster, setting state.json directly
    infectCollectionState();

    // assert all replicas are in down state
    List<Replica> replicas = getCollectionState(COLLECTION_NAME).getReplicas();
    for (Replica replica: replicas){
      assertEquals(replica.getState(), Replica.State.DOWN);
    }

    // assert all nodes as active
    assertEquals(3, cluster.getSolrClient().getClusterStateProvider().getLiveNodes().size());

    SolrClient client = cluster.getJettySolrRunner(0).newClient();

    // This will bring down nodes with infected collection without SOLR-13793 fix
    SolrException error = expectThrows(SolrException.class,
        "Request should fail after trying all replica nodes once",
        () -> client.query(COLLECTION_NAME, new SolrQuery("*:*").setRows(0))
    );

    client.close();

    assertEquals(error.code(), SolrException.ErrorCode.INVALID_STATE.code);
    assertTrue(error.getMessage().contains("No active replicas found for collection: " + COLLECTION_NAME));

  }

  private void infectCollectionState() throws Exception {
    byte[] collectionState = cluster.getZkClient().getData("/collections/" + COLLECTION_NAME + "/state.json",
        null, null, true);

    Map<String,Map<String,?>> infectedState = (Map<String,Map<String,?>>) Utils.fromJSON(collectionState);
    Map<String, Object> shards = (Map<String, Object>) infectedState.get(COLLECTION_NAME).get("shards");
    for(Map.Entry<String, Object> shard: shards.entrySet()) {
      Map<String, Object> replicas = (Map<String, Object>) ((Map<String, Object>) shard.getValue() ).get("replicas");
      for (Map.Entry<String, Object> replica : replicas.entrySet()) {
        ((Map<String, Object>) replica.getValue()).put("state", Replica.State.DOWN.toString());
      }
    }

    cluster.getZkClient().setData("/collections/" + COLLECTION_NAME + "/state.json", Utils.toJSON(infectedState)
        , true);
  }

}
