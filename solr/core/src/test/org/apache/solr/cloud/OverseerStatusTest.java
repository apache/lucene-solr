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

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.BeforeClass;
import org.junit.Test;

public class OverseerStatusTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();;
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void test() throws Exception {

    // find existing command counts because collection may be created by base test class too
    int numCollectionCreates = 0, numOverseerCreates = 0;

    String collectionName = "overseer_status_test";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1).process(cluster.getSolrClient());

    NamedList<Object> resp = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient()).getResponse();
    NamedList<Object> collection_operations = (NamedList<Object>) resp.get("collection_operations");
    NamedList<Object> overseer_operations = (NamedList<Object>) resp.get("overseer_operations");
    SimpleOrderedMap<Object> createcollection
        = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
    assertEquals("No stats for create in OverseerCollectionProcessor", numCollectionCreates + 1, createcollection.get("requests"));
    createcollection = (SimpleOrderedMap<Object>) overseer_operations.get(CollectionParams.CollectionAction.CREATE.toLower());
    assertEquals("No stats for create in Overseer", numOverseerCreates + 1, createcollection.get("requests"));

    // Reload the collection
    CollectionAdminRequest.reloadCollection(collectionName).process(cluster.getSolrClient());

    resp = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient()).getResponse();
    collection_operations = (NamedList<Object>) resp.get("collection_operations");
    SimpleOrderedMap<Object> reload = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.RELOAD.toLower());
    assertEquals("No stats for reload in OverseerCollectionProcessor", 1, reload.get("requests"));

    HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
        "Split shard for non existent collection should have failed",
        () -> CollectionAdminRequest
            .splitShard("non_existent_collection")
            .setShardName("non_existent_shard")
            .process(cluster.getSolrClient())
    );

    resp = new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient()).getResponse();
    collection_operations = (NamedList<Object>) resp.get("collection_operations");
    SimpleOrderedMap<Object> split = (SimpleOrderedMap<Object>) collection_operations.get(CollectionParams.CollectionAction.SPLITSHARD.toLower());
    assertEquals("No stats for split in OverseerCollectionProcessor", 1, split.get("errors"));
    assertNotNull(split.get("recent_failures"));

    SimpleOrderedMap<Object> amIleader = (SimpleOrderedMap<Object>) collection_operations.get("am_i_leader");
    assertNotNull("OverseerCollectionProcessor amILeader stats should not be null", amIleader);
    assertNotNull(amIleader.get("requests"));
    assertTrue(Integer.parseInt(amIleader.get("requests").toString()) > 0);
    assertNotNull(amIleader.get("errors"));
    assertNotNull(amIleader.get("avgTimePerRequest"));

    amIleader = (SimpleOrderedMap<Object>) overseer_operations.get("am_i_leader");
    assertNotNull("Overseer amILeader stats should not be null", amIleader);
    assertNotNull(amIleader.get("requests"));
    assertTrue(Integer.parseInt(amIleader.get("requests").toString()) > 0);
    assertNotNull(amIleader.get("errors"));
    assertNotNull(amIleader.get("avgTimePerRequest"));

    SimpleOrderedMap<Object> updateState = (SimpleOrderedMap<Object>) overseer_operations.get("update_state");
    assertNotNull("Overseer update_state stats should not be null", updateState);
    assertNotNull(updateState.get("requests"));
    assertTrue(Integer.parseInt(updateState.get("requests").toString()) > 0);
    assertNotNull(updateState.get("errors"));
    assertNotNull(updateState.get("avgTimePerRequest"));

  }
}
