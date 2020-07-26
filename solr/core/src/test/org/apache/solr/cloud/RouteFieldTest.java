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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import static org.apache.solr.client.solrj.response.schema.SchemaResponse.*;
import static org.apache.solr.common.util.Utils.makeMap;

public class RouteFieldTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String ROUTE_FIELD = "route_field";
  private static final String COLL_ROUTE = "routeFieldTest";
  private static final String COLL_ID = "routeIdTest";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1)
        .addConfig("conf", configset("cloud-managed"))
        .configure();
  }

  // Test for seeing if we actually respect the route field
  // We should be able to create two collections, one using ID and one using
  // a route field. We should then be able to index docs to each, using
  // the route field and id field and find the same docs on the same shards.
  @Test
  public void routeFieldTest() throws Exception {
    log.info("Starting routeFieldTest");

    assertEquals("Failed to  create collection routeFieldTest",
        0,
        CollectionAdminRequest.createCollection(COLL_ROUTE, "conf", 2, 1)
            .setRouterField(ROUTE_FIELD)
            .setMaxShardsPerNode(2)
            .process(cluster.getSolrClient()).getStatus());

    List<SchemaRequest.Update> updateList = new ArrayList<>();
    updateList.add(new SchemaRequest.AddField(makeMap("name", ROUTE_FIELD, "type", "string", "indexed", "true", "stored", "true")));
    updateList.add(new SchemaRequest.AddField(makeMap("name", "sorter", "type", "int", "indexed", "true", "stored", "true")));
    SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(updateList);
    UpdateResponse multipleUpdatesResponse = multiUpdateRequest.process(cluster.getSolrClient(), COLL_ROUTE);
    assertNull("Error adding fields", multipleUpdatesResponse.getResponse().get("errors"));

    assertEquals("Failed to  create collection routeIdTest"
        , 0
        , CollectionAdminRequest.createCollection(COLL_ID, "conf", 2, 1)
            .setMaxShardsPerNode(2)
            .process(cluster.getSolrClient()).getStatus());

    // We now have two collections, add the same docs to each with the proper
    // fields so the id field is used in one collection and ROUTE_FIELD in the other..
    List<SolrInputDocument> docsRoute = new ArrayList<>();
    List<SolrInputDocument> docsId = new ArrayList<>();
    int lim = random().nextInt(50) + 50;
    for (int idx = 0; idx < lim; ++idx) {
      SolrInputDocument doc = new SolrInputDocument();
      // id should be irrelevant for routing, but we want to insure that
      // if somehow we _do_ use id to route, we don't use the same ID
      // as the docs we're adding to the collection routed by id.
      doc.addField("id", idx + 1_500_000);
      doc.addField(ROUTE_FIELD, idx);
      doc.addField("sorter", idx);
      docsRoute.add(doc);

      doc = new SolrInputDocument();
      doc.addField("id", idx);
      doc.addField("sorter", idx);
      docsId.add(doc);
    }
    cluster.getSolrClient().add(COLL_ROUTE, docsRoute);
    cluster.getSolrClient().add(COLL_ID, docsId);

    cluster.getSolrClient().commit(COLL_ROUTE, true, true);
    cluster.getSolrClient().commit(COLL_ID, true, true);

    checkShardsHaveSameDocs();
  }

  private void checkShardsHaveSameDocs() throws IOException, SolrServerException {

    CloudSolrClient client = cluster.getSolrClient();

    DocCollection docColl = client.getZkStateReader().getClusterState().getCollection(COLL_ROUTE);
    List<Replica> reps = new ArrayList<>(docColl.getSlice("shard1").getReplicas());
    String urlRouteShard1 = reps.get(0).get("base_url") + "/" + reps.get(0).get("core");

    reps = new ArrayList<>(docColl.getSlice("shard2").getReplicas());
    String urlRouteShard2 = reps.get(0).get("base_url") + "/" + reps.get(0).get("core");

    docColl = client.getZkStateReader().getClusterState().getCollection(COLL_ID);
    reps = new ArrayList<>(docColl.getSlice("shard1").getReplicas());
    String urlIdShard1 = reps.get(0).get("base_url") + "/" + reps.get(0).get("core");

    reps = new ArrayList<>(docColl.getSlice("shard2").getReplicas());
    String urlIdShard2 = reps.get(0).get("base_url") + "/" + reps.get(0).get("core");

    assertNotEquals("URLs shouldn't be the same", urlRouteShard1, urlIdShard1);
    assertNotEquals("URLs shouldn't be the same", urlRouteShard2, urlIdShard2);
    compareShardDocs(urlIdShard1, urlRouteShard1);
    compareShardDocs(urlIdShard2, urlRouteShard2);
  }

  private void compareShardDocs(String urlId, String urlRoute) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    QueryRequest request = new QueryRequest(params);
    params.add("distrib", "false");
    params.add(CommonParams.Q, "*:*");
    params.add(CommonParams.SORT, "sorter asc");
    params.add(CommonParams.ROWS, "1000");

    HttpSolrClient httpSC = new HttpSolrClient.Builder(urlId).build();
    SolrDocumentList docsId = (SolrDocumentList) httpSC.request(request).get("response");
    httpSC.close();

    httpSC = new HttpSolrClient.Builder(urlRoute).build();
    SolrDocumentList docsRoute = (SolrDocumentList) httpSC.request(request).get("response");
    httpSC.close();

    assertEquals("We should have the exact same number of docs on each shard", docsId.getNumFound(), docsRoute.getNumFound());
    for (int idx = 0; idx < docsId.getNumFound(); ++idx) {
      int idId = Integer.parseInt((String) docsId.get(idx).getFieldValue("id"));
      int idRoute = Integer.parseInt((String) docsRoute.get(idx).getFieldValue("id"));
      assertEquals("Docs with Ids 1.5M different should be on exactly the same shard and in the same order when sorted",
          idId, idRoute - 1_500_000);
    }
  }
}
