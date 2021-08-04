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

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.cloud.TestPullReplica.assertNumberOfReplicas;
import static org.apache.solr.cloud.TestPullReplica.assertUlogPresence;
import static org.apache.solr.cloud.TestPullReplica.waitForDeletion;
import static org.apache.solr.cloud.TestPullReplica.waitForNumDocsInAllReplicas;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

@Slow
public class TestPullReplicaWithAuth extends SolrCloudTestCase {

  private static final String USER = "solr";
  private static final String PASS = "SolrRocksAgain";
  private static final String collectionName = "testPullReplicaWithAuth";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    final String SECURITY_JSON = Utils.toJSONString
        (Utils.makeMap("authorization",
            Utils.makeMap("class", RuleBasedAuthorizationPlugin.class.getName(),
                "user-role", singletonMap(USER, "admin"),
                "permissions", singletonList(Utils.makeMap("name", "all", "role", "admin"))),
            "authentication",
            Utils.makeMap("class", BasicAuthPlugin.class.getName(),
                "blockUnknown", true,
                "credentials", singletonMap(USER, getSaltedHashedValue(PASS)))));

    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(USER, PASS);
    return req;
  }

  private QueryResponse queryWithBasicAuth(HttpSolrClient client, SolrQuery q) throws IOException, SolrServerException {
    return withBasicAuth(new QueryRequest(q)).process(client);
  }

  @Test
  public void testPKIAuthWorksForPullReplication() throws Exception {
    int numPullReplicas = 2;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, numPullReplicas);
    create.setMaxShardsPerNode(2);
    withBasicAuth(create)
        .processAndWait(cluster.getSolrClient(), 10);
    waitForState("Expected collection to be created with 1 shard and " + (numPullReplicas + 1) + " replicas",
        collectionName, clusterShape(1, numPullReplicas + 1));
    DocCollection docCollection =
        assertNumberOfReplicas(collectionName, 1, 0, numPullReplicas, false, true);

    int numDocs = 0;
    CloudSolrClient solrClient = cluster.getSolrClient();
    for (int i = 0; i < 5; i++) {
      numDocs++;

      UpdateRequest ureq = withBasicAuth(new UpdateRequest());
      ureq.add(new SolrInputDocument("id", String.valueOf(numDocs), "foo", "bar"));
      ureq.commit(solrClient, collectionName);

      Slice s = docCollection.getSlices().iterator().next();
      try (HttpSolrClient leaderClient = getHttpSolrClient(s.getLeader().getCoreUrl())) {
        assertEquals(numDocs, queryWithBasicAuth(leaderClient, new SolrQuery("*:*")).getResults().getNumFound());
      }

      List<Replica> pullReplicas = s.getReplicas(EnumSet.of(Replica.Type.PULL));
      waitForNumDocsInAllReplicas(numDocs, pullReplicas, "*:*", USER, PASS);

      for (Replica r : pullReplicas) {
        try (HttpSolrClient pullReplicaClient = getHttpSolrClient(r.getCoreUrl())) {
          QueryResponse statsResponse = queryWithBasicAuth(pullReplicaClient, new SolrQuery("qt", "/admin/plugins", "stats", "true"));
          // adds is a gauge, which is null for PULL replicas
          assertNull("Replicas shouldn't process the add document request: " + statsResponse,
              getUpdateHandlerMetric(statsResponse, "UPDATE.updateHandler.adds"));
          assertEquals("Replicas shouldn't process the add document request: " + statsResponse,
              0L, getUpdateHandlerMetric(statsResponse, "UPDATE.updateHandler.cumulativeAdds.count"));
        }
      }
    }

    CollectionAdminResponse response =
        withBasicAuth(CollectionAdminRequest.reloadCollection(collectionName)).process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertUlogPresence(docCollection);

    // add another pull replica to ensure it can pull the indexes
    Slice s = docCollection.getSlices().iterator().next();
    List<Replica> pullReplicas = s.getReplicas(EnumSet.of(Replica.Type.PULL));
    assertEquals(numPullReplicas, pullReplicas.size());
    response = withBasicAuth(CollectionAdminRequest.addReplicaToShard(collectionName, s.getName(), Replica.Type.PULL)).process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    numPullReplicas = numPullReplicas + 1; // added a PULL
    waitForState("Expected collection to be created with 1 shard and " + (numPullReplicas + 1) + " replicas",
        collectionName, clusterShape(1, numPullReplicas + 1));

    docCollection =
        assertNumberOfReplicas(collectionName, 1, 0, numPullReplicas, false, true);
    s = docCollection.getSlices().iterator().next();
    pullReplicas = s.getReplicas(EnumSet.of(Replica.Type.PULL));
    assertEquals(numPullReplicas, pullReplicas.size());
    waitForNumDocsInAllReplicas(numDocs, pullReplicas, "*:*", USER, PASS);

    withBasicAuth(CollectionAdminRequest.deleteCollection(collectionName)).process(cluster.getSolrClient());
    waitForDeletion(collectionName);
  }

  @SuppressWarnings("unchecked")
  private Object getUpdateHandlerMetric(QueryResponse statsResponse, String metric) {
    return ((Map<String, Object>) statsResponse.getResponse().findRecursive("plugins", "UPDATE", "updateHandler", "stats")).get(metric);
  }
}
