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

package org.apache.solr.client.solrj.impl;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RandomizeSSL;
import org.junit.BeforeClass;
import org.junit.Test;

@RandomizeSSL(1.0)
public class HttpClusterStateSSLTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupClusterWithSSL() throws Exception {
    System.setProperty("solr.storeBaseUrl", "true");
    configureCluster(1)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHttpClusterStateWithSSL() throws Exception {
    URL url0 = cluster.getJettySolrRunner(0).getBaseUrl();
    assertEquals("https", url0.getProtocol());

    final String collectionId = "test-https-cluster-state";
    final int numShards = 3;
    final int rf = 2;
    final int expectedReplicas = numShards * rf;
    CollectionAdminRequest.createCollection(collectionId, "conf", numShards, rf)
        .setPerReplicaState(false)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionId, numShards, numShards * rf);

    // verify the base_url is actually stored with https in it on the server-side
    byte[] stateJsonBytes = cluster.getZkClient().getData(ZkStateReader.getCollectionPath(collectionId), null, null, true);
    assertNotNull(stateJsonBytes);
    Map<String, Object> replicasMap =
        (Map<String, Object>) Utils.getObjectByPath(Utils.fromJSON(stateJsonBytes), false, "/" + collectionId + "/shards/shard1/replicas");
    assertEquals(2, replicasMap.size());
    for (Object replicaObj : replicasMap.values()) {
      Map<String, Object> replicaData = (Map<String, Object>) replicaObj;
      String baseUrl = (String) replicaData.get("base_url");
      assertTrue(baseUrl != null && baseUrl.startsWith("https://"));
    }

    // verify the http derived cluster state (on the client side) agrees with what the server stored
    try (CloudSolrClient httpBasedCloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(url0.toExternalForm())).build()) {
      ClusterStateProvider csp = httpBasedCloudSolrClient.getClusterStateProvider();
      assertTrue(csp instanceof HttpClusterStateProvider);
      verifyUrlSchemeInClusterState(csp.getClusterState(), collectionId, expectedReplicas);
    }

    // http2
    try (CloudHttp2SolrClient http2BasedClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(url0.toExternalForm())).build()) {
      ClusterStateProvider csp = http2BasedClient.getClusterStateProvider();
      assertTrue(csp instanceof Http2ClusterStateProvider);
      verifyUrlSchemeInClusterState(csp.getClusterState(), collectionId, expectedReplicas);
    }

    // Zk cluster state now
    ClusterStateProvider csp = cluster.getSolrClient().getClusterStateProvider();
    assertTrue(csp instanceof ZkClientClusterStateProvider);
    verifyUrlSchemeInClusterState(csp.getClusterState(), collectionId, expectedReplicas);
  }

  private void verifyUrlSchemeInClusterState(final ClusterState cs, final String collectionId, final int expectedReplicas) {
    DocCollection dc = cs.getCollection(collectionId);
    assertNotNull(dc);
    List<Replica> replicas = dc.getReplicas();
    assertNotNull(replicas);
    assertEquals(expectedReplicas, replicas.size());
    for (Replica r : replicas) {
      String baseUrl = r.getBaseUrl();
      assertNotNull(baseUrl);
      assertTrue(baseUrl.startsWith("https://"));
    }
  }
}
