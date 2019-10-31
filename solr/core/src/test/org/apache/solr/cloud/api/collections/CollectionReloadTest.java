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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.RetryUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Verifies cluster state remains consistent after collection reload.
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class CollectionReloadTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @Test
  public void testReloadedLeaderStateAfterZkSessionLoss() throws Exception {

    log.info("testReloadedLeaderStateAfterZkSessionLoss initialized OK ... running test logic");

    final String testCollectionName = "c8n_1x1";
    CollectionAdminRequest.createCollection(testCollectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());

    Replica leader
        = cluster.getSolrClient().getZkStateReader().getLeaderRetry(testCollectionName, "shard1", DEFAULT_TIMEOUT);

    long coreStartTime = getCoreStatus(leader).getCoreStartTime().getTime();
    CollectionAdminRequest.reloadCollection(testCollectionName).process(cluster.getSolrClient());

    RetryUtil.retryUntil("Timed out waiting for core to reload", 30, 1000, TimeUnit.MILLISECONDS, () -> {
      long restartTime = 0;
      try {
        restartTime = getCoreStatus(leader).getCoreStartTime().getTime();
      } catch (Exception e) {
        log.warn("Exception getting core start time: {}", e.getMessage());
        return false;
      }
      return restartTime > coreStartTime;
    });

    final int initialStateVersion = getCollectionState(testCollectionName).getZNodeVersion();

    cluster.expireZkSession(cluster.getReplicaJetty(leader));

    waitForState("Timed out waiting for core to re-register as ACTIVE after session expiry", testCollectionName, (n, c) -> {
      log.info("Collection state: {}", c.toString());
      Replica expiredReplica = c.getReplica(leader.getName());
      return expiredReplica.getState() == Replica.State.ACTIVE && c.getZNodeVersion() > initialStateVersion;
    });

    log.info("testReloadedLeaderStateAfterZkSessionLoss succeeded ... shutting down now!");
  }
  
  @Repeat(iterations=5)
  public void testCreateReloadDeleteAllNrt() throws Exception {
    testCreateReloadDelete("testCreateReloadDeleteAllNrt", 3, 0, 0);
  }
  
  @Repeat(iterations=5)
  public void testCreateReloadDeleteAllTlog() throws Exception {
    testCreateReloadDelete("testCreateReloadDeleteAllTlog", 0, 3, 0);
  }
  
  @Repeat(iterations=5)
  public void testCreateReloadDeletePull() throws Exception {
    testCreateReloadDelete("testCreateReloadDeletePull", 0, 1, 2);
  }
  
  private void testCreateReloadDelete(String collectionName, int nrtReplicas, int tlogReplicas, int pullReplicas) throws Exception {
    int numShards = 3;
    createCollection(numShards,  nrtReplicas, tlogReplicas, pullReplicas, collectionName);
    boolean reloaded = false;
    while (true) {
      waitForState("Timeout waiting for collection " + collectionName, collectionName, clusterShape(numShards, numShards * (nrtReplicas + tlogReplicas + pullReplicas)));
      if (reloaded) {
        break;
      } else {
        // reload
        assertSuccessfulAdminRequest(
            CollectionAdminRequest.reloadCollection(collectionName).process(cluster.getSolrClient()));
        reloaded = true;
      }
    }
    assertSuccessfulAdminRequest(
        CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient()));
  }

  private void assertSuccessfulAdminRequest(CollectionAdminResponse response) {
    assertEquals("Unexpected response status", 0, response.getStatus());
    assertTrue("Unsuccessful response: " + response, response.isSuccess());
  }
  
  private void createCollection(int numShards,  int nrtReplicas, int tlogReplicas, int pullReplicas, String collectionName) throws SolrServerException, IOException {
    switch (random().nextInt(3)) {
      case 0:
        log.info("Creating collection with SolrJ");
        // Sometimes use SolrJ
        assertSuccessfulAdminRequest(
            CollectionAdminRequest.createCollection(collectionName, "conf", numShards, nrtReplicas, tlogReplicas, pullReplicas)
              .setMaxShardsPerNode(100)
              .process(cluster.getSolrClient()));
        break;
      case 1:
        log.info("Creating collection with V1 API");
        // Sometimes use v1 API
        String url = String.format(Locale.ROOT, "%s/admin/collections?action=CREATE&name=%s&collection.configName=%s&numShards=%s&maxShardsPerNode=%s&nrtReplicas=%s&tlogReplicas=%s&pullReplicas=%s",
            cluster.getRandomJetty(random()).getBaseUrl(),
            collectionName, "conf",
            numShards,
            100,          // maxShardsPerNode
            nrtReplicas, 
            tlogReplicas,
            pullReplicas); 
        HttpGet createCollectionGet = new HttpGet(url);
        cluster.getSolrClient().getHttpClient().execute(createCollectionGet);
        break;
      case 2:
        log.info("Creating collection with V2 API");
        // Sometimes use V2 API
        url = cluster.getRandomJetty(random()).getBaseUrl().toString() + "/____v2/c";
        String requestBody = String.format(Locale.ROOT, "{create:{name:%s, config:%s, numShards:%s, maxShardsPerNode:%s, nrtReplicas:%s, tlogReplicas:%s, pullReplicas:%s}}",
            collectionName, "conf",
            numShards,    // numShards
            100, // maxShardsPerNode
            nrtReplicas,
            tlogReplicas,
            pullReplicas);
        HttpPost createCollectionPost = new HttpPost(url);
        createCollectionPost.setHeader("Content-type", "application/json");
        createCollectionPost.setEntity(new StringEntity(requestBody));
        HttpResponse httpResponse = cluster.getSolrClient().getHttpClient().execute(createCollectionPost);
        assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        break;
    }
  }
}
