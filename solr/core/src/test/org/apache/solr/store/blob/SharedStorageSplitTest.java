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
package org.apache.solr.store.blob;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.api.collections.SplitByPrefixTest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for shard splitting in conjunction with shared storage
 */
public class SharedStorageSplitTest extends SolrCloudSharedStoreTestCase  {
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(2)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
    
    // we don't use this in testing
    Path sharedStoreRootPath = createTempDir("tempDir");
    CoreStorageClient storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    // configure same client for each runner, this isn't a concurrency test so this is fine
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), runner);
    }
  }
  
  
  @AfterClass
  public static void teardownTest() throws Exception {
    shutdownCluster();
  }

  void doSplitShard(String collectionName, boolean sharedStorage, int repFactor, int nPrefixes, int nDocsPerPrefix) throws Exception {

    if (sharedStorage) {
      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, 0, 0, 0)
          .setMaxShardsPerNode(100)
          .setSharedIndex(true)
          .setSharedReplicas(repFactor)
          .process(cluster.getSolrClient());
    } else {
      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, repFactor)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());
    }

    cluster.waitForActiveCollection(collectionName, 1, repFactor);

    CloudSolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);

    if (random().nextBoolean()) {
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          client.add(sdoc("id", prefix + "!doc" + j));
        }
      }
      client.commit(collectionName, true, true, false);
    } else {
      // Try all docs in the same update request
      UpdateRequest updateReq = new UpdateRequest();
      for (int i = 0; i < nPrefixes; i++) {
        String prefix = "a" + i;
        for (int j = 0; j < nDocsPerPrefix; j++) {
          updateReq.add(sdoc("id", prefix + "!doc" + j));
        }
      }
      UpdateResponse ursp = updateReq.commit(client, collectionName);
      assertEquals(0, ursp.getStatus());
    }

    checkExpectedDocs(client, repFactor, nPrefixes * nDocsPerPrefix);

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
        .setSplitByPrefix(true)
        .setShardName("shard1");
    splitShard.process(client);
    waitForState("Timed out waiting for sub shards to be active.",
        collectionName, activeClusterShape(2, 3*repFactor));  // 2 repFactor for the new split shards, 1 repFactor for old replicas

    checkExpectedDocs(client, repFactor, nPrefixes * nDocsPerPrefix);
  }

  void checkExpectedDocs(CloudSolrClient client, int repFactor, long numExpected) throws Exception {
    String collectionName = client.getDefaultCollection();
    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();

    if (repFactor > 1) {
      for (Slice slice : slices) {
        for (Replica replica : slice.getReplicas()) {
          SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
          try {
            replicaClient.query(params("q", "*:* priming pull", "distrib", "false"));
          } finally {
            replicaClient.close();
          }
        }
      }

      // TODO super ugly and inappropriate but the pull shouldn't take long. At some point we'll
      // make our end-to-end async testing nicer by supporting test listeners for the async tasks
      Thread.sleep(5000);
    }

    long totCount = 0;
      for (Slice slice : slices) {
        if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
        long lastReplicaCount = -1;
        for (Replica replica : slice.getReplicas()) {
          SolrClient replicaClient = getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName());
          long numFound = 0;
          try {
            numFound = replicaClient.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound();
          } finally {
            replicaClient.close();
          }
          if (lastReplicaCount >= 0) {
            assertEquals("Replica doc count for " + replica, lastReplicaCount, numFound);
          }
          lastReplicaCount = numFound;
        }
        totCount += lastReplicaCount;
    }

    assertEquals(numExpected, totCount);

    long cloudClientDocs = client.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals(numExpected, cloudClientDocs);
  }

  @Test
  public void testSplit() throws Exception {
    doSplitShard("c1", true, 1, 2, 2);
    doSplitShard("c2", true, 2, 2, 2);
  }

}
