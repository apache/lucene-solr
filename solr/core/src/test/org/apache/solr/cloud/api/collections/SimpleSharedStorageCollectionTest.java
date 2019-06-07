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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests related to shared storage based collections, i.e. collections having only replicas of type {@link Replica.Type#SHARED}.
 */
public class SimpleSharedStorageCollectionTest extends SolrCloudTestCase {
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(3)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
  }

  @Test
  public void testCreateCollection() throws Exception {
    String collectionName = "BlobBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0).setSharedIndex(true).setSharedReplicas(1);
    create.process(cloudClient).getResponse();
    
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
  }

  @Test
  public void testAddReplica() throws Exception {
    String collectionName = "BlobBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0).setSharedIndex(true).setSharedReplicas(1);

    // Create the collection
    create.process(cloudClient).getResponse();
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    try {
      // Let the request fail cleanly just in case, but in reality it fails with an exception since we throw a Runtime from down below
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.NRT)
          .process(cloudClient);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Can't add a NRT replica to a collection backed by shared storage"));
    }

    // Adding a SHARED replica is expected to work ok
    assertTrue(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.SHARED)
        .process(cloudClient).isSuccess());
  }
}
