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

import java.util.Map;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests related to shared storage based collections (collection having only replicas of type {@link Replica.Type#SHARED}).
 * and ensuring the expected shared collection specific metadata exists when creating collections, modifying collections, 
 * creating shards, or modifying shards for collections.
 */
public class SharedStorageShardMetadataTest extends SolrCloudTestCase  {
  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(3)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
  }
  
  /**
   * Test CreateCollection stores the sharedIndex and SharedShardName in ZooKeeper. SharedShardName should be per
   * shard metadata object
   */
  @Test
  public void testCreateCollection() throws Exception {
    String collectionName = "BlobBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0)
        .setSharedIndex(true)
        .setSharedReplicas(1);
    
    create.process(cloudClient);
    
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);

    assertSharedCollectionMetadataExists(collection);
  }
  
  /**
   * Test that creating a shard via CREATESHARD correctly stores the SharedShardName on the new shard. Note, only collections
   * using implicit routers can be issued CREATESHARD via the collections API. 
   */
  @Test
  public void testCreateShardOnImplicitRouterCollection() throws Exception {
    String collectionName = "BlobBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // create the collection w/ implicit router
    CollectionAdminRequest.Create create = CollectionAdminRequest
        .createCollectionWithImplicitRouter(collectionName, "conf", "shard1", 0)
          .setSharedIndex(true)
          .setSharedReplicas(1);
    
    create.process(cloudClient);

    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    // sending a CREATESHARD command w/ the sharedReplicas query params should work correctly 
    CollectionAdminRequest.CreateShard createShard = CollectionAdminRequest.createShard(collectionName, "shard2")
        .setSharedReplicas(1);
    createShard.process(cloudClient);
    
    waitForState("Timed-out wait for createshard to finish", collectionName, clusterShape(2, 2));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertSharedCollectionMetadataExists(collection);
    
    // sending a CREATESHARD command w/out the sharedReplicas query params should work correctly
    createShard = CollectionAdminRequest.createShard(collectionName, "shard3");
    createShard.process(cloudClient);
    
    // the default sharedReplicas value is 0
    waitForState("Timed-out wait for createshard to finish", collectionName, clusterShape(3, 2));
    collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertSharedCollectionMetadataExists(collection);
  }
  
  /**
   * Test that splitting a shard creates two shards correctly storing their respective SharedStoreName
   */
  @Test
  public void testSplitShard() throws Exception {
    String collectionName = "BlobBasedCollectionName3";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, 1, 0)
        .setSharedIndex(true)
        .setSharedReplicas(1);
    
    create.process(cloudClient);
    
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    
    // sending a split shard command should generate two createshard commands that create two new sub-shards in zookeeper
    CollectionAdminRequest.SplitShard split = CollectionAdminRequest.splitShard(collectionName)
        .setShardName("shard1");
    split.process(cloudClient);
     
    // we haven't deleted the old shard so we should expect 3 total shards and 2 active ones
    waitForState("Timed-out wait for split to finish", collectionName, activeClusterShape(2, 3));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(2, collection.getActiveSlices().size());
    assertEquals(3, collection.getSlices().size());
    
    // single shard sanity check; <shardname>_<counter> is the nomenclature for sub shards
    Slice slice = collection.getActiveSlicesMap().get("shard1_1");
    assertEquals(collectionName + "_shard1_1", slice.getProperties().get(ZkStateReader.SHARED_SHARD_NAME));
        
    // check them all
    assertSharedCollectionMetadataExists(collection);
  }
  
  private void assertSharedCollectionMetadataExists(DocCollection collection) {
    // verify that we've stored in ZooKeeper the sharedIndex field indicating the collection is a shared one
    assertTrue("Shared collection was created but field " + ZkStateReader.SHARED_INDEX + 
        " was missing from ZooKeeper metadata or not set", collection.getSharedIndex());

    // verify we've generated and stored in ZooKeeper a sharedShardName per shard, the name should match the one
    // generated by Assign.buildSharedSharedName
    for (Slice slice : collection.getSlices()) {
      Map<String, Object> props = slice.getProperties();
      assertEquals("Shared Shard Name is missing from the ZooKeeper metadata upon collection or shard creation",
          Assign.buildSharedShardName(collection.getName(), slice.getName()), props.get(ZkStateReader.SHARED_SHARD_NAME));
    } 
  }
}
