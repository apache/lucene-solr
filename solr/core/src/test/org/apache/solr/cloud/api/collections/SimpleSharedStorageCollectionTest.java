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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * Tests related to shared storage based collections, i.e. collectiong having only replicas of type {@link Replica.Type#SHARED}.
 */
public class SimpleSharedStorageCollectionTest extends AbstractFullDistribZkTestBase {

  public SimpleSharedStorageCollectionTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void testCreateCollection() throws Exception {
    String collectionName = "BlobBasedCollectionName1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,1,0).setSharedIndex(true).setSharedReplicas(1);

    NamedList<Object> request = create.process(cloudClient).getResponse();

    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
  }

  @Test
  @ShardsFixed(num = 1)
  public void testAddReplica() throws Exception {
    String collectionName = "BlobBasedCollectionName2";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,1,0).setSharedIndex(true).setSharedReplicas(1);

    NamedList<Object> request = create.process(cloudClient).getResponse();

    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

    try {
      // Let the request fail cleanly just in case, but in reality it fails with an exception since we throw a Runtime from down below
      assertFalse(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.NRT).process(cloudClient).isSuccess());
    } catch (Exception e) {
      assert e.getMessage().contains("Can't add a NRT replica to a collection backed by shared storage");
    }

    // Adding a SHARED replica is expected to work ok
    assertTrue(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.SHARED).process(cloudClient).isSuccess());
  }
}
