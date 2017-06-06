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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;


public class DeleteReplicaTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void deleteLiveReplicaTest() throws Exception {

    final String collectionName = "delLiveColl";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .process(cluster.getSolrClient());

    DocCollection state = getCollectionState(collectionName);
    Slice shard = getRandomShard(state);
    Replica replica = getRandomReplica(shard, (r) -> r.getState() == Replica.State.ACTIVE);

    CoreStatus coreStatus = getCoreStatus(replica);
    Path dataDir = Paths.get(coreStatus.getDataDirectory());

    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteReplica(collectionName, shard.getName(), replica.getName())
          .setOnlyIfDown(true)
          .process(cluster.getSolrClient());
    });
    assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().contains("state is 'active'"));
    assertTrue("Data directory for " + replica.getName() + " should not have been deleted", Files.exists(dataDir));

    CollectionAdminRequest.deleteReplica(collectionName, shard.getName(), replica.getName())
        .process(cluster.getSolrClient());
    waitForState("Expected replica " + replica.getName() + " to have been removed", collectionName, (n, c) -> {
      Slice testShard = c.getSlice(shard.getName());
      return testShard.getReplica(replica.getName()) == null;
    });

    assertFalse("Data directory for " + replica.getName() + " should have been removed", Files.exists(dataDir));

  }

  @Test
  public void deleteReplicaAndVerifyDirectoryCleanup() throws Exception {

    final String collectionName = "deletereplica_test";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2).process(cluster.getSolrClient());

    Replica leader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collectionName, "shard1");

    //Confirm that the instance and data directory exist
    CoreStatus coreStatus = getCoreStatus(leader);
    assertTrue("Instance directory doesn't exist", Files.exists(Paths.get(coreStatus.getInstanceDirectory())));
    assertTrue("DataDirectory doesn't exist", Files.exists(Paths.get(coreStatus.getDataDirectory())));

    CollectionAdminRequest.deleteReplica(collectionName, "shard1",leader.getName())
        .process(cluster.getSolrClient());

    Replica newLeader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collectionName, "shard1");

    assertFalse(leader.equals(newLeader));

    //Confirm that the instance and data directory were deleted by default
    assertFalse("Instance directory still exists", Files.exists(Paths.get(coreStatus.getInstanceDirectory())));
    assertFalse("DataDirectory still exists", Files.exists(Paths.get(coreStatus.getDataDirectory())));
  }

  @Test
  public void deleteReplicaByCount() throws Exception {

    final String collectionName = "deleteByCount";
    pickRandom(
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 3),
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 1, 1),
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1, 0, 2),
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 0, 1, 2))
    .process(cluster.getSolrClient());
    waitForState("Expected a single shard with three replicas", collectionName, clusterShape(1, 3));

    CollectionAdminRequest.deleteReplicasFromShard(collectionName, "shard1", 2).process(cluster.getSolrClient());
    waitForState("Expected a single shard with a single replica", collectionName, clusterShape(1, 1));
    
    try {
      CollectionAdminRequest.deleteReplicasFromShard(collectionName, "shard1", 1).process(cluster.getSolrClient());
      fail("Expected Exception, Can't delete the last replica by count");
    } catch (SolrException e) {
      // expected
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
      assertTrue(e.getMessage().contains("There is only one replica available"));
    }
    DocCollection docCollection = getCollectionState(collectionName);
    // We know that since leaders are preserved, PULL replicas should not be left alone in the shard
    assertEquals(0, docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    

  }

  @Test
  public void deleteReplicaByCountForAllShards() throws Exception {

    final String collectionName = "deleteByCountNew";
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2).process(cluster.getSolrClient());
    waitForState("Expected two shards with two replicas each", collectionName, clusterShape(2, 2));

    CollectionAdminRequest.deleteReplicasFromAllShards(collectionName, 1).process(cluster.getSolrClient());
    waitForState("Expected two shards with one replica each", collectionName, clusterShape(2, 1));

  }

}

