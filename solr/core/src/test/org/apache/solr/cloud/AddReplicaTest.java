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

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;
import static org.apache.solr.client.solrj.response.RequestStatusState.FAILED;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AddReplicaTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    cluster.deleteAllCollections();
  }

  @Test
  public void testAddMultipleReplicas() throws Exception  {

    String collection = "testAddMultipleReplicas";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf1", 1, 1);
    create.setMaxShardsPerNode(2);
    cloudClient.request(create);
    cluster.waitForActiveCollection(collection, 1, 1);

    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collection, "shard1")
        .setNrtReplicas(1)
        .setTlogReplicas(1)
        .setPullReplicas(1);
    RequestStatusState status = addReplica.processAndWait(collection + "_xyz1", cloudClient, 120);
    assertEquals(COMPLETED, status);
    
    cluster.waitForActiveCollection(collection, 1, 4);
    
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
    assertNotNull(docCollection);
    assertEquals(4, docCollection.getReplicas().size());
    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());

    // try to add 5 more replicas which should fail because numNodes(4)*maxShardsPerNode(2)=8 and 4 replicas already exist
    addReplica = CollectionAdminRequest.addReplicaToShard(collection, "shard1")
        .setNrtReplicas(3)
        .setTlogReplicas(1)
        .setPullReplicas(1);
    status = addReplica.processAndWait(collection + "_xyz1", cloudClient, 120);
    assertEquals(FAILED, status);
    docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
    assertNotNull(docCollection);
    // sanity check that everything is as before
    assertEquals(4, docCollection.getReplicas().size());
    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());

    // but adding any number of replicas is supported if an explicit create node set is specified
    // so test that as well
    LinkedHashSet<String> createNodeSet = new LinkedHashSet<>(2);
    createNodeSet.add(cluster.getRandomJetty(random()).getNodeName());
    while (true)  {
      String nodeName = cluster.getRandomJetty(random()).getNodeName();
      if (createNodeSet.add(nodeName))  break;
    }
    addReplica = CollectionAdminRequest.addReplicaToShard(collection, "shard1")
        .setNrtReplicas(3)
        .setTlogReplicas(1)
        .setPullReplicas(1)
        .setCreateNodeSet(String.join(",", createNodeSet));
    status = addReplica.processAndWait(collection + "_xyz1", cloudClient, 120);
    assertEquals(COMPLETED, status);
    waitForState("Timedout wait for collection to be created", collection, clusterShape(1, 9));
    docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
    assertNotNull(docCollection);
    // sanity check that everything is as before
    assertEquals(9, docCollection.getReplicas().size());
    assertEquals(5, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
  }

  @Test
  public void test() throws Exception {
    
    String collection = "addreplicatest_coll";

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf1", 2, 1);
    create.setMaxShardsPerNode(2);
    cloudClient.request(create);
    
    cluster.waitForActiveCollection(collection, 2, 2);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    String sliceName = coll.getSlices().iterator().next().getName();
    Collection<Replica> replicas = coll.getSlice(sliceName).getReplicas();
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collection, sliceName);
    addReplica.processAsync("000", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("000");
    CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
    assertNotSame(rsp.getRequestStatus(), COMPLETED);
    
    // wait for async request success
    boolean success = false;
    for (int i = 0; i < 200; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == COMPLETED) {
        success = true;
        break;
      }
      assertNotSame(rsp.toString(), rsp.getRequestStatus(), RequestStatusState.FAILED);
      Thread.sleep(500);
    }
    assertTrue(success);
    
    Collection<Replica> replicas2 = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getSlice(sliceName).getReplicas();
    replicas2.removeAll(replicas);
    assertEquals(1, replicas2.size());

    // use waitForFinalState
    addReplica.setWaitForFinalState(true);
    addReplica.processAsync("001", cloudClient);
    requestStatus = CollectionAdminRequest.requestStatus("001");
    rsp = requestStatus.process(cloudClient);
    assertNotSame(rsp.getRequestStatus(), COMPLETED);
    // wait for async request success
    success = false;
    for (int i = 0; i < 200; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == COMPLETED) {
        success = true;
        break;
      }
      assertNotSame(rsp.toString(), rsp.getRequestStatus(), RequestStatusState.FAILED);
      Thread.sleep(500);
    }
    assertTrue(success);
    // let the client watch fire
    Thread.sleep(1000);
    clusterState = cloudClient.getZkStateReader().getClusterState();
    coll = clusterState.getCollection(collection);
    Collection<Replica> replicas3 = coll.getSlice(sliceName).getReplicas();
    replicas3.removeAll(replicas);
    String replica2 = replicas2.iterator().next().getName();
    assertEquals(2, replicas3.size());
    for (Replica replica : replicas3) {
      if (replica.getName().equals(replica2)) {
        continue; // may be still recovering
      }
      assertSame(coll.toString() + "\n" + replica.toString(), replica.getState(), Replica.State.ACTIVE);
    }
  }
}
