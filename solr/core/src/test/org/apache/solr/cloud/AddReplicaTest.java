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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Ignore // nocommit leaking
public class AddReplicaTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static AtomicInteger asyncId = new AtomicInteger(100);


  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception  {
    super.tearDown();
    cluster.getZkClient().printLayout();
  }

  @Test
  @Ignore // nocommit - adding too many replicas?
  public void testAddMultipleReplicas() throws Exception  {

    String collection = "testAddMultipleReplicas";
    CloudHttp2SolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf1", 1, 1);
    create.setMaxShardsPerNode(20);
    cloudClient.request(create);

    cluster.waitForActiveCollection(collection, 1, 1);

    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collection, "s1")
        .setNrtReplicas(1)
        .setTlogReplicas(1)
        .setPullReplicas(1);
    CollectionAdminResponse status = addReplica.process(cloudClient, collection + "_xyz1");


     assertTrue(status.isSuccess());

    cluster.waitForActiveCollection(collection, 1, 4);
    
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
    assertNotNull(docCollection);
    assertEquals(docCollection.toString(), 4, docCollection.getReplicas().size());
    assertEquals(docCollection.toString(), 2, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(docCollection.toString(), 1, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    assertEquals(docCollection.toString(), 1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());

    // try to add 5 more replicas which should fail because numNodes(4)*maxShardsPerNode(2)=8 and 4 replicas already exist
// nocommit - maybe this only worked with the right assign policy?
//    addReplica = CollectionAdminRequest.addReplicaToShard(collection, "shard1")
//        .setNrtReplicas(3)
//        .setTlogReplicas(1)
//        .setPullReplicas(1);
//    try {
//      addReplica.process(cloudClient, collection + "_xyz1");
//      fail("expected fail");
//    } catch (SolrException e) {
//
//    }

//    docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
//    assertNotNull(docCollection);
//    // sanity check that everything is as before
//    assertEquals(4, docCollection.getReplicas().size());
//    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
//    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
//    assertEquals(1, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());

    // but adding any number of replicas is supported if an explicit create node set is specified
    // so test that as well
    LinkedHashSet<String> createNodeSet = new LinkedHashSet<>(2);
    createNodeSet.add(cluster.getRandomJetty(random()).getNodeName());
    while (true)  {
      String nodeName = cluster.getRandomJetty(random()).getNodeName();
      if (createNodeSet.add(nodeName))  break;
    }
    assert createNodeSet.size() > 0;
    addReplica = CollectionAdminRequest.addReplicaToShard(collection, "s1")
        .setNrtReplicas(3)
        .setTlogReplicas(1)
        .setPullReplicas(1)
        .setCreateNodeSet(String.join(",", createNodeSet));
    status = addReplica.process(cloudClient, collection + "_xyz1");


    assertTrue(status.isSuccess());

    cluster.waitForActiveCollection(collection, 1, 9);

    docCollection = cloudClient.getZkStateReader().getClusterState().getCollectionOrNull(collection);
    assertNotNull(docCollection);
    // sanity check that everything is as before
//    assertEquals(9, docCollection.getReplicas().size());
//    assertEquals(5, docCollection.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
//    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
//    assertEquals(2, docCollection.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
  }

  @Test
  //@Ignore // nocommit we were not failing on all errors in the http2 solr client - this weirdly keeps failing, saying an asyncid that should be free is already claimed.
  public void test() throws Exception {
    
    String collection = "addreplicatest_coll";

    CloudHttp2SolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf1", 2, 1);
    create.setMaxShardsPerNode(100);
    cloudClient.request(create);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    String sliceName = coll.getSlices().iterator().next().getName();
    Collection<Replica> replicas = coll.getSlice(sliceName).getReplicas();
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collection, sliceName);
    int aid1 = asyncId.incrementAndGet();
    addReplica.processAsync(Integer.toString(aid1), cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(Integer.toString(aid1));
    CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);

    assertNotSame(rsp.getRequestStatus(), COMPLETED);
    
    // wait for async request success
    boolean success = false;
    for (int i = 0; i < 600; i++) {
      rsp = requestStatus.process(cloudClient);
      //System.out.println("resp:" + rsp);
      if (rsp.getRequestStatus() == COMPLETED) {
        success = true;
        break;
      }
      assertNotSame(rsp.toString(), rsp.getRequestStatus(), RequestStatusState.FAILED);
      Thread.sleep(100);
    }
    assertTrue(success);

    // use waitForFinalState - doesn't exist, just dont do async
   // addReplica.setWaitForFinalState(true);
    int aid2 = asyncId.incrementAndGet();
    addReplica.processAsync(Integer.toString(aid2), cloudClient);
    requestStatus = CollectionAdminRequest.requestStatus(Integer.toString(aid2));
    rsp = requestStatus.process(cloudClient);

    cluster.waitForActiveCollection(collection, 2, 4);

    // nocommit - this should be able to wait now, look into basecloudclients wait for cluster state call

  }
}
