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

import java.lang.invoke.MethodHandles;
import java.util.Collection;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LogLevel("org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;")
public class AddReplicaTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  //commented 2-Aug-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void test() throws Exception {
    cluster.waitForAllNodes(5000);
    String collection = "addreplicatest_coll";

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collection, "conf1", 2, 1);
    create.setMaxShardsPerNode(2);
    cloudClient.request(create);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection coll = clusterState.getCollection(collection);
    String sliceName = coll.getSlices().iterator().next().getName();
    Collection<Replica> replicas = coll.getSlice(sliceName).getReplicas();
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collection, sliceName);
    addReplica.processAsync("000", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("000");
    CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
    assertTrue(rsp.getRequestStatus() != RequestStatusState.COMPLETED);
    // wait for async request success
    boolean success = false;
    for (int i = 0; i < 200; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse(rsp.toString(), rsp.getRequestStatus() == RequestStatusState.FAILED);
      Thread.sleep(500);
    }
    assertTrue(success);
    Collection<Replica> replicas2 = cloudClient.getZkStateReader().getClusterState().getCollection(collection).getSlice(sliceName).getReplicas();
    replicas2.removeAll(replicas);
    assertEquals(1, replicas2.size());
    Replica r = replicas2.iterator().next();
    assertTrue(r.toString(), r.getState() != Replica.State.ACTIVE);

    // use waitForFinalState
    addReplica.setWaitForFinalState(true);
    addReplica.processAsync("001", cloudClient);
    requestStatus = CollectionAdminRequest.requestStatus("001");
    rsp = requestStatus.process(cloudClient);
    assertTrue(rsp.getRequestStatus() != RequestStatusState.COMPLETED);
    // wait for async request success
    success = false;
    for (int i = 0; i < 200; i++) {
      rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse(rsp.toString(), rsp.getRequestStatus() == RequestStatusState.FAILED);
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
      assertTrue(coll.toString() + "\n" + replica.toString(), replica.getState() == Replica.State.ACTIVE);
    }
  }
}
