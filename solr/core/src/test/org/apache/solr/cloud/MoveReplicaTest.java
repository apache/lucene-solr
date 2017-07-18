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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoveReplicaTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    cluster.waitForAllNodes(5000);
    String coll = "movereplicatest_coll";
    log.info("total_jettys: " + cluster.getJettySolrRunners().size());
    int REPLICATION = 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, REPLICATION);
    create.setMaxShardsPerNode(2);
    cloudClient.request(create);

    Replica replica = getRandomReplica(coll, cloudClient);
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node)) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);
    String shardId = null;
    for (Slice slice : cloudClient.getZkStateReader().getClusterState().getCollection(coll).getSlices()) {
      if (slice.getReplicas().contains(replica)) {
        shardId = slice.getName();
      }
    }

    CollectionAdminRequest.MoveReplica moveReplica = createMoveReplicaRequest(coll, replica, targetNode);
    moveReplica.processAsync("000", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("000");
    // wait for async request success
    boolean success = false;
    for (int i = 0; i < 200; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse(rsp.getRequestStatus() == RequestStatusState.FAILED);
      Thread.sleep(500);
    }
    assertTrue(success);
    checkNumOfCores(cloudClient, replica.getNodeName(), 0);
    assertTrue("should be at least one core on target node!", getNumOfCores(cloudClient, targetNode) > 0);
    // wait for recovery
    boolean recovered = false;
    for (int i = 0; i < 300; i++) {
      DocCollection collState = getCollectionState(coll);
      log.debug("###### " + collState);
      Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
      boolean allActive = true;
      boolean hasLeaders = true;
      if (replicas != null && !replicas.isEmpty()) {
        for (Replica r : replicas) {
          if (!r.getNodeName().equals(targetNode)) {
            continue;
          }
          if (!r.isActive(Collections.singleton(targetNode))) {
            log.info("Not active: " + r);
            allActive = false;
          }
        }
      } else {
        allActive = false;
      }
      for (Slice slice : collState.getSlices()) {
        if (slice.getLeader() == null) {
          hasLeaders = false;
        }
      }
      if (allActive && hasLeaders) {
        // check the number of active replicas
        assertEquals("total number of replicas", REPLICATION, replicas.size());
        recovered = true;
        break;
      } else {
        log.info("--- waiting, allActive=" + allActive + ", hasLeaders=" + hasLeaders);
        Thread.sleep(1000);
      }
    }
    assertTrue("replica never fully recovered", recovered);

    moveReplica = createMoveReplicaRequest(coll, replica, targetNode, shardId);
    moveReplica.process(cloudClient);
    checkNumOfCores(cloudClient, replica.getNodeName(), 1);
    // wait for recovery
    recovered = false;
    for (int i = 0; i < 300; i++) {
      DocCollection collState = getCollectionState(coll);
      log.debug("###### " + collState);
      Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
      boolean allActive = true;
      boolean hasLeaders = true;
      if (replicas != null && !replicas.isEmpty()) {
        for (Replica r : replicas) {
          if (!r.getNodeName().equals(replica.getNodeName())) {
            continue;
          }
          if (!r.isActive(Collections.singleton(replica.getNodeName()))) {
            log.info("Not active yet: " + r);
            allActive = false;
          }
        }
      } else {
        allActive = false;
      }
      for (Slice slice : collState.getSlices()) {
        if (slice.getLeader() == null) {
          hasLeaders = false;
        }
      }
      if (allActive && hasLeaders) {
        assertEquals("total number of replicas", REPLICATION, replicas.size());
        recovered = true;
        break;
      } else {
        Thread.sleep(1000);
      }
    }
    assertTrue("replica never fully recovered", recovered);
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(String coll, Replica replica, String targetNode, String shardId) {
    if (random().nextBoolean()) {
      return new CollectionAdminRequest.MoveReplica(coll, shardId, targetNode, replica.getNodeName());
    } else  {
      // for backcompat testing of SOLR-11068
      // todo remove in solr 8.0
      return new BackCompatMoveReplicaRequest(coll, shardId, targetNode, replica.getNodeName());
    }
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(String coll, Replica replica, String targetNode) {
    if (random().nextBoolean()) {
      return new CollectionAdminRequest.MoveReplica(coll, replica.getName(), targetNode);
    } else  {
      // for backcompat testing of SOLR-11068
      // todo remove in solr 8.0
      return new BackCompatMoveReplicaRequest(coll, replica.getName(), targetNode);
    }
  }

  /**
   * Added for backcompat testing
   * todo remove in solr 8.0
   */
  static class BackCompatMoveReplicaRequest extends CollectionAdminRequest.MoveReplica {
    public BackCompatMoveReplicaRequest(String collection, String replica, String targetNode) {
      super(collection, replica, targetNode);
    }

    public BackCompatMoveReplicaRequest(String collection, String shard, String sourceNode, String targetNode) {
      super(collection, shard, sourceNode, targetNode);
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      if (randomlyMoveReplica) {
        params.set(CollectionParams.FROM_NODE, sourceNode);
      }
      return params;
    }
  }

  private Replica getRandomReplica(String coll, CloudSolrClient cloudClient) {
    List<Replica> replicas = cloudClient.getZkStateReader().getClusterState().getCollection(coll).getReplicas();
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  private void checkNumOfCores(CloudSolrClient cloudClient, String nodeName, int expectedCores) throws IOException, SolrServerException {
    assertEquals(nodeName + " does not have expected number of cores",expectedCores, getNumOfCores(cloudClient, nodeName));
  }

  private int getNumOfCores(CloudSolrClient cloudClient, String nodeName) throws IOException, SolrServerException {
    try (HttpSolrClient coreclient = getHttpSolrClient(cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      return status.getCoreStatus().size();
    }
  }
}
