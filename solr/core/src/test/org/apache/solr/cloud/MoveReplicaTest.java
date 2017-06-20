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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
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

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, 2);
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

    CollectionAdminRequest.MoveReplica moveReplica = new CollectionAdminRequest.MoveReplica(coll, replica.getName(), targetNode);
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
      Thread.sleep(50);
    }
    assertTrue(success);
    checkNumOfCores(cloudClient, replica.getNodeName(), 0);
    checkNumOfCores(cloudClient, targetNode, 2);

    moveReplica = new CollectionAdminRequest.MoveReplica(coll, shardId, targetNode, replica.getNodeName());
    moveReplica.process(cloudClient);
    checkNumOfCores(cloudClient, replica.getNodeName(), 1);
    checkNumOfCores(cloudClient, targetNode, 1);
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
