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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

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
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceNodeTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(6)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    cluster.waitForAllNodes(5000);
    String coll = "replacenodetest_coll";
    log.info("total_jettys: " + cluster.getJettySolrRunners().size());

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String emptyNode = l.remove(0);
    String node2bdecommissioned = l.get(0);
    CollectionAdminRequest.Create create;
    // NOTE: always using the createCollection that takes in 'int' for all types of replicas, so we never
    // have to worry about null checking when comparing the Create command with the final Slices
    create = pickRandom(
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2,0,0),
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,1,0),
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,1),
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,0,1),
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,2,0),
                        // check also replicationFactor 1
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,0,0),
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,0)
    );
    create.setCreateNodeSet(StrUtils.join(l, ',')).setMaxShardsPerNode(3);
    cloudClient.request(create);
    log.info("excluded_node : {}  ", emptyNode);
    createReplaceNodeRequest(node2bdecommissioned, emptyNode, null).processAsync("000", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("000");
    boolean success = false;
    for (int i = 0; i < 300; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse(rsp.getRequestStatus() == RequestStatusState.FAILED);
      Thread.sleep(50);
    }
    assertTrue(success);
    try (HttpSolrClient coreclient = getHttpSolrClient(cloudClient.getZkStateReader().getBaseUrlForNodeName(node2bdecommissioned))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      assertTrue(status.getCoreStatus().size() == 0);
    }

    //let's do it back
    createReplaceNodeRequest(emptyNode, node2bdecommissioned, Boolean.TRUE).processAsync("001", cloudClient);
    requestStatus = CollectionAdminRequest.requestStatus("001");

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
    try (HttpSolrClient coreclient = getHttpSolrClient(cloudClient.getZkStateReader().getBaseUrlForNodeName(emptyNode))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      assertEquals("Expecting no cores but found some: " + status.getCoreStatus(), 0, status.getCoreStatus().size());
    }
    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(coll);
    assertEquals(create.getNumShards().intValue(), collection.getSlices().size());
    for (Slice s:collection.getSlices()) {
      assertEquals(create.getNumNrtReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
      assertEquals(create.getNumTlogReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
      assertEquals(create.getNumPullReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    }
  }

  private CollectionAdminRequest.AsyncCollectionAdminRequest createReplaceNodeRequest(String sourceNode, String targetNode, Boolean parallel) {
    if (random().nextBoolean()) {
      return new CollectionAdminRequest.ReplaceNode(sourceNode, targetNode).setParallel(parallel);
    } else  {
      // test back compat with old param names
      // todo remove in solr 8.0
      return new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.REPLACENODE)  {
        @Override
        public SolrParams getParams() {
          ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
          params.set("source", sourceNode);
          params.set("target", targetNode);
          if (parallel != null) params.set("parallel", parallel.toString());
          return params;
        }
      };
    }
  }
}
