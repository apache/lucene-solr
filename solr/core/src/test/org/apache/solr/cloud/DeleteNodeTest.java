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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

// nocommit flakey
@Ignore
public class DeleteNodeTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);
    configureCluster(TEST_NIGHTLY ? 6 : 3)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .formatZk(true).configure();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    String coll = "deletenodetest_coll";
    cloudClient.getZkStateReader().forciblyRefreshAllClusterStateSlow();
    ClusterState state = cloudClient.getZkStateReader().getClusterState();
    Set<String> liveNodes = state.getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    // NOTE: must be more than a single nrt replica or it will not let you delete a node
    CollectionAdminRequest.Create create = SolrTestCaseJ4.pickRandom(
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 1, 0),
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 1, 1),
        // check RF=1
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 1, 0)
        );
    create = create.setCreateNodeSet(StrUtils.join(l, ',')).setMaxShardsPerNode(20);
    cloudClient.request(create);
    state = cloudClient.getZkStateReader().getClusterState();
    String node2bdecommissioned = l.get(0);
    // check what replicas are on the node, and whether the call should fail

    //new CollectionAdminRequest.DeleteNode(node2bdecommissioned).processAsync("003", cloudClient);
    new CollectionAdminRequest.DeleteNode(node2bdecommissioned).process(cloudClient);
   // CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("003");
    CollectionAdminRequest.RequestStatusResponse rsp = null;
//    if (shouldFail) {
//      for (int i = 0; i < 10; i++) {
//        rsp = requestStatus.process(cloudClient);
//        if (rsp.getRequestStatus() == RequestStatusState.FAILED || rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
//          break;
//        }
//        Thread.sleep(500);
//      }
//    } else {
//      rsp = requestStatus.process(cloudClient);
//    }
    if (log.isInfoEnabled()) {
      log.info("####### DocCollection after: {}", cloudClient.getZkStateReader().getClusterState().getCollection(coll));
    }
//    if (shouldFail) {
//      assertTrue(String.valueOf(rsp), rsp.getRequestStatus() == RequestStatusState.FAILED);
//    } else {
//      assertFalse(String.valueOf(rsp), rsp.getRequestStatus() == RequestStatusState.FAILED);
//    }
  }
}
