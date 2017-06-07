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


import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.util.StrUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class DeleteNodeTest extends SolrCloudTestCase {
  
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
    CloudSolrClient cloudClient = cluster.getSolrClient();
    String coll = "deletenodetest_coll";
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    CollectionAdminRequest.Create create = pickRandom(
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 1, 0),
        CollectionAdminRequest.createCollection(coll, "conf1", 5, 0, 1, 1));
    create.setCreateNodeSet(StrUtils.join(l, ',')).setMaxShardsPerNode(3);
    cloudClient.request(create);
    String node2bdecommissioned = l.get(0);
    new CollectionAdminRequest.DeleteNode(node2bdecommissioned).processAsync("003", cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus("003");
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
  }
}
