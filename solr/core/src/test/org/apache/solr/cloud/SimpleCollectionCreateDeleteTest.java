package org.apache.solr.cloud;

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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class SimpleCollectionCreateDeleteTest extends AbstractFullDistribZkTestBase {

  public SimpleCollectionCreateDeleteTest() {
    sliceCount = 1;
  }

  @Test
  @ShardsFixed(num = 1)
  public void test() throws Exception {
    String overseerNode = OverseerCollectionProcessor.getLeaderNode(cloudClient.getZkStateReader().getZkClient());
    String notOverseerNode = null;
    for (CloudJettyRunner cloudJetty : cloudJettys) {
      if (!overseerNode.equals(cloudJetty.nodeName)) {
        notOverseerNode = cloudJetty.nodeName;
        break;
      }
    }
    CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
    String collectionName = "SimpleCollectionCreateDeleteTest";
    create.setCollectionName(collectionName);
    create.setNumShards(1);
    create.setReplicationFactor(1);
    create.setCreateNodeSet(overseerNode);
    ModifiableSolrParams params = new ModifiableSolrParams(create.getParams());
    params.set("stateFormat", "2");
    QueryRequest req = new QueryRequest(params);
    req.setPath("/admin/collections");
    NamedList<Object> request = cloudClient.request(req);

    if (request.get("success") != null) {
      assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

      CollectionAdminRequest.Delete delete = new CollectionAdminRequest.Delete();
      delete.setCollectionName(collectionName);
      cloudClient.request(delete);

      assertFalse(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));

      // create collection again on a node other than the overseer leader
      create = new CollectionAdminRequest.Create();
      create.setCollectionName(collectionName);
      create.setNumShards(1);
      create.setReplicationFactor(1);
      create.setCreateNodeSet(notOverseerNode);
      params = new ModifiableSolrParams(create.getParams());
      params.set("stateFormat", "2");
      req = new QueryRequest(params);
      req.setPath("/admin/collections");
      request = cloudClient.request(req);
      assertTrue("Collection creation should not have failed", request.get("success") != null);
    }
  }
}
