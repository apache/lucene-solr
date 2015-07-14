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

import static org.apache.solr.cloud.CollectionsAPIDistributedZkTest.*;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

public class DeleteInactiveReplicaTest extends AbstractFullDistribZkTestBase{

  @Test
  public void deleteInactiveReplicaTest() throws Exception {
    try (CloudSolrClient client = createCloudClient(null)) {

      String collectionName = "delDeadColl";

      setClusterProp(client, ZkStateReader.LEGACY_CLOUD, "false");

      int replicationFactor = 2;
      int numShards = 2;
      int maxShardsPerNode = ((((numShards+1) * replicationFactor) / getCommonCloudSolrClient()
          .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

      Map<String,List<Integer>> collectionInfos = new HashMap<>();
      createCollection(collectionInfos, collectionName, numShards, replicationFactor, maxShardsPerNode, client, null);

      waitForRecoveriesToFinish(collectionName, false);

      Thread.sleep(3000);

      boolean stopped = false;
      JettySolrRunner stoppedJetty = null;
      StringBuilder sb = new StringBuilder();
      Replica replica1 = null;
      Slice shard1 = null;
      long timeout = System.currentTimeMillis() + 3000;
      DocCollection testcoll = null;
      while (!stopped && System.currentTimeMillis() < timeout) {
        testcoll = client.getZkStateReader().getClusterState().getCollection(collectionName);
        for (JettySolrRunner jetty : jettys)
          sb.append(jetty.getBaseUrl()).append(",");

        for (Slice slice : testcoll.getActiveSlices()) {
          for (Replica replica : slice.getReplicas())
            for (JettySolrRunner jetty : jettys) {
              URL baseUrl = null;
              try {
                baseUrl = jetty.getBaseUrl();
              } catch (Exception e) {
                continue;
              }
              if (baseUrl.toString().startsWith(
                  replica.getStr(ZkStateReader.BASE_URL_PROP))) {
                stoppedJetty = jetty;
                ChaosMonkey.stop(jetty);
                replica1 = replica;
                shard1 = slice;
                stopped = true;
                break;
              }
            }
        }
        Thread.sleep(100);
      }


      if (!stopped) {
        fail("Could not find jetty to stop in collection " + testcoll
            + " jettys: " + sb);
      }

      long endAt = System.currentTimeMillis() + 3000;
      boolean success = false;
      while (System.currentTimeMillis() < endAt) {
        testcoll = client.getZkStateReader()
            .getClusterState().getCollection(collectionName);
        if (testcoll.getSlice(shard1.getName()).getReplica(replica1.getName()).getState() != Replica.State.ACTIVE) {
          success = true;
        }
        if (success) break;
        Thread.sleep(100);
      }

      log.info("removed_replicas {}/{} ", shard1.getName(), replica1.getName());
      DeleteReplicaTest.removeAndWaitForReplicaGone(collectionName, client, replica1,
          shard1.getName());
      ChaosMonkey.start(stoppedJetty);
      log.info("restarted jetty");

      Map m = Utils.makeMap("qt", "/admin/cores", "action", "status");

      try (SolrClient queryClient = new HttpSolrClient(replica1.getStr(ZkStateReader.BASE_URL_PROP))) {
        NamedList<Object> resp = queryClient.request(new QueryRequest(new MapSolrParams(m)));
        assertNull("The core is up and running again",
            ((NamedList) resp.get("status")).get(replica1.getStr("core")));
      }

      Exception exp = null;

      try {

        m = Utils.makeMap(
            "action", CoreAdminParams.CoreAdminAction.CREATE.toString(),
            ZkStateReader.COLLECTION_PROP, collectionName,
            ZkStateReader.SHARD_ID_PROP, "shard2",
            CoreAdminParams.NAME, "testcore");

        QueryRequest request = new QueryRequest(new MapSolrParams(m));
        request.setPath("/admin/cores");
        NamedList<Object> rsp = client.request(request);
      } catch (Exception e) {
        exp = e;
        log.info("error_expected", e);
      }
      assertNotNull("Exception expected", exp);
      setClusterProp(client, ZkStateReader.LEGACY_CLOUD, null);

    }

  }
}
