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

package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShardPreferences extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public void testOnlyNRT() throws Exception {
    String COLLECTION_NAME = "nrt_coll";
    MiniSolrCloudCluster cluster =
        configureCluster(2)
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .addConfig("conf", configset("configset-2"))
            .configure();
    try {
      JettySolrRunner leaderjetty = cluster.getJettySolrRunner(1);
      String leaderNode = leaderjetty.getNodeName();
      JettySolrRunner replicaJetty = cluster.getJettySolrRunner(0);
      String replicaNode = replicaJetty.getNodeName();

      CollectionAdminRequest
          .createCollection(COLLECTION_NAME, "conf", 2, 1)
          .setCreateNodeSet(leaderNode)
          .setMaxShardsPerNode(4)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);
      DocCollection coll = cluster.getSolrClient().getClusterStateProvider().getCollection(COLLECTION_NAME);
      log.info("THE_STATE1:{}" ,Utils.toJSONString(coll));

      CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "shard1", Replica.Type.PULL)
          .setCreateNodeSet(replicaNode)
          .process(cluster.getSolrClient());
      CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "shard2", Replica.Type.PULL)
          .setCreateNodeSet(replicaNode)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 2, 4);
      coll = cluster.getSolrClient().getClusterStateProvider().getCollection(COLLECTION_NAME);
      Map<String , Replica.Type> coreTypes = new HashMap<>();

      coll.forEachReplica((s, replica) -> coreTypes.put(replica.getCoreName(), replica.getType()));
      log.info("coreTypes: {}", coreTypes);

      log.info("THE_STATE2:{}" ,Utils.toJSONString(coll));

//      Replica pullReplica = coll.getReplica((s, replica) -> replica.getType() == Replica.Type.PULL);
//      Replica nrtReplica = coll.getReplica((s, replica) -> replica.getType() == Replica.Type.NRT);
//      assertNotNull(pullReplica);
      new UpdateRequest()
          .add("id", "1", "string", "Hello World 1!")
          .add("id", "2", "string", "Hello World 2!")
          .commit(cluster.getSolrClient(), COLLECTION_NAME);


      checkRsp(cluster.getSolrClient().getHttpClient(),
          leaderjetty.getBaseUrl() + "/" + COLLECTION_NAME + "/select?q=*:*&omitHeader=true&fl=id,string,[core]&shards.preference=replica.type:NRT",
          o -> {
            List l = (List) o;
            if(l != null && l.size()>1) {
              for (Object o1 : l) {
                Map m = (Map) o1;
                Object typ = coreTypes.get(m.get("[core]"));
                assertEquals(Replica.Type.NRT, typ);
                log.info("doc: {} is from type: {}", m.get("id"),typ);
              }
              return true;
            } else {
              return false;
            }
          });

      checkRsp(cluster.getSolrClient().getHttpClient(),
          replicaJetty.getBaseUrl() + "/" + COLLECTION_NAME + "/select?q=*:*&omitHeader=true&fl=id,string,[core]&shards.preference=replica.type:PULL",
          o -> {
            List l = (List) o;
            if(l != null && l.size()>1) {
              for (Object o1 : l) {
                Map m = (Map) o1;

                Object typ = coreTypes.get(m.get("[core]"));
                assertEquals(Replica.Type.PULL, typ);
                log.info("doc: {} is from type: {}", m.get("id"), typ);
              }
              return true;
            } else {
              return false;
            }
          });

      checkRsp(cluster.getSolrClient().getHttpClient(),
          replicaJetty.getBaseUrl() + "/" + COLLECTION_NAME + "/select?q=*:*&omitHeader=true&fl=id,string,[core]&shards.preference=replica.type:NRT",
          o -> {
            List l = (List) o;
            if(l != null && l.size()>1) {
              for (Object o1 : l) {
                Map m = (Map) o1;
                Object typ = coreTypes.get(m.get("[core]"));
                assertEquals(Replica.Type.NRT, typ);
                log.info("doc: {} is from type: {}", m.get("id"), typ);
              }
              return true;
            } else {
              return false;
            }
          });

    } finally {
      cluster.shutdown();
    }
  }

  private Predicate<Object> getObjectPredicate(Replica replica) {
    return o -> {
      List l = (List) Utils.getObjectByPath(o, false, "response/docs");
      if(l != null && l.size() > 0) {
        Map doc = (Map) l.get(0);
        assertEquals(replica.getCoreName(),doc.get("[core]"));
        return true;
      }
      return false;
    };
  }

  private void checkRsp(HttpClient client, String url, Predicate<Object> test) throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      Object results = Utils.executeGET(client, url, Utils.JSONCONSUMER);
      log.info("waiting after attempt={}, data :{}",i, Utils.toJSONString(results));
      if(test.test((List) Utils.getObjectByPath(results, false, "response/docs"))) break;
      Thread.sleep(1000);
    }
  }
}
