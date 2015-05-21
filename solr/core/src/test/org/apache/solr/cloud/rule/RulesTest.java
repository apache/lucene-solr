package org.apache.solr.cloud.rule;

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

import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.core.CoreContainer.COLLECTIONS_HANDLER_PATH;

public class RulesTest extends AbstractFullDistribZkTestBase {
  static final Logger log = LoggerFactory.getLogger(RulesTest.class);

  @Test
  @ShardsFixed(num = 5)
  public void doIntegrationTest() throws Exception {
    String rulesColl = "rulesColl";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setShards("shard1");
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setReplicationFactor(2);
      create.setRule("cores:<4", "node:*,replica:<2", "freedisk:>1");
      create.setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

    }

    DocCollection rulesCollection = cloudClient.getZkStateReader().getClusterState().getCollection(rulesColl);
    List list = (List) rulesCollection.get("rule");
    assertEquals(3, list.size());
    assertEquals ( "<4", ((Map)list.get(0)).get("cores"));
    assertEquals("<2", ((Map) list.get(1)).get("replica"));
    assertEquals(">1", ((Map) list.get(2)).get("freedisk"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));

    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.CreateShard createShard = new CollectionAdminRequest.CreateShard();
      createShard.setCollectionName(rulesColl);
      createShard.setShardName("shard2");
      rsp = createShard.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

      CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica();
      addReplica.setCollectionName(rulesColl);
      addReplica.setShardName("shard2");
      rsp = createShard.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());
    }


  }

  @Test
  public void testModifyColl() throws Exception {
    String rulesColl = "modifyColl";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setNumShards(1);
      create.setReplicationFactor(2);
      create.setRule("cores:<4", "node:*,replica:1", "freedisk:>1");
      create.setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());
      ModifiableSolrParams p = new ModifiableSolrParams();
      p.add("collection", rulesColl);
      p.add("action", "MODIFYCOLLECTION");
      p.add("rule", "cores:<5");
      p.add("rule", "node:*,replica:1");
      p.add("rule", "freedisk:>5");
      p.add("autoAddReplicas", "true");
      client.request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p));
    }


    for (int i = 0; i < 20; i++) {
      DocCollection rulesCollection = ZkStateReader.getCollectionLive(cloudClient.getZkStateReader(), rulesColl);
      log.info("version_of_coll {}  ", rulesCollection.getZNodeVersion());
      List list = (List) rulesCollection.get("rule");
      assertEquals(3, list.size());
      if (!"<5".equals(((Map) list.get(0)).get("cores"))) {
        if (i < 19) {
          Thread.sleep(100);
          continue;
        }

      }
      assertEquals("<5", ((Map) list.get(0)).get("cores"));
      assertEquals("1", ((Map) list.get(1)).get("replica"));
      assertEquals(">5", ((Map) list.get(2)).get("freedisk"));
      assertEquals("true", String.valueOf(rulesCollection.getProperties().get("autoAddReplicas")));
      list = (List) rulesCollection.get("snitch");
      assertEquals(1, list.size());
      assertEquals("ImplicitSnitch", ((Map) list.get(0)).get("class"));
    }
  }



}
