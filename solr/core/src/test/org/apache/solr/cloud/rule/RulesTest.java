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
package org.apache.solr.cloud.rule;

import java.lang.invoke.MethodHandles;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.junit.matchers.JUnitMatchers.containsString;

public class RulesTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  @ShardsFixed(num = 5)
  public void doIntegrationTest() throws Exception {
    final long minGB = (random().nextBoolean() ? 1 : 0);
    assumeTrue("doIntegrationTest needs minGB="+minGB+" usable disk space", ImplicitSnitch.getUsableSpaceInGB() > minGB);
    String rulesColl = "rulesColl";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create()
              .setCollectionName(rulesColl)
              .setShards("shard1")
              .setRouterName(ImplicitDocRouter.NAME)
              .setReplicationFactor(2)
              .setRule("cores:<4", "node:*,replica:<2", "freedisk:>"+minGB)
              .setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

    }

    DocCollection rulesCollection = cloudClient.getZkStateReader().getClusterState().getCollection(rulesColl);
    List list = (List) rulesCollection.get("rule");
    assertEquals(3, list.size());
    assertEquals ( "<4", ((Map)list.get(0)).get("cores"));
    assertEquals("<2", ((Map) list.get(1)).get("replica"));
    assertEquals(">"+minGB, ((Map) list.get(2)).get("freedisk"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));

    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.CreateShard createShard = new CollectionAdminRequest.CreateShard()
              .setCollectionName(rulesColl)
              .setShardName("shard2");
      rsp = createShard.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

      CollectionAdminRequest.AddReplica addReplica = new CollectionAdminRequest.AddReplica()
              .setCollectionName(rulesColl)
              .setShardName("shard2");
      rsp = addReplica.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());
    }


  }

  @Test
  public void testPortRule() throws Exception {
    String rulesColl = "portRuleColl";
    String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    String port = "-1";
    Matcher hostAndPortMatcher = Pattern.compile("(?:https?://)?([^:]+):(\\d+)").matcher(baseUrl);
    if (hostAndPortMatcher.find()) {
      port = hostAndPortMatcher.group(2);
    }
    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setShards("shard1");
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setReplicationFactor(2);
      create.setRule("port:" + port);
      create.setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

    }

    DocCollection rulesCollection = cloudClient.getZkStateReader().getClusterState().getCollection(rulesColl);
    List list = (List) rulesCollection.get("rule");
    assertEquals(1, list.size());
    assertEquals(port, ((Map) list.get(0)).get("port"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));
  }

  @Test
  public void testHostFragmentRule() throws Exception {
    String rulesColl = "ipRuleColl";
    String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    String ip_1 = "-1";
    String ip_2 = "-1";
    Matcher hostAndPortMatcher = Pattern.compile("(?:https?://)?([^:]+):(\\d+)").matcher(baseUrl);
    if (hostAndPortMatcher.find()) {
      String[] ipFragments = hostAndPortMatcher.group(1).split("\\.");
      ip_1 = ipFragments[ipFragments.length - 1];
      ip_2 = ipFragments[ipFragments.length - 2];
    }

    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setShards("shard1");
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setReplicationFactor(2);
      create.setRule("ip_2:" + ip_2, "ip_1:" + ip_1);
      create.setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());

    }

    DocCollection rulesCollection = cloudClient.getZkStateReader().getClusterState().getCollection(rulesColl);
    List<Map> list = (List<Map>) rulesCollection.get("rule");
    assertEquals(2, list.size());
    assertEquals(ip_2, list.get(0).get("ip_2"));
    assertEquals(ip_1, list.get(1).get("ip_1"));

    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals("ImplicitSnitch", list.get(0).get("class"));
  }


  @Test
  public void testHostFragmentRuleThrowsExceptionWhenIpDoesNotMatch() throws Exception {
    String rulesColl = "ipRuleColl";
    String baseUrl = getBaseUrl((HttpSolrClient) clients.get(0));
    String ip_1 = "-1";
    String ip_2 = "-1";
    Matcher hostAndPortMatcher = Pattern.compile("(?:https?://)?([^:]+):(\\d+)").matcher(baseUrl);
    if (hostAndPortMatcher.find()) {
      String[] ipFragments = hostAndPortMatcher.group(1).split("\\.");
      ip_1 = ipFragments[ipFragments.length - 1];
      ip_2 = ipFragments[ipFragments.length - 2];
    }

    try (SolrClient client = createNewSolrClient("", baseUrl)) {
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
      create.setCollectionName(rulesColl);
      create.setShards("shard1");
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setReplicationFactor(2);

      create.setRule("ip_2:" + ip_2, "ip_1:" + ip_1 + "9999");
      create.setSnitch("class:ImplicitSnitch");

      expectedException.expect(HttpSolrClient.RemoteSolrException.class);
      expectedException.expectMessage(containsString("ip_1"));

      create.process(client);
    }

  }


  @Test
  public void testModifyColl() throws Exception {
    final long minGB1 = (random().nextBoolean() ? 1 : 0);
    final long minGB2 = 5;
    assumeTrue("testModifyColl needs minGB1="+minGB1+" usable disk space", ImplicitSnitch.getUsableSpaceInGB() > minGB1);
    assumeTrue("testModifyColl needs minGB2="+minGB2+" usable disk space", ImplicitSnitch.getUsableSpaceInGB() > minGB2);
    String rulesColl = "modifyColl";
    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse rsp;
      CollectionAdminRequest.Create create = new CollectionAdminRequest.Create()
              .setCollectionName(rulesColl)
              .setNumShards(1)
              .setReplicationFactor(2)
              .setRule("cores:<4", "node:*,replica:1", "freedisk:>"+minGB1)
              .setSnitch("class:ImplicitSnitch");
      rsp = create.process(client);
      assertEquals(0, rsp.getStatus());
      assertTrue(rsp.isSuccess());
      ModifiableSolrParams p = new ModifiableSolrParams();
      p.add("collection", rulesColl);
      p.add("action", "MODIFYCOLLECTION");
      p.add("rule", "cores:<5");
      p.add("rule", "node:*,replica:1");
      p.add("rule", "freedisk:>"+minGB2);
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
      assertEquals(">"+minGB2, ((Map) list.get(2)).get("freedisk"));
      assertEquals("true", String.valueOf(rulesCollection.getProperties().get("autoAddReplicas")));
      list = (List) rulesCollection.get("snitch");
      assertEquals(1, list.size());
      assertEquals("ImplicitSnitch", ((Map) list.get(0)).get("class"));
    }
  }
}
