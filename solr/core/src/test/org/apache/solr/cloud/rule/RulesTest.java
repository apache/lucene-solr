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
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.junit.matchers.JUnitMatchers.containsString;

@LuceneTestCase.Slow
public class RulesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @After
  public void removeCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void doIntegrationTest() throws Exception {
    final long minGB = (random().nextBoolean() ? 1 : 0);
    assumeTrue("doIntegrationTest needs minGB="+minGB+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(Paths.get("/")) > minGB);

    String rulesColl = "rulesColl";
    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("cores:<4", "node:*,replica:<2", "freedisk:>"+minGB)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());

    DocCollection rulesCollection = getCollectionState(rulesColl);

    List list = (List) rulesCollection.get("rule");
    assertEquals(3, list.size());
    assertEquals ( "<4", ((Map)list.get(0)).get("cores"));
    assertEquals("<2", ((Map) list.get(1)).get("replica"));
    assertEquals(">"+minGB, ((Map) list.get(2)).get("freedisk"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));

    CollectionAdminRequest.createShard(rulesColl, "shard2").process(cluster.getSolrClient());
    CollectionAdminRequest.addReplicaToShard(rulesColl, "shard2").process(cluster.getSolrClient());

  }

  @Test
  public void testPortRule() throws Exception {

    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String port = Integer.toString(jetty.getLocalPort());

    String rulesColl = "portRuleColl";
    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("port:" + port)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());

    DocCollection rulesCollection = getCollectionState(rulesColl);

    List list = (List) rulesCollection.get("rule");
    assertEquals(1, list.size());
    assertEquals(port, ((Map) list.get(0)).get("port"));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals ( "ImplicitSnitch", ((Map)list.get(0)).get("class"));

  }

  @Test
  public void testHostFragmentRule() throws Exception {

    String rulesColl = "hostFragment";

    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String host = jetty.getBaseUrl().getHost();
    String[] ipFragments = host.split("\\.");
    String ip_1 = ipFragments[ipFragments.length - 1];
    String ip_2 = ipFragments[ipFragments.length - 2];

    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("ip_2:" + ip_2, "ip_1:" + ip_1)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());

    DocCollection rulesCollection = getCollectionState(rulesColl);
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

    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String host = jetty.getBaseUrl().getHost();
    String[] ipFragments = host.split("\\.");
    String ip_1 = ipFragments[ipFragments.length - 1];
    String ip_2 = ipFragments[ipFragments.length - 2];

    expectedException.expect(HttpSolrClient.RemoteSolrException.class);
    expectedException.expectMessage(containsString("ip_1"));

    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("ip_2:" + ip_2, "ip_1:" + ip_1 + "9999")
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());

  }


  @Test
  public void testModifyColl() throws Exception {

    final long minGB1 = (random().nextBoolean() ? 1 : 0);
    final long minGB2 = 5;
    assumeTrue("testModifyColl needs minGB1="+minGB1+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(Paths.get("/")) > minGB1);
    assumeTrue("testModifyColl needs minGB2="+minGB2+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(Paths.get("/")) > minGB2);

    String rulesColl = "modifyColl";
    CollectionAdminRequest.createCollection(rulesColl, "conf", 1, 2)
        .setRule("cores:<4", "node:*,replica:1", "freedisk:>" + minGB1)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());


    // TODO: Make a MODIFYCOLLECTION SolrJ class
    ModifiableSolrParams p = new ModifiableSolrParams();
    p.add("collection", rulesColl);
    p.add("action", "MODIFYCOLLECTION");
    p.add("rule", "cores:<5");
    p.add("rule", "node:*,replica:1");
    p.add("rule", "freedisk:>"+minGB2);
    p.add("autoAddReplicas", "true");
    cluster.getSolrClient().request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p));

    DocCollection rulesCollection = getCollectionState(rulesColl);
    log.info("version_of_coll {}  ", rulesCollection.getZNodeVersion());
    List list = (List) rulesCollection.get("rule");
    assertEquals(3, list.size());
    assertEquals("<5", ((Map) list.get(0)).get("cores"));
    assertEquals("1", ((Map) list.get(1)).get("replica"));
    assertEquals(">"+minGB2, ((Map) list.get(2)).get("freedisk"));
    assertEquals("true", String.valueOf(rulesCollection.getProperties().get("autoAddReplicas")));
    list = (List) rulesCollection.get("snitch");
    assertEquals(1, list.size());
    assertEquals("ImplicitSnitch", ((Map) list.get(0)).get("class"));

  }
}
