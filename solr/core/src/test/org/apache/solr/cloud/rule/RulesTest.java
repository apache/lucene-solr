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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.junit.matchers.JUnitMatchers.containsString;

@LuceneTestCase.Slow
public class RulesTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @After
  public void removeCollections() throws Exception {
    cluster.deleteAllCollections();
    // clear any cluster policy test methods may have set
    cluster.getSolrClient().getZkStateReader().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        "{}".getBytes(StandardCharsets.UTF_8), true);
  }

  @Test
  public void doIntegrationTest() throws Exception {
    assertEquals("Sanity Check: someone changed the cluster; " +
                 "test logic requires specific number of jetty nodes",
                 5, cluster.getJettySolrRunners().size());
    
    final long minGB = (random().nextBoolean() ? 1 : 0);
    final Path toTest = Paths.get("").toAbsolutePath();
    assumeTrue("doIntegrationTest needs minGB="+minGB+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(toTest) > minGB);

    String rulesColl = "rulesColl";
    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("cores:<4", "node:*,replica:<2", "freedisk:>"+minGB)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(rulesColl, 1, 2);
    
    DocCollection rulesCollection = getCollectionState(rulesColl);

    @SuppressWarnings({"rawtypes"})
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

    waitForState("Should have found shard1 w/2active replicas + shard2 w/1active replica",
                 rulesColl, (liveNodes, collection) -> {
                   // short circut if collection is deleted
                   // or we don't yet have the correct number of slices
                   if (null == collection || 2 != collection.getSlices().size()) {
                     return false;
                   }
                   final Set<String> replicaNodes = new HashSet<>();
                   for (Slice slice : collection.getSlices()) {
                     // short circut if our slice isn't active
                     if (Slice.State.ACTIVE != slice.getState()) {
                       return false;
                     }
                     if (slice.getName().equals("shard1")) {
                       // for shard1, we should have 2 fully live replicas
                       final List<Replica> liveReplicas = slice.getReplicas
                         ((r) -> r.isActive(liveNodes));
                       if (2 != liveReplicas.size()) {
                         return false;
                       }
                       replicaNodes.addAll(liveReplicas.stream().map
                                           (Replica::getNodeName).collect(Collectors.toList()));
                     } else if (slice.getName().equals("shard2")) {
                       // for shard2, we should have 3 fully live replicas
                       final List<Replica> liveReplicas = slice.getReplicas
                         ((r) -> r.isActive(liveNodes));
                       if (3 != liveReplicas.size()) {
                         return false;
                       }
                       replicaNodes.addAll(liveReplicas.stream().map
                                           (Replica::getNodeName).collect(Collectors.toList()));
                     } else {
                       // WTF?
                       return false;
                     }
                   }
                   // now sanity check that the rules were *obeyed* and
                   // each replica is on a unique node
                   return 5 == replicaNodes.size();
                 });

    // adding an additional replica should fail since our rule says at most one replica
    // per node, and we know every node already has one replica
    expectedException.expect(HttpSolrClient.RemoteSolrException.class);
    expectedException.expectMessage(containsString("current number of eligible live nodes 0"));
    CollectionAdminRequest.addReplicaToShard(rulesColl, "shard2").process(cluster.getSolrClient());
    
  }

  @Test
  public void testPortRuleInPresenceOfClusterPolicy() throws Exception  {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String port = Integer.toString(jetty.getLocalPort());

    // this cluster policy prohibits having any replicas on a node with the above port
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'replica': 0, 'port':'" + port + "'}" +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    cluster.getSolrClient().request(req);

    // but this collection is created with a replica placement rule that says all replicas must be created
    // on a node with above port (in direct conflict with the cluster policy)
    String rulesColl = "portRuleColl2";
    CollectionAdminRequest.createCollectionWithImplicitRouter(rulesColl, "conf", "shard1", 2)
        .setRule("port:" + port)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());
    
    waitForState("Collection should have followed port rule w/ImplicitSnitch, not cluster policy",
                 rulesColl, (liveNodes, rulesCollection) -> {
                   // first sanity check that the collection exists & the rules/snitch are listed
                   if (null == rulesCollection) {
                     return false;
                   } else {
                     @SuppressWarnings({"rawtypes"})
                     List list = (List) rulesCollection.get("rule");
                     if (null == list || 1 != list.size()) {
                       return false;
                     }
                     if (! port.equals(((Map) list.get(0)).get("port"))) {
                       return false;
                     }
                     list = (List) rulesCollection.get("snitch");
                     if (null == list || 1 != list.size()) {
                       return false;
                     }
                     if (! "ImplicitSnitch".equals(((Map)list.get(0)).get("class"))) {
                       return false;
                     }
                   }
                   if (2 != rulesCollection.getReplicas().size()) {
                     return false;
                   }
                   // now sanity check that the rules were *obeyed*
                   // (and the contradictory policy was ignored)
                   return rulesCollection.getReplicas().stream().allMatch
                     (replica -> (replica.getNodeName().contains(port) &&
                                  replica.isActive(liveNodes)));
                 });
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

    waitForState("Collection should have followed port rule w/ImplicitSnitch, not cluster policy",
                 rulesColl, (liveNodes, rulesCollection) -> {
                   // first sanity check that the collection exists & the rules/snitch are listed
                   if (null == rulesCollection) {
                     return false;
                   } else {
                     @SuppressWarnings({"rawtypes"})
                     List list = (List) rulesCollection.get("rule");
                     if (null == list || 1 != list.size()) {
                       return false;
                     }
                     if (! port.equals(((Map) list.get(0)).get("port"))) {
                       return false;
                     }
                     list = (List) rulesCollection.get("snitch");
                     if (null == list || 1 != list.size()) {
                       return false;
                     }
                     if (! "ImplicitSnitch".equals(((Map)list.get(0)).get("class"))) {
                       return false;
                     }
                   }
                   if (2 != rulesCollection.getReplicas().size()) {
                     return false;
                   }
                   // now sanity check that the rules were *obeyed*
                   return rulesCollection.getReplicas().stream().allMatch
                     (replica -> (replica.getNodeName().contains(port) &&
                                  replica.isActive(liveNodes)));
                 });
  }

  @Test
  @SuppressWarnings({"unchecked"})
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
    
    cluster.waitForActiveCollection(rulesColl, 1, 2);

    DocCollection rulesCollection = getCollectionState(rulesColl);
    @SuppressWarnings({"rawtypes"})
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
  public void testInvokeApi() throws Exception {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    try (SolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      GenericSolrRequest req =  new GenericSolrRequest(GET, "/____v2/node/invoke", new ModifiableSolrParams()
          .add("class", ImplicitSnitch.class.getName())
          .add("cores", "1")
          .add("freedisk", "1")
      );
      SimpleSolrResponse rsp = req.process(client);
      assertNotNull(((Map) rsp.getResponse().get(ImplicitSnitch.class.getName())).get("cores"));
      assertNotNull(((Map) rsp.getResponse().get(ImplicitSnitch.class.getName())).get("freedisk"));
    }
  }


  @Test
  public void testModifyColl() throws Exception {
    final Path toTest = Paths.get("").toAbsolutePath();

    final long minGB1 = (random().nextBoolean() ? 1 : 0);
    final long minGB2 = 5;
    assumeTrue("testModifyColl needs minGB1="+minGB1+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(toTest) > minGB1);
    assumeTrue("testModifyColl needs minGB2="+minGB2+" usable disk space",
        ImplicitSnitch.getUsableSpaceInGB(toTest) > minGB2);

    String rulesColl = "modifyColl";
    CollectionAdminRequest.createCollection(rulesColl, "conf", 1, 2)
        .setRule("cores:<4", "node:*,replica:1", "freedisk:>" + minGB1)
        .setSnitch("class:ImplicitSnitch")
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(rulesColl, 1, 2);


    // TODO: Make a MODIFYCOLLECTION SolrJ class
    ModifiableSolrParams p = new ModifiableSolrParams();
    p.add("collection", rulesColl);
    p.add("action", "MODIFYCOLLECTION");
    p.add("rule", "cores:<5");
    p.add("rule", "node:*,replica:1");
    p.add("rule", "freedisk:>"+minGB2);
    p.add("autoAddReplicas", "true");
    cluster.getSolrClient().request(new GenericSolrRequest(POST, COLLECTIONS_HANDLER_PATH, p));

    waitForState("Should have found updated rules in DocCollection",
                 rulesColl, (liveNodes, rulesCollection) -> {
                   if (null == rulesCollection) {
                     return false;
                   } 
                   @SuppressWarnings({"rawtypes"})
                   List list = (List) rulesCollection.get("rule");
                   if (null == list || 3 != list.size()) {
                     return false;
                   }
                   if (! "<5".equals(((Map) list.get(0)).get("cores"))) {
                     return false;
                   }
                   if (! "1".equals(((Map) list.get(1)).get("replica"))) {
                     return false;
                   }
                   if (! (">"+minGB2).equals(((Map) list.get(2)).get("freedisk"))) {
                     return false;
                   }
                   if (! "true".equals(String.valueOf(rulesCollection.getProperties().get("autoAddReplicas")))) {
                     return false;
                   }
                   list = (List) rulesCollection.get("snitch");
                   if (null == list || 1 != list.size()) {
                     return false;
                   }
                   if (! "ImplicitSnitch".equals(((Map) list.get(0)).get("class"))) {
                     return false;
                   }
                   return true;
                 });
    
  }
}
