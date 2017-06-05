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
package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.SolrClientDataProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestPolicyCloud extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @After
  public void after() throws Exception {
    cluster.deleteAllCollections();
    cluster.getSolrClient().getZkStateReader().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        "{}".getBytes(StandardCharsets.UTF_8), true);
  }

  public void testCreateCollectionAddShardUsingPolicy() throws Exception {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    int port = jetty.getLocalPort();

    String commands =  "{set-policy :{c1 : [{replica:1 , shard:'#EACH', port: '" + port + "'}]}}";
    cluster.getSolrClient().request(AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, commands));
    Map<String, Object> json = Utils.getJson(cluster.getZkClient(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, true);
    assertEquals("full json:"+ Utils.toJSONString(json) , "#EACH",
        Utils.getObjectByPath(json, true, "/policies/c1[0]/shard"));
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", null, "s1,s2", 1)
        .setPolicy("c1")
        .process(cluster.getSolrClient());

    DocCollection coll = getCollectionState("policiesTest");
    assertEquals("c1", coll.getPolicyName());
    assertEquals(2,coll.getReplicas().size());
    coll.forEachReplica((s, replica) -> assertEquals(jetty.getNodeName(), replica.getNodeName()));
    CollectionAdminRequest.createShard("policiesTest", "s3").process(cluster.getSolrClient());
    coll = getCollectionState("policiesTest");
    assertEquals(1, coll.getSlice("s3").getReplicas().size());
    coll.getSlice("s3").forEach(replica -> assertEquals(jetty.getNodeName(), replica.getNodeName()));
  }

  public void testDataProvider() throws IOException, SolrServerException, KeeperException, InterruptedException {
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "shard1", 2)
        .process(cluster.getSolrClient());
    DocCollection rulesCollection = getCollectionState("policiesTest");
    SolrClientDataProvider provider = new SolrClientDataProvider(cluster.getSolrClient());
    Map<String, Object> val = provider.getNodeValues(rulesCollection.getReplicas().get(0).getNodeName(), Arrays.asList(
        "freedisk",
        "cores",
        "heapUsage",
        "sysLoadAvg"));
    assertNotNull(val.get("freedisk"));
    assertNotNull(val.get("heapUsage"));
    assertNotNull(val.get("sysLoadAvg"));
    assertTrue(((Number) val.get("cores")).intValue() > 0);
    assertTrue("freedisk value is " + ((Number) val.get("freedisk")).doubleValue(),  Double.compare(((Number) val.get("freedisk")).doubleValue(), 0.0d) > 0);
    assertTrue("heapUsage value is " + ((Number) val.get("heapUsage")).doubleValue(), Double.compare(((Number) val.get("heapUsage")).doubleValue(), 0.0d) > 0);
    assertTrue("sysLoadAvg value is " + ((Number) val.get("sysLoadAvg")).doubleValue(), Double.compare(((Number) val.get("sysLoadAvg")).doubleValue(), 0.0d) > 0);
    String overseerNode = OverseerTaskProcessor.getLeaderNode(cluster.getZkClient());
    cluster.getSolrClient().request(CollectionAdminRequest.addRole(overseerNode, "overseer"));
    for (int i = 0; i < 10; i++) {
      Map<String, Object> data = Utils.getJson(cluster.getZkClient(), ZkStateReader.ROLES, true);
      if (i >= 9 && data.isEmpty()) {
        throw new RuntimeException("NO overseer node created");
      }
      Thread.sleep(100);
    }
    val = provider.getNodeValues(overseerNode, Arrays.asList(
        "nodeRole",
        "ip_1", "ip_2", "ip_3", "ip_4",
        "sysprop.java.version",
        "sysprop.java.vendor"));
    assertEquals("overseer", val.get("nodeRole"));
    assertNotNull(val.get("ip_1"));
    assertNotNull(val.get("ip_2"));
    assertNotNull(val.get("ip_3"));
    assertNotNull(val.get("ip_4"));
    assertNotNull(val.get("sysprop.java.version"));
    assertNotNull(val.get("sysprop.java.vendor"));
  }
}
