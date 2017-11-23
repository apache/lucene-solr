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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

public class TestUtilizeNode extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    JettySolrRunner overseerJetty = null;
    String overseerLeader = (String) overSeerStatus.get("leader");
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerJetty = jetty;
        break;
      }
    }
    if (overseerJetty == null) {
      fail("no overseer leader!");
    }
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Before
  public void beforeTest() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void test() throws Exception {
    cluster.waitForAllNodes(5000);
    int REPLICATION = 2;
    String coll = "utilizenodecoll";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, REPLICATION);
    cloudClient.request(create);

    JettySolrRunner runner = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);


    cloudClient.request(new CollectionAdminRequest.UtilizeNode(runner.getNodeName()));


    assertTrue(getReplicaCount(cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(coll), runner.getNodeName()) > 0);

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'port':" + runner.getLocalPort() +
        ", 'replica':0}" +
        "    ]" +
        "}";

    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    cloudClient.request(req);
    NamedList<Object> response = cloudClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    JettySolrRunner runner2 = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);

    cloudClient.request(new CollectionAdminRequest.UtilizeNode(runner2.getNodeName()));
    assertTrue("no replica should be present in  "+runner.getNodeName(),getReplicaCount(cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(coll), runner.getNodeName()) == 0);

    assertTrue(getReplicaCount(cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollection(coll), runner2.getNodeName()) > 0);

  }

  private int getReplicaCount(DocCollection collection, String node) {
    AtomicInteger count = new AtomicInteger();

    collection.forEachReplica((s, replica) -> {
      if (replica.getNodeName().equals(node)) {
        count.incrementAndGet();
      }
    });
    return count.get();
  }

}
