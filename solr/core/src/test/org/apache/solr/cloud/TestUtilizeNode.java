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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;org.apache.solr.client.solrj.impl.SolrClientDataProvider=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper=TRACE")
public class TestUtilizeNode extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(3)
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
    int REPLICATION = 2;
    String coll = "utilizenodecoll";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    log.info("Creating Collection...");
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf1", 2, REPLICATION)
        .setMaxShardsPerNode(2);
    cloudClient.request(create);

    log.info("Spinning up additional jettyX...");
    JettySolrRunner jettyX = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);

    assertNoReplicas("jettyX should not yet be utilized: ", coll, jettyX);

    if (log.isInfoEnabled()) {
      log.info("Sending UTILIZE command for jettyX ({})", jettyX.getNodeName());
    }
    cloudClient.request(new CollectionAdminRequest.UtilizeNode(jettyX.getNodeName()));

    // TODO: aparently we can't assert this? ...
    //
    // assertSomeReplicas("jettyX should now be utilized: ", coll, jettyX);
    //
    // ... it appears from the docs that unless there are policy violations,
    // this can be ignored unless jettyX has less "load" then other jetty instances?
    //
    // if the above is true, that means that this test is incredibly weak...
    // unless we know jettyX has at least one replica, then all the subsequent testing of the
    // port blacklist & additional UTILIZE command for jettyY are a waste of time.
    //
    // should we skip spinning up a *new* jettyX, and instead just pick an existing jetty?

    if (log.isInfoEnabled()) {
      log.info("jettyX replicas prior to being blacklisted: {}", getReplicaList(coll, jettyX));
    }
    
    String setClusterPolicyCommand = "{" +
      " 'set-cluster-policy': [" +
      "    {'port':" + jettyX.getLocalPort() +
      "     , 'replica':0}" +
      "  ]" +
      "}";
    if (log.isInfoEnabled()) {
      log.info("Setting new policy to blacklist jettyX ({}) port={}",
          jettyX.getNodeName(), jettyX.getLocalPort());
    }
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    NamedList<Object> response = cloudClient.request(req);
    assertEquals(req + " => " + response,
                 "success", response.get("result").toString());

    log.info("Spinning up additional jettyY...");
    JettySolrRunner jettyY = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    
    assertNoReplicas("jettyY should not yet be utilized: ", coll, jettyY);
    if (log.isInfoEnabled()) {
      log.info("jettyX replicas prior to utilizing jettyY: {}", getReplicaList(coll, jettyX));
      log.info("Sending UTILIZE command for jettyY ({})", jettyY.getNodeName()); // logOk
    }
    cloudClient.request(new CollectionAdminRequest.UtilizeNode(jettyY.getNodeName()));

    assertSomeReplicas("jettyY should now be utilized: ", coll, jettyY);
    
    assertNoReplicas("jettyX should no longer be utilized: ", coll, jettyX); 
    

  }

  /**
   * Gets the list of replicas for the specified collection hosted on the specified node
   * and then asserts that it has no replicas
   */
  private void assertNoReplicas(String prefix, String collectionName, JettySolrRunner jettyNode) throws IOException {
                                
    final List<Replica> replicas = getReplicaList(collectionName, jettyNode);
    assertEquals(prefix + " " + jettyNode.getNodeName() + " => " + replicas,
                 0, replicas.size());
  }
  
  /**
   * Gets the list of replicas for the specified collection hosted on the specified node
   * and then asserts that it there is at least one
   */
  private void assertSomeReplicas(String prefix, String collectionName, JettySolrRunner jettyNode) throws IOException {
                                
    final List<Replica> replicas = getReplicaList(collectionName, jettyNode);
    assertTrue(prefix + " " + jettyNode.getNodeName() + " => " + replicas,
               0 < replicas.size());
  }
  
  /**
   * Returns a list of all Replicas for the specified collection hosted on the specified node using
   * an <em>uncached</em> ClusterState call (so it should be authoritative from ZK).
   */
  private List<Replica> getReplicaList(String collectionName, JettySolrRunner jettyNode) throws IOException {
    DocCollection collection = cluster.getSolrClient().getClusterStateProvider()
      // we do *NOT* want to trust the cache, because anytime we call this method we have just
      // done a lot of mucking with the cluster
      .getClusterState().getCollectionOrNull(collectionName, false);
    
    List<Replica> results = new ArrayList<>(3);
    if (collection != null) {
      collection.forEachReplica((s, replica) -> {
        if (replica.getNodeName().equals(jettyNode.getNodeName())) {
          results.add(replica);
        }
      });
    }
    return results;
  }

}
