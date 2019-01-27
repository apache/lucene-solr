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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
public class LeaderElectionIntegrationTest extends SolrCloudTestCase {
  private final static int NUM_REPLICAS_OF_SHARD1 = 5;
  
  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(6)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }


  private void createCollection(String collection) throws IOException, SolrServerException {
    assertEquals(0, CollectionAdminRequest.createCollection(collection,
        "conf", 2, 1)
        .setMaxShardsPerNode(1).process(cluster.getSolrClient()).getStatus());
    for (int i = 1; i < NUM_REPLICAS_OF_SHARD1; i++) {
      assertTrue(
          CollectionAdminRequest.addReplicaToShard(collection, "shard1").process(cluster.getSolrClient()).isSuccess()
      );
    }
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testSimpleSliceLeaderElection() throws Exception {
    String collection = "collection1";
    createCollection(collection);

    List<JettySolrRunner> stoppedRunners = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      // who is the leader?
      String leader = getLeader(collection);
      JettySolrRunner jetty = getRunner(leader);
      assertNotNull(jetty);
      assertTrue("shard1".equals(jetty.getCoreContainer().getCores().iterator().next()
          .getCoreDescriptor().getCloudDescriptor().getShardId()));
      jetty.stop();
      stoppedRunners.add(jetty);

      // poll until leader change is visible
      for (int j = 0; j < 90; j++) {
        String currentLeader = getLeader(collection);
        if(!leader.equals(currentLeader)) {
          break;
        }
        Thread.sleep(500);
      }

      leader = getLeader(collection);
      int retry = 0;
      while (jetty == getRunner(leader)) {
        if (retry++ == 60) {
          break;
        }
        Thread.sleep(1000);
      }

      if (jetty == getRunner(leader)) {
        cluster.getZkClient().printLayoutToStdOut();
        fail("We didn't find a new leader! " + jetty + " was close, but it's still showing as the leader");
      }

      assertTrue("shard1".equals(getRunner(leader).getCoreContainer().getCores().iterator().next()
          .getCoreDescriptor().getCloudDescriptor().getShardId()));
    }

    for (JettySolrRunner runner : stoppedRunners) {
      runner.start();
    }
    waitForState("Expected to see nodes come back " + collection, collection,
        (n, c) -> {
          return n.size() == 6;
        });
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());

    // testLeaderElectionAfterClientTimeout
    collection = "collection2";
    createCollection(collection);

    // TODO: work out the best timing here...
    System.setProperty("zkClientTimeout", Integer.toString(ZkTestServer.TICK_TIME * 2 + 100));
    // timeout the leader
    String leader = getLeader(collection);
    JettySolrRunner jetty = getRunner(leader);
    ZkController zkController = jetty.getCoreContainer().getZkController();

    zkController.getZkClient().getSolrZooKeeper().closeCnxn();
    cluster.getZkServer().expire(zkController.getZkClient().getSolrZooKeeper().getSessionId());

    for (int i = 0; i < 60; i++) { // wait till leader is changed
      if (jetty != getRunner(getLeader(collection))) {
        break;
      }
      Thread.sleep(100);
    }

    // make sure we have waited long enough for the first leader to have come back
    Thread.sleep(ZkTestServer.TICK_TIME * 2 + 100);

    // kill everyone but the first leader that should have reconnected by now
    for (JettySolrRunner jetty2 : cluster.getJettySolrRunners()) {
      if (jetty != jetty2) {
        jetty2.stop();
      }
    }

    for (int i = 0; i < 320; i++) { // wait till leader is changed
      try {
        if (jetty == getRunner(getLeader(collection))) {
          break;
        }
        Thread.sleep(100);
      } catch (Exception e) {
        continue;
      }
    }

    assertEquals(jetty, getRunner(getLeader(collection)));
  }

  private JettySolrRunner getRunner(String nodeName) {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()){
      if (!jettySolrRunner.isStopped() && nodeName.equals(jettySolrRunner.getNodeName())) return jettySolrRunner;
    }
    return null;
  }

  private String getLeader(String collection) throws InterruptedException {
    
    ZkNodeProps props = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collection, "shard1", 30000);
    String leader = props.getStr(ZkStateReader.NODE_NAME_PROP);
    
    return leader;
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    System.clearProperty("solrcloud.skip.autorecovery");
  }
}
