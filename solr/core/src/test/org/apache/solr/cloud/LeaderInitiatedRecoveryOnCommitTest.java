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

import java.io.File;
import java.util.List;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.junit.After;
import org.junit.Before;

public class LeaderInitiatedRecoveryOnCommitTest extends BasicDistributedZkTest {

  private static final long sleepMsBeforeHealPartition = 2000L;

  public LeaderInitiatedRecoveryOnCommitTest() {
    super();
    sliceCount = 1;
    shardCount = 4;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }

  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("numShards");

    try {
      super.tearDown();
    } catch (Exception exc) {
    }

    resetExceptionIgnores();

    // close socket proxies after super.tearDown
    if (!proxies.isEmpty()) {
      for (SocketProxy proxy : proxies.values()) {
        proxy.close();
      }
    }
  }

  @Override
  public void doTest() throws Exception {
    oneShardTest();
    multiShardTest();
  }

  private void multiShardTest() throws Exception {
    // create a collection that has 1 shard and 3 replicas
    String testCollectionName = "c8n_2x2_commits";
    createCollection(testCollectionName, 2, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 2, 2, 30);
    assertTrue("Expected 1 replicas for collection " + testCollectionName
            + " but found " + notLeaders.size() + "; clusterState: "
            + printClusterStateInfo(),
        notLeaders.size() == 1);

    // let's put the leader in it's own partition, no replicas can contact it now
    Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    SocketProxy leaderProxy = getProxyForReplica(leader);
    leaderProxy.close();

    // let's find the leader of shard2 and ask him to commit
    Replica shard2Leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard2");
    HttpSolrServer server = new HttpSolrServer(ZkCoreNodeProps.getCoreUrl(shard2Leader.getStr("base_url"), shard2Leader.getStr("core")));
    server.commit();

    Thread.sleep(sleepMsBeforeHealPartition);

    cloudClient.getZkStateReader().updateClusterState(true); // get the latest state
    leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    assertEquals("Leader was not active", "active", leader.getStr("state"));

    leaderProxy.reopen();
    Thread.sleep(sleepMsBeforeHealPartition);

    // try to clean up
    try {
      CollectionAdminRequest req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  private void oneShardTest() throws Exception {
    // create a collection that has 1 shard and 3 replicas
    String testCollectionName = "c8n_1x3_commits";
    createCollection(testCollectionName, 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, 30);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
            + " but found " + notLeaders.size() + "; clusterState: "
            + printClusterStateInfo(),
        notLeaders.size() == 2);

    // let's put the leader in it's own partition, no replicas can contact it now
    Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    SocketProxy leaderProxy = getProxyForReplica(leader);
    leaderProxy.close();

    Replica replica = notLeaders.get(0);
    HttpSolrServer server = new HttpSolrServer(ZkCoreNodeProps.getCoreUrl(replica.getStr("base_url"), replica.getStr("core")));
    server.commit();

    Thread.sleep(sleepMsBeforeHealPartition);

    cloudClient.getZkStateReader().updateClusterState(true); // get the latest state
    leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    assertEquals("Leader was not active", "active", leader.getStr("state"));

    leaderProxy.reopen();
    Thread.sleep(sleepMsBeforeHealPartition);

    // try to clean up
    try {
      CollectionAdminRequest req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  /**
   * Overrides the parent implementation to install a SocketProxy in-front of the Jetty server.
   */
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
                                     String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    return createProxiedJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride);
  }

}