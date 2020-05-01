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

import org.apache.http.NoHttpResponseException;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.RTimer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.List;

public class HttpPartitionOnCommitTest extends BasicDistributedZkTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long sleepMsBeforeHealPartition = 2000L;

  private final boolean onlyLeaderIndexes = random().nextBoolean();

  @BeforeClass
  public static void setupSysProps() {
    System.setProperty("socketTimeout", "5000");
    System.setProperty("distribUpdateSoTimeout", "5000");
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("solr.retries.on.forward", "0");
    System.setProperty("solr.retries.to.followers", "0"); 
  }
  
  public HttpPartitionOnCommitTest() {
    super();
    sliceCount = 1;
    fixShardCount(4);
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  @Override
  @Test
  public void test() throws Exception {
    oneShardTest();
    multiShardTest();
  }

  private void multiShardTest() throws Exception {

    log.info("Running multiShardTest");

    // create a collection that has 2 shard and 2 replicas
    String testCollectionName = "c8n_2x2_commits";
    createCollection(testCollectionName, "conf1", 2, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 2, 2, 30);
    assertTrue("Expected 1 replicas for collection " + testCollectionName
            + " but found " + notLeaders.size() + "; clusterState: "
            + printClusterStateInfo(),
        notLeaders.size() == 1);

    if (log.isInfoEnabled()) {
      log.info("All replicas active for {}", testCollectionName);
    }

    // let's put the leader in its own partition, no replicas can contact it now
    Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    if (log.isInfoEnabled()) {
      log.info("Creating partition to leader at {}", leader.getCoreUrl());
    }
    SocketProxy leaderProxy = getProxyForReplica(leader);
    leaderProxy.close();

    // let's find the leader of shard2 and ask him to commit
    Replica shard2Leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard2");
    sendCommitWithRetry(shard2Leader);

    Thread.sleep(sleepMsBeforeHealPartition);

    cloudClient.getZkStateReader().forceUpdateCollection(testCollectionName); // get the latest state
    leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    assertSame("Leader was not active", Replica.State.ACTIVE, leader.getState());

    if (log.isInfoEnabled()) {
      log.info("Healing partitioned replica at {}", leader.getCoreUrl());
    }
    leaderProxy.reopen();
    Thread.sleep(sleepMsBeforeHealPartition);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);

    log.info("multiShardTest completed OK");
  }

  private void oneShardTest() throws Exception {
    log.info("Running oneShardTest");

    // create a collection that has 1 shard and 3 replicas
    String testCollectionName = "c8n_1x3_commits";
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, 30);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
            + " but found " + notLeaders.size() + "; clusterState: "
            + printClusterStateInfo(),
        notLeaders.size() == 2);

    log.info("All replicas active for {}", testCollectionName);

    // let's put the leader in its own partition, no replicas can contact it now
    Replica leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    if (log.isInfoEnabled()) {
      log.info("Creating partition to leader at {}", leader.getCoreUrl());
    }

    SocketProxy leaderProxy = getProxyForReplica(leader);
    leaderProxy.close();

    Replica replica = notLeaders.get(0);
    sendCommitWithRetry(replica);
    Thread.sleep(sleepMsBeforeHealPartition);

    cloudClient.getZkStateReader().forceUpdateCollection(testCollectionName); // get the latest state
    leader = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    assertSame("Leader was not active", Replica.State.ACTIVE, leader.getState());

    if (log.isInfoEnabled()) {
      log.info("Healing partitioned replica at {}", leader.getCoreUrl());
    }
    leaderProxy.reopen();
    Thread.sleep(sleepMsBeforeHealPartition);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);

    log.info("oneShardTest completed OK");
  }

  /**
   * Overrides the parent implementation to install a SocketProxy in-front of the Jetty server.
   */
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
                                     String shardList, String solrConfigOverride, String schemaOverride, Replica.Type replicaType)
      throws Exception {
    return createProxiedJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, replicaType);
  }

  protected void sendCommitWithRetry(Replica replica) throws Exception {
    String replicaCoreUrl = replica.getCoreUrl();
    log.info("Sending commit request to: {}", replicaCoreUrl);
    final RTimer timer = new RTimer();
    try (HttpSolrClient client = getHttpSolrClient(replicaCoreUrl)) {
      try {
        client.commit();

        if (log.isInfoEnabled()) {
          log.info("Sent commit request to {} OK, took {}ms", replicaCoreUrl, timer.getTime());
        }
      } catch (Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        if (rootCause instanceof NoHttpResponseException) {
          log.warn("No HTTP response from sending commit request to {}; will re-try after waiting 3 seconds", replicaCoreUrl);
          Thread.sleep(3000);
          client.commit();
          log.info("Second attempt at sending commit to {} succeeded", replicaCoreUrl);
        } else {
          throw exc;
        }
      }
    }
  }

}
