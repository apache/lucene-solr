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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests leader-initiated recovery scenarios after a leader node fails
 * and one of the replicas is out-of-sync.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class LeaderFailoverAfterPartitionTest extends HttpPartitionTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public LeaderFailoverAfterPartitionTest() {
    super();
  }


  @Test
  //28-June-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void test() throws Exception {
    waitForThingsToLevelOut(30000);

    // kill a leader and make sure recovery occurs as expected
    testRf3WithLeaderFailover();
  }

  protected void testRf3WithLeaderFailover() throws Exception {
    // now let's create a partition in one of the replicas and outright
    // kill the leader ... see what happens
    // create a collection that has 1 shard but 3 replicas
    String testCollectionName = "c8n_1x3_lf"; // _lf is leader fails
    createCollection(testCollectionName, "conf1", 1, 3, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);
    
    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
        + " but found " + notLeaders.size() + "; clusterState: "
        + printClusterStateInfo(testCollectionName),
        notLeaders.size() == 2);
        
    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy0 = null;
    proxy0 = getProxyForReplica(notLeaders.get(0));
    
    proxy0.close();
    
    // indexing during a partition
    sendDoc(2);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy0.reopen();
    
    SocketProxy proxy1 = getProxyForReplica(notLeaders.get(1));
    
    proxy1.close();
    
    sendDoc(3);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    proxy1.reopen();
    
    // sent 4 docs in so far, verify they are on the leader and replica
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive); 
    
    sendDoc(4);
    
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 4);    
        
    Replica leader = 
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    String leaderNode = leader.getNodeName();
    assertNotNull("Could not find leader for shard1 of "+
      testCollectionName+"; clusterState: "+printClusterStateInfo(testCollectionName), leader);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));
    
    // since maxShardsPerNode is 1, we're safe to kill the leader
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);    
    proxy0 = getProxyForReplica(notLeaders.get(0));
    proxy0.close();
        
    // indexing during a partition
    // doc should be on leader and 1 replica
    sendDoc(5);
    
    try (HttpSolrClient server = getHttpSolrClient(leader, testCollectionName)) {
      assertDocExists(server, testCollectionName, "5");
    }

    try (HttpSolrClient server = getHttpSolrClient(notLeaders.get(1), testCollectionName)) {
      assertDocExists(server, testCollectionName, "5");
    }
  
    Thread.sleep(sleepMsBeforeHealPartition);
    
    String shouldNotBeNewLeaderNode = notLeaders.get(0).getNodeName();

    //chaosMonkey.expireSession(leaderJetty);
    // kill the leader
    leaderJetty.stop();
    if (leaderJetty.isRunning())
      fail("Failed to stop the leader on "+leaderNode);
        
    SocketProxy oldLeaderProxy = getProxyForReplica(leader);
    if (oldLeaderProxy != null) {
      oldLeaderProxy.close();      
    } else {
      log.warn("No SocketProxy found for old leader node {}",leaderNode);
    }

    Thread.sleep(10000); // give chance for new leader to be elected.
    
    Replica newLeader = 
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 60000);
        
    assertNotNull("No new leader was elected after 60 seconds; clusterState: "+
      printClusterStateInfo(testCollectionName),newLeader);
        
    assertTrue("Expected node "+shouldNotBeNewLeaderNode+
        " to NOT be the new leader b/c it was out-of-sync with the old leader! ClusterState: "+
        printClusterStateInfo(testCollectionName),
        !shouldNotBeNewLeaderNode.equals(newLeader.getNodeName()));
    
    proxy0.reopen();
    
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(90, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      List<Replica> activeReps = getActiveOrRecoveringReplicas(testCollectionName, "shard1");
      if (activeReps.size() >= 2) break;
      Thread.sleep(1000);
    }

    List<Replica> participatingReplicas = getActiveOrRecoveringReplicas(testCollectionName, "shard1");
    assertTrue("Expected 2 of 3 replicas to be active but only found "+
            participatingReplicas.size()+"; "+participatingReplicas+"; clusterState: "+
            printClusterStateInfo(testCollectionName),
        participatingReplicas.size() >= 2);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(6));
    doc.addField("a_t", "hello" + 6);
    sendDocsWithRetry(Collections.singletonList(doc), 1, 3, 1);

    Set<String> replicasToCheck = new HashSet<>();
    for (Replica stillUp : participatingReplicas)
      replicasToCheck.add(stillUp.getName());
    waitToSeeReplicasActive(testCollectionName, "shard1", replicasToCheck, 90);
    assertDocsExistInAllReplicas(participatingReplicas, testCollectionName, 1, 6);

    // try to clean up
    attemptCollectionDelete(cloudClient, testCollectionName);
  }
}
