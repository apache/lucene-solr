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

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for PREPRECOVERY CoreAdmin API
 */
public class TestPrepRecovery extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    // the default is 180s and our waitForState times out in 90s
    // so we lower this so that we can still test timeouts
    System.setProperty("leaderConflictResolveWait", "500");
    System.setProperty("prepRecoveryReadTimeoutExtraWait", "0");

    
    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();
  }

  @Test
  @Ignore // nocommit
  public void testLeaderUnloaded() throws Exception {
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();

    String collectionName = "testLeaderUnloaded";
    CollectionAdminRequest.createCollection(collectionName, 1, 2)
        .setMaxShardsPerNode(100)
        .process(solrClient);

    JettySolrRunner newNode = cluster.startJettySolrRunner();

    String newNodeName = newNode.getNodeName();

    // add a replica to the new node so that it starts watching the collection
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(newNodeName)
        .process(solrClient);

    cluster.waitForActiveCollection(collectionName, 1, 3);

    // now delete the leader
    Replica leader = solrClient.getZkStateReader().getLeaderRetry(collectionName, "shard1");
    CollectionAdminRequest.deleteReplica(collectionName, "shard1", leader.getName())
        .process(solrClient);

    // add another replica to the new node. When it starts recovering, it will likely have stale state
    // and ask the erstwhile leader to PREPRECOVERY which will hang for about 30 seconds
    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
        .setNode(newNodeName)
        .process(solrClient);
  }

  @Test
  @Nightly
  public void testLeaderNotResponding() throws Exception {
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();

    String collectionName = "testLeaderNotResponding";
    CollectionAdminRequest.createCollection(collectionName, 1, 1)
        .setMaxShardsPerNode(100)
        .process(solrClient);

    TestInjection.prepRecoveryOpPauseForever = "true:100";
    try {
      // in the absence of fixes made in SOLR-9716, prep recovery waits forever and the following statement
      // times out
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .process(solrClient);
    } finally {
      TestInjection.prepRecoveryOpPauseForever = null;
      TestInjection.notifyPauseForeverDone();
    }
  }
}
