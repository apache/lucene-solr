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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies cluster state remains consistent after collection reload.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class CollectionReloadTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public CollectionReloadTest() {
    super();
    sliceCount = 1;
  }
  
  @Test
  public void testReloadedLeaderStateAfterZkSessionLoss() throws Exception {
    waitForThingsToLevelOut(30000);

    log.info("testReloadedLeaderStateAfterZkSessionLoss initialized OK ... running test logic");

    String testCollectionName = "c8n_1x1";
    String shardId = "shard1";
    createCollectionRetry(testCollectionName, 1, 1, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    Replica leader = getShardLeader(testCollectionName, shardId, 30 /* timeout secs */);

    // reload collection and wait to see the core report it has been reloaded
    boolean wasReloaded = reloadCollection(leader, testCollectionName);
    assertTrue("Collection '"+testCollectionName+"' failed to reload within a reasonable amount of time!",
        wasReloaded);

    // cause session loss
    chaosMonkey.expireSession(getJettyOnPort(getReplicaPort(leader)));

    // TODO: have to wait a while for the node to get marked down after ZK session loss
    // but tests shouldn't be so timing dependent!
    Thread.sleep(15000);

    // wait up to 15 seconds to see the replica in the active state
    String replicaState = null;
    int timeoutSecs = 15;
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      // state of leader should be active after session loss recovery - see SOLR-7338
      cloudClient.getZkStateReader().forceUpdateCollection(testCollectionName);
      ClusterState cs = cloudClient.getZkStateReader().getClusterState();
      Slice slice = cs.getSlice(testCollectionName, shardId);
      replicaState = slice.getReplica(leader.getName()).getStr(ZkStateReader.STATE_PROP);
      if ("active".equals(replicaState))
        break;

      Thread.sleep(1000);
    }
    assertEquals("Leader state should be active after recovering from ZK session loss, but after " +
        timeoutSecs + " seconds, it is " + replicaState, "active", replicaState);

    // try to clean up
    try {
      new CollectionAdminRequest.Delete()
              .setCollectionName(testCollectionName).process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }

    log.info("testReloadedLeaderStateAfterZkSessionLoss succeeded ... shutting down now!");
  }
}
