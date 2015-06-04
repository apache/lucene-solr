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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies cluster state remains consistent after collection reload.
 */
@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class CollectionReloadTest extends AbstractFullDistribZkTestBase {

  protected static final transient Logger log = LoggerFactory.getLogger(CollectionReloadTest.class);

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

    Replica leader = null;
    String replicaState = null;
    int timeoutSecs = 30;
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      Replica tmp = null;
      try {
        tmp = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
      } catch (Exception exc) {}
      if (tmp != null && "active".equals(tmp.getStr(ZkStateReader.STATE_PROP))) {
        leader = tmp;
        replicaState = "active";
        break;
      }
      Thread.sleep(1000);
    }
    assertNotNull("Could not find active leader for " + shardId + " of " +
        testCollectionName + " after "+timeoutSecs+" secs; clusterState: " +
        printClusterStateInfo(testCollectionName), leader);

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
    timeoutSecs = 15;
    timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      // state of leader should be active after session loss recovery - see SOLR-7338
      cloudClient.getZkStateReader().updateClusterState(true);
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

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    ZkCoreNodeProps coreProps = new ZkCoreNodeProps(replica);
    String coreName = coreProps.getCoreName();
    boolean reloadedOk = false;
    try (HttpSolrClient client = new HttpSolrClient(coreProps.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      Thread.sleep(1000);

      // send reload command for the collection
      log.info("Sending RELOAD command for "+testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);
      Thread.sleep(2000); // reload can take a short while

      // verify reload is done, waiting up to 30 seconds for slow test environments
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        statusResp = CoreAdminRequest.getStatus(coreName, client);
        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
        if (startTimeAfterReload > leaderCoreStartTime) {
          reloadedOk = true;
          break;
        }
        // else ... still waiting to see the reloaded core report a later start time
        Thread.sleep(1000);
      }
    }
    return reloadedOk;
  }

  private void createCollectionRetry(String testCollectionName, int numShards, int replicationFactor, int maxShardsPerNode)
      throws SolrServerException, IOException {
    CollectionAdminResponse resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
      resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);
      if (resp.getResponse().get("failure") != null)
        fail("Could not create " + testCollectionName);
    }
  }
}
