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

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateLog;
import org.junit.Test;

public class TestLeaderRecoverFromLogOnStartup extends AbstractFullDistribZkTestBase {
  @Override
  public void distribSetUp() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    super.distribSetUp();
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    AtomicInteger countReplayLog = new AtomicInteger(0);
    DirectUpdateHandler2.commitOnClose = false;
    UpdateLog.testing_logReplayFinishHook = new Runnable() {
      @Override
      public void run() {
        countReplayLog.incrementAndGet();
      }
    };

    String testCollectionName = "testCollection";
    createCollection(testCollectionName, 2, 2, 1);
    waitForRecoveriesToFinish(false);

    cloudClient.setDefaultCollection(testCollectionName);
    cloudClient.add(sdoc("id", "1"));
    cloudClient.add(sdoc("id", "2"));
    cloudClient.add(sdoc("id", "3"));
    cloudClient.add(sdoc("id", "4"));

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    QueryResponse resp = cloudClient.query(params);
    assertEquals(0, resp.getResults().getNumFound());

    ChaosMonkey.stop(jettys);
    ChaosMonkey.stop(controlJetty);
    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));
    ChaosMonkey.start(jettys);
    ChaosMonkey.start(controlJetty);
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), testCollectionName, 120000));

    cloudClient.commit();
    resp = cloudClient.query(params);
    assertEquals(4, resp.getResults().getNumFound());
    // Make sure all nodes is recover from tlog
    assertEquals(4, countReplayLog.get());
  }
}
