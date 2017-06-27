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
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestOnReconnectListenerSupport extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TestOnReconnectListenerSupport() {
    super();
    sliceCount = 2;
    fixShardCount(3);
  }

  @BeforeClass
  public static void initSysProperties() {
    System.setProperty("managed.schema.mutable", "false");
    System.setProperty("enable.update.log", "true");
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-managed-schema.xml";
  }

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(30000);

    String testCollectionName = "c8n_onreconnect_1x1";
    String shardId = "shard1";
    createCollectionRetry(testCollectionName, "conf1", 1, 1, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    Replica leader = getShardLeader(testCollectionName, shardId, 30 /* timeout secs */);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));

    // get the ZkController for the node hosting the leader
    CoreContainer cores = leaderJetty.getCoreContainer();
    ZkController zkController = cores.getZkController();
    assertNotNull("ZkController is null", zkController);

    String leaderCoreName = leader.getStr(CORE_NAME_PROP);
    String leaderCoreId;
    try (SolrCore leaderCore = cores.getCore(leaderCoreName)) {
      assertNotNull("SolrCore for "+leaderCoreName+" not found!", leaderCore);
      leaderCoreId = leaderCore.getName()+":"+leaderCore.getStartNanoTime();
    }

    // verify the ZkIndexSchemaReader is a registered OnReconnect listener
    Set<OnReconnect> listeners = zkController.getCurrentOnReconnectListeners();
    assertNotNull("ZkController returned null OnReconnect listeners", listeners);
    ZkIndexSchemaReader expectedListener = null;
    for (OnReconnect listener : listeners) {
      if (listener instanceof ZkIndexSchemaReader) {
        ZkIndexSchemaReader reader = (ZkIndexSchemaReader)listener;
        if (leaderCoreId.equals(reader.getUniqueCoreId())) {
          expectedListener = reader;
          break;
        }
      }
    }
    assertNotNull("ZkIndexSchemaReader for core " + leaderCoreName +
        " not registered as an OnReconnect listener and should be", expectedListener);

    // reload the collection
    boolean wasReloaded = reloadCollection(leader, testCollectionName);
    assertTrue("Collection '" + testCollectionName + "' failed to reload within a reasonable amount of time!",
        wasReloaded);

    // after reload, the new core should be registered as an OnReconnect listener and the old should not be
    String reloadedLeaderCoreId;
    try (SolrCore leaderCore = cores.getCore(leaderCoreName)) {
      reloadedLeaderCoreId = leaderCore.getName()+":"+leaderCore.getStartNanoTime();
    }

    // they shouldn't be equal after reload
    assertTrue(!leaderCoreId.equals(reloadedLeaderCoreId));

    listeners = zkController.getCurrentOnReconnectListeners();
    assertNotNull("ZkController returned null OnReconnect listeners", listeners);

    expectedListener = null; // reset
    for (OnReconnect listener : listeners) {
      if (listener instanceof ZkIndexSchemaReader) {
        ZkIndexSchemaReader reader = (ZkIndexSchemaReader)listener;
        if (leaderCoreId.equals(reader.getUniqueCoreId())) {
          fail("Previous core "+leaderCoreId+
              " should no longer be a registered OnReconnect listener! Current listeners: "+listeners);
        } else if (reloadedLeaderCoreId.equals(reader.getUniqueCoreId())) {
          expectedListener = reader;
          break;
        }
      }
    }

    assertNotNull("ZkIndexSchemaReader for core "+reloadedLeaderCoreId+
        " not registered as an OnReconnect listener and should be", expectedListener);

    // try to clean up
    try {
      CollectionAdminRequest.deleteCollection(testCollectionName).process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }

    listeners = zkController.getCurrentOnReconnectListeners();
    for (OnReconnect listener : listeners) {
      if (listener instanceof ZkIndexSchemaReader) {
        ZkIndexSchemaReader reader = (ZkIndexSchemaReader)listener;
        if (reloadedLeaderCoreId.equals(reader.getUniqueCoreId())) {
          fail("Previous core "+reloadedLeaderCoreId+
              " should no longer be a registered OnReconnect listener after collection delete!");
        }
      }
    }

    log.info("TestOnReconnectListenerSupport succeeded ... shutting down now!");
  }
}
