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
package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.RetryUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies cluster state remains consistent after collection reload.
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class CollectionReloadTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @Test
  public void testReloadedLeaderStateAfterZkSessionLoss() throws Exception {

    log.info("testReloadedLeaderStateAfterZkSessionLoss initialized OK ... running test logic");

    final String testCollectionName = "c8n_1x1";
    CollectionAdminRequest.createCollection(testCollectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());

    Replica leader
        = cluster.getSolrClient().getZkStateReader().getLeaderRetry(testCollectionName, "shard1", DEFAULT_TIMEOUT);

    long coreStartTime = getCoreStatus(leader).getCoreStartTime().getTime();
    CollectionAdminRequest.reloadCollection(testCollectionName).process(cluster.getSolrClient());

    RetryUtil.retryUntil("Timed out waiting for core to reload", 30, 1000, TimeUnit.MILLISECONDS, () -> {
      long restartTime = 0;
      try {
        restartTime = getCoreStatus(leader).getCoreStartTime().getTime();
      } catch (Exception e) {
        log.warn("Exception getting core start time: ", e);
        return false;
      }
      return restartTime > coreStartTime;
    });

    final int initialStateVersion = getCollectionState(testCollectionName).getZNodeVersion();

    cluster.expireZkSession(cluster.getReplicaJetty(leader));

    waitForState("Timed out waiting for core to re-register as ACTIVE after session expiry", testCollectionName, (n, c) -> {
      log.info("Collection state: {}", c);
      Replica expiredReplica = c.getReplica(leader.getName());
      return expiredReplica.getState() == Replica.State.ACTIVE && c.getZNodeVersion() > initialStateVersion;
    });

    log.info("testReloadedLeaderStateAfterZkSessionLoss succeeded ... shutting down now!");
  }
}
