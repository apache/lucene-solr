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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateShardHandler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloudRecovery extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";
  private static boolean onlyLeaderIndexes;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    onlyLeaderIndexes = random().nextBoolean();
    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 2, onlyLeaderIndexes?0:2,onlyLeaderIndexes?2:0,0)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);

    //SOLR-12314 : assert that these values are from the solr.xml file and not UpdateShardHandlerConfig#DEFAULT
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      UpdateShardHandler shardHandler = jettySolrRunner.getCoreContainer().getUpdateShardHandler();
      int socketTimeout = shardHandler.getSocketTimeout();
      int connectionTimeout = shardHandler.getConnectionTimeout();
      assertEquals(340000, socketTimeout);
      assertEquals(45000, connectionTimeout);
    }
  }

  @Before
  public void resetCollection() throws IOException, SolrServerException {
    cluster.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    cluster.getSolrClient().commit(COLLECTION);
  }

  @Test
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Jul-2018
  public void leaderRecoverFromLogOnStartupTest() throws Exception {
    AtomicInteger countReplayLog = new AtomicInteger(0);
    DirectUpdateHandler2.commitOnClose = false;
    UpdateLog.testing_logReplayFinishHook = countReplayLog::incrementAndGet;

    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.add(COLLECTION, sdoc("id", "1"));
    cloudClient.add(COLLECTION, sdoc("id", "2"));
    cloudClient.add(COLLECTION, sdoc("id", "3"));
    cloudClient.add(COLLECTION, sdoc("id", "4"));

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    QueryResponse resp = cloudClient.query(COLLECTION, params);
    assertEquals(0, resp.getResults().getNumFound());

    ChaosMonkey.stop(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));
    ChaosMonkey.start(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), COLLECTION, 120000));

    resp = cloudClient.query(COLLECTION, params);
    assertEquals(4, resp.getResults().getNumFound());
    // Make sure all nodes is recover from tlog
    if (onlyLeaderIndexes) {
      // Leader election can be kicked off, so 2 tlog replicas will replay its tlog before becoming new leader
      assertTrue( countReplayLog.get() >=2);
    } else {
      assertEquals(4, countReplayLog.get());
    }

    // check metrics
    int replicationCount = 0;
    int errorsCount = 0;
    int skippedCount = 0;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      SolrMetricManager manager = jetty.getCoreContainer().getMetricManager();
      List<String> registryNames = manager.registryNames().stream()
          .filter(s -> s.startsWith("solr.core.")).collect(Collectors.toList());
      for (String registry : registryNames) {
        Map<String, Metric> metrics = manager.registry(registry).getMetrics();
        Timer timer = (Timer)metrics.get("REPLICATION.peerSync.time");
        Counter counter = (Counter)metrics.get("REPLICATION.peerSync.errors");
        Counter skipped = (Counter)metrics.get("REPLICATION.peerSync.skipped");
        replicationCount += timer.getCount();
        errorsCount += counter.getCount();
        skippedCount += skipped.getCount();
      }
    }
    if (onlyLeaderIndexes) {
      assertTrue(replicationCount >= 2);
    } else {
      assertEquals(2, replicationCount);
    }
  }

  @Test
  public void corruptedLogTest() throws Exception {
    AtomicInteger countReplayLog = new AtomicInteger(0);
    DirectUpdateHandler2.commitOnClose = false;
    UpdateLog.testing_logReplayFinishHook = countReplayLog::incrementAndGet;

    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.add(COLLECTION, sdoc("id", "1000"));
    cloudClient.add(COLLECTION, sdoc("id", "1001"));
    for (int i = 0; i < 10; i++) {
      cloudClient.add(COLLECTION, sdoc("id", String.valueOf(i)));
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    QueryResponse resp = cloudClient.query(COLLECTION, params);
    assertEquals(0, resp.getResults().getNumFound());

    int logHeaderSize = Integer.MAX_VALUE;
    Map<String, byte[]> contentFiles = new HashMap<>();
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        File tlogFolder = new File(solrCore.getUlogDir(), UpdateLog.TLOG_NAME);
        String[] tLogFiles = tlogFolder.list();
        Arrays.sort(tLogFiles);
        String lastTLogFile = tlogFolder.getAbsolutePath() + "/" + tLogFiles[tLogFiles.length - 1];
        try (FileInputStream inputStream = new FileInputStream(lastTLogFile)){
          byte[] tlogBytes = IOUtils.toByteArray(inputStream);
          contentFiles.put(lastTLogFile, tlogBytes);
          logHeaderSize = Math.min(tlogBytes.length, logHeaderSize);
        }
      }
    }

    ChaosMonkey.stop(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));

    for (Map.Entry<String, byte[]> entry : contentFiles.entrySet()) {
      byte[] tlogBytes = entry.getValue();

      if (tlogBytes.length <= logHeaderSize) continue;
      try (FileOutputStream stream = new FileOutputStream(entry.getKey())) {
        int skipLastBytes = Math.max(random().nextInt(tlogBytes.length - logHeaderSize), 2);
        for (int i = 0; i < entry.getValue().length - skipLastBytes; i++) {
          stream.write(tlogBytes[i]);
        }
      }
    }

    ChaosMonkey.start(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), COLLECTION, 120000));

    resp = cloudClient.query(COLLECTION, params);
    // Make sure cluster still healthy
    assertTrue(resp.getResults().getNumFound() >= 2);
  }

}
