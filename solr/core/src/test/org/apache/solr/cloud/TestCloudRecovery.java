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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCloudRecovery extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    TestInjection.prepRecoveryOpPauseForever = "true:30";
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");

    configureCluster(2)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest
        .createCollection(COLLECTION, "config", 2, 2)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(),
        false, true, 30);
  }

  @AfterClass
  public static void afterClass() {
    TestInjection.reset();
  }

  @Before
  public void resetCollection() throws IOException, SolrServerException {
    cluster.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    cluster.getSolrClient().commit(COLLECTION);
  }

  @Test
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
    assertEquals(4, countReplayLog.get());
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
    Map<File, byte[]> contentFiles = new HashMap<>();
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        File tlogFolder = new File(solrCore.getUlogDir(), UpdateLog.TLOG_NAME);
        String[] tLogFiles = tlogFolder.list();
        Arrays.sort(tLogFiles);
        File lastTLogFile = new File(tlogFolder.getAbsolutePath() + "/" + tLogFiles[tLogFiles.length - 1]);
        byte[] tlogBytes = IOUtils.toByteArray(new FileInputStream(lastTLogFile));
        contentFiles.put(lastTLogFile, tlogBytes);
        logHeaderSize = Math.min(tlogBytes.length, logHeaderSize);
      }
    }

    ChaosMonkey.stop(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllReplicasNotLive(cloudClient.getZkStateReader(), 45000));

    for (Map.Entry<File, byte[]> entry : contentFiles.entrySet()) {
      byte[] tlogBytes = entry.getValue();

      if (tlogBytes.length <= logHeaderSize) continue;
      FileOutputStream stream = new FileOutputStream(entry.getKey());
      int skipLastBytes = Math.max(random().nextInt(tlogBytes.length - logHeaderSize), 2);
      for (int i = 0; i < entry.getValue().length - skipLastBytes; i++) {
        stream.write(tlogBytes[i]);
      }
      stream.close();
    }

    ChaosMonkey.start(cluster.getJettySolrRunners());
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cloudClient.getZkStateReader(), COLLECTION, 120000));

    resp = cloudClient.query(COLLECTION, params);
    // Make sure cluster still healthy
    assertTrue(resp.getResults().getNumFound() >= 2);
  }

}
