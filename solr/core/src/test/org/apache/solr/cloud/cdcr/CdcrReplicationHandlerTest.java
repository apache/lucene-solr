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
package org.apache.solr.cloud.cdcr;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is testing the cdcr extension to the {@link org.apache.solr.handler.ReplicationHandler} and
 * {@link org.apache.solr.handler.IndexFetcher}.
 */
@Nightly
public class CdcrReplicationHandlerTest extends BaseCdcrDistributedZkTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void distribSetUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    createTargetCollection = false;     // we do not need the target cluster
    shardCount = 1; // we need only one shard
    // we need a persistent directory, otherwise the UpdateHandler will erase existing tlog files after restarting a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    super.distribSetUp();
  }

  /**
   * Test the scenario where the slave is killed from the start. The replication
   * strategy should fetch all the missing tlog files from the leader.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testFullReplication() throws Exception {
    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    slaves.get(0).jetty.stop();

    for (int i = 0; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 10; j < (i * 10) + 10; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertNumDocs(100, SOURCE_COLLECTION);

    // Restart the slave node to trigger Replication strategy
    this.restartServer(slaves.get(0));

    this.assertUpdateLogsEquals(SOURCE_COLLECTION, 10);
  }

  /**
   * Test the scenario where the slave is killed before receiving all the documents. The replication
   * strategy should fetch all the missing tlog files from the leader.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testPartialReplication() throws Exception {
    for (int i = 0; i < 5; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 20; j < (i * 20) + 20; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    slaves.get(0).jetty.stop();

    for (int i = 5; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 20; j < (i * 20) + 20; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertNumDocs(200, SOURCE_COLLECTION);

    // Restart the slave node to trigger Replication strategy
    this.restartServer(slaves.get(0));

    // at this stage, the slave should have replicated the 5 missing tlog files
    this.assertUpdateLogsEquals(SOURCE_COLLECTION, 10);
  }

  /**
   * Test the scenario where the slave is killed before receiving a commit. This creates a truncated tlog
   * file on the slave node. The replication strategy should detect this truncated file, and fetch the
   * non-truncated file from the leader.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testPartialReplicationWithTruncatedTlog() throws Exception {
    CloudSolrClient client = createCloudClient(SOURCE_COLLECTION);
    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);

    try {
      for (int i = 0; i < 10; i++) {
        for (int j = i * 20; j < (i * 20) + 20; j++) {
          client.add(getDoc(id, Integer.toString(j)));

          // Stop the slave in the middle of a batch to create a truncated tlog on the slave
          if (j == 45) {
            slaves.get(0).jetty.stop();
          }

        }
        commit(SOURCE_COLLECTION);
      }
    } finally {
      client.close();
    }

    assertNumDocs(200, SOURCE_COLLECTION);

    // Restart the slave node to trigger Replication recovery
    this.restartServer(slaves.get(0));

    // at this stage, the slave should have replicated the 5 missing tlog files
    this.assertUpdateLogsEquals(SOURCE_COLLECTION, 10);
  }

  /**
   * Test the scenario where the slave first recovered with a PeerSync strategy, then with a Replication strategy.
   * The PeerSync strategy will generate a single tlog file for all the missing updates on the slave node.
   * If a Replication strategy occurs at a later stage, it should remove this tlog file generated by PeerSync
   * and fetch the corresponding tlog files from the leader.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testPartialReplicationAfterPeerSync() throws Exception {
    for (int i = 0; i < 5; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 10; j < (i * 10) + 10; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    slaves.get(0).jetty.stop();

    for (int i = 5; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 10; j < (i * 10) + 10; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertNumDocs(100, SOURCE_COLLECTION);

    // Restart the slave node to trigger PeerSync recovery
    // (the update windows between leader and slave is small enough)
    this.restartServer(slaves.get(0));

    slaves.get(0).jetty.stop();

    for (int i = 10; i < 15; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 20; j < (i * 20) + 20; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    // restart the slave node to trigger Replication recovery
    this.restartServer(slaves.get(0));

    // at this stage, the slave should have replicated the 5 missing tlog files
    this.assertUpdateLogsEquals(SOURCE_COLLECTION, 15);
  }

  /**
   * Test the scenario where the slave is killed while the leader is still receiving updates.
   * The slave should buffer updates while in recovery, then replay them at the end of the recovery.
   * If updates were properly buffered and replayed, then the slave should have the same number of documents
   * than the leader. This checks if cdcr tlog replication interferes with buffered updates - SOLR-8263.
   */
  @Test
  @ShardsFixed(num = 2)
  public void testReplicationWithBufferedUpdates() throws Exception {
    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);

    AtomicInteger numDocs = new AtomicInteger(0);
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new DefaultSolrThreadFactory("cdcr-test-update-scheduler"));
    executor.scheduleWithFixedDelay(new UpdateThread(numDocs), 10, 10, TimeUnit.MILLISECONDS);

    // Restart the slave node to trigger Replication strategy
    this.restartServer(slaves.get(0));

    // shutdown the update thread and wait for its completion
    executor.shutdown();
    executor.awaitTermination(500, TimeUnit.MILLISECONDS);

    // check that we have the expected number of documents in the cluster
    assertNumDocs(numDocs.get(), SOURCE_COLLECTION);

    // check that we have the expected number of documents on the slave
    assertNumDocs(numDocs.get(), slaves.get(0));
  }

  private void assertNumDocs(int expectedNumDocs, CloudJettyRunner jetty)
  throws InterruptedException, IOException, SolrServerException {
    SolrClient client = createNewSolrServer(jetty.url);
    try {
      int cnt = 30; // timeout after 15 seconds
      AssertionError lastAssertionError = null;
      while (cnt > 0) {
        try {
          assertEquals(expectedNumDocs, client.query(new SolrQuery("*:*")).getResults().getNumFound());
          return;
        }
        catch (AssertionError e) {
          lastAssertionError = e;
          cnt--;
          Thread.sleep(500);
        }
      }
      throw new AssertionError("Timeout while trying to assert number of documents @ " + jetty.url, lastAssertionError);
    } finally {
      client.close();
    }
  }

  private class UpdateThread implements Runnable {

    private AtomicInteger numDocs;

    private UpdateThread(AtomicInteger numDocs) {
      this.numDocs = numDocs;
    }

    @Override
    public void run() {
      try {
        List<SolrInputDocument> docs = new ArrayList<>();
        for (int j = numDocs.get(); j < (numDocs.get() + 10); j++) {
          docs.add(getDoc(id, Integer.toString(j)));
        }
        index(SOURCE_COLLECTION, docs);
        numDocs.getAndAdd(10);
        log.info("Sent batch of {} updates - numDocs:{}", docs.size(), numDocs);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  private List<CloudJettyRunner> getShardToSlaveJetty(String collection, String shard) {
    List<CloudJettyRunner> jetties = new ArrayList<>(shardToJetty.get(collection).get(shard));
    CloudJettyRunner leader = shardToLeaderJetty.get(collection).get(shard);
    jetties.remove(leader);
    return jetties;
  }

  /**
   * Asserts that the update logs are in sync between the leader and slave. The leader and the slaves
   * must have identical tlog files.
   */
  protected void assertUpdateLogsEquals(String collection, int numberOfTLogs) throws Exception {
    CollectionInfo info = collectInfo(collection);
    Map<String, List<CollectionInfo.CoreInfo>> shardToCoresMap = info.getShardToCoresMap();

    for (String shard : shardToCoresMap.keySet()) {
      Map<Long, Long> leaderFilesMeta = this.getFilesMeta(info.getLeader(shard).ulogDir);
      Map<Long, Long> slaveFilesMeta = this.getFilesMeta(info.getReplicas(shard).get(0).ulogDir);

      assertEquals("Incorrect number of tlog files on the leader", numberOfTLogs, leaderFilesMeta.size());
      assertEquals("Incorrect number of tlog files on the slave", numberOfTLogs, slaveFilesMeta.size());

      for (Long leaderFileVersion : leaderFilesMeta.keySet()) {
        assertTrue("Slave is missing a tlog for version " + leaderFileVersion, slaveFilesMeta.containsKey(leaderFileVersion));
        assertEquals("Slave's tlog file size differs for version " + leaderFileVersion, leaderFilesMeta.get(leaderFileVersion), slaveFilesMeta.get(leaderFileVersion));
      }
    }
  }

  private Map<Long, Long> getFilesMeta(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }

    Map<Long, Long> filesMeta = new HashMap<>();
    for (File tlogFile : file.listFiles()) {
      filesMeta.put(Math.abs(Long.parseLong(tlogFile.getName().substring(tlogFile.getName().lastIndexOf('.') + 1))), tlogFile.length());
    }
    return filesMeta;
  }

}
