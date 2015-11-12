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

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrParams;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Nightly
public class CdcrReplicationDistributedZkTest extends BaseCdcrDistributedZkTest {

  @Override
  public void distribSetUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    super.distribSetUp();
  }

  /**
   * Checks that the test framework handles properly the creation and deletion of collections and the
   * restart of servers.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testDeleteCreateSourceCollection() throws Exception {
    log.info("Indexing documents");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);
    index(TARGET_COLLECTION, docs);

    assertNumDocs(10, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Restarting leader @ source_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    assertNumDocs(10, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Clearing source_collection");

    this.clearSourceCollection();

    assertNumDocs(0, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Restarting leader @ target_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(TARGET_COLLECTION).get(SHARD1));

    assertNumDocs(0, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Clearing target_collection");

    this.clearTargetCollection();

    assertNumDocs(0, SOURCE_COLLECTION);
    assertNumDocs(0, TARGET_COLLECTION);

    assertCollectionExpectations(SOURCE_COLLECTION);
    assertCollectionExpectations(TARGET_COLLECTION);
  }

  @Test
  @ShardsFixed(num = 4)
  public void testTargetCollectionNotAvailable() throws Exception {
    // send start action to first shard
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    NamedList status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.ProcessState.STARTED.toLower(), status.get(CdcrParams.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STARTED, CdcrParams.BufferState.ENABLED);

    // Kill all the servers of the target
    this.deleteCollection(TARGET_COLLECTION);

    // Index a few documents to trigger the replication
    index(SOURCE_COLLECTION, getDoc(id, "a"));
    index(SOURCE_COLLECTION, getDoc(id, "b"));
    index(SOURCE_COLLECTION, getDoc(id, "c"));
    index(SOURCE_COLLECTION, getDoc(id, "d"));
    index(SOURCE_COLLECTION, getDoc(id, "e"));
    index(SOURCE_COLLECTION, getDoc(id, "f"));

    assertNumDocs(6, SOURCE_COLLECTION);

    // we need to wait until the replicator thread is triggered
    int cnt = 15; // timeout after 15 seconds
    AssertionError lastAssertionError = null;
    while (cnt > 0) {
      try {
        rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.ERRORS);
        NamedList collections = (NamedList) ((NamedList) rsp.get(CdcrParams.ERRORS)).getVal(0);
        NamedList errors = (NamedList) collections.get(TARGET_COLLECTION);
        assertTrue(0 < (Long) errors.get(CdcrParams.CONSECUTIVE_ERRORS));
        NamedList lastErrors = (NamedList) errors.get(CdcrParams.LAST);
        assertNotNull(lastErrors);
        assertTrue(0 < lastErrors.size());
        return;
      }
      catch (AssertionError e) {
        lastAssertionError = e;
        cnt--;
        Thread.sleep(1000);
      }
    }

    throw new AssertionError("Timeout while trying to assert replication errors", lastAssertionError);
  }

  @Test
  @ShardsFixed(num = 4)
  public void testReplicationStartStop() throws Exception {
    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertNumDocs(10, SOURCE_COLLECTION);
    assertNumDocs(0, TARGET_COLLECTION);

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertNumDocs(10, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertNumDocs(110, SOURCE_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertNumDocs(110, SOURCE_COLLECTION);
    assertNumDocs(110, TARGET_COLLECTION);
  }

  /**
   * Check that the replication manager is properly restarted after a node failure.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testReplicationAfterRestart() throws Exception {
    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertNumDocs(10, SOURCE_COLLECTION);

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Restarting shard1");

    this.restartServers(shardToJetty.get(SOURCE_COLLECTION).get(SHARD1));

    log.info("Indexing 100 documents");

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertNumDocs(110, SOURCE_COLLECTION);

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertNumDocs(110, TARGET_COLLECTION);
  }

  /**
   * Check that the replication manager is properly started after a change of leader.
   * This test also checks that the log readers on the new leaders are initialised with
   * the target's checkpoint.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testReplicationAfterLeaderChange() throws Exception {
    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertNumDocs(10, SOURCE_COLLECTION);

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertNumDocs(10, TARGET_COLLECTION);

    log.info("Restarting target leaders");

    // Close all the leaders, then restart them
    this.restartServer(shardToLeaderJetty.get(TARGET_COLLECTION).get(SHARD1));
    this.restartServer(shardToLeaderJetty.get(TARGET_COLLECTION).get(SHARD2));

    log.info("Restarting source leaders");

    // Close all the leaders, then restart them
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2));

    log.info("Checking queue size of new source leaders");

    // If the log readers of the new leaders are initialised with the target's checkpoint, the
    // queue size must be inferior to the current number of documents indexed.
    // The queue might be not completely empty since the new target checkpoint is probably not the
    // last document received
    assertTrue(this.getQueueSize(SOURCE_COLLECTION, SHARD1) < 10);
    assertTrue(this.getQueueSize(SOURCE_COLLECTION, SHARD2) < 10);

    log.info("Indexing 100 documents");

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertNumDocs(110, SOURCE_COLLECTION);

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertNumDocs(110, TARGET_COLLECTION);
  }

  /**
   * Check that the update logs are synchronised between leader and non-leader nodes
   * when CDCR is on and buffer is disabled
   */
  @Test
  @ShardsFixed(num = 4)
  public void testUpdateLogSynchronisation() throws Exception {
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    for (int i = 0; i < 100; i++) {
      // will perform a commit for every document and will create one tlog file per commit
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i)));
    }

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // Check that the replication was done properly
    assertNumDocs(100, SOURCE_COLLECTION);
    assertNumDocs(100, TARGET_COLLECTION);

    // Get the number of tlog files on the replicas (should be equal to the number of documents indexed)
    int nTlogs = getNumberOfTlogFilesOnReplicas(SOURCE_COLLECTION);

    // Disable the buffer - ulog synch should start on non-leader nodes
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    int cnt = 15; // timeout after 15 seconds
    while (cnt > 0) {
      // Index a new document with a commit to trigger update log cleaning
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(50)));

      // Check the update logs on non-leader nodes, the number of tlog files should decrease
      int n = getNumberOfTlogFilesOnReplicas(SOURCE_COLLECTION);
      if (n < nTlogs) return;

      cnt--;
      Thread.sleep(1000);
    }

    throw new AssertionError("Timeout while trying to assert update logs @ source_collection");
  }

  /**
   * Check that the buffer is always activated on non-leader nodes.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testBufferOnNonLeader() throws Exception {
    // buffering is enabled by default, so disable it
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    // Index documents
    for (int i = 0; i < 200; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i))); // will perform a commit for every document
    }

    // And immediately, close all the leaders, then restart them. It is likely that the replication will not be
    // performed fully, and therefore be continued by the new leader
    // At this stage, the new leader must have been elected
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2));

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    // Commit to make the documents visible on the target
    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertNumDocs(200, SOURCE_COLLECTION);
    assertNumDocs(200, TARGET_COLLECTION);
  }

  /**
   * Check the ops statistics.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testOps() throws Exception {
    // Index documents
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    NamedList rsp = this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.OPS);
    NamedList collections = (NamedList) ((NamedList) rsp.get(CdcrParams.OPERATIONS_PER_SECOND)).getVal(0);
    NamedList ops = (NamedList) collections.get(TARGET_COLLECTION);
    double opsAll = (Double) ops.get(CdcrParams.COUNTER_ALL);
    double opsAdds = (Double) ops.get(CdcrParams.COUNTER_ADDS);
    assertTrue(opsAll > 0);
    assertEquals(opsAll, opsAdds, 0);

    double opsDeletes = (Double) ops.get(CdcrParams.COUNTER_DELETES);
    assertEquals(0, opsDeletes, 0);
  }

  /**
   * Check that batch updates with deletes
   */
  @Test
  @ShardsFixed(num = 4)
  public void testBatchAddsWithDelete() throws Exception {
    // Index 50 documents
    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 50; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Delete 10 documents: 10-19
    List<String> ids = new ArrayList<>();
    for (int id = 10; id < 20; id++) {
      ids.add(Integer.toString(id));
    }
    deleteById(SOURCE_COLLECTION, ids);

    // Index 10 documents
    docs = new ArrayList<>();
    for (; start < 60; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Delete 1 document: 50
    ids = new ArrayList<>();
    ids.add(Integer.toString(50));
    deleteById(SOURCE_COLLECTION, ids);

    // Index 10 documents
    docs = new ArrayList<>();
    for (; start < 70; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertNumDocs(59, SOURCE_COLLECTION);
    assertNumDocs(59, TARGET_COLLECTION);
  }

  /**
   * Checks that batches are correctly constructed when batch boundaries are reached.
   */
  @Test
  @ShardsFixed(num = 4)
  public void testBatchBoundaries() throws Exception {
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    log.info("Indexing documents");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 128; i++) { // should create two full batches (default batch = 64)
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);

    assertNumDocs(128, SOURCE_COLLECTION);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertNumDocs(128, SOURCE_COLLECTION);
    assertNumDocs(128, TARGET_COLLECTION);
  }

  /**
   * Check resilience of replication with delete by query executed on targets
   */
  @Test
  @ShardsFixed(num = 4)
  public void testResilienceWithDeleteByQueryOnTarget() throws Exception {
    // Index 50 documents
    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 50; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertNumDocs(50, SOURCE_COLLECTION);
    assertNumDocs(50, TARGET_COLLECTION);

    deleteByQuery(SOURCE_COLLECTION, "*:*");
    deleteByQuery(TARGET_COLLECTION, "*:*");

    assertNumDocs(0, SOURCE_COLLECTION);
    assertNumDocs(0, TARGET_COLLECTION);

    docs.clear();
    for (; start < 100; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertNumDocs(50, SOURCE_COLLECTION);
    assertNumDocs(50, TARGET_COLLECTION);

    deleteByQuery(TARGET_COLLECTION, "*:*");

    assertNumDocs(50, SOURCE_COLLECTION);
    assertNumDocs(0, TARGET_COLLECTION);

    // Restart CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    this.waitForCdcrStateReplication(SOURCE_COLLECTION);

    docs.clear();
    for (; start < 150; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertNumDocs(100, SOURCE_COLLECTION);
    assertNumDocs(50, TARGET_COLLECTION);
  }

  private int numberOfFiles(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }
    log.debug("Update log dir {} contains: {}", dir, file.listFiles());
    return file.listFiles().length;
  }

  private int getNumberOfTlogFilesOnReplicas(String collection) throws Exception {
    CollectionInfo info = collectInfo(collection);
    Map<String, List<CollectionInfo.CoreInfo>> shardToCoresMap = info.getShardToCoresMap();

    int count = 0;

    for (String shard : shardToCoresMap.keySet()) {
      for (int i = 0; i < replicationFactor - 1; i++) {
        count += numberOfFiles(info.getReplicas(shard).get(i).ulogDir);
      }
    }

    return count;
  }

}

