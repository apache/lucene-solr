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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrParams;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Slow
public class CdcrReplicationDistributedZkTest extends BaseCdcrDistributedZkTest {

  @Override
  public void distribSetUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    super.distribSetUp();
  }

  @Test
  @ShardsFixed(num = 4)
  public void doTest() throws Exception {
    this.doTestDeleteCreateSourceCollection();
    this.doTestTargetCollectionNotAvailable();
    this.doTestReplicationStartStop();
    this.doTestReplicationAfterRestart();
    this.doTestReplicationAfterLeaderChange();
    this.doTestUpdateLogSynchronisation();
    this.doTestBufferOnNonLeader();
    this.doTestOps();
    this.doTestBatchAddsWithDelete();
    this.doTestBatchBoundaries();
    this.doTestResilienceWithDeleteByQueryOnTarget();
  }

  /**
   * Checks that the test framework handles properly the creation and deletion of collections and the
   * restart of servers.
   */
  public void doTestDeleteCreateSourceCollection() throws Exception {
    log.info("Indexing documents");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);
    index(TARGET_COLLECTION, docs);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting leader @ source_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Clearing source_collection");

    this.clearSourceCollection();

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting leader @ target_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(TARGET_COLLECTION).get(SHARD1));

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Clearing target_collection");

    this.clearTargetCollection();

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    assertCollectionExpectations(SOURCE_COLLECTION);
    assertCollectionExpectations(TARGET_COLLECTION);
  }

  public void doTestTargetCollectionNotAvailable() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

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

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));

    Thread.sleep(1000); // wait a bit for the replicator thread to be triggered

    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrParams.CdcrAction.ERRORS);
    NamedList collections = (NamedList) ((NamedList) rsp.get(CdcrParams.ERRORS)).getVal(0);
    NamedList errors = (NamedList) collections.get(TARGET_COLLECTION);
    assertTrue(0 < (Long) errors.get(CdcrParams.CONSECUTIVE_ERRORS));
    NamedList lastErrors = (NamedList) errors.get(CdcrParams.LAST);
    assertNotNull(lastErrors);
    assertTrue(0 < lastErrors.size());
  }

  public void doTestReplicationStartStop() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection(); // this might log a warning to indicate he was not able to delete the collection (collection was deleted in the previous test)

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the replication manager is properly restarted after a node failure.
   */
  public void doTestReplicationAfterRestart() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting shard1");

    this.restartServers(shardToJetty.get(SOURCE_COLLECTION).get(SHARD1));

    log.info("Indexing 100 documents");

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the replication manager is properly started after a change of leader.
   * This test also checks that the log readers on the new leaders are initialised with
   * the target's checkpoint.
   */
  public void doTestReplicationAfterLeaderChange() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

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

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the update logs are synchronised between leader and non-leader nodes
   */
  public void doTestUpdateLogSynchronisation() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // buffering is enabled by default, so disable it
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    for (int i = 0; i < 50; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i))); // will perform a commit for every document
    }

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // Stop CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);

    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));

    index(SOURCE_COLLECTION, getDoc(id, Integer.toString(0))); // trigger update log cleaning on the non-leader nodes

    // some of the tlogs should be trimmed, we must have less than 50 tlog files on both leader and non-leader
    assertUpdateLogs(SOURCE_COLLECTION, 50);

    for (int i = 50; i < 100; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i)));
    }

    index(SOURCE_COLLECTION, getDoc(id, Integer.toString(0))); // trigger update log cleaning on the non-leader nodes

    // at this stage, we should have created one tlog file per document, and some of them must have been cleaned on the
    // leader since we are not buffering and replication is stopped, (we should have exactly 10 tlog files on the leader
    // and 11 on the non-leader)
    // the non-leader must have synchronised its update log with its leader
    assertUpdateLogs(SOURCE_COLLECTION, 50);
  }

  /**
   * Check that the buffer is always activated on non-leader nodes.
   */
  public void doTestBufferOnNonLeader() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // buffering is enabled by default, so disable it
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

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

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertEquals(200, getNumDocs(SOURCE_COLLECTION));
    assertEquals(200, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check the ops statistics.
   */
  public void doTestOps() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // Index documents
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

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
  public void doTestBatchAddsWithDelete() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

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

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertEquals(59, getNumDocs(SOURCE_COLLECTION));
    assertEquals(59, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Checks that batches are correctly constructed when batch boundaries are reached.
   */
  public void doTestBatchBoundaries() throws Exception {
    invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Indexing documents");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 128; i++) { // should create two full batches (default batch = 64)
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(128, getNumDocs(SOURCE_COLLECTION));

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(128, getNumDocs(SOURCE_COLLECTION));
    assertEquals(128, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check resilience of replication with delete by query executed on targets
   */
  public void doTestResilienceWithDeleteByQueryOnTarget() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // Index 50 documents
    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 50; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));

    deleteByQuery(SOURCE_COLLECTION, "*:*");
    deleteByQuery(TARGET_COLLECTION, "*:*");

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    docs.clear();
    for (; start < 100; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));

    deleteByQuery(TARGET_COLLECTION, "*:*");

    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    // Restart CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);
    Thread.sleep(500); // wait a bit for the state to synch
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    docs.clear();
    for (; start < 150; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertEquals(100, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));
  }

}

