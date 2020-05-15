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

package org.apache.solr.store.shared;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.process.CorePuller;
import org.apache.solr.store.blob.process.CorePusher;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link SharedCoreIndexingBatchProcessor}
 */
public class SharedCoreIndexingBatchProcessorTest extends  SolrCloudSharedStoreTestCase {

  private static final String COLLECTION_NAME = "sharedCollection";
  private static final String SHARD_NAME = "shard1";

  private SolrCore core;
  private CorePuller corePuller;
  private CorePusher corePusher;
  private ReentrantReadWriteLock corePullLock;
  private SharedCoreIndexingBatchProcessor processor;

  @BeforeClass
  public static void setupCluster() throws Exception {
    assumeWorkingMockito();
    setupCluster(1);
  }

  @Before
  public void setupTest() throws Exception {
    assertEquals("wrong number of nodes", 1, cluster.getJettySolrRunners().size());
    CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();

    int maxShardsPerNode = 1;
    int numReplicas = 1;
    setupSharedCollectionWithShardNames(COLLECTION_NAME, maxShardsPerNode, numReplicas, SHARD_NAME);
    DocCollection collection = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);

    assertEquals("wrong number of replicas", 1, collection.getReplicas().size());
    core = cc.getCore(collection.getReplicas().get(0).getCoreName());

    assertNotNull("core is null", core);

    corePuller = Mockito.spy(new CorePuller());
    corePusher = Mockito.spy(new CorePusher());
    processor = new SharedCoreIndexingBatchProcessor(core, core.getCoreContainer().getZkController().getClusterState()) {
      @Override
      protected CorePuller getCorePuller() {
        return corePuller;
      }

      @Override
      protected CorePusher getCorePusher() {
        return corePusher;
      }
    };
    processor = Mockito.spy(processor);
    corePullLock = core.getCoreContainer().getSharedStoreManager().getSharedCoreConcurrencyController().getCorePullLock(
        COLLECTION_NAME, SHARD_NAME, core.getName());
  }

  @After
  public void teardownTest() throws Exception {
    if (core != null) {
      core.close();
    }
    if (processor != null) {
      processor.close();
      assertEquals("read lock count is wrong", 0, corePullLock.getReadLockCount());
    }
    if (cluster != null) {
      cluster.deleteAllCollections();
    }
  }

  /**
   * Tests that first add/delete starts an indexing batch.
   */
  @Test
  public void testAddOrDeleteStart() throws Exception {
    verify(processor, never()).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
  }

  /**
   * Tests that two adds/deletes only start an indexing batch once.
   */
  @Test
  public void testTwoAddOrDeleteOnlyStartOnce() throws Exception {
    verify(processor, never()).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
  }

  /**
   * Tests that commit does finish an indexing batch.
   */
  @Test
  public void testCommitDoesFinish() throws Exception {
    verify(processor, never()).finishIndexingBatch();
    processCommit();
    verify(processor).finishIndexingBatch();
  }

  /**
   * Tests that a stale core is pulled at the start of an indexing batch.
   */
  @Test
  public void testStaleCoreIsPulledAtStart() throws Exception {
    verify(processor, never()).startIndexingBatch();
    verify(corePuller, never()).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    verify(corePuller).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
  }

  /**
   * Tests that an up-to-date core is not pulled at the start of an indexing batch.
   */
  @Test
  public void testUpToDateCoreIsNotPulledAtStart() throws Exception {
    SharedShardMetadataController.SharedShardVersionMetadata shardVersionMetadata = core.getCoreContainer()
        .getSharedStoreManager().getSharedShardMetadataController().readMetadataValue(COLLECTION_NAME, SHARD_NAME);
    ClusterState clusterState = core.getCoreContainer().getZkController().getClusterState();
    DocCollection collection = clusterState.getCollection(COLLECTION_NAME);
    String sharedShardName = (String) collection.getSlicesMap().get(SHARD_NAME).get(ZkStateReader.SHARED_SHARD_NAME);
    corePuller.pullCoreFromSharedStore(core, sharedShardName, shardVersionMetadata, true);
    verify(corePuller).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
    verify(processor, never()).startIndexingBatch();
    processAddOrDelete();
    verify(processor).startIndexingBatch();
    verify(corePuller).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
  }

  /**
   * Tests that a read lock is acquired even when the start encounters an error.
   */
  @Test
  public void testReadLockIsAcquiredEvenStartEncountersError() throws Exception {
    doThrow(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "pull failed"))
        .when(corePuller).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
    verify(processor, never()).startIndexingBatch();
    verify(corePuller, never()).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
    assertEquals("wrong pull read lock count", 0, corePullLock.getReadLockCount());
    boolean errorOccurred = false;
    try {
      processor.startIndexingBatch();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue("Wrong exception thrown", ex.getMessage().contains("pull failed"));
    }
    assertEquals("wrong pull read lock count", 1, corePullLock.getReadLockCount());
    verify(processor).startIndexingBatch();
    verify(corePuller).pullCoreFromSharedStore(any(), any(), any(), anyBoolean());
  }

  /**
   * Tests that an indexing batch with some work does push to the shared store.
   */
  @Test
  public void testCommitAfterAddOrDeleteDoesPush() throws Exception {
    processAddOrDelete();
    processCommit();
    verify(corePusher).pushCoreToSharedStore(any(), any());
  }

  /**
   * Tests that an indexing batch with no work does not push to the shared store.
   */
  @Test
  public void testIsolatedCommitDoesNotPush() throws Exception {
    processCommit();
    verify(corePusher, never()).pushCoreToSharedStore(any(), any());
  }

  /**
   * Tests that an already committed indexing batch throws if a doc is added/deleted again.
   */
  @Test
  public void testAddOrDeleteAfterCommitThrows() throws Exception {
    processAddOrDelete();
    commitAndThenAddOrDeleteDoc();
  }

  /**
   * Tests that an already isolated committed indexing batch throws if a doc is added/deleted again.
   */
  @Test
  public void testAddOrDeleteAfterIsolatedCommitThrows() throws Exception {
    commitAndThenAddOrDeleteDoc();
  }

  private void commitAndThenAddOrDeleteDoc() {
    processCommit();
    try {
      processAddOrDelete();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue( "wrong exception thrown", 
          ex.getMessage().contains("Why are we adding/deleting a doc through an already committed indexing batch?"));
    }
  }

  /**
   * Tests that an already committed indexing batch throws if committed again.
   */
  @Test
  public void testCommitAfterCommitThrows() throws Exception {
    processAddOrDelete();
    doDoubleCommit();
  }

  /**
   * Tests that an already isolated committed indexing batch throws if committed again.
   */
  @Test
  public void testCommitAfterIsolatedCommitThrows() throws Exception {
    doDoubleCommit();
  }

  private void doDoubleCommit() {
    processCommit();
    try {
      processCommit();
      fail("No exception thrown");
    } catch (Exception ex) {
      assertTrue( "wrong exception thrown",
          ex.getMessage().contains("Why are we committing an already committed indexing batch?"));
    }
  }


  private void processAddOrDelete() {
    processor.addOrDeleteGoingToBeIndexedLocally();
  }

  private void processCommit() {
    processor.hardCommitCompletedLocally();
  }

}
