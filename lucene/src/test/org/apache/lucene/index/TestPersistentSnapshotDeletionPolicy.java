package org.apache.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPersistentSnapshotDeletionPolicy extends TestSnapshotDeletionPolicy {
  // Keep it a class member so that getDeletionPolicy can use it
  private Directory snapshotDir;
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    snapshotDir = newDirectory(random);
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    snapshotDir.close();
    super.tearDown();
  }
  
  @Override
  protected SnapshotDeletionPolicy getDeletionPolicy() throws IOException {
    IndexWriter.unlock(snapshotDir);
    return new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.CREATE,
        TEST_VERSION_CURRENT);
  }

  @Override
  protected SnapshotDeletionPolicy getDeletionPolicy(Map<String, String> snapshots) throws IOException {
    SnapshotDeletionPolicy sdp = getDeletionPolicy();
    if (snapshots != null) {
      for (Entry<String, String> e: snapshots.entrySet()) {
        sdp.registerSnapshotInfo(e.getKey(), e.getValue(), null);
      }
    }
    return sdp;
  }

  @Override
  @Test
  public void testExistingSnapshots() throws Exception {
    int numSnapshots = 3;
    Directory dir = newDirectory(random);
    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) getDeletionPolicy();
    IndexWriter writer = new IndexWriter(dir, getConfig(random, psdp));
    prepareIndexAndSnapshots(psdp, writer, numSnapshots, "snapshot");
    writer.close();
    psdp.close();

    // Re-initialize and verify snapshots were persisted
    psdp = new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.APPEND,
        TEST_VERSION_CURRENT);
    new IndexWriter(dir, getConfig(random, psdp)).close();

    assertSnapshotExists(dir, psdp, numSnapshots);
    assertEquals(numSnapshots, psdp.getSnapshots().size());
    psdp.close();
    dir.close();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testIllegalSnapshotId() throws Exception {
    getDeletionPolicy().snapshot("$SNAPSHOTS_DOC$");
  }
  
  @Test
  public void testInvalidSnapshotInfos() throws Exception {
    // Add the correct number of documents (1), but without snapshot information
    IndexWriter writer = new IndexWriter(snapshotDir, getConfig(random, null));
    writer.addDocument(new Document());
    writer.close();
    PersistentSnapshotDeletionPolicy dp = null;
    try {
      new PersistentSnapshotDeletionPolicy(
          new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.APPEND,
          TEST_VERSION_CURRENT);
      fail("should not have succeeded to read from an invalid Directory");
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void testNoSnapshotInfos() throws Exception {
    // Initialize an empty index in snapshotDir - PSDP should initialize successfully.
    new IndexWriter(snapshotDir, getConfig(random, null)).close();
    new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.APPEND,
        TEST_VERSION_CURRENT).close();
  }

  @Test(expected=IllegalStateException.class)
  public void testTooManySnapshotInfos() throws Exception {
    // Write two documents to the snapshots directory - illegal.
    IndexWriter writer = new IndexWriter(snapshotDir, getConfig(random, null));
    writer.addDocument(new Document());
    writer.addDocument(new Document());
    writer.close();
    
    new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.APPEND,
        TEST_VERSION_CURRENT).close();
    fail("should not have succeeded to open an invalid directory");
  }

  @Test
  public void testSnapshotRelease() throws Exception {
    Directory dir = newDirectory(random);
    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) getDeletionPolicy();
    IndexWriter writer = new IndexWriter(dir, getConfig(random, psdp));
    prepareIndexAndSnapshots(psdp, writer, 1, "snapshot");
    writer.close();

    psdp.release("snapshot0");
    psdp.close();

    psdp = new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), snapshotDir, OpenMode.APPEND,
        TEST_VERSION_CURRENT);
    assertEquals("Should have no snapshots !", 0, psdp.getSnapshots().size());
    psdp.close();
    dir.close();
  }

}
