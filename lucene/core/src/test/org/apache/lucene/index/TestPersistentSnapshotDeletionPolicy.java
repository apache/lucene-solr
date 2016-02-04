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
package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPersistentSnapshotDeletionPolicy extends TestSnapshotDeletionPolicy {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  private SnapshotDeletionPolicy getDeletionPolicy(Directory dir) throws IOException {
    return new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.CREATE);
  }

  @Test
  public void testExistingSnapshots() throws Exception {
    int numSnapshots = 3;
    MockDirectoryWrapper dir = newMockDirectory();
    dir.setEnableVirusScanner(false); // test relies on files actually being deleted
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy(dir)));
    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    assertNull(psdp.getLastSaveFile());
    prepareIndexAndSnapshots(psdp, writer, numSnapshots);
    assertNotNull(psdp.getLastSaveFile());
    writer.close();

    // Make sure only 1 save file exists:
    int count = 0;
    for(String file : dir.listAll()) {
      if (file.startsWith(PersistentSnapshotDeletionPolicy.SNAPSHOTS_PREFIX)) {
        count++;
      }
    }
    assertEquals(1, count);

    // Make sure we fsync:
    dir.crash();
    dir.clearCrash();

    // Re-initialize and verify snapshots were persisted
    psdp = new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.APPEND);

    writer = new IndexWriter(dir, getConfig(random(), psdp));
    psdp = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();

    assertEquals(numSnapshots, psdp.getSnapshots().size());
    assertEquals(numSnapshots, psdp.getSnapshotCount());
    assertSnapshotExists(dir, psdp, numSnapshots, false);

    writer.addDocument(new Document());
    writer.commit();
    snapshots.add(psdp.snapshot());
    assertEquals(numSnapshots+1, psdp.getSnapshots().size());
    assertEquals(numSnapshots+1, psdp.getSnapshotCount());
    assertSnapshotExists(dir, psdp, numSnapshots+1, false);

    writer.close();
    dir.close();
  }

  @Test
  public void testNoSnapshotInfos() throws Exception {
    Directory dir = newDirectory();
    new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.CREATE);
    dir.close();
  }

  @Test
  public void testMissingSnapshots() throws Exception {
    Directory dir = newDirectory();
    try {
      new PersistentSnapshotDeletionPolicy(
                                           new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.APPEND);
      fail("did not hit expected exception");
    } catch (IllegalStateException ise) {
      // expected
    }
    dir.close();
  }

  public void testExceptionDuringSave() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    dir.failOn(new MockDirectoryWrapper.Failure() {
      @Override
      public void eval(MockDirectoryWrapper dir) throws IOException {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (int i = 0; i < trace.length; i++) {
          if (PersistentSnapshotDeletionPolicy.class.getName().equals(trace[i].getClassName()) && "persist".equals(trace[i].getMethodName())) {
            throw new IOException("now fail on purpose");
          }
        }
      }
      });
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), new PersistentSnapshotDeletionPolicy(
                                         new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.CREATE_OR_APPEND)));
    writer.addDocument(new Document());
    writer.commit();

    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    try {
      psdp.snapshot();
    } catch (IOException ioe) {
      if (ioe.getMessage().equals("now fail on purpose")) {
        // ok
      } else {
        throw ioe;
      }
    }
    assertEquals(0, psdp.getSnapshotCount());
    writer.close();
    assertEquals(1, DirectoryReader.listCommits(dir).size());
    dir.close();
  }

  @Test
  public void testSnapshotRelease() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy(dir)));
    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    prepareIndexAndSnapshots(psdp, writer, 1);
    writer.close();

    psdp.release(snapshots.get(0));

    psdp = new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.APPEND);
    assertEquals("Should have no snapshots !", 0, psdp.getSnapshotCount());
    dir.close();
  }

  @Test
  public void testSnapshotReleaseByGeneration() throws Exception {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy(dir)));
    PersistentSnapshotDeletionPolicy psdp = (PersistentSnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    prepareIndexAndSnapshots(psdp, writer, 1);
    writer.close();

    psdp.release(snapshots.get(0).getGeneration());

    psdp = new PersistentSnapshotDeletionPolicy(
        new KeepOnlyLastCommitDeletionPolicy(), dir, OpenMode.APPEND);
    assertEquals("Should have no snapshots !", 0, psdp.getSnapshotCount());
    dir.close();
  }
}
