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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.ThreadInterruptedException;
import org.junit.Test;

//
// This was developed for Lucene In Action,
// http://lucenebook.com
//

public class TestSnapshotDeletionPolicy extends LuceneTestCase {
  public static final String INDEX_PATH = "test.snapshots";
  
  protected IndexWriterConfig getConfig(Random random, IndexDeletionPolicy dp) {
    IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random));
    if (dp != null) {
      conf.setIndexDeletionPolicy(dp);
    }
    return conf;
  }

  protected void checkSnapshotExists(Directory dir, IndexCommit c) throws Exception {
    String segFileName = c.getSegmentsFileName();
    assertTrue("segments file not found in directory: " + segFileName, slowFileExists(dir, segFileName));
  }

  protected void checkMaxDoc(IndexCommit commit, int expectedMaxDoc) throws Exception {
    IndexReader reader = DirectoryReader.open(commit);
    try {
      assertEquals(expectedMaxDoc, reader.maxDoc());
    } finally {
      reader.close();
    }
  }

  protected List<IndexCommit> snapshots = new ArrayList<>();

  protected void prepareIndexAndSnapshots(SnapshotDeletionPolicy sdp,
      IndexWriter writer, int numSnapshots)
      throws RuntimeException, IOException {
    for (int i = 0; i < numSnapshots; i++) {
      // create dummy document to trigger commit.
      writer.addDocument(new Document());
      writer.commit();
      snapshots.add(sdp.snapshot());
    }
  }

  protected SnapshotDeletionPolicy getDeletionPolicy() throws IOException {
    return new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
  }

  protected void assertSnapshotExists(Directory dir, SnapshotDeletionPolicy sdp, int numSnapshots, boolean checkIndexCommitSame) throws Exception {
    for (int i = 0; i < numSnapshots; i++) {
      IndexCommit snapshot = snapshots.get(i);
      checkMaxDoc(snapshot, i + 1);
      checkSnapshotExists(dir, snapshot);
      if (checkIndexCommitSame) {
        assertSame(snapshot, sdp.getIndexCommit(snapshot.getGeneration()));
      } else {
        assertEquals(snapshot.getGeneration(), sdp.getIndexCommit(snapshot.getGeneration()).getGeneration());
      }
    }
  }
  
  @Test
  public void testSnapshotDeletionPolicy() throws Exception {
    Directory fsDir = newDirectory();
    runTest(random(), fsDir);
    fsDir.close();
  }

  private void runTest(Random random, Directory dir) throws Exception {
    // we use the IW unref'ed files check which is unaware of retries:
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    // Run for ~1 seconds
    final long stopTime = System.currentTimeMillis() + 1000;

    SnapshotDeletionPolicy dp = getDeletionPolicy();
    final IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random))
        .setIndexDeletionPolicy(dp)
        .setMaxBufferedDocs(2));

    // Verify we catch misuse:
    try {
      dp.snapshot();
      fail("did not hit exception");
    } catch(IllegalStateException ise) {
      // expected
    }
    dp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    writer.commit();
    
    final Thread t = new Thread() {
        @Override
        public void run() {
          Document doc = new Document();
          FieldType customType = new FieldType(TextField.TYPE_STORED);
          customType.setStoreTermVectors(true);
          customType.setStoreTermVectorPositions(true);
          customType.setStoreTermVectorOffsets(true);
          doc.add(newField("content", "aaa", customType));
          do {
            for(int i=0;i<27;i++) {
              try {
                writer.addDocument(doc);
              } catch (Throwable t) {
                t.printStackTrace(System.out);
                fail("addDocument failed");
              }
              if (i%2 == 0) {
                try {
                  writer.commit();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
            try {
              Thread.sleep(1);
            } catch (InterruptedException ie) {
              throw new ThreadInterruptedException(ie);
            }
          } while(System.currentTimeMillis() < stopTime);
        }
      };

    t.start();

    // While the above indexing thread is running, take many
    // backups:
    do {
      backupIndex(dir, dp);
      Thread.sleep(20);
    } while(t.isAlive());

    t.join();

    // Add one more document to force writer to commit a
    // final segment, so deletion policy has a chance to
    // delete again:
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setStoreTermVectors(true);
    customType.setStoreTermVectorPositions(true);
    customType.setStoreTermVectorOffsets(true);
    doc.add(newField("content", "aaa", customType));
    writer.addDocument(doc);

    // Make sure we don't have any leftover files in the
    // directory:
    writer.close();
    TestIndexWriter.assertNoUnreferencedFiles(dir, "some files were not deleted but should have been");
  }

  /**
   * Example showing how to use the SnapshotDeletionPolicy to take a backup.
   * This method does not really do a backup; instead, it reads every byte of
   * every file just to test that the files indeed exist and are readable even
   * while the index is changing.
   */
  public void backupIndex(Directory dir, SnapshotDeletionPolicy dp) throws Exception {
    // To backup an index we first take a snapshot:
    IndexCommit snapshot = dp.snapshot();
    try {
      copyFiles(dir, snapshot);
    } finally {
      // Make sure to release the snapshot, otherwise these
      // files will never be deleted during this IndexWriter
      // session:
      dp.release(snapshot);
    }
  }

  private void copyFiles(Directory dir, IndexCommit cp) throws Exception {

    // While we hold the snapshot, and nomatter how long
    // we take to do the backup, the IndexWriter will
    // never delete the files in the snapshot:
    Collection<String> files = cp.getFileNames();
    for (final String fileName : files) { 
      // NOTE: in a real backup you would not use
      // readFile; you would need to use something else
      // that copies the file to a backup location.  This
      // could even be a spawned shell process (eg "tar",
      // "zip") that takes the list of files and builds a
      // backup.
      readFile(dir, fileName);
    }
  }

  byte[] buffer = new byte[4096];

  private void readFile(Directory dir, String name) throws Exception {
    IndexInput input = dir.openInput(name, newIOContext(random()));
    try {
      long size = dir.fileLength(name);
      long bytesLeft = size;
      while (bytesLeft > 0) {
        final int numToRead;
        if (bytesLeft < buffer.length)
          numToRead = (int) bytesLeft;
        else
          numToRead = buffer.length;
        input.readBytes(buffer, 0, numToRead, false);
        bytesLeft -= numToRead;
      }
      // Don't do this in your real backups!  This is just
      // to force a backup to take a somewhat long time, to
      // make sure we are exercising the fact that the
      // IndexWriter should not delete this file even when I
      // take my time reading it.
      Thread.sleep(1);
    } finally {
      input.close();
    }
  }

  
  @Test
  public void testBasicSnapshots() throws Exception {
    int numSnapshots = 3;
    
    // Create 3 snapshots: snapshot0, snapshot1, snapshot2
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // we verify some files get deleted
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy()));
    SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    prepareIndexAndSnapshots(sdp, writer, numSnapshots);
    writer.close();
    
    assertEquals(numSnapshots, sdp.getSnapshots().size());
    assertEquals(numSnapshots, sdp.getSnapshotCount());
    assertSnapshotExists(dir, sdp, numSnapshots, true);

    // open a reader on a snapshot - should succeed.
    DirectoryReader.open(snapshots.get(0)).close();

    // open a new IndexWriter w/ no snapshots to keep and assert that all snapshots are gone.
    sdp = getDeletionPolicy();
    writer = new IndexWriter(dir, getConfig(random(), sdp));
    writer.deleteUnusedFiles();
    writer.close();
    assertEquals("no snapshots should exist", 1, DirectoryReader.listCommits(dir).size());
    dir.close();
  }

  @Test
  public void testMultiThreadedSnapshotting() throws Exception {
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // test relies on files actually being deleted
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    final IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy()));
    final SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();

    Thread[] threads = new Thread[10];
    final IndexCommit[] snapshots = new IndexCommit[threads.length];
    final CountDownLatch startingGun = new CountDownLatch(1);
    for (int i = 0; i < threads.length; i++) {
      final int finalI = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            writer.addDocument(new Document());
            writer.commit();
            snapshots[finalI] = sdp.snapshot();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };
      threads[i].setName("t" + i);
    }

    for (Thread t : threads) {
      t.start();
    }
    
    startingGun.countDown();

    for (Thread t : threads) {
      t.join();
    }

    // Do one last commit, so that after we release all snapshots, we stay w/ one commit
    writer.addDocument(new Document());
    writer.commit();
    
    for (int i=0;i<threads.length;i++) {
      sdp.release(snapshots[i]);
      writer.deleteUnusedFiles();
    }
    assertEquals(1, DirectoryReader.listCommits(dir).size());
    writer.close();
    dir.close();
  }

  @Test
  public void testRollbackToOldSnapshot() throws Exception {
    int numSnapshots = 2;
    Directory dir = newDirectory();

    SnapshotDeletionPolicy sdp = getDeletionPolicy();
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), sdp));
    prepareIndexAndSnapshots(sdp, writer, numSnapshots);
    writer.close();

    // now open the writer on "snapshot0" - make sure it succeeds
    writer = new IndexWriter(dir, getConfig(random(), sdp).setIndexCommit(snapshots.get(0)));
    // this does the actual rollback
    writer.commit();
    writer.deleteUnusedFiles();
    assertSnapshotExists(dir, sdp, numSnapshots - 1, false);
    writer.close();

    // but 'snapshot1' files will still exist (need to release snapshot before they can be deleted).
    String segFileName = snapshots.get(1).getSegmentsFileName();
    assertTrue("snapshot files should exist in the directory: " + segFileName, slowFileExists(dir, segFileName));

    dir.close();
  }

  @Test
  public void testReleaseSnapshot() throws Exception {
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // we rely upon existence of files
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy()));
    SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    prepareIndexAndSnapshots(sdp, writer, 1);
    
    // Create another commit - we must do that, because otherwise the "snapshot"
    // files will still remain in the index, since it's the last commit.
    writer.addDocument(new Document());
    writer.commit();
    
    // Release
    String segFileName = snapshots.get(0).getSegmentsFileName();
    sdp.release(snapshots.get(0));
    writer.deleteUnusedFiles();
    writer.close();
    assertFalse("segments file should not be found in dirctory: " + segFileName, slowFileExists(dir, segFileName));
    dir.close();
  }

  @Test
  public void testSnapshotLastCommitTwice() throws Exception {
    Directory dir = newDirectory();

    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy()));
    SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    writer.addDocument(new Document());
    writer.commit();
    
    IndexCommit s1 = sdp.snapshot();
    IndexCommit s2 = sdp.snapshot();
    assertSame(s1, s2); // should be the same instance
    
    // create another commit
    writer.addDocument(new Document());
    writer.commit();
    
    // release "s1" should not delete "s2"
    sdp.release(s1);
    writer.deleteUnusedFiles();
    checkSnapshotExists(dir, s2);
    
    writer.close();
    dir.close();
  }
  
  @Test
  public void testMissingCommits() throws Exception {
    // Tests the behavior of SDP when commits that are given at ctor are missing
    // on onInit().
    Directory dir = newDirectory();
    if (dir instanceof MockDirectoryWrapper) {
      // we rely upon existence of files
      ((MockDirectoryWrapper)dir).setEnableVirusScanner(false);
    }
    IndexWriter writer = new IndexWriter(dir, getConfig(random(), getDeletionPolicy()));
    SnapshotDeletionPolicy sdp = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
    writer.addDocument(new Document());
    writer.commit();
    IndexCommit s1 = sdp.snapshot();

    // create another commit, not snapshotted.
    writer.addDocument(new Document());
    writer.close();

    // open a new writer w/ KeepOnlyLastCommit policy, so it will delete "s1"
    // commit.
    new IndexWriter(dir, getConfig(random(), null)).close();
    
    assertFalse("snapshotted commit should not exist", slowFileExists(dir, s1.getSegmentsFileName()));
    dir.close();
  }
}
