package org.apache.lucene;
// Intentionally not in org.apache.lucene.index, to assert
// that we do not require any package private access.

/**
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

import java.util.Iterator;
import java.util.Collection;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TestIndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

//
// This was developed for Lucene In Action,
// http://lucenebook.com
//

public class TestSnapshotDeletionPolicy extends LuceneTestCase
{
  public static final String INDEX_PATH = "test.snapshots";

  public void testSnapshotDeletionPolicy() throws IOException {
    File dir = new File(System.getProperty("tempDir"), INDEX_PATH);
    try {
      Directory fsDir = FSDirectory.getDirectory(dir);
      runTest(fsDir);
      fsDir.close();
    } finally {
      _TestUtil.rmDir(dir);
    }

    MockRAMDirectory dir2 = new MockRAMDirectory();
    runTest(dir2);
  }

  public void testReuseAcrossWriters() throws IOException {
    Directory dir = new MockRAMDirectory();

    SnapshotDeletionPolicy dp = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    IndexWriter writer = new IndexWriter(dir, true,new StandardAnalyzer(), dp);
    // Force frequent commits
    writer.setMaxBufferedDocs(2);
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    for(int i=0;i<7;i++)
      writer.addDocument(doc);
    IndexCommit cp = (IndexCommit) dp.snapshot();
    copyFiles(dir, cp);
    writer.close();
    copyFiles(dir, cp);
    
    writer = new IndexWriter(dir, true, new StandardAnalyzer(), dp);
    copyFiles(dir, cp);
    for(int i=0;i<7;i++)
      writer.addDocument(doc);
    copyFiles(dir, cp);
    writer.close();
    copyFiles(dir, cp);
    dp.release();
    writer = new IndexWriter(dir, true, new StandardAnalyzer(), dp);
    writer.close();
    try {
      copyFiles(dir, cp);
      fail("did not hit expected IOException");
    } catch (IOException ioe) {
      // expected
    }
    dir.close();
  }

  private void runTest(Directory dir) throws IOException {
    // Run for ~7 seconds
    final long stopTime = System.currentTimeMillis() + 7000;

    SnapshotDeletionPolicy dp = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
    final IndexWriter writer = new IndexWriter(dir, true, new StandardAnalyzer(), dp);

    // Force frequent commits
    writer.setMaxBufferedDocs(2);

    final Thread t = new Thread() {
        public void run() {
          Document doc = new Document();
          doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
          while(System.currentTimeMillis() < stopTime) {
            for(int i=0;i<27;i++) {
              try {
                writer.addDocument(doc);
              } catch (Throwable t) {
                t.printStackTrace(System.out);
                fail("addDocument failed");
              }
            }
            try {
              Thread.sleep(1);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            }
          }
        }
      };

    t.start();

    // While the above indexing thread is running, take many
    // backups:
    while(System.currentTimeMillis() < stopTime) {
      backupIndex(dir, dp);
      try {
        Thread.sleep(20);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      if (!t.isAlive())
        break;
    }

    try {
      t.join();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    // Add one more document to force writer to commit a
    // final segment, so deletion policy has a chance to
    // delete again:
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);

    // Make sure we don't have any leftover files in the
    // directory:
    writer.close();
    TestIndexWriter.assertNoUnreferencedFiles(dir, "some files were not deleted but should have been");
  }

  /** Example showing how to use the SnapshotDeletionPolicy
   *  to take a backup.  This method does not really do a
   *  backup; instead, it reads every byte of every file
   *  just to test that the files indeed exist and are
   *  readable even while the index is changing. */
  public void backupIndex(Directory dir, SnapshotDeletionPolicy dp) throws IOException {
    // To backup an index we first take a snapshot:
    try {
      copyFiles(dir, (IndexCommit) dp.snapshot());
    } finally {
      // Make sure to release the snapshot, otherwise these
      // files will never be deleted during this IndexWriter
      // session:
      dp.release();
    }
  }

  private void copyFiles(Directory dir, IndexCommit cp) throws IOException {

    // While we hold the snapshot, and nomatter how long
    // we take to do the backup, the IndexWriter will
    // never delete the files in the snapshot:
    Collection files = cp.getFileNames();
    Iterator it = files.iterator();
    while(it.hasNext()) {
      final String fileName = (String) it.next();
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

  private void readFile(Directory dir, String name) throws IOException {
    IndexInput input = dir.openInput(name);
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
      try {
        Thread.sleep(1);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    } finally {
      input.close();
    }
  }
}

