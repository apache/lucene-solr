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


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestIndexWriterFromReader extends LuceneTestCase {

  // Pull NRT reader immediately after writer has committed
  public void testRightAfterCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());

    IndexWriter w2 = new IndexWriter(dir, iwc);
    r.close();

    assertEquals(1, w2.maxDoc());
    w2.addDocument(new Document());
    assertEquals(2, w2.maxDoc());
    w2.close();
    
    IndexReader r2 = DirectoryReader.open(dir);
    assertEquals(2, r2.maxDoc());
    r2.close();
    dir.close();
  }

  // Open from non-NRT reader
  public void testFromNonNRTReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.maxDoc());
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());

    IndexWriter w2 = new IndexWriter(dir, iwc);
    assertEquals(1, r.maxDoc());
    r.close();

    assertEquals(1, w2.maxDoc());
    w2.addDocument(new Document());
    assertEquals(2, w2.maxDoc());
    w2.close();
    
    IndexReader r2 = DirectoryReader.open(dir);
    assertEquals(2, r2.maxDoc());
    r2.close();
    dir.close();
  }

  // Pull NRT reader from a writer on a new index with no commit:
  public void testWithNoFirstCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    DirectoryReader r = DirectoryReader.open(w);
    w.rollback();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());

    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, iwc);
    });
    assertEquals("cannot use IndexWriterConfig.setIndexCommit() when index has no commit", expected.getMessage());
      
    r.close();
    dir.close();
  }

  // Pull NRT reader after writer has committed and then indexed another doc:
  public void testAfterCommitThenIndex() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(2, r.maxDoc());
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());

    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, iwc);
    });
    assertTrue(expected.getMessage().contains("the provided reader is stale: its prior commit file"));

    r.close();
    dir.close();
  }

  // NRT rollback: pull NRT reader after writer has committed and then before indexing another doc
  public void testNRTRollback() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());

    // Add another doc
    w.addDocument(new Document());
    assertEquals(2, w.maxDoc());
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, iwc);
    });
    assertTrue(expected.getMessage().contains("the provided reader is stale: its prior commit file"));      

    r.close();
    dir.close();
  }

  public void testRandom() throws Exception {
    Directory dir = newDirectory();

    int numOps = atLeast(100);

    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());

    // We must have a starting commit for this test because whenever we rollback with
    // an NRT reader, the commit before that NRT reader must exist
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    int nrtReaderNumDocs = 0;
    int writerNumDocs = 0;

    boolean commitAfterNRT = false;

    Set<Integer> liveIDs = new HashSet<>();
    Set<Integer> nrtLiveIDs = new HashSet<>();

    for(int op=0;op<numOps;op++) {
      if (VERBOSE) {
        System.out.println("\nITER op=" + op + " nrtReaderNumDocs=" + nrtReaderNumDocs + " writerNumDocs=" + writerNumDocs + " r=" + r + " r.numDocs()=" + r.numDocs());
      }

      assertEquals(nrtReaderNumDocs, r.numDocs());
      int x = random().nextInt(5);

      switch(x) {

      case 0:
        if (VERBOSE) {
          System.out.println("  add doc id=" + op);
        }
        // add doc
        Document doc = new Document();
        doc.add(newStringField("id", ""+op, Field.Store.NO));
        w.addDocument(doc);
        liveIDs.add(op);
        writerNumDocs++;
        break;

      case 1:
        if (VERBOSE) {
          System.out.println("  delete doc");
        }
        // delete docs
        if (liveIDs.size() > 0) {
          int id = random().nextInt(op);
          if (VERBOSE) {
            System.out.println("    id=" + id);
          }
          w.deleteDocuments(new Term("id", ""+id));
          if (liveIDs.remove(id)) {
            if (VERBOSE) {
              System.out.println("    really deleted");
            }
            writerNumDocs--;
          }
        } else {
          if (VERBOSE) {
            System.out.println("    nothing to delete yet");
          }
        }
        break;

      case 2:
        // reopen NRT reader
        if (VERBOSE) {
          System.out.println("  reopen NRT reader");
        }
        DirectoryReader r2 = DirectoryReader.openIfChanged(r);
        if (r2 != null) {
          r.close();
          r = r2;
          if (VERBOSE) {
            System.out.println("    got new reader oldNumDocs=" + nrtReaderNumDocs + " newNumDocs=" + writerNumDocs);
          }
          nrtReaderNumDocs = writerNumDocs;
          nrtLiveIDs = new HashSet<>(liveIDs);
        } else {
          if (VERBOSE) {
            System.out.println("    reader is unchanged");
          }
          assertEquals(nrtReaderNumDocs, r.numDocs());
        }
        commitAfterNRT = false;
        break;

      case 3:
        if (commitAfterNRT == false) {
          // rollback writer to last nrt reader
          if (random().nextBoolean()) {
            if (VERBOSE) {
              System.out.println("  close writer and open new writer from non-NRT reader numDocs=" + w.numDocs());
            }
            w.close();
            r.close();
            r = DirectoryReader.open(dir);
            assertEquals(writerNumDocs, r.numDocs());
            nrtReaderNumDocs = writerNumDocs;
            nrtLiveIDs = new HashSet<>(liveIDs);
          } else {
            if (VERBOSE) {
              System.out.println("  rollback writer and open new writer from NRT reader numDocs=" + w.numDocs());
            }
            w.rollback();
          }
          IndexWriterConfig iwc = newIndexWriterConfig();
          iwc.setIndexCommit(r.getIndexCommit());
          w = new IndexWriter(dir, iwc);
          writerNumDocs = nrtReaderNumDocs;
          liveIDs = new HashSet<>(nrtLiveIDs);
          r.close();
          r = DirectoryReader.open(w);
        }
        break;

      case 4:
        if (VERBOSE) {
          System.out.println("    commit");
        }
        w.commit();
        commitAfterNRT = true;
        break;
      }
    }

    IOUtils.close(w, r, dir);
  }

  public void testConsistentFieldNumbers() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    // Empty first commit:
    w.commit();

    Document doc = new Document();
    doc.add(newStringField("f0", "foo", Field.Store.NO));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());

    doc = new Document();
    doc.add(newStringField("f1", "foo", Field.Store.NO));
    w.addDocument(doc);

    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);
    r.close();
    assertEquals(2, r2.maxDoc());
    w.rollback();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r2.getIndexCommit());

    IndexWriter w2 = new IndexWriter(dir, iwc);
    r2.close();

    doc = new Document();
    doc.add(newStringField("f1", "foo", Field.Store.NO));
    doc.add(newStringField("f0", "foo", Field.Store.NO));
    w2.addDocument(doc);
    w2.close();
    dir.close();
  }

  public void testInvalidOpenMode() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    iwc.setIndexCommit(r.getIndexCommit());
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new IndexWriter(dir, iwc);
    });
    assertEquals("cannot use IndexWriterConfig.setIndexCommit() with OpenMode.CREATE", expected.getMessage());

    IOUtils.close(r, dir);
  }

  public void testOnClosedReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());
    IndexCommit commit = r.getIndexCommit();
    r.close();
    w.close();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(commit);
    expectThrows(AlreadyClosedException.class, () -> {
      new IndexWriter(dir, iwc);
    });

    IOUtils.close(r, dir);
  }

  public void testStaleNRTReader() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(1, r.maxDoc());
    w.addDocument(new Document());

    DirectoryReader r2 = DirectoryReader.openIfChanged(r);
    assertNotNull(r2);
    r2.close();
    w.rollback();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());
    w = new IndexWriter(dir, iwc);
    assertEquals(1, w.numDocs());

    r.close();
    DirectoryReader r3 = DirectoryReader.open(w);
    assertEquals(1, r3.numDocs());
    
    w.addDocument(new Document());
    DirectoryReader r4 = DirectoryReader.openIfChanged(r3);
    r3.close();
    assertEquals(2, r4.numDocs());
    r4.close();
    w.close();

    IOUtils.close(r, dir);
  }

  public void testAfterRollback() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(2, r.maxDoc());
    w.rollback();

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());
    w = new IndexWriter(dir, iwc);
    assertEquals(2, w.numDocs());

    r.close();
    w.close();

    DirectoryReader r2 = DirectoryReader.open(dir);
    assertEquals(2, r2.numDocs());
    IOUtils.close(r2, dir);
  }

  // Pull NRT reader after writer has committed and then indexed another doc:
  public void testAfterCommitThenIndexKeepCommits() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();

    // Keep all commits:
    iwc.setIndexDeletionPolicy(new IndexDeletionPolicy() {
        @Override
        public void onInit(List<? extends IndexCommit> commits) {
        }

        @Override
        public void onCommit(List<? extends IndexCommit> commits) {
        }
      });

    IndexWriter w = new IndexWriter(dir, iwc);
    w.addDocument(new Document());
    w.commit();
    w.addDocument(new Document());

    DirectoryReader r = DirectoryReader.open(w);
    assertEquals(2, r.maxDoc());
    w.addDocument(new Document());

    DirectoryReader r2 = DirectoryReader.open(w);
    assertEquals(3, r2.maxDoc());
    IOUtils.close(r2, w);

    // r is not stale because, even though we've committed the original writer since it was open, we are keeping all commit points:
    iwc = newIndexWriterConfig();
    iwc.setIndexCommit(r.getIndexCommit());
    IndexWriter w2 = new IndexWriter(dir, iwc);
    assertEquals(2, w2.maxDoc());
    IOUtils.close(r, w2, dir);
  }
}
