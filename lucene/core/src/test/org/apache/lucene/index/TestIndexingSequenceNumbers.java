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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestIndexingSequenceNumbers extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    long a = w.addDocument(new Document());
    long b = w.addDocument(new Document());
    assertTrue(b > a);
    w.close();
    dir.close();
  }

  public void testAfterRefresh() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    long a = w.addDocument(new Document());
    DirectoryReader.open(w).close();
    long b = w.addDocument(new Document());
    assertTrue(b > a);
    w.close();
    dir.close();
  }

  public void testAfterCommit() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    long a = w.addDocument(new Document());
    w.commit();
    long b = w.addDocument(new Document());
    assertTrue(b > a);
    w.close();
    dir.close();
  }

  @Nightly
  public void testStressUpdateSameID() throws Exception {
    int iters = atLeast(100);
    for(int iter=0;iter<iters;iter++) {
      Directory dir = newFSDirectory(createTempDir());
      final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 5)];
      final CountDownLatch startingGun = new CountDownLatch(1);
      final long[] seqNos = new long[threads.length];
      final Term id = new Term("id", "id");
      // multiple threads update the same document
      for(int i=0;i<threads.length;i++) {
        final int threadID = i;
        threads[i] = new Thread() {
            @Override
            public void run() {
              try {
                Document doc = new Document();
                doc.add(new StoredField("thread", threadID));
                doc.add(new StringField("id", "id", Field.Store.NO));
                startingGun.await();
                for(int j=0;j<100;j++) {
                  if (random().nextBoolean()) {
                    seqNos[threadID] = w.updateDocument(id, doc);
                  } else {
                    seqNos[threadID] = w.updateDocuments(id, Arrays.asList(doc));
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };
        threads[i].start();
      }
      startingGun.countDown();
      for(Thread thread : threads) {
        thread.join();
      }

      // now confirm that the reported sequence numbers agree with the index:
      int maxThread = 0;
      Set<Long> allSeqNos = new HashSet<>();
      for(int i=0;i<threads.length;i++) {
        allSeqNos.add(seqNos[i]);
        if (seqNos[i] > seqNos[maxThread]) {
          maxThread = i;
        }
      }
      // make sure all sequence numbers were different
      assertEquals(threads.length, allSeqNos.size());
      DirectoryReader r = w.getReader();
      IndexSearcher s = newSearcher(r);
      TopDocs hits = s.search(new TermQuery(id), 1);
      assertEquals("maxDoc: " + r.maxDoc(), 1, hits.totalHits.value);
      Document doc = r.document(hits.scoreDocs[0].doc);
      assertEquals(maxThread, doc.getField("thread").numericValue().intValue());
      r.close();
      w.close();
      dir.close();
    }
  }

  static class Operation {
    // 0 = update, 1 = delete, 2 = commit, 3 = add
    byte what;
    int id;
    int threadID;
    long seqNo;
  }

  @Nightly
  public void testStressConcurrentCommit() throws Exception {
    final int opCount = atLeast(10000);
    final int idCount = TestUtil.nextInt(random(), 10, 1000);

    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    // Cannot use RIW since it randomly commits:
    final IndexWriter w = new IndexWriter(dir, iwc);

    final int numThreads = TestUtil.nextInt(random(), 2, 10);
    Thread[] threads = new Thread[numThreads];
    //System.out.println("TEST: iter=" + iter + " opCount=" + opCount + " idCount=" + idCount + " threadCount=" + threads.length);
    final CountDownLatch startingGun = new CountDownLatch(1);
    List<List<Operation>> threadOps = new ArrayList<>();

    Object commitLock = new Object();
    final List<Operation> commits = new ArrayList<>();

    // multiple threads update the same set of documents, and we randomly commit, recording the commit seqNo and then opening each commit in
    // the end to verify it reflects the correct updates
    for(int i=0;i<threads.length;i++) {
      final List<Operation> ops = new ArrayList<>();
      threadOps.add(ops);
      final int threadID = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              for(int i=0;i<opCount;i++) {
                Operation op = new Operation();
                op.threadID = threadID;
                if (random().nextInt(500) == 17) {
                  op.what = 2;
                  synchronized(commitLock) {
                    op.seqNo = w.commit();
                    if (op.seqNo != -1) {
                      commits.add(op);
                    }
                  }
                } else {
                  op.id = random().nextInt(idCount);
                  Term idTerm = new Term("id", "" + op.id);
                  if (random().nextInt(10) == 1) {
                    op.what = 1;
                    if (random().nextBoolean()) {
                      op.seqNo = w.deleteDocuments(idTerm);
                    } else {
                      op.seqNo = w.deleteDocuments(new TermQuery(idTerm));
                    }
                  } else {
                    Document doc = new Document();
                    doc.add(new StoredField("thread", threadID));
                    doc.add(new StringField("id", "" + op.id, Field.Store.NO));
                    if (random().nextBoolean()) {
                      List<Document> docs = new ArrayList<>();
                      docs.add(doc);
                      op.seqNo = w.updateDocuments(idTerm, docs);
                    } else {
                      op.seqNo = w.updateDocument(idTerm, doc);
                    }
                    op.what = 0;
                  }
                  ops.add(op);
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].start();
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }

    Operation commitOp = new Operation();
    commitOp.seqNo = w.commit();
    if (commitOp.seqNo != -1) {
      commits.add(commitOp);
    }

    List<IndexCommit> indexCommits = DirectoryReader.listCommits(dir);
    assertEquals(commits.size(), indexCommits.size());

    int[] expectedThreadIDs = new int[idCount];
    long[] seqNos = new long[idCount];
      
    //System.out.println("TEST: " + commits.size() + " commits");
    for(int i=0;i<commits.size();i++) {
      // this commit point should reflect all operations <= this seqNo
      long commitSeqNo = commits.get(i).seqNo;
      //System.out.println("  commit " + i + ": seqNo=" + commitSeqNo + " segs=" + indexCommits.get(i));

      Arrays.fill(expectedThreadIDs, -1);
      Arrays.fill(seqNos, 0);

      for(int threadID=0;threadID<threadOps.size();threadID++) {
        long lastSeqNo = 0;
        for(Operation op : threadOps.get(threadID)) {
          if (op.seqNo <= commitSeqNo && op.seqNo > seqNos[op.id]) {
            seqNos[op.id] = op.seqNo;
            if (op.what == 0) {
              expectedThreadIDs[op.id] = threadID;
            } else {
              expectedThreadIDs[op.id] = -1;
            }
          }

          assertTrue(op.seqNo > lastSeqNo);
          lastSeqNo = op.seqNo;
        }
      }

      DirectoryReader r = DirectoryReader.open(indexCommits.get(i));
      IndexSearcher s = new IndexSearcher(r);

      for(int id=0;id<idCount;id++) {
        //System.out.println("TEST: check id=" + id + " expectedThreadID=" + expectedThreadIDs[id]);
        TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1);
                                  
        if (expectedThreadIDs[id] != -1) {
          assertEquals(1, hits.totalHits.value);
          Document doc = r.document(hits.scoreDocs[0].doc);
          int actualThreadID = doc.getField("thread").numericValue().intValue();
          if (expectedThreadIDs[id] != actualThreadID) {
            System.out.println("FAIL: id=" + id + " expectedThreadID=" + expectedThreadIDs[id] + " vs actualThreadID=" + actualThreadID + " commitSeqNo=" + commitSeqNo + " numThreads=" + numThreads);
            for(int threadID=0;threadID<threadOps.size();threadID++) {
              for(Operation op : threadOps.get(threadID)) {
                if (id == op.id) {
                  System.out.println("  threadID=" + threadID + " seqNo=" + op.seqNo + " " + (op.what == 2 ? "updated" : "deleted"));
                }
              }
            }
            assertEquals("id=" + id, expectedThreadIDs[id], actualThreadID);
          }
        } else if (hits.totalHits.value != 0) {
          System.out.println("FAIL: id=" + id + " expectedThreadID=" + expectedThreadIDs[id] + " vs totalHits=" + hits.totalHits.value + " commitSeqNo=" + commitSeqNo + " numThreads=" + numThreads);
          for(int threadID=0;threadID<threadOps.size();threadID++) {
            for(Operation op : threadOps.get(threadID)) {
              if (id == op.id) {
                System.out.println("  threadID=" + threadID + " seqNo=" + op.seqNo + " " + (op.what == 2 ? "updated" : "del"));
              }
            }
          }
          assertEquals(0, hits.totalHits.value);
        }
      }
      w.close();
      r.close();
    }

    dir.close();
  }

  @Nightly
  public void testStressConcurrentDocValuesUpdatesCommit() throws Exception {
    final int opCount = atLeast(10000);
    final int idCount = TestUtil.nextInt(random(), 10, 1000);

    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    // Cannot use RIW since it randomly commits:
    final IndexWriter w = new IndexWriter(dir, iwc);

    final int numThreads = TestUtil.nextInt(random(), 2, 10);
    if (VERBOSE) {
      System.out.println("TEST: numThreads=" + numThreads);
    }
    Thread[] threads = new Thread[numThreads];
    //System.out.println("TEST: iter=" + iter + " opCount=" + opCount + " idCount=" + idCount + " threadCount=" + threads.length);
    final CountDownLatch startingGun = new CountDownLatch(1);
    List<List<Operation>> threadOps = new ArrayList<>();

    Object commitLock = new Object();
    final List<Operation> commits = new ArrayList<>();

    List<Operation> ops1 = new ArrayList<>();
    threadOps.add(ops1);

    // pre-index every ID so none are missing:
    for(int id=0;id<idCount;id++) {
      int threadID = 0;
      Operation op = new Operation();
      op.threadID = threadID;
      op.id = id;

      Document doc = new Document();
      doc.add(new StoredField("thread", threadID));
      doc.add(new NumericDocValuesField("thread", threadID));
      doc.add(new StringField("id", "" + id, Field.Store.NO));
      op.seqNo = w.addDocument(doc);
      ops1.add(op);
    }

    // multiple threads update the same set of documents, and we randomly commit, recording the commit seqNo and then opening each commit in
    // the end to verify it reflects the correct updates
    for(int i=0;i<threads.length;i++) {
      final List<Operation> ops;
      if (i == 0) {
        ops = threadOps.get(0);
      } else {
        ops = new ArrayList<>();
        threadOps.add(ops);
      }

      final int threadID = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              for(int i=0;i<opCount;i++) {
                Operation op = new Operation();
                op.threadID = threadID;
                if (random().nextInt(500) == 17) {
                  op.what = 2;
                  synchronized(commitLock) {
                    op.seqNo = w.commit();
                    if (op.seqNo != -1) {
                      commits.add(op);
                    }
                  }
                } else {
                  op.id = random().nextInt(idCount);
                  Term idTerm = new Term("id", "" + op.id);
                  op.seqNo = w.updateNumericDocValue(idTerm, "thread", threadID);
                  op.what = 0;
                  ops.add(op);
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].setName("thread" + i);
      threads[i].start();
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }

    Operation commitOp = new Operation();
    commitOp.seqNo = w.commit();
    if (commitOp.seqNo != -1) {
      commits.add(commitOp);
    }

    List<IndexCommit> indexCommits = DirectoryReader.listCommits(dir);
    assertEquals(commits.size(), indexCommits.size());

    int[] expectedThreadIDs = new int[idCount];
    long[] seqNos = new long[idCount];
      
    //System.out.println("TEST: " + commits.size() + " commits");
    for(int i=0;i<commits.size();i++) {
      // this commit point should reflect all operations <= this seqNo
      long commitSeqNo = commits.get(i).seqNo;
      //System.out.println("  commit " + i + ": seqNo=" + commitSeqNo + " segs=" + indexCommits.get(i));

      Arrays.fill(expectedThreadIDs, -1);
      Arrays.fill(seqNos, 0);

      for(int threadID=0;threadID<threadOps.size();threadID++) {
        long lastSeqNo = 0;
        for(Operation op : threadOps.get(threadID)) {
          if (op.seqNo <= commitSeqNo && op.seqNo > seqNos[op.id]) {
            seqNos[op.id] = op.seqNo;
            assert op.what == 0;
            expectedThreadIDs[op.id] = threadID;
          }

          assertTrue(op.seqNo > lastSeqNo);
          lastSeqNo = op.seqNo;
        }
      }

      DirectoryReader r = DirectoryReader.open(indexCommits.get(i));
      IndexSearcher s = new IndexSearcher(r);

      for(int id=0;id<idCount;id++) {
        //System.out.println("TEST: check id=" + id + " expectedThreadID=" + expectedThreadIDs[id]);
        TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1);
        NumericDocValues docValues = MultiDocValues.getNumericValues(r, "thread");

        // We pre-add all ids up front:
        assert expectedThreadIDs[id] != -1;
        assertEquals(1, hits.totalHits.value);
        int hitDoc = hits.scoreDocs[0].doc;
        assertEquals(hitDoc, docValues.advance(hitDoc));
        int actualThreadID = (int) docValues.longValue();
        if (expectedThreadIDs[id] != actualThreadID) {
          System.out.println("FAIL: commit=" + i + " (of " + commits.size() + ") id=" + id + " expectedThreadID=" + expectedThreadIDs[id] + " vs actualThreadID=" + actualThreadID + " commitSeqNo=" + commitSeqNo + " numThreads=" + numThreads + " reader=" + r + " commit=" + indexCommits.get(i));
          for(int threadID=0;threadID<threadOps.size();threadID++) {
            for(Operation op : threadOps.get(threadID)) {
              if (id == op.id) {
                System.out.println("  threadID=" + threadID + " seqNo=" + op.seqNo);
              }
            }
          }
          assertEquals("id=" + id + " docID=" + hits.scoreDocs[0].doc, expectedThreadIDs[id], actualThreadID);
        }
      }
      w.close();
      r.close();
    }

    dir.close();
  }

  @Nightly
  public void testStressConcurrentAddAndDeleteAndCommit() throws Exception {
    final int opCount = atLeast(10000);
    final int idCount = TestUtil.nextInt(random(), 10, 1000);

    Directory dir = newFSDirectory(createTempDir());
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);

    // Cannot use RIW since it randomly commits:
    final IndexWriter w = new IndexWriter(dir, iwc);

    final int numThreads = TestUtil.nextInt(random(), 2, 5);
    Thread[] threads = new Thread[numThreads];
    //System.out.println("TEST: iter=" + iter + " opCount=" + opCount + " idCount=" + idCount + " threadCount=" + threads.length);
    final CountDownLatch startingGun = new CountDownLatch(1);
    List<List<Operation>> threadOps = new ArrayList<>();

    Object commitLock = new Object();
    final List<Operation> commits = new ArrayList<>();

    // multiple threads update the same set of documents, and we randomly commit
    for(int i=0;i<threads.length;i++) {
      final List<Operation> ops = new ArrayList<>();
      threadOps.add(ops);
      final int threadID = i;
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              for(int i=0;i<opCount;i++) {
                Operation op = new Operation();
                op.threadID = threadID;
                if (random().nextInt(500) == 17) {
                  op.what = 2;
                  synchronized(commitLock) {
                    op.seqNo = w.commit();
                    if (op.seqNo != -1) {
                      commits.add(op);
                    }
                  }
                } else {
                  op.id = random().nextInt(idCount);
                  Term idTerm = new Term("id", "" + op.id);
                  if (random().nextInt(10) == 1) {
                    op.what = 1;
                    if (random().nextBoolean()) {
                      op.seqNo = w.deleteDocuments(idTerm);
                    } else {
                      op.seqNo = w.deleteDocuments(new TermQuery(idTerm));
                    }
                  } else {
                    Document doc = new Document();
                    doc.add(new StoredField("threadop", threadID + "-" + ops.size()));
                    doc.add(new StringField("id", "" + op.id, Field.Store.NO));
                    if (random().nextBoolean()) {
                      List<Document> docs = new ArrayList<>();
                      docs.add(doc);
                      op.seqNo = w.addDocuments(docs);
                    } else {
                      op.seqNo = w.addDocument(doc);
                    }
                    op.what = 3;
                  }
                  ops.add(op);
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].setName("thread" + threadID);
      threads[i].start();
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }

    Operation commitOp = new Operation();
    commitOp.seqNo = w.commit();
    if (commitOp.seqNo != -1) {
      commits.add(commitOp);
    }

    List<IndexCommit> indexCommits = DirectoryReader.listCommits(dir);
    assertEquals(commits.size(), indexCommits.size());

    // how many docs with this id are expected:
    int[] expectedCounts = new int[idCount];
    long[] lastDelSeqNos = new long[idCount];
      
    //System.out.println("TEST: " + commits.size() + " commits");
    for(int i=0;i<commits.size();i++) {
      // this commit point should reflect all operations <= this seqNo
      long commitSeqNo = commits.get(i).seqNo;
      //System.out.println("  commit " + i + ": seqNo=" + commitSeqNo + " segs=" + indexCommits.get(i));

      // first find the highest seqNo of the last delete op, for each id, prior to this commit:
      Arrays.fill(lastDelSeqNos, -1);
      for(int threadID=0;threadID<threadOps.size();threadID++) {
        long lastSeqNo = 0;
        for(Operation op : threadOps.get(threadID)) {
          if (op.what == 1 && op.seqNo <= commitSeqNo && op.seqNo > lastDelSeqNos[op.id]) {
            lastDelSeqNos[op.id] = op.seqNo;
          }

          // within one thread the seqNos must only increase:
          assertTrue(op.seqNo > lastSeqNo);
          lastSeqNo = op.seqNo;
        }
      }

      // then count how many adds happened since the last delete and before this commit:
      Arrays.fill(expectedCounts, 0);
      for(int threadID=0;threadID<threadOps.size();threadID++) {
        for(Operation op : threadOps.get(threadID)) {
          if (op.what == 3 && op.seqNo <= commitSeqNo && op.seqNo > lastDelSeqNos[op.id]) {
            expectedCounts[op.id]++;
          }
        }
      }

      DirectoryReader r = DirectoryReader.open(indexCommits.get(i));
      IndexSearcher s = new IndexSearcher(r);

      for(int id=0;id<idCount;id++) {
        //System.out.println("TEST: check id=" + id + " expectedThreadID=" + expectedThreadIDs[id]);
        int actualCount = s.count(new TermQuery(new Term("id", ""+id)));
        if (expectedCounts[id] != actualCount) {
          System.out.println("TEST: FAIL r=" + r + " id=" + id + " commitSeqNo=" + commitSeqNo);
          for(int threadID=0;threadID<threadOps.size();threadID++) {
            int opCount2 = 0;
            for(Operation op : threadOps.get(threadID)) {
              if (op.id == id) {
                boolean shouldCount = op.seqNo <= commitSeqNo && op.seqNo > lastDelSeqNos[op.id];
                System.out.println("  id=" + id + " what=" + op.what + " threadop=" + threadID + "-" + opCount2 + " seqNo=" + op.seqNo + " vs lastDelSeqNo=" + lastDelSeqNos[op.id] + " shouldCount=" + shouldCount);
              }
              opCount2++;
            }
          }
          TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1+actualCount);
          for(ScoreDoc hit : hits.scoreDocs) {
            System.out.println("  hit: " + s.doc(hit.doc).get("threadop"));
          }

          for(LeafReaderContext ctx : r.leaves()) {
            System.out.println("  sub=" + ctx.reader());
            Bits liveDocs = ctx.reader().getLiveDocs();
            for(int docID=0;docID<ctx.reader().maxDoc();docID++) {
              System.out.println("    docID=" + docID + " threadop=" + ctx.reader().document(docID).get("threadop") + (liveDocs != null && liveDocs.get(docID) == false ? " (deleted)" : ""));
            }
          }

          assertEquals("commit " + i + " of " + commits.size() + " id=" + id + " reader=" + r, expectedCounts[id], actualCount);
        }
      }
      w.close();
      r.close();
    }

    dir.close();
  }

  public void testDeleteAll() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    long a = w.addDocument(new Document());
    long b = w.deleteAll();
    assertTrue(a < b);
    long c = w.commit();
    assertTrue(b < c);
    w.close();
    dir.close();
  }
}
