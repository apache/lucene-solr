package org.apache.lucene.index;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util._TestUtil;
import org.junit.Test;

// TODO
//   - mix in optimize, addIndexes
//   - randomoly mix in non-congruent docs

// NOTE: This is a copy of TestNRTThreads, but swapping in
// NRTManager for adding/updating/searching

public class TestNRTManager extends LuceneTestCase {

  private static class SubDocs {
    public final String packID;
    public final List<String> subIDs;
    public boolean deleted;

    public SubDocs(String packID, List<String> subIDs) {
      this.packID = packID;
      this.subIDs = subIDs;
    }
  }

  // TODO: is there a pre-existing way to do this!!!
  private Document cloneDoc(Document doc1) {
    final Document doc2 = new Document();
    for(Fieldable f : doc1.getFields()) {
      Field field1 = (Field) f;
      
      Field field2 = new Field(field1.name(),
                               field1.stringValue(),
                               field1.isStored() ? Field.Store.YES : Field.Store.NO,
                               field1.isIndexed() ? (field1.isTokenized() ? Field.Index.ANALYZED : Field.Index.NOT_ANALYZED) : Field.Index.NO);
      if (field1.getOmitNorms()) {
        field2.setOmitNorms(true);
      }
      if (field1.getOmitTermFreqAndPositions()) {
        field2.setOmitTermFreqAndPositions(true);
      }
      doc2.add(field2);
    }

    return doc2;
  }

  @Test
  public void testNRTManager() throws Exception {

    final long t0 = System.currentTimeMillis();

    if (CodecProvider.getDefault().getDefaultFieldCodec().equals("SimpleText")) {
      // no
      CodecProvider.getDefault().setDefaultFieldCodec("Standard");
    }

    final LineFileDocs docs = new LineFileDocs(random);
    final File tempDir = _TestUtil.getTempDir("nrtopenfiles");
    final MockDirectoryWrapper _dir = newFSDirectory(tempDir);
    _dir.setCheckIndexOnClose(false);  // don't double-checkIndex, we do it ourselves
    Directory dir = _dir;
    final IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    if (LuceneTestCase.TEST_NIGHTLY) {
      // newIWConfig makes smallish max seg size, which
      // results in tons and tons of segments for this test
      // when run nightly:
      MergePolicy mp = conf.getMergePolicy();
      if (mp instanceof TieredMergePolicy) {
        ((TieredMergePolicy) mp).setMaxMergedSegmentMB(5000.);
      } else if (mp instanceof LogByteSizeMergePolicy) {
        ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1000.);
      } else if (mp instanceof LogMergePolicy) {
        ((LogMergePolicy) mp).setMaxMergeDocs(100000);
      }
    }

    conf.setMergedSegmentWarmer(new IndexWriter.IndexReaderWarmer() {
      @Override
      public void warm(IndexReader reader) throws IOException {
        if (VERBOSE) {
          System.out.println("TEST: now warm merged reader=" + reader);
        }
        final int maxDoc = reader.maxDoc();
        final Bits delDocs = reader.getDeletedDocs();
        int sum = 0;
        final int inc = Math.max(1, maxDoc/50);
        for(int docID=0;docID<maxDoc;docID += inc) {
          if (delDocs == null || !delDocs.get(docID)) {
            final Document doc = reader.document(docID);
            sum += doc.getFields().size();
          }
        }

        IndexSearcher searcher = newSearcher(reader);
        sum += searcher.search(new TermQuery(new Term("body", "united")), 10).totalHits;
        searcher.close();

        if (VERBOSE) {
          System.out.println("TEST: warm visited " + sum + " fields");
        }
      }
      });

    if (random.nextBoolean()) {
      if (VERBOSE) {
        System.out.println("TEST: wrap NRTCachingDir");
      }

      NRTCachingDirectory nrtDir = new NRTCachingDirectory(dir, 5.0, 60.0);
      conf.setMergeScheduler(nrtDir.getMergeScheduler());
      dir = nrtDir;
    }
    
    final IndexWriter writer = new IndexWriter(dir, conf);
    
    if (VERBOSE) {
      writer.setInfoStream(System.out);
    }
    _TestUtil.reduceOpenFiles(writer);
    //System.out.println("TEST: conf=" + writer.getConfig());

    final ExecutorService es = random.nextBoolean() ? null : Executors.newCachedThreadPool(new NamedThreadFactory("NRT search threads"));

    final double minReopenSec = 0.01 + 0.05 * random.nextDouble();
    final double maxReopenSec = minReopenSec * (1.0 + 10 * random.nextDouble());

    if (VERBOSE) {
      System.out.println("TEST: make NRTManager maxReopenSec=" + maxReopenSec + " minReopenSec=" + minReopenSec);
    }

    final NRTManager nrt = new NRTManager(writer, es);
    final NRTManagerReopenThread nrtThread = new NRTManagerReopenThread(nrt, maxReopenSec, minReopenSec);
    nrtThread.setName("NRT Reopen Thread");
    nrtThread.setPriority(Math.min(Thread.currentThread().getPriority()+2, Thread.MAX_PRIORITY));
    nrtThread.setDaemon(true);
    nrtThread.start();

    final int NUM_INDEX_THREADS = _TestUtil.nextInt(random, 1, 3);
    final int NUM_SEARCH_THREADS = _TestUtil.nextInt(random, 1, 3);
    //final int NUM_INDEX_THREADS = 1;
    //final int NUM_SEARCH_THREADS = 1;
    if (VERBOSE) {
      System.out.println("TEST: " + NUM_INDEX_THREADS + " index threads; " + NUM_SEARCH_THREADS + " search threads");
    }

    final int RUN_TIME_SEC = LuceneTestCase.TEST_NIGHTLY ? 300 : RANDOM_MULTIPLIER;

    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicInteger addCount = new AtomicInteger();
    final AtomicInteger delCount = new AtomicInteger();
    final AtomicInteger packCount = new AtomicInteger();
    final List<Long> lastGens = new ArrayList<Long>();

    final Set<String> delIDs = Collections.synchronizedSet(new HashSet<String>());
    final List<SubDocs> allSubDocs = Collections.synchronizedList(new ArrayList<SubDocs>());

    final long stopTime = System.currentTimeMillis() + RUN_TIME_SEC*1000;
    Thread[] threads = new Thread[NUM_INDEX_THREADS];
    for(int thread=0;thread<NUM_INDEX_THREADS;thread++) {
      threads[thread] = new Thread() {
          @Override
          public void run() {
            // TODO: would be better if this were cross thread, so that we make sure one thread deleting anothers added docs works:
            final List<String> toDeleteIDs = new ArrayList<String>();
            final List<SubDocs> toDeleteSubDocs = new ArrayList<SubDocs>();

            long gen = 0;
            while(System.currentTimeMillis() < stopTime && !failed.get()) {

              //System.out.println(Thread.currentThread().getName() + ": cycle");
              try {
                // Occassional longish pause if running
                // nightly
                if (LuceneTestCase.TEST_NIGHTLY && random.nextInt(6) == 3) {
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": now long sleep");
                  }
                  Thread.sleep(_TestUtil.nextInt(random, 50, 500));
                }

                // Rate limit ingest rate:
                Thread.sleep(_TestUtil.nextInt(random, 1, 10));
                if (VERBOSE) {
                  System.out.println(Thread.currentThread() + ": done sleep");
                }

                Document doc = docs.nextDoc();
                if (doc == null) {
                  break;
                }
                final String addedField;
                if (random.nextBoolean()) {
                  addedField = "extra" + random.nextInt(10);
                  doc.add(new Field(addedField, "a random field", Field.Store.NO, Field.Index.ANALYZED));
                } else {
                  addedField = null;
                }
                if (random.nextBoolean()) {

                  if (random.nextBoolean()) {
                    // Add a pack of adjacent sub-docs
                    final String packID;
                    final SubDocs delSubDocs;
                    if (toDeleteSubDocs.size() > 0 && random.nextBoolean()) {
                      delSubDocs = toDeleteSubDocs.get(random.nextInt(toDeleteSubDocs.size()));
                      assert !delSubDocs.deleted;
                      toDeleteSubDocs.remove(delSubDocs);
                      // reuse prior packID
                      packID = delSubDocs.packID;
                    } else {
                      delSubDocs = null;
                      // make new packID
                      packID = packCount.getAndIncrement() + "";
                    }

                    final Field packIDField = newField("packID", packID, Field.Store.YES, Field.Index.NOT_ANALYZED);
                    final List<String> docIDs = new ArrayList<String>();
                    final SubDocs subDocs = new SubDocs(packID, docIDs);
                    final List<Document> docsList = new ArrayList<Document>();

                    allSubDocs.add(subDocs);
                    doc.add(packIDField);
                    docsList.add(cloneDoc(doc));
                    docIDs.add(doc.get("docid"));

                    final int maxDocCount = _TestUtil.nextInt(random, 1, 10);
                    while(docsList.size() < maxDocCount) {
                      doc = docs.nextDoc();
                      if (doc == null) {
                        break;
                      }
                      docsList.add(cloneDoc(doc));
                      docIDs.add(doc.get("docid"));
                    }
                    addCount.addAndGet(docsList.size());

                    if (delSubDocs != null) {
                      delSubDocs.deleted = true;
                      delIDs.addAll(delSubDocs.subIDs);
                      delCount.addAndGet(delSubDocs.subIDs.size());
                      if (VERBOSE) {
                        System.out.println("TEST: update pack packID=" + delSubDocs.packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      gen = nrt.updateDocuments(new Term("packID", delSubDocs.packID), docsList);
                      /*
                      // non-atomic:
                      nrt.deleteDocuments(new Term("packID", delSubDocs.packID));
                      for(Document subDoc : docsList) {
                        nrt.addDocument(subDoc);
                      }
                      */
                    } else {
                      if (VERBOSE) {
                        System.out.println("TEST: add pack packID=" + packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      gen = nrt.addDocuments(docsList);
                      
                      /*
                      // non-atomic:
                      for(Document subDoc : docsList) {
                        nrt.addDocument(subDoc);
                      }
                      */
                    }
                    doc.removeField("packID");

                    if (random.nextInt(5) == 2) {
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": buffer del id:" + packID);
                      }
                      toDeleteSubDocs.add(subDocs);
                    }

                    // randomly verify the add/update "took":
                    if (random.nextInt(20) == 2) {
                      final boolean applyDeletes = delSubDocs != null;
                      final IndexSearcher s = nrt.get(gen, applyDeletes);
                      try {
                        assertEquals(docsList.size(), s.search(new TermQuery(new Term("packID", packID)), 10).totalHits);
                      } finally {
                        nrt.release(s);
                      }
                    }

                  } else {
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": add doc docid:" + doc.get("docid"));
                    }

                    gen = nrt.addDocument(doc);
                    addCount.getAndIncrement();

                    // randomly verify the add "took":
                    if (random.nextInt(20) == 2) {
                      //System.out.println(Thread.currentThread().getName() + ": verify");
                      final IndexSearcher s = nrt.get(gen, false);
                      //System.out.println(Thread.currentThread().getName() + ": got s=" + s);
                      try {
                        assertEquals(1, s.search(new TermQuery(new Term("docid", doc.get("docid"))), 10).totalHits);
                      } finally {
                        nrt.release(s);
                      }
                      //System.out.println(Thread.currentThread().getName() + ": done verify");
                    }

                    if (random.nextInt(5) == 3) {
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                      }
                      toDeleteIDs.add(doc.get("docid"));
                    }
                  }
                } else {
                  // we use update but it never replaces a
                  // prior doc
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": update doc id:" + doc.get("docid"));
                  }
                  gen = nrt.updateDocument(new Term("docid", doc.get("docid")), doc);
                  addCount.getAndIncrement();

                  // randomly verify the add "took":
                  if (random.nextInt(20) == 2) {
                    final IndexSearcher s = nrt.get(gen, true);
                    try {
                      assertEquals(1, s.search(new TermQuery(new Term("docid", doc.get("docid"))), 10).totalHits);
                    } finally {
                      nrt.release(s);
                    }
                  }

                  if (random.nextInt(5) == 3) {
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                    }
                    toDeleteIDs.add(doc.get("docid"));
                  }
                }

                if (random.nextInt(30) == 17) {
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": apply " + toDeleteIDs.size() + " deletes");
                  }
                  for(String id : toDeleteIDs) {
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": del term=id:" + id);
                    }
                    gen = nrt.deleteDocuments(new Term("docid", id));

                    // randomly verify the delete "took":
                    if (random.nextInt(20) == 7) {
                      final IndexSearcher s = nrt.get(gen, true);
                      try {
                        assertEquals(0, s.search(new TermQuery(new Term("docid", id)), 10).totalHits);
                      } finally {
                        nrt.release(s);
                      }
                    }
                  }

                  final int count = delCount.addAndGet(toDeleteIDs.size());
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": tot " + count + " deletes");
                  }
                  delIDs.addAll(toDeleteIDs);
                  toDeleteIDs.clear();

                  for(SubDocs subDocs : toDeleteSubDocs) {
                    assertTrue(!subDocs.deleted);
                    gen = nrt.deleteDocuments(new Term("packID", subDocs.packID));
                    subDocs.deleted = true;
                    if (VERBOSE) {
                      System.out.println("  del subs: " + subDocs.subIDs + " packID=" + subDocs.packID);
                    }
                    delIDs.addAll(subDocs.subIDs);
                    delCount.addAndGet(subDocs.subIDs.size());

                    // randomly verify the delete "took":
                    if (random.nextInt(20) == 7) {
                      final IndexSearcher s = nrt.get(gen, true);
                      try {
                        assertEquals(0, s.search(new TermQuery(new Term("packID", subDocs.packID)), 1).totalHits);
                      } finally {
                        nrt.release(s);
                      }
                    }
                  }
                  toDeleteSubDocs.clear();
                }
                if (addedField != null) {
                  doc.removeField(addedField);
                }
              } catch (Throwable t) {
                System.out.println(Thread.currentThread().getName() + ": FAILED: hit exc");
                t.printStackTrace();
                failed.set(true);
                throw new RuntimeException(t);
              }
            }

            lastGens.add(gen);
            if (VERBOSE) {
              System.out.println(Thread.currentThread().getName() + ": indexing done");
            }
          }
        };
      threads[thread].setDaemon(true);
      threads[thread].start();
    }

    if (VERBOSE) {
      System.out.println("TEST: DONE start indexing threads [" + (System.currentTimeMillis()-t0) + " ms]");
    }

    // let index build up a bit
    Thread.sleep(100);

    // silly starting guess:
    final AtomicInteger totTermCount = new AtomicInteger(100);

    // run search threads
    final Thread[] searchThreads = new Thread[NUM_SEARCH_THREADS];
    final AtomicInteger totHits = new AtomicInteger();

    if (VERBOSE) {
      System.out.println("TEST: start search threads");
    }

    for(int thread=0;thread<NUM_SEARCH_THREADS;thread++) {
      searchThreads[thread] = new Thread() {
          @Override
          public void run() {
            while(System.currentTimeMillis() < stopTime && !failed.get()) {
              final IndexSearcher s = nrt.get(random.nextBoolean());
              try {
                try {
                  smokeTestSearcher(s);
                  if (s.getIndexReader().numDocs() > 0) {
                    Fields fields = MultiFields.getFields(s.getIndexReader());
                    if (fields == null) {
                      continue;
                    }
                    Terms terms = fields.terms("body");
                    if (terms == null) {
                      continue;
                    }

                    TermsEnum termsEnum = terms.iterator();
                    int seenTermCount = 0;
                    int shift;
                    int trigger;
                    if (totTermCount.get() < 10) {
                      shift = 0;
                      trigger = 1;
                    } else {
                      trigger = totTermCount.get()/10;
                      shift = random.nextInt(trigger);
                    }

                    while(System.currentTimeMillis() < stopTime) {
                      BytesRef term = termsEnum.next();
                      if (term == null) {
                        if (seenTermCount == 0) {
                          break;
                        }
                        totTermCount.set(seenTermCount);
                        seenTermCount = 0;
                        if (totTermCount.get() < 10) {
                          shift = 0;
                          trigger = 1;
                        } else {
                          trigger = totTermCount.get()/10;
                          //System.out.println("trigger " + trigger);
                          shift = random.nextInt(trigger);
                        }
                        termsEnum.seek(new BytesRef(""));
                        continue;
                      }
                      seenTermCount++;
                      // search 10 terms
                      if (trigger == 0) {
                        trigger = 1;
                      }
                      if ((seenTermCount + shift) % trigger == 0) {
                        //if (VERBOSE) {
                        //System.out.println(Thread.currentThread().getName() + " now search body:" + term.utf8ToString());
                        //}
                        totHits.addAndGet(runQuery(s, new TermQuery(new Term("body", term))));
                      }
                    }
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": search done");
                    }
                  }
                } finally {
                  nrt.release(s);
                }
              } catch (Throwable t) {
                System.out.println(Thread.currentThread().getName() + ": FAILED: hit exc");
                failed.set(true);
                t.printStackTrace(System.out);
                throw new RuntimeException(t);
              }
            }
          }
        };
      searchThreads[thread].setDaemon(true);
      searchThreads[thread].start();
    }

    if (VERBOSE) {
      System.out.println("TEST: now join");
    }
    for(int thread=0;thread<NUM_INDEX_THREADS;thread++) {
      threads[thread].join();
    }
    for(int thread=0;thread<NUM_SEARCH_THREADS;thread++) {
      searchThreads[thread].join();
    }

    if (VERBOSE) {
      System.out.println("TEST: done join [" + (System.currentTimeMillis()-t0) + " ms]; addCount=" + addCount + " delCount=" + delCount);
      System.out.println("TEST: search totHits=" + totHits);
    }

    long maxGen = 0;
    for(long gen : lastGens) {
      maxGen = Math.max(maxGen, gen);
    }

    final IndexSearcher s = nrt.get(maxGen, true);

    boolean doFail = false;
    for(String id : delIDs) {
      final TopDocs hits = s.search(new TermQuery(new Term("docid", id)), 1);
      if (hits.totalHits != 0) {
        System.out.println("doc id=" + id + " is supposed to be deleted, but got docID=" + hits.scoreDocs[0].doc);
        doFail = true;
      }
    }

    // Make sure each group of sub-docs are still in docID order:
    for(SubDocs subDocs : allSubDocs) {
      if (!subDocs.deleted) {
        // We sort by relevance but the scores should be identical so sort falls back to by docID:
        TopDocs hits = s.search(new TermQuery(new Term("packID", subDocs.packID)), 20);
        assertEquals(subDocs.subIDs.size(), hits.totalHits);
        int lastDocID = -1;
        int startDocID = -1;
        for(ScoreDoc scoreDoc : hits.scoreDocs) {
          final int docID = scoreDoc.doc;
          if (lastDocID != -1) {
            assertEquals(1+lastDocID, docID);
          } else {
            startDocID = docID;
          }
          lastDocID = docID;
          final Document doc = s.doc(docID);
          assertEquals(subDocs.packID, doc.get("packID"));
        }

        lastDocID = startDocID - 1;
        for(String subID : subDocs.subIDs) {
          hits = s.search(new TermQuery(new Term("docid", subID)), 1);
          assertEquals(1, hits.totalHits);
          final int docID = hits.scoreDocs[0].doc;
          if (lastDocID != -1) {
            assertEquals(1+lastDocID, docID);
          }
          lastDocID = docID;
        }          
      } else {
        for(String subID : subDocs.subIDs) {
          assertEquals(0, s.search(new TermQuery(new Term("docid", subID)), 1).totalHits);
        }
      }
    }
    
    final int endID = Integer.parseInt(docs.nextDoc().get("docid"));
    for(int id=0;id<endID;id++) {
      String stringID = ""+id;
      if (!delIDs.contains(stringID)) {
        final TopDocs hits = s.search(new TermQuery(new Term("docid", stringID)), 1);
        if (hits.totalHits != 1) {
          System.out.println("doc id=" + stringID + " is not supposed to be deleted, but got hitCount=" + hits.totalHits);
          doFail = true;
        }
      }
    }
    assertFalse(doFail);

    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), s.getIndexReader().numDocs());
    nrt.release(s);

    if (es != null) {
      es.shutdown();
      es.awaitTermination(1, TimeUnit.SECONDS);
    }

    writer.commit();
    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), writer.numDocs());

    if (VERBOSE) {
      System.out.println("TEST: now close NRTManager");
    }
    nrtThread.close();
    nrt.close();
    assertFalse(writer.anyNonBulkMerges);
    writer.close(false);
    _TestUtil.checkIndex(dir);
    dir.close();
    _TestUtil.rmDir(tempDir);
    docs.close();

    if (VERBOSE) {
      System.out.println("TEST: done [" + (System.currentTimeMillis()-t0) + " ms]");
    }
  }

  private int runQuery(IndexSearcher s, Query q) throws Exception {
    s.search(q, 10);
    return s.search(q, null, 10, new Sort(new SortField("title", SortField.STRING))).totalHits;
  }

  private void smokeTestSearcher(IndexSearcher s) throws Exception {
    runQuery(s, new TermQuery(new Term("body", "united")));
    runQuery(s, new TermQuery(new Term("titleTokenized", "states")));
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term("body", "united"));
    pq.add(new Term("body", "states"));
    runQuery(s, pq);
  }
}
