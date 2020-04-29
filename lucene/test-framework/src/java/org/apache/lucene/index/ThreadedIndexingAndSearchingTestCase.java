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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FailOnNonBulkMergesInfoStream;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TestUtil;

// TODO
//   - mix in forceMerge, addIndexes
//   - randomly mix in non-congruent docs

/** Utility class that spawns multiple indexing and
 *  searching threads. */
public abstract class ThreadedIndexingAndSearchingTestCase extends LuceneTestCase {

  protected final AtomicBoolean failed = new AtomicBoolean();
  protected final AtomicInteger addCount = new AtomicInteger();
  protected final AtomicInteger delCount = new AtomicInteger();
  protected final AtomicInteger packCount = new AtomicInteger();

  protected Directory dir;
  protected IndexWriter writer;

  private static class SubDocs {
    public final String packID;
    public final List<String> subIDs;
    public boolean deleted;

    public SubDocs(String packID, List<String> subIDs) {
      this.packID = packID;
      this.subIDs = subIDs;
    }
  }

  // Called per-search
  protected abstract IndexSearcher getCurrentSearcher() throws Exception;

  protected abstract IndexSearcher getFinalSearcher() throws Exception;

  protected void releaseSearcher(IndexSearcher s) throws Exception {
  }

  // Called once to run searching
  protected abstract void doSearching(ExecutorService es, long stopTime) throws Exception;

  protected Directory getDirectory(Directory in) {
    return in;
  }

  protected void updateDocuments(Term id, List<? extends Iterable<? extends IndexableField>> docs) throws Exception {
    writer.updateDocuments(id, docs);
  }

  protected void addDocuments(Term id, List<? extends Iterable<? extends IndexableField>> docs) throws Exception {
    writer.addDocuments(docs);
  }

  protected void addDocument(Term id, Iterable<? extends IndexableField> doc) throws Exception {
    writer.addDocument(doc);
  }

  protected void updateDocument(Term term, Iterable<? extends IndexableField> doc) throws Exception {
    writer.updateDocument(term, doc);
  }

  protected void deleteDocuments(Term term) throws Exception {
    writer.deleteDocuments(term);
  }

  protected void doAfterIndexingThreadDone() {
  }

  private Thread[] launchIndexingThreads(final LineFileDocs docs,
                                         int numThreads,
                                         final long stopTime,
                                         final Set<String> delIDs,
                                         final Set<String> delPackIDs,
                                         final List<SubDocs> allSubDocs) {
    final Thread[] threads = new Thread[numThreads];
    for(int thread=0;thread<numThreads;thread++) {
      threads[thread] = new Thread() {
          @Override
          public void run() {
            // TODO: would be better if this were cross thread, so that we make sure one thread deleting anothers added docs works:
            final List<String> toDeleteIDs = new ArrayList<>();
            final List<SubDocs> toDeleteSubDocs = new ArrayList<>();
            while(System.currentTimeMillis() < stopTime && !failed.get()) {
              try {

                // Occasional longish pause if running
                // nightly
                if (LuceneTestCase.TEST_NIGHTLY && random().nextInt(6) == 3) {
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": now long sleep");
                  }
                  Thread.sleep(TestUtil.nextInt(random(), 50, 500));
                }

                // Rate limit ingest rate:
                if (random().nextInt(7) == 5) {
                  Thread.sleep(TestUtil.nextInt(random(), 1, 10));
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": done sleep");
                  }
                }

                Document doc = docs.nextDoc();
                if (doc == null) {
                  break;
                }

                // Maybe add randomly named field
                final String addedField;
                if (random().nextBoolean()) {
                  addedField = "extra" + random().nextInt(40);
                  doc.add(newTextField(addedField, "a random field", Field.Store.YES));
                } else {
                  addedField = null;
                }

                if (random().nextBoolean()) {

                  if (random().nextBoolean()) {
                    // Add/update doc block:
                    final String packID;
                    final SubDocs delSubDocs;
                    if (toDeleteSubDocs.size() > 0 && random().nextBoolean()) {
                      delSubDocs = toDeleteSubDocs.get(random().nextInt(toDeleteSubDocs.size()));
                      assert !delSubDocs.deleted;
                      toDeleteSubDocs.remove(delSubDocs);
                      // Update doc block, replacing prior packID
                      packID = delSubDocs.packID;
                    } else {
                      delSubDocs = null;
                      // Add doc block, using new packID
                      packID = packCount.getAndIncrement() + "";
                    }

                    final Field packIDField = newStringField("packID", packID, Field.Store.YES);
                    final List<String> docIDs = new ArrayList<>();
                    final SubDocs subDocs = new SubDocs(packID, docIDs);
                    final List<Document> docsList = new ArrayList<>();

                    allSubDocs.add(subDocs);
                    doc.add(packIDField);
                    docsList.add(TestUtil.cloneDocument(doc));
                    docIDs.add(doc.get("docid"));

                    final int maxDocCount = TestUtil.nextInt(random(), 1, 10);
                    while(docsList.size() < maxDocCount) {
                      doc = docs.nextDoc();
                      if (doc == null) {
                        break;
                      }
                      docsList.add(TestUtil.cloneDocument(doc));
                      docIDs.add(doc.get("docid"));
                    }
                    addCount.addAndGet(docsList.size());

                    final Term packIDTerm = new Term("packID", packID);

                    if (delSubDocs != null) {
                      delSubDocs.deleted = true;
                      delIDs.addAll(delSubDocs.subIDs);
                      delCount.addAndGet(delSubDocs.subIDs.size());
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": update pack packID=" + delSubDocs.packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      updateDocuments(packIDTerm, docsList);
                    } else {
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": add pack packID=" + packID + " count=" + docsList.size() + " docs=" + docIDs);
                      }
                      addDocuments(packIDTerm, docsList);
                    }
                    doc.removeField("packID");

                    if (random().nextInt(5) == 2) {
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": buffer del id:" + packID);
                      }
                      toDeleteSubDocs.add(subDocs);
                    }

                  } else {
                    // Add single doc
                    final String docid = doc.get("docid");
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": add doc docid:" + docid);
                    }
                    addDocument(new Term("docid", docid), doc);
                    addCount.getAndIncrement();

                    if (random().nextInt(5) == 3) {
                      if (VERBOSE) {
                        System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                      }
                      toDeleteIDs.add(docid);
                    }
                  }
                } else {

                  // Update single doc, but we never re-use
                  // and ID so the delete will never
                  // actually happen:
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": update doc id:" + doc.get("docid"));
                  }
                  final String docid = doc.get("docid");
                  updateDocument(new Term("docid", docid), doc);
                  addCount.getAndIncrement();

                  if (random().nextInt(5) == 3) {
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": buffer del id:" + doc.get("docid"));
                    }
                    toDeleteIDs.add(docid);
                  }
                }

                if (random().nextInt(30) == 17) {
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": apply " + toDeleteIDs.size() + " deletes");
                  }
                  for(String id : toDeleteIDs) {
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": del term=id:" + id);
                    }
                    deleteDocuments(new Term("docid", id));
                  }
                  final int count = delCount.addAndGet(toDeleteIDs.size());
                  if (VERBOSE) {
                    System.out.println(Thread.currentThread().getName() + ": tot " + count + " deletes");
                  }
                  delIDs.addAll(toDeleteIDs);
                  toDeleteIDs.clear();

                  for(SubDocs subDocs : toDeleteSubDocs) {
                    assert !subDocs.deleted;
                    delPackIDs.add(subDocs.packID);
                    deleteDocuments(new Term("packID", subDocs.packID));
                    subDocs.deleted = true;
                    if (VERBOSE) {
                      System.out.println(Thread.currentThread().getName() + ": del subs: " + subDocs.subIDs + " packID=" + subDocs.packID);
                    }
                    delIDs.addAll(subDocs.subIDs);
                    delCount.addAndGet(subDocs.subIDs.size());
                  }
                  toDeleteSubDocs.clear();
                }
                if (addedField != null) {
                  doc.removeField(addedField);
                }
              } catch (Throwable t) {
                System.out.println(Thread.currentThread().getName() + ": hit exc");
                t.printStackTrace();
                failed.set(true);
                throw new RuntimeException(t);
              }
            }
            if (VERBOSE) {
              System.out.println(Thread.currentThread().getName() + ": indexing done");
            }

            doAfterIndexingThreadDone();
          }
        };
      threads[thread].start();
    }

    return threads;
  }

  protected void runSearchThreads(final long stopTimeMS) throws Exception {
    final int numThreads = TEST_NIGHTLY ? TestUtil.nextInt(random(), 1, 5) : 2;
    final Thread[] searchThreads = new Thread[numThreads];
    final AtomicLong totHits = new AtomicLong();

    // silly starting guess:
    final AtomicInteger totTermCount = new AtomicInteger(100);

    // TODO: we should enrich this to do more interesting searches
    for(int thread=0;thread<searchThreads.length;thread++) {
      searchThreads[thread] = new Thread() {
          @Override
          public void run() {
            if (VERBOSE) {
              System.out.println(Thread.currentThread().getName() + ": launch search thread");
            }
            while (System.currentTimeMillis() < stopTimeMS && !failed.get()) {
              try {
                final IndexSearcher s = getCurrentSearcher();
                try {
                  // Verify 1) IW is correctly setting
                  // diagnostics, and 2) segment warming for
                  // merged segments is actually happening:
                  for(final LeafReaderContext sub : s.getIndexReader().leaves()) {
                    SegmentReader segReader = (SegmentReader) sub.reader();
                    Map<String,String> diagnostics = segReader.getSegmentInfo().info.getDiagnostics();
                    assertNotNull(diagnostics);
                    String source = diagnostics.get("source");
                    assertNotNull(source);
                    if (source.equals("merge")) {
                      assertTrue("sub reader " + sub + " wasn't warmed: warmed=" + warmed + " diagnostics=" + diagnostics + " si=" + segReader.getSegmentInfo(),
                                 !assertMergedSegmentsWarmed || warmed.containsKey(segReader.core));
                    }
                  }
                  if (s.getIndexReader().numDocs() > 0) {
                    smokeTestSearcher(s);
                    Terms terms = MultiTerms.getTerms(s.getIndexReader(), "body");
                    if (terms == null) {
                      continue;
                    }
                    TermsEnum termsEnum = terms.iterator();
                    int seenTermCount = 0;
                    int shift;
                    int trigger; 
                    if (totTermCount.get() < 30) {
                      shift = 0;
                      trigger = 1;
                    } else {
                      trigger = totTermCount.get()/30;
                      shift = random().nextInt(trigger);
                    }
                    while (System.currentTimeMillis() < stopTimeMS) {
                      BytesRef term = termsEnum.next();
                      if (term == null) {
                        totTermCount.set(seenTermCount);
                        break;
                      }
                      seenTermCount++;
                      // search 30 terms
                      if ((seenTermCount + shift) % trigger == 0) {
                        //if (VERBOSE) {
                        //System.out.println(Thread.currentThread().getName() + " now search body:" + term.utf8ToString());
                        //}
                        totHits.addAndGet(runQuery(s, new TermQuery(new Term("body", BytesRef.deepCopyOf(term)))));
                      }
                    }
                    //if (VERBOSE) {
                    //System.out.println(Thread.currentThread().getName() + ": search done");
                    //}
                  }
                } finally {
                  releaseSearcher(s);
                }
              } catch (Throwable t) {
                System.out.println(Thread.currentThread().getName() + ": hit exc");
                failed.set(true);
                t.printStackTrace(System.out);
                throw new RuntimeException(t);
              }
            }
          }
        };
      searchThreads[thread].start();
    }

    for(Thread thread : searchThreads) {
      thread.join();
    }

    if (VERBOSE) {
      System.out.println("TEST: DONE search: totHits=" + totHits);
    }
  }

  protected void doAfterWriter(ExecutorService es) throws Exception {
  }

  protected void doClose() throws Exception {
  }

  protected boolean assertMergedSegmentsWarmed = true;

  private final Map<SegmentCoreReaders,Boolean> warmed = Collections.synchronizedMap(new WeakHashMap<SegmentCoreReaders,Boolean>());

  public void runTest(String testName) throws Exception {

    failed.set(false);
    addCount.set(0);
    delCount.set(0);
    packCount.set(0);

    final long t0 = System.currentTimeMillis();

    Random random = new Random(random().nextLong());
    final LineFileDocs docs = new LineFileDocs(random);
    final Path tempDir = createTempDir(testName);
    dir = getDirectory(newMockFSDirectory(tempDir)); // some subclasses rely on this being MDW
    if (dir instanceof BaseDirectoryWrapper) {
      ((BaseDirectoryWrapper) dir).setCheckIndexOnClose(false); // don't double-checkIndex, we do it ourselves.
    }
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    final IndexWriterConfig conf = newIndexWriterConfig(analyzer).setCommitOnClose(false);
    conf.setInfoStream(new FailOnNonBulkMergesInfoStream());
    if (conf.getMergePolicy() instanceof MockRandomMergePolicy) {
      ((MockRandomMergePolicy)conf.getMergePolicy()).setDoNonBulkMerges(false);
    }

    ensureSaneIWCOnNightly(conf);

    conf.setMergedSegmentWarmer((reader) -> {
      if (VERBOSE) {
        System.out.println("TEST: now warm merged reader=" + reader);
      }
      warmed.put(((SegmentReader) reader).core, Boolean.TRUE);
      final int maxDoc = reader.maxDoc();
      final Bits liveDocs = reader.getLiveDocs();
      int sum = 0;
      final int inc = Math.max(1, maxDoc/50);
      for(int docID=0;docID<maxDoc;docID += inc) {
        if (liveDocs == null || liveDocs.get(docID)) {
          final Document doc = reader.document(docID);
          sum += doc.getFields().size();
        }
      }

      IndexSearcher searcher = newSearcher(reader, false);
      sum += searcher.search(new TermQuery(new Term("body", "united")), 10).totalHits.value;

      if (VERBOSE) {
        System.out.println("TEST: warm visited " + sum + " fields");
      }
    });

    if (VERBOSE) {
      conf.setInfoStream(new PrintStreamInfoStream(System.out) {
          @Override
          public void message(String component, String message) {
            if ("TP".equals(component)) {
              return; // ignore test points!
            }
            super.message(component, message);
          }
        });
    }
    writer = new IndexWriter(dir, conf);
    TestUtil.reduceOpenFiles(writer);

    final ExecutorService es = random().nextBoolean() ? null : Executors.newCachedThreadPool(new NamedThreadFactory(testName));

    doAfterWriter(es);

    final int NUM_INDEX_THREADS = TestUtil.nextInt(random(), 2, 4);

    final int RUN_TIME_MSEC = LuceneTestCase.TEST_NIGHTLY ? 300000 : 100 * RANDOM_MULTIPLIER;

    final Set<String> delIDs = Collections.synchronizedSet(new HashSet<String>());
    final Set<String> delPackIDs = Collections.synchronizedSet(new HashSet<String>());
    final List<SubDocs> allSubDocs = Collections.synchronizedList(new ArrayList<SubDocs>());

    final long stopTime = System.currentTimeMillis() + RUN_TIME_MSEC;

    final Thread[] indexThreads = launchIndexingThreads(docs, NUM_INDEX_THREADS, stopTime, delIDs, delPackIDs, allSubDocs);

    if (VERBOSE) {
      System.out.println("TEST: DONE start " + NUM_INDEX_THREADS + " indexing threads [" + (System.currentTimeMillis()-t0) + " ms]");
    }

    // Let index build up a bit
    Thread.sleep(100);

    doSearching(es, stopTime);

    if (VERBOSE) {
      System.out.println("TEST: all searching done [" + (System.currentTimeMillis()-t0) + " ms]");
    }
    
    for(Thread thread : indexThreads) {
      thread.join();
    }

    if (VERBOSE) {
      System.out.println("TEST: done join indexing threads [" + (System.currentTimeMillis()-t0) + " ms]; addCount=" + addCount + " delCount=" + delCount);
    }

    final IndexSearcher s = getFinalSearcher();
    if (VERBOSE) {
      System.out.println("TEST: finalSearcher=" + s);
    }

    assertFalse(failed.get());

    boolean doFail = false;

    // Verify: make sure delIDs are in fact deleted:
    for(String id : delIDs) {
      final TopDocs hits = s.search(new TermQuery(new Term("docid", id)), 1);
      if (hits.totalHits.value != 0) {
        System.out.println("doc id=" + id + " is supposed to be deleted, but got " + hits.totalHits.value + " hits; first docID=" + hits.scoreDocs[0].doc);
        doFail = true;
      }
    }

    // Verify: make sure delPackIDs are in fact deleted:
    for(String id : delPackIDs) {
      final TopDocs hits = s.search(new TermQuery(new Term("packID", id)), 1);
      if (hits.totalHits.value != 0) {
        System.out.println("packID=" + id + " is supposed to be deleted, but got " + hits.totalHits.value + " matches");
        doFail = true;
      }
    }

    // Verify: make sure each group of sub-docs are still in docID order:
    for(SubDocs subDocs : allSubDocs) {
      TopDocs hits = s.search(new TermQuery(new Term("packID", subDocs.packID)), 20);
      if (!subDocs.deleted) {
        // We sort by relevance but the scores should be identical so sort falls back to by docID:
        if (hits.totalHits.value != subDocs.subIDs.size()) {
          System.out.println("packID=" + subDocs.packID + ": expected " + subDocs.subIDs.size() + " hits but got " + hits.totalHits.value);
          doFail = true;
        } else {
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
            assertEquals(1, hits.totalHits.value);
            final int docID = hits.scoreDocs[0].doc;
            if (lastDocID != -1) {
              assertEquals(1+lastDocID, docID);
            }
            lastDocID = docID;
          }
        }
      } else {
        // Pack was deleted -- make sure its docs are
        // deleted.  We can't verify packID is deleted
        // because we can re-use packID for update:
        for(String subID : subDocs.subIDs) {
          assertEquals(0, s.search(new TermQuery(new Term("docid", subID)), 1).totalHits.value);
        }
      }
    }

    // Verify: make sure all not-deleted docs are in fact
    // not deleted:
    final int endID = Integer.parseInt(docs.nextDoc().get("docid"));
    docs.close();

    for(int id=0;id<endID;id++) {
      String stringID = ""+id;
      if (!delIDs.contains(stringID)) {
        final TopDocs hits = s.search(new TermQuery(new Term("docid", stringID)), 1);
        if (hits.totalHits.value != 1) {
          System.out.println("doc id=" + stringID + " is not supposed to be deleted, but got hitCount=" + hits.totalHits.value + "; delIDs=" + delIDs);
          doFail = true;
        }
      }
    }
    assertFalse(doFail);
    
    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), s.getIndexReader().numDocs());
    releaseSearcher(s);

    writer.commit();

    assertEquals("index=" + writer.segString() + " addCount=" + addCount + " delCount=" + delCount, addCount.get() - delCount.get(), writer.getDocStats().numDocs);

    doClose();

    try {
      writer.commit();
    } finally {
      writer.close();
    }

    // Cannot close until after writer is closed because
    // writer has merged segment warmer that uses IS to run
    // searches, and that IS may be using this es!
    if (es != null) {
      es.shutdown();
      es.awaitTermination(1, TimeUnit.SECONDS);
    }

    TestUtil.checkIndex(dir);
    dir.close();

    if (VERBOSE) {
      System.out.println("TEST: done [" + (System.currentTimeMillis()-t0) + " ms]");
    }
  }

  private long runQuery(IndexSearcher s, Query q) throws Exception {
    s.search(q, 10);
    long hitCount = s.search(q, 10, new Sort(new SortField("titleDV", SortField.Type.STRING))).totalHits.value;
    final Sort dvSort = new Sort(new SortField("titleDV", SortField.Type.STRING));
    long hitCount2 = s.search(q, 10, dvSort).totalHits.value;
    assertEquals(hitCount, hitCount2);
    return hitCount;
  }

  protected void smokeTestSearcher(IndexSearcher s) throws Exception {
    runQuery(s, new TermQuery(new Term("body", "united")));
    runQuery(s, new TermQuery(new Term("titleTokenized", "states")));
    PhraseQuery pq = new PhraseQuery("body", "united", "states");
    runQuery(s, pq);
  }
}
