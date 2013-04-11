package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestStressNRT extends LuceneTestCase {
  volatile DirectoryReader reader;

  final ConcurrentHashMap<Integer,Long> model = new ConcurrentHashMap<Integer,Long>();
  Map<Integer,Long> committedModel = new HashMap<Integer,Long>();
  long snapshotCount;
  long committedModelClock;
  volatile int lastId;
  final String field = "val_l";
  Object[] syncArr;

  private void initModel(int ndocs) {
    snapshotCount = 0;
    committedModelClock = 0;
    lastId = 0;

    syncArr = new Object[ndocs];

    for (int i=0; i<ndocs; i++) {
      model.put(i, -1L);
      syncArr[i] = new Object();
    }
    committedModel.putAll(model);
  }

  public void test() throws Exception {
    // update variables
    final int commitPercent = random().nextInt(20);
    final int softCommitPercent = random().nextInt(100); // what percent of the commits are soft
    final int deletePercent = random().nextInt(50);
    final int deleteByQueryPercent = random().nextInt(25);
    final int ndocs = atLeast(50);
    final int nWriteThreads = _TestUtil.nextInt(random(), 1, TEST_NIGHTLY ? 10 : 5);
    final int maxConcurrentCommits = _TestUtil.nextInt(random(), 1, TEST_NIGHTLY ? 10 : 5);   // number of committers at a time... needed if we want to avoid commit errors due to exceeding the max
    
    final boolean tombstones = random().nextBoolean();

    // query variables
    final AtomicLong operations = new AtomicLong(atLeast(10000));  // number of query operations to perform in total

    final int nReadThreads = _TestUtil.nextInt(random(), 1, TEST_NIGHTLY ? 10 : 5);
    initModel(ndocs);

    final FieldType storedOnlyType = new FieldType();
    storedOnlyType.setStored(true);

    if (VERBOSE) {
      System.out.println("\n");
      System.out.println("TEST: commitPercent=" + commitPercent);
      System.out.println("TEST: softCommitPercent=" + softCommitPercent);
      System.out.println("TEST: deletePercent=" + deletePercent);
      System.out.println("TEST: deleteByQueryPercent=" + deleteByQueryPercent);
      System.out.println("TEST: ndocs=" + ndocs);
      System.out.println("TEST: nWriteThreads=" + nWriteThreads);
      System.out.println("TEST: nReadThreads=" + nReadThreads);
      System.out.println("TEST: maxConcurrentCommits=" + maxConcurrentCommits);
      System.out.println("TEST: tombstones=" + tombstones);
      System.out.println("TEST: operations=" + operations);
      System.out.println("\n");
    }

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();

    Directory dir = newDirectory();

    final RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    writer.setDoRandomForceMergeAssert(false);
    writer.commit();
    reader = DirectoryReader.open(dir);

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.get() > 0) {
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                  Map<Integer,Long> newCommittedModel;
                  long version;
                  DirectoryReader oldReader;

                  synchronized(TestStressNRT.this) {
                    newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                    version = snapshotCount++;
                    oldReader = reader;
                    oldReader.incRef();  // increment the reference since we will use this for reopening
                  }

                  DirectoryReader newReader;
                  if (rand.nextInt(100) < softCommitPercent) {
                    // assertU(h.commit("softCommit","true"));
                    if (random().nextBoolean()) {
                      if (VERBOSE) {
                        System.out.println("TEST: " + Thread.currentThread().getName() + ": call writer.getReader");
                      }
                      newReader = writer.getReader(true);
                    } else {
                      if (VERBOSE) {
                        System.out.println("TEST: " + Thread.currentThread().getName() + ": reopen reader=" + oldReader + " version=" + version);
                      }
                      newReader = DirectoryReader.openIfChanged(oldReader, writer.w, true);
                    }
                  } else {
                    // assertU(commit());
                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": commit+reopen reader=" + oldReader + " version=" + version);
                    }
                    writer.commit();
                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": now reopen after commit");
                    }
                    newReader = DirectoryReader.openIfChanged(oldReader);
                  }

                  // Code below assumes newReader comes w/
                  // extra ref:
                  if (newReader == null) {
                    oldReader.incRef();
                    newReader = oldReader;
                  }

                  oldReader.decRef();

                  synchronized(TestStressNRT.this) {
                    // install the new reader if it's newest (and check the current version since another reader may have already been installed)
                    //System.out.println(Thread.currentThread().getName() + ": newVersion=" + newReader.getVersion());
                    assert newReader.getRefCount() > 0;
                    assert reader.getRefCount() > 0;
                    if (newReader.getVersion() > reader.getVersion()) {
                      if (VERBOSE) {
                        System.out.println("TEST: " + Thread.currentThread().getName() + ": install new reader=" + newReader);
                      }
                      reader.decRef();
                      reader = newReader;

                      // Silly: forces fieldInfos to be
                      // loaded so we don't hit IOE on later
                      // reader.toString
                      newReader.toString();

                      // install this snapshot only if it's newer than the current one
                      if (version >= committedModelClock) {
                        if (VERBOSE) {
                          System.out.println("TEST: " + Thread.currentThread().getName() + ": install new model version=" + version);
                        }
                        committedModel = newCommittedModel;
                        committedModelClock = version;
                      } else {
                        if (VERBOSE) {
                          System.out.println("TEST: " + Thread.currentThread().getName() + ": skip install new model version=" + version);
                        }
                      }
                    } else {
                      // if the same reader, don't decRef.
                      if (VERBOSE) {
                        System.out.println("TEST: " + Thread.currentThread().getName() + ": skip install new reader=" + newReader);
                      }
                      newReader.decRef();
                    }
                  }
                }
                numCommitting.decrementAndGet();
              } else {

                int id = rand.nextInt(ndocs);
                Object sync = syncArr[id];

                // set the lastId before we actually change it sometimes to try and
                // uncover more race conditions between writing and reading
                boolean before = random().nextBoolean();
                if (before) {
                  lastId = id;
                }

                // We can't concurrently update the same document and retain our invariants of increasing values
                // since we can't guarantee what order the updates will be executed.
                synchronized (sync) {
                  Long val = model.get(id);
                  long nextVal = Math.abs(val)+1;

                  if (oper < commitPercent + deletePercent) {
                    // assertU("<delete><id>" + id + "</id></delete>");

                    // add tombstone first
                    if (tombstones) {
                      Document d = new Document();
                      d.add(newStringField("id", "-"+Integer.toString(id), Field.Store.YES));
                      d.add(newField(field, Long.toString(nextVal), storedOnlyType));
                      writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                    }

                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": term delDocs id:" + id + " nextVal=" + nextVal);
                    }
                    writer.deleteDocuments(new Term("id",Integer.toString(id)));
                    model.put(id, -nextVal);
                  } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                    //assertU("<delete><query>id:" + id + "</query></delete>");

                    // add tombstone first
                    if (tombstones) {
                      Document d = new Document();
                      d.add(newStringField("id", "-"+Integer.toString(id), Field.Store.YES));
                      d.add(newField(field, Long.toString(nextVal), storedOnlyType));
                      writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                    }

                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": query delDocs id:" + id + " nextVal=" + nextVal);
                    }
                    writer.deleteDocuments(new TermQuery(new Term("id", Integer.toString(id))));
                    model.put(id, -nextVal);
                  } else {
                    // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                    Document d = new Document();
                    d.add(newStringField("id", Integer.toString(id), Field.Store.YES));
                    d.add(newField(field, Long.toString(nextVal), storedOnlyType));
                    if (VERBOSE) {
                      System.out.println("TEST: " + Thread.currentThread().getName() + ": u id:" + id + " val=" + nextVal);
                    }
                    writer.updateDocument(new Term("id", Integer.toString(id)), d);
                    if (tombstones) {
                      // remove tombstone after new addition (this should be optional?)
                      writer.deleteDocuments(new Term("id","-"+Integer.toString(id)));
                    }
                    model.put(id, nextVal);
                  }
                }

                if (!before) {
                  lastId = id;
                }
              }
            }
          } catch (Throwable e) {
            System.out.println(Thread.currentThread().getName() + ": FAILED: unexpected exception");
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }

    for (int i=0; i<nReadThreads; i++) {
      Thread thread = new Thread("READER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            IndexReader lastReader = null;
            IndexSearcher lastSearcher = null;

            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              long val;
              DirectoryReader r;
              synchronized(TestStressNRT.this) {
                val = committedModel.get(id);
                r = reader;
                r.incRef();
              }

              if (VERBOSE) {
                System.out.println("TEST: " + Thread.currentThread().getName() + ": s id=" + id + " val=" + val + " r=" + r.getVersion());
              }

              //  sreq = req("wt","json", "q","id:"+Integer.toString(id), "omitHeader","true");
              IndexSearcher searcher;
              if (r == lastReader) {
                // Just re-use lastSearcher, else
                // newSearcher may create too many thread
                // pools (ExecutorService):
                searcher = lastSearcher;
              } else {
                searcher = newSearcher(r);
                lastReader = r;
                lastSearcher = searcher;
              }
              Query q = new TermQuery(new Term("id",Integer.toString(id)));
              TopDocs results = searcher.search(q, 10);

              if (results.totalHits == 0 && tombstones) {
                // if we couldn't find the doc, look for its tombstone
                q = new TermQuery(new Term("id","-"+Integer.toString(id)));
                results = searcher.search(q, 1);
                if (results.totalHits == 0) {
                  if (val == -1L) {
                    // expected... no doc was added yet
                    r.decRef();
                    continue;
                  }
                  fail("No documents or tombstones found for id " + id + ", expected at least " + val + " reader=" + r);
                }
              }

              if (results.totalHits == 0 && !tombstones) {
                // nothing to do - we can't tell anything from a deleted doc without tombstones
              } else {
                // we should have found the document, or its tombstone
                if (results.totalHits != 1) {
                  System.out.println("FAIL: hits id:" + id + " val=" + val);
                  for(ScoreDoc sd : results.scoreDocs) {
                    final Document doc = r.document(sd.doc);
                    System.out.println("  docID=" + sd.doc + " id:" + doc.get("id") + " foundVal=" + doc.get(field));
                  }
                  fail("id=" + id + " reader=" + r + " totalHits=" + results.totalHits);
                }
                Document doc = searcher.doc(results.scoreDocs[0].doc);
                long foundVal = Long.parseLong(doc.get(field));
                if (foundVal < Math.abs(val)) {
                  fail("foundVal=" + foundVal + " val=" + val + " id=" + id + " reader=" + r);
                }
              }

              r.decRef();
            }
          } catch (Throwable e) {
            operations.set(-1L);
            System.out.println(Thread.currentThread().getName() + ": FAILED: unexpected exception");
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    writer.close();
    if (VERBOSE) {
      System.out.println("TEST: close reader=" + reader);
    }
    reader.close();
    dir.close();
  }
}
