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
package org.apache.solr.search;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import static org.apache.solr.core.SolrCore.verbose;

public class TestStressLucene extends TestRTGBase {

  // The purpose of this test is to roughly model how solr uses lucene
  DirectoryReader reader;
  @Test
  public void testStressLuceneNRT() throws Exception {
    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = 1+random().nextInt(5);
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 5 + random().nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;

    final AtomicLong operations = new AtomicLong(100000);  // number of query operations to perform in total
    int nReadThreads = 5 + random().nextInt(25);
    final boolean tombstones = random().nextBoolean();
    final boolean syncCommits = random().nextBoolean();

    verbose("commitPercent=", commitPercent);
    verbose("softCommitPercent=",softCommitPercent);
    verbose("deletePercent=",deletePercent);
    verbose("deleteByQueryPercent=", deleteByQueryPercent);
    verbose("ndocs=", ndocs);
    verbose("nWriteThreads=", nWriteThreads);
    verbose("nReadThreads=", nReadThreads);
    verbose("maxConcurrentCommits=", maxConcurrentCommits);
    verbose("operations=", operations);
    verbose("tombstones=", tombstones);
    verbose("syncCommits=", syncCommits);

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<>();


    final FieldType idFt = new FieldType();
    idFt.setStored(true);
    idFt.setOmitNorms(true);
    idFt.setTokenized(false);
    idFt.setIndexOptions(IndexOptions.DOCS);

    final FieldType ft2 = new FieldType();
    ft2.setStored(true);


    // model how solr does locking - only allow one thread to do a hard commit at once, and only one thread to do a soft commit, but
    // a hard commit in progress does not stop a soft commit.
    final Lock hardCommitLock = syncCommits ? new ReentrantLock() : null;
    final Lock reopenLock = syncCommits ? new ReentrantLock() : null;


    // RAMDirectory dir = new RAMDirectory();
    // final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()));

    Directory dir = newDirectory();

    final RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig(new MockAnalyzer(random())));
    writer.setDoRandomForceMergeAssert(false);

    // writer.commit();
    // reader = IndexReader.open(dir);
    // make this reader an NRT reader from the start to avoid the first non-writer openIfChanged
    // to only opening at the last commit point.
    reader = DirectoryReader.open(writer.w);

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
                  Map<Integer,DocInfo> newCommittedModel;
                  long version;
                  DirectoryReader oldReader;

                  boolean softCommit = rand.nextInt(100) < softCommitPercent;

                  if (!softCommit) {
                    // only allow one hard commit to proceed at once
                    if (hardCommitLock != null) hardCommitLock.lock();
                    verbose("hardCommit start");

                    writer.commit();
                  }

                  if (reopenLock != null) reopenLock.lock();

                  synchronized(globalLock) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                    oldReader = reader;
                    oldReader.incRef();  // increment the reference since we will use this for reopening
                  }

                  if (!softCommit) {
                    // must commit after taking a snapshot of the model
                    // writer.commit();
                  }

                  verbose("reopen start using", oldReader);

                  DirectoryReader newReader;
                  if (softCommit) {
                    newReader = DirectoryReader.openIfChanged(oldReader, writer.w, true);
                  } else {
                    // will only open to last commit
                    newReader = DirectoryReader.openIfChanged(oldReader);
                  }


                  if (newReader == null) {
                    oldReader.incRef();
                    newReader = oldReader;
                  }
                  oldReader.decRef();

                  verbose("reopen result", newReader);

                  synchronized(globalLock) {
                    assert newReader.getRefCount() > 0;
                    assert reader.getRefCount() > 0;

                    // install the new reader if it's newest (and check the current version since another reader may have already been installed)
                    if (newReader.getVersion() > reader.getVersion()) {
                      reader.decRef();
                      reader = newReader;

                      // install this snapshot only if it's newer than the current one
                      if (version >= committedModelClock) {
                        committedModel = newCommittedModel;
                        committedModelClock = version;
                      }

                    } else {
                      // close if unused
                      newReader.decRef();
                    }

                  }

                  if (reopenLock != null) reopenLock.unlock();

                  if (!softCommit) {
                    if (hardCommitLock != null) hardCommitLock.unlock();
                  }

                }
                numCommitting.decrementAndGet();
                continue;
              }


              int id = rand.nextInt(ndocs);
              Object sync = syncArr[id];

              // set the lastId before we actually change it sometimes to try and
              // uncover more race conditions between writing and reading
              boolean before = rand.nextBoolean();
              if (before) {
                lastId = id;
              }

              // We can't concurrently update the same document and retain our invariants of increasing values
              // since we can't guarantee what order the updates will be executed.
              synchronized (sync) {
                DocInfo info = model.get(id);
                long val = info.val;
                long nextVal = Math.abs(val)+1;

                if (oper < commitPercent + deletePercent) {
                  // add tombstone first
                  if (tombstones) {
                    Document d = new Document();
                    d.add(new Field("id","-"+Integer.toString(id), idFt));
                    d.add(new Field(FIELD, Long.toString(nextVal), ft2));
                    verbose("adding tombstone for id",id,"val=",nextVal);
                    writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                  }

                  verbose("deleting id",id,"val=",nextVal);
                  writer.deleteDocuments(new Term("id",Integer.toString(id)));
                  model.put(id, new DocInfo(0,-nextVal));
                  verbose("deleting id",id,"val=",nextVal,"DONE");

                } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                  //assertU("<delete><query>id:" + id + "</query></delete>");

                  // add tombstone first
                  if (tombstones) {
                    Document d = new Document();
                    d.add(new Field("id","-"+Integer.toString(id), idFt));
                    d.add(new Field(FIELD, Long.toString(nextVal), ft2));
                    verbose("adding tombstone for id",id,"val=",nextVal);
                    writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                  }

                  verbose("deleteByQuery",id,"val=",nextVal);
                  writer.deleteDocuments(new TermQuery(new Term("id", Integer.toString(id))));
                  model.put(id, new DocInfo(0,-nextVal));
                  verbose("deleteByQuery",id,"val=",nextVal,"DONE");
                } else {
                  // model.put(id, nextVal);   // uncomment this and this test should fail.

                  // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                  Document d = new Document();
                  d.add(new Field("id",Integer.toString(id), idFt));
                  d.add(new Field(FIELD, Long.toString(nextVal), ft2));
                  verbose("adding id",id,"val=",nextVal);
                  writer.updateDocument(new Term("id", Integer.toString(id)), d);
                  if (tombstones) {
                    // remove tombstone after new addition (this should be optional?)
                    verbose("deleting tombstone for id",id);
                    writer.deleteDocuments(new Term("id","-"+Integer.toString(id)));
                    verbose("deleting tombstone for id",id,"DONE");
                  }

                  model.put(id, new DocInfo(0,nextVal));
                  verbose("adding id",id,"val=",nextVal,"DONE");
                }
              }

              if (!before) {
                lastId = id;
              }
            }
          } catch (Exception  ex) {
            throw new RuntimeException(ex);
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
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              DocInfo info;
              synchronized(globalLock) {
                info = committedModel.get(id);
              }
              long val = info.val;

              IndexReader r;
              synchronized(globalLock) {
                r = reader;
                r.incRef();
              }

              int docid = getFirstMatch(r, new Term("id",Integer.toString(id)));

              if (docid < 0 && tombstones) {
                // if we couldn't find the doc, look for its tombstone
                docid = getFirstMatch(r, new Term("id","-"+Integer.toString(id)));
                if (docid < 0) {
                  if (val == -1L) {
                    // expected... no doc was added yet
                    r.decRef();
                    continue;
                  }
                  verbose("ERROR: Couldn't find a doc  or tombstone for id", id, "using reader",r,"expected value",val);
                  fail("No documents or tombstones found for id " + id + ", expected at least " + val);
                }
              }

              if (docid < 0 && !tombstones) {
                // nothing to do - we can't tell anything from a deleted doc without tombstones
              } else {
                if (docid < 0) {
                  verbose("ERROR: Couldn't find a doc for id", id, "using reader",r);
                }
                assertTrue(docid >= 0);   // we should have found the document, or its tombstone
                Document doc = r.document(docid);
                long foundVal = Long.parseLong(doc.get(FIELD));
                if (foundVal < Math.abs(val)) {
                  verbose("ERROR: id",id,"model_val=",val," foundVal=",foundVal,"reader=",reader);
                }
                assertTrue(foundVal >= Math.abs(val));
              }

              r.decRef();
            }
          } catch (Throwable e) {
            operations.set(-1L);
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
    reader.close();
    dir.close();
  }



}
