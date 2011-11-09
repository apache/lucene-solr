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
package org.apache.solr.search;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static  org.apache.solr.core.SolrCore.verbose;

public class TestRealTimeGet extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema12.xml");
  }

  @Test
  public void testGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    assertU(adoc("id","1"));
    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1")
        ,"=={" +
        "  'response':{'numFound':1,'start':0,'docs':[" +
        "      {" +
        "        'id':'1'}]" +
        "  }}}"
    );

    assertU(commit());

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1")
        ,"=={" +
        "  'response':{'numFound':1,'start':0,'docs':[" +
        "      {" +
        "        'id':'1'}]" +
        "  }}}"
    );

    assertU(delI("1"));

    assertJQ(req("q","id:1")
        ,"/response/numFound==1"
    );
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );
    assertJQ(req("qt","/get","ids","1")
        ,"=={'response':{'numFound':0,'start':0,'docs':[]}}"
    );

  }


  /***
  @Test
  public void testGetRealtime() throws Exception {
    SolrQueryRequest sr1 = req("q","foo");
    IndexReader r1 = sr1.getCore().getRealtimeReader();

    assertU(adoc("id","1"));

    IndexReader r2 = sr1.getCore().getRealtimeReader();
    assertNotSame(r1, r2);
    int refcount = r2.getRefCount();

    // make sure a new reader wasn't opened
    IndexReader r3 = sr1.getCore().getRealtimeReader();
    assertSame(r2, r3);
    assertEquals(refcount+1, r3.getRefCount());

    assertU(commit());

    // this is not critical, but currently a commit does not refresh the reader
    // if nothing has changed
    IndexReader r4 = sr1.getCore().getRealtimeReader();
    assertEquals(refcount+2, r4.getRefCount());


    r1.decRef();
    r2.decRef();
    r3.decRef();
    r4.decRef();
    sr1.close();
  }
  ***/


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

  @Test
  public void testStressGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    // req().getCore().getUpdateHandler().getIndexWriterProvider().getIndexWriter(req().getCore()).setInfoStream(System.out);

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // real-time get isn't currently supported with delete-by-query
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 60;
    // final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 5 + random.nextInt(25);


    verbose("commitPercent=", commitPercent);
    verbose("softCommitPercent=",softCommitPercent);
    verbose("deletePercent=",deletePercent);
    verbose("deleteByQueryPercent=", deleteByQueryPercent);
    verbose("ndocs=", ndocs);
    verbose("nWriteThreads=", nWriteThreads);
    verbose("nReadThreads=", nReadThreads);
    verbose("percentRealtimeQuery=", percentRealtimeQuery);
    verbose("maxConcurrentCommits=", maxConcurrentCommits);
    verbose("operations=", operations);


    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          try {
          while (operations.get() > 0) {
            int oper = rand.nextInt(100);

            if (oper < commitPercent) {
              if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                Map<Integer,Long> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                  version = snapshotCount++;
                  verbose("took snapshot version=",version);
                }

                if (rand.nextInt(100) < softCommitPercent) {
                  verbose("softCommit start");
                  assertU(h.commit("softCommit","true"));
                  verbose("softCommit end");
                } else {
                  verbose("hardCommit start");
                  assertU(commit());
                  verbose("hardCommit end");
                }

                synchronized(TestRealTimeGet.this) {
                  // install this model snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
                    if (VERBOSE) {
                      verbose("installing new committedModel version="+committedModelClock);
                    }
                    committedModel = newCommittedModel;
                    committedModelClock = version;
                  }
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
              Long val = model.get(id);
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                if (VERBOSE) {
                  verbose("deleting id",id,"val=",nextVal);
                }

                assertU("<delete><id>" + id + "</id></delete>");
                model.put(id, -nextVal);
                if (VERBOSE) {
                  verbose("deleting id", id, "val=",nextVal,"DONE");
                }
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                if (VERBOSE) {
                  verbose("deleteByQuery id ",id, "val=",nextVal);
                }

                assertU("<delete><query>id:" + id + "</query></delete>");
                model.put(id, -nextVal);
                if (VERBOSE) {
                  verbose("deleteByQuery id",id, "val=",nextVal,"DONE");
                }
              } else {
                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal);
                }

                assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                model.put(id, nextVal);

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"DONE");
                }

              }
            }

            if (!before) {
              lastId = id;
            }
          }
        } catch (Throwable e) {
          operations.set(-1L);
          SolrException.log(log, e);
          fail(e.getMessage());
        }
        }
      };

      threads.add(thread);
    }


    for (int i=0; i<nReadThreads; i++) {
      Thread thread = new Thread("READER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              long val;

              if (realTime) {
                val = model.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  val = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                verbose("querying id", id);
              }
              SolrQueryRequest sreq;
              if (realTime) {
                sreq = req("wt","json", "qt","/get", "ids",Integer.toString(id));
              } else {
                sreq = req("wt","json", "q","id:"+Integer.toString(id), "omitHeader","true");
              }

              String response = h.query(sreq);
              Map rsp = (Map)ObjectBuilder.fromJSON(response);
              List doclist = (List)(((Map)rsp.get("response")).get("docs"));
              if (doclist.size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else {
                assertEquals(1, doclist.size());
                long foundVal = (Long)(((Map)doclist.get(0)).get(field));
                if (foundVal < Math.abs(val)) {
                  verbose("ERROR, id", id, "foundVal=",foundVal,"model val=",val,"realTime=",realTime);
                  assertTrue(foundVal >= Math.abs(val));
                }
              }
            }
          }
          catch (Throwable e) {
            operations.set(-1L);
            SolrException.log(log, e);
            fail(e.getMessage());
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

  }




  // The purpose of this test is to roughly model how solr uses lucene
  IndexReader reader;
  @Test
  public void testStressLuceneNRT() throws Exception {
    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // real-time get isn't currently supported with delete-by-query
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

    final AtomicLong operations = new AtomicLong(10000);  // number of query operations to perform in total - crank up if
    int nReadThreads = 5 + random.nextInt(25);
    final boolean tombstones = random.nextBoolean();
    final boolean syncCommits = random.nextBoolean();

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

    List<Thread> threads = new ArrayList<Thread>();


    final FieldType idFt = new FieldType();
    idFt.setIndexed(true);
    idFt.setStored(true);
    idFt.setOmitNorms(true);
    idFt.setTokenized(false);
    idFt.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);

    final FieldType ft2 = new FieldType();
    ft2.setIndexed(false);
    ft2.setStored(true);


    // model how solr does locking - only allow one thread to do a hard commit at once, and only one thread to do a soft commit, but
    // a hard commit in progress does not stop a soft commit.
    final Lock hardCommitLock = syncCommits ? new ReentrantLock() : null;
    final Lock reopenLock = syncCommits ? new ReentrantLock() : null;


    // RAMDirectory dir = new RAMDirectory();
    // final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_40, new WhitespaceAnalyzer(Version.LUCENE_40)));

    Directory dir = newDirectory();

    final RandomIndexWriter writer = new RandomIndexWriter(random, dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)));
    writer.setDoRandomOptimizeAssert(false);

    // writer.commit();
    // reader = IndexReader.open(dir);
    // make this reader an NRT reader from the start to avoid the first non-writer openIfChanged
    // to only opening at the last commit point.
    reader = IndexReader.open(writer.w, true);

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          try {
            while (operations.get() > 0) {
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                  Map<Integer,Long> newCommittedModel;
                  long version;
                  IndexReader oldReader;

                  boolean softCommit = rand.nextInt(100) < softCommitPercent;

                  if (!softCommit) {
                    // only allow one hard commit to proceed at once
                    if (hardCommitLock != null) hardCommitLock.lock();
                    verbose("hardCommit start");

                    writer.commit();
                  }

                  if (reopenLock != null) reopenLock.lock();

                  synchronized(TestRealTimeGet.this) {
                    newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                    version = snapshotCount++;
                    oldReader = reader;
                    oldReader.incRef();  // increment the reference since we will use this for reopening
                  }

                  if (!softCommit) {
                    // must commit after taking a snapshot of the model
                    // writer.commit();
                  }

                  verbose("reopen start using", oldReader);

                  IndexReader newReader;
                  if (softCommit) {
                    newReader = IndexReader.openIfChanged(oldReader, writer.w, true);
                  } else {
                    // will only open to last commit
                   newReader = IndexReader.openIfChanged(oldReader);
                  }


                  if (newReader == null) {
                    oldReader.incRef();
                    newReader = oldReader;
                  }
                  oldReader.decRef();

                  verbose("reopen result", newReader);

                  synchronized(TestRealTimeGet.this) {
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
                Long val = model.get(id);
                long nextVal = Math.abs(val)+1;

                if (oper < commitPercent + deletePercent) {
                  // add tombstone first
                  if (tombstones) {
                    Document d = new Document();
                    d.add(new Field("id","-"+Integer.toString(id), idFt));
                    d.add(new Field(field, Long.toString(nextVal), ft2));
                    verbose("adding tombstone for id",id,"val=",nextVal);
                    writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                  }

                  verbose("deleting id",id,"val=",nextVal);
                  writer.deleteDocuments(new Term("id",Integer.toString(id)));
                  model.put(id, -nextVal);
                  verbose("deleting id",id,"val=",nextVal,"DONE");

                } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                  //assertU("<delete><query>id:" + id + "</query></delete>");

                  // add tombstone first
                  if (tombstones) {
                    Document d = new Document();
                    d.add(new Field("id","-"+Integer.toString(id), idFt));
                    d.add(new Field(field, Long.toString(nextVal), ft2));
                    verbose("adding tombstone for id",id,"val=",nextVal);
                    writer.updateDocument(new Term("id", "-"+Integer.toString(id)), d);
                  }

                  verbose("deleteByQuery",id,"val=",nextVal);
                  writer.deleteDocuments(new TermQuery(new Term("id", Integer.toString(id))));
                  model.put(id, -nextVal);
                  verbose("deleteByQuery",id,"val=",nextVal,"DONE");
                } else {
                  // model.put(id, nextVal);   // uncomment this and this test should fail.

                  // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                  Document d = new Document();
                  d.add(new Field("id",Integer.toString(id), idFt));
                  d.add(new Field(field, Long.toString(nextVal), ft2));
                  verbose("adding id",id,"val=",nextVal);
                  writer.updateDocument(new Term("id", Integer.toString(id)), d);
                  if (tombstones) {
                    // remove tombstone after new addition (this should be optional?)
                    verbose("deleting tombstone for id",id);
                    writer.deleteDocuments(new Term("id","-"+Integer.toString(id)));
                    verbose("deleting tombstone for id",id,"DONE");
                  }

                  model.put(id, nextVal);
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
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              long val;

              synchronized(TestRealTimeGet.this) {
                val = committedModel.get(id);
              }


              IndexReader r;
              synchronized(TestRealTimeGet.this) {
                r = reader;
                r.incRef();
              }

              int docid = getFirstMatch(r, new Term("id",Integer.toString(id)));

              if (docid < 0 && tombstones) {
                // if we couldn't find the doc, look for it's tombstone
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
                assertTrue(docid >= 0);   // we should have found the document, or it's tombstone
                Document doc = r.document(docid);
                long foundVal = Long.parseLong(doc.get(field));
                if (foundVal < Math.abs(val)) {
                  verbose("ERROR: id",id,"model_val=",val," foundVal=",foundVal,"reader=",reader);
                }
                assertTrue(foundVal >= Math.abs(val));
              }

              r.decRef();
            }
          }
          catch (Throwable e) {
            operations.set(-1L);
            SolrException.log(log,e);
            fail(e.toString());
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


  public int getFirstMatch(IndexReader r, Term t) throws IOException {
    Fields fields = MultiFields.getFields(r);
    if (fields == null) return -1;
    Terms terms = fields.terms(t.field());
    if (terms == null) return -1;
    BytesRef termBytes = t.bytes();
    DocsEnum docs = terms.docs(MultiFields.getLiveDocs(r), termBytes, null);
    if (docs == null) return -1;
    int id = docs.nextDoc();
    if (id != DocIdSetIterator.NO_MORE_DOCS) {
      int next = docs.nextDoc();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
    }
    return id == DocIdSetIterator.NO_MORE_DOCS ? -1 : id;
  }

}
