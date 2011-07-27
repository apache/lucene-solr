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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestRealTimeGet extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema12.xml");
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


  final ConcurrentHashMap<Integer,Long> sanityModel = new ConcurrentHashMap<Integer,Long>();


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
    // update variables
    final int commitPercent = 10;
    final int softCommitPercent = 50; // what percent of the commits are soft
    final int deletePercent = 8;
    final int deleteByQueryPercent = 4;
    final int ndocs = 100;
    int nWriteThreads = 10;
    final int maxConcurrentCommits = 2;   // number of committers at a time... needed if we want to avoid commit errors due to exceeding the max

    // query variables
    final int percentRealtimeQuery = 0;   // realtime get is not implemented yet
    final AtomicLong operations = new AtomicLong(0);  // number of query operations to perform in total       // TODO: once lucene level passes, we can move on to the solr level
    int nReadThreads = 10;

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          while (operations.get() > 0) {
            int oper = rand.nextInt(100);

            if (oper < commitPercent) {
              if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                Map<Integer,Long> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                  version = snapshotCount++;
                }

                if (rand.nextInt(100) < softCommitPercent)
                  assertU(h.commit("softCommit","true"));
                else
                  assertU(commit());

                synchronized(TestRealTimeGet.this) {
                  // install this snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
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
            boolean before = random.nextBoolean();
            if (before) {
              lastId = id;
            }

            // We can't concurrently update the same document and retain our invariants of increasing values
            // since we can't guarantee what order the updates will be executed.
            synchronized (sync) {
              Long val = model.get(id);
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                assertU("<delete><id>" + id + "</id></delete>");
                model.put(id, -nextVal);
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                assertU("<delete><query>id:" + id + "</query></delete>");
                model.put(id, -nextVal);
              } else {
                assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                model.put(id, nextVal);
              }
            }

            if (!before) {
              lastId = id;
            }
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
              int oper = rand.nextInt(100);
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
                assertTrue(foundVal >= Math.abs(val));
              }
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

  }




  IndexReader reader;

  @Test
  public void testStressLuceneNRT() throws Exception {
    // update variables
    final int commitPercent = 10;
    final int softCommitPercent = 50; // what percent of the commits are soft
    final int deletePercent = 8;
    final int deleteByQueryPercent = 4;
    final int ndocs = 100;
    int nWriteThreads = 10;
    final int maxConcurrentCommits = 2;   // number of committers at a time... needed if we want to avoid commit errors due to exceeding the max

    // query variables
    final AtomicLong operations = new AtomicLong(10000000);  // number of query operations to perform in total       // TODO: temporarily high due to lack of stability
    int nReadThreads = 10;

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();

    RAMDirectory dir = new RAMDirectory();
    final IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_40, new WhitespaceAnalyzer(Version.LUCENE_40)));
    writer.commit();
    reader = IndexReader.open(dir);

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

                  synchronized(TestRealTimeGet.this) {
                    newCommittedModel = new HashMap<Integer,Long>(model);  // take a snapshot
                    version = snapshotCount++;
                    oldReader = reader;
                    oldReader.incRef();  // increment the reference since we will use this for reopening
                  }

                  IndexReader newReader;
                  if (rand.nextInt(100) < softCommitPercent) {
                    // assertU(h.commit("softCommit","true"));
                    newReader = oldReader.reopen(writer, true);
                  } else {
                    // assertU(commit());
                    writer.commit();
                    newReader = oldReader.reopen();
                  }

                  synchronized(TestRealTimeGet.this) {
                    // install the new reader if it's newest (and check the current version since another reader may have already been installed)
                    if (newReader.getVersion() > reader.getVersion()) {
                      reader.decRef();
                      reader = newReader;

                      // install this snapshot only if it's newer than the current one
                      if (version >= committedModelClock) {
                        committedModel = newCommittedModel;
                        committedModelClock = version;
                      }

                    } else if (newReader != oldReader) {
                      // if the same reader, don't decRef.
                      newReader.decRef();
                    }

                    oldReader.decRef();
                  }
                }
                numCommitting.decrementAndGet();
                continue;
              }


              int id = rand.nextInt(ndocs);
              Object sync = syncArr[id];

              // set the lastId before we actually change it sometimes to try and
              // uncover more race conditions between writing and reading
              boolean before = random.nextBoolean();
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
                  writer.deleteDocuments(new Term("id",Integer.toString(id)));
                  model.put(id, -nextVal);
                } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                  //assertU("<delete><query>id:" + id + "</query></delete>");
                  writer.deleteDocuments(new TermQuery(new Term("id", Integer.toString(id))));
                  model.put(id, -nextVal);
                } else {
                  // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                  Document d = new Document();
                  d.add(new Field("id",Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
                  d.add(new Field(field, Long.toString(nextVal), Field.Store.YES, Field.Index.NO));
                  writer.updateDocument(new Term("id", Integer.toString(id)), d);
                  model.put(id, nextVal);
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

              //  sreq = req("wt","json", "q","id:"+Integer.toString(id), "omitHeader","true");
              IndexSearcher searcher = new IndexSearcher(r);
              Query q = new TermQuery(new Term("id",Integer.toString(id)));
              TopDocs results = searcher.search(q, 10);

              if (results.totalHits == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else {
                assertEquals(1, results.totalHits);
                Document doc = searcher.doc(results.scoreDocs[0].doc);
                long foundVal = Long.parseLong(doc.get(field));
                if (foundVal < Math.abs(val)) {
                  System.out.println("model_val="+val+" foundVal="+foundVal);
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

  }



}
