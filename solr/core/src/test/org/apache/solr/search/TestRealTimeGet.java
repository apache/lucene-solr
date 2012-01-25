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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionInfo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static  org.apache.solr.core.SolrCore.verbose;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.SEEN_LEADER;

public class TestRealTimeGet extends SolrTestCaseJ4 {
  private static String SEEN_LEADER_VAL="true"; // value that means we've seen the leader and have version info (i.e. we are a non-leader replica)


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
    assertJQ(req("qt","/get", "id","1", "fl","id")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1", "fl","id")
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
    assertJQ(req("qt","/get","id","1", "fl","id")
        ,"=={'doc':{'id':'1'}}"
    );
    assertJQ(req("qt","/get","ids","1", "fl","id")
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


    assertU(adoc("id","10"));
    assertU(adoc("id","11"));
    assertJQ(req("qt","/get","id","10", "fl","id")
        ,"=={'doc':{'id':'10'}}"
    );
    assertU(delQ("id:10 abcdef"));
    assertJQ(req("qt","/get","id","10")
        ,"=={'doc':null}"
    );
    assertJQ(req("qt","/get","id","11", "fl","id")
        ,"=={'doc':{'id':'11'}}"
    );


  }


  @Test
  public void testVersions() throws Exception {
    clearIndex();
    assertU(commit());

    long version = addAndGetVersion(sdoc("id","1") , null);

    assertJQ(req("q","id:1")
        ,"/response/numFound==0"
    );

    // test version is there from rtg
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // test version is there from the index
    assertU(commit());
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate an update from the leader
    version += 10;
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version))), params(SEEN_LEADER,SEEN_LEADER_VAL));

    // test version is there from rtg
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a version less than that does not take affect
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(SEEN_LEADER,SEEN_LEADER_VAL));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a delete w/ version less than that does not take affect
    // TODO: also allow passing version on delete instead of on URL?
    updateJ(jsonDelId("1"), params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_",Long.toString(version - 1)));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // make sure reordering detection also works after a commit
    assertU(commit());

    // simulate reordering: test that a version less than that does not take affect
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(SEEN_LEADER,SEEN_LEADER_VAL));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // simulate reordering: test that a delete w/ version less than that does not take affect
    updateJ(jsonDelId("1"), params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_",Long.toString(version - 1)));

    // test that version hasn't changed
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':{'id':'1','_version_':" + version + "}}"
    );

    // now simulate a normal delete from the leader
    version += 5;
    updateJ(jsonDelId("1"), params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_",Long.toString(version)));

    // make sure a reordered add doesn't take affect.
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(SEEN_LEADER,SEEN_LEADER_VAL));

    // test that it's still deleted
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );

    // test that we can remember the version of a delete after a commit
    assertU(commit());

    // make sure a reordered add doesn't take affect.
    updateJ(jsonAdd(sdoc("id","1", "_version_",Long.toString(version - 1))), params(SEEN_LEADER,SEEN_LEADER_VAL));

    // test that it's still deleted
    assertJQ(req("qt","/get","id","1")
        ,"=={'doc':null}"
    );

    version = addAndGetVersion(sdoc("id","2"), null);
    long version2 = deleteByQueryAndGetVersion("id:2", null);
    assertTrue(Math.abs(version2) > version );
    
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


  final ConcurrentHashMap<Integer,DocInfo> model = new ConcurrentHashMap<Integer,DocInfo>();
  Map<Integer,DocInfo> committedModel = new HashMap<Integer,DocInfo>();
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
      model.put(i, new DocInfo(0, -1L));
      syncArr[i] = new Object();
    }
    committedModel.putAll(model);
  }


  static class DocInfo {
    long version;
    long val;

    public DocInfo(long version, long val) {
      this.version = version;
      this.val = val;
    }

    public String toString() {
      return "{version="+version+",val="+val+"\"";
    }
  }

  @Test
  public void testStressGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    // req().getCore().getUpdateHandler().getIndexWriterProvider().getIndexWriter(req().getCore()).setInfoStream(System.out);

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 1+random.nextInt(5);
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 60;
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
                Map<Integer,DocInfo> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,DocInfo>(model);  // take a snapshot
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
            // Even with versions, we can't remove the sync because increasing versions does not mean increasing vals.
            synchronized (sync) {
              DocInfo info = model.get(id);

              long val = info.val;
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                if (VERBOSE) {
                  verbose("deleting id",id,"val=",nextVal);
                }

                // assertU("<delete><id>" + id + "</id></delete>");
                Long version = deleteAndGetVersion(Integer.toString(id), null);

                model.put(id, new DocInfo(version, -nextVal));
                if (VERBOSE) {
                  verbose("deleting id", id, "val=",nextVal,"DONE");
                }
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                if (VERBOSE) {
                  verbose("deleteByQuery id ",id, "val=",nextVal);
                }

                assertU("<delete><query>id:" + id + "</query></delete>");
                model.put(id, new DocInfo(-1L, -nextVal));
                if (VERBOSE) {
                  verbose("deleteByQuery id",id, "val=",nextVal,"DONE");
                }
              } else {
                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal);
                }

                // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                Long version = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal)), null);
                model.put(id, new DocInfo(version, nextVal));

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"DONE");
                }

              }
            }   // end sync

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
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  info = committedModel.get(id);
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
                long foundVer = (Long)(((Map)doclist.get(0)).get("_version_"));
                if (foundVal < Math.abs(info.val)
                    || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                  verbose("ERROR, id=", id, "found=",response,"model",info);
                  assertTrue(false);
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


  // This version doesn't synchronize on id to tell what update won, but instead uses versions
  @Test
  public void testStressGetRealtimeVersions() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 1 + random.nextInt(5);
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 5 + random.nextInt(25);



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
                Map<Integer,DocInfo> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,DocInfo>(model);  // take a snapshot
                  version = snapshotCount++;
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
            // Even with versions, we can't remove the sync because increasing versions does not mean increasing vals.
            //
            // NOTE: versioning means we can now remove the sync and tell what update "won"
            // synchronized (sync) {
              DocInfo info = model.get(id);

              long val = info.val;
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                verbose("deleting id",id,"val=",nextVal);

                Long version = deleteAndGetVersion(Integer.toString(id), null);
                assertTrue(version < 0);

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

                verbose("deleting id", id, "val=",nextVal,"DONE");
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                verbose("deleteByQyery id",id,"val=",nextVal);

                Long version = deleteByQueryAndGetVersion("id:"+Integer.toString(id), null);
                assertTrue(version < 0);

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

                verbose("deleteByQyery id", id, "val=",nextVal,"DONE");
              } else {
                verbose("adding id", id, "val=", nextVal);

                // assertU(adoc("id",Integer.toString(id), field, Long.toString(nextVal)));
                Long version = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal)), null);
                assertTrue(version > 0);

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (version > currInfo.version) {
                    model.put(id, new DocInfo(version, nextVal));
                  }
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"DONE");
                }

              }
            // }   // end sync

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
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  info = committedModel.get(id);
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
                long foundVer = (Long)(((Map)doclist.get(0)).get("_version_"));
                if (foundVer < Math.abs(info.version)
                    || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                  verbose("ERROR, id=", id, "found=",response,"model",info);
                  assertTrue(false);
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

  // This version simulates updates coming from the leader and sometimes being reordered
  @Test
  public void testStressReorderVersions() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // delete-by-query can't be reordered on replicas
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 5 + random.nextInt(25);

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();


    final AtomicLong testVersion = new AtomicLong(0);

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
                Map<Integer,DocInfo> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,DocInfo>(model);  // take a snapshot
                  version = snapshotCount++;
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


            int id;

            if (rand.nextBoolean()) {
              id = rand.nextInt(ndocs);
            } else {
              id = lastId;  // reuse the last ID half of the time to force more race conditions
            }

            // set the lastId before we actually change it sometimes to try and
            // uncover more race conditions between writing and reading
            boolean before = rand.nextBoolean();
            if (before) {
              lastId = id;
            }

            DocInfo info = model.get(id);

            long val = info.val;
            long nextVal = Math.abs(val)+1;

            // the version we set on the update should determine who wins
            // These versions are not derived from the actual leader update handler hand hence this
            // test may need to change depending on how we handle version numbers.
            long version = testVersion.incrementAndGet();

            // yield after getting the next version to increase the odds of updates happening out of order
            if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deletePercent) {
                verbose("deleting id",id,"val=",nextVal,"version",version);

                Long returnedVersion = deleteAndGetVersion(Integer.toString(id), params("_version_",Long.toString(-version), SEEN_LEADER,SEEN_LEADER_VAL));

                // TODO: returning versions for these types of updates is redundant
                // but if we do return, they had better be equal
                if (returnedVersion != null) {
                  assertEquals(-version, returnedVersion.longValue());
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

                verbose("deleting id", id, "val=",nextVal,"version",version,"DONE");
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {

              } else {
                verbose("adding id", id, "val=", nextVal,"version",version);

                Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal), "_version_",Long.toString(version)), params(SEEN_LEADER,SEEN_LEADER_VAL));
                if (returnedVersion != null) {
                  assertEquals(version, returnedVersion.longValue());
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (version > currInfo.version) {
                    model.put(id, new DocInfo(version, nextVal));
                  }
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"version",version,"DONE");
                }

              }
            // }   // end sync

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
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  info = committedModel.get(id);
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
                long foundVer = (Long)(((Map)doclist.get(0)).get("_version_"));
                if (foundVer < Math.abs(info.version)
                    || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                  verbose("ERROR, id=", id, "found=",response,"model",info);
                  assertTrue(false);
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

  // This points to the live model when state is ACTIVE, but a snapshot of the
  // past when recovering.
  volatile ConcurrentHashMap<Integer,DocInfo> visibleModel;

  // This version simulates updates coming from the leader and sometimes being reordered
  // and tests the ability to buffer updates and apply them later
  @Test
  public void testStressRecovery() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random.nextInt(10);
    final int softCommitPercent = 30+random.nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // real-time get isn't currently supported with delete-by-query
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 2 + random.nextInt(10);  // fewer write threads to give recovery thread more of a chance

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong(atLeast(75));  // number of recovery loops to perform
    int nReadThreads = 2 + random.nextInt(10);  // fewer read threads to give writers more of a chance

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<Thread>();


    final AtomicLong testVersion = new AtomicLong(0);


    final UpdateHandler uHandler = h.getCore().getUpdateHandler();
    final UpdateLog uLog = uHandler.getUpdateLog();
    final VersionInfo vInfo = uLog.getVersionInfo();
    final Object stateChangeLock = new Object();
    this.visibleModel = model;
    final Semaphore[] writePermissions = new Semaphore[nWriteThreads];
    for (int i=0; i<nWriteThreads; i++) writePermissions[i] = new Semaphore(Integer.MAX_VALUE, false);

    final Semaphore readPermission = new Semaphore(Integer.MAX_VALUE, false);

    for (int i=0; i<nWriteThreads; i++) {
      final int threadNum = i;

      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random.nextInt());
        Semaphore writePermission = writePermissions[threadNum];

        @Override
        public void run() {
          try {
          while (operations.get() > 0) {
            writePermission.acquire();

            int oper = rand.nextInt(10);

            if (oper < commitPercent) {
              if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                Map<Integer,DocInfo> newCommittedModel;
                long version;

                synchronized(TestRealTimeGet.this) {
                  newCommittedModel = new HashMap<Integer,DocInfo>(model);  // take a snapshot
                  version = snapshotCount++;
                }

                synchronized (stateChangeLock) {
                  // These commits won't take affect if we are in recovery mode,
                  // so change the version to -1 so we won't update our model.
                  if (uLog.getState() != UpdateLog.State.ACTIVE) version = -1;
                  if (rand.nextInt(100) < softCommitPercent) {
                    verbose("softCommit start");
                    assertU(h.commit("softCommit","true"));
                    verbose("softCommit end");
                  } else {
                    verbose("hardCommit start");
                    assertU(commit());
                    verbose("hardCommit end");
                  }
                }

                synchronized(TestRealTimeGet.this) {
                  // install this model snapshot only if it's newer than the current one
                  // install this model only if we are not in recovery mode.
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


            int id;

            if (rand.nextBoolean()) {
              id = rand.nextInt(ndocs);
            } else {
              id = lastId;  // reuse the last ID half of the time to force more race conditions
            }

            // set the lastId before we actually change it sometimes to try and
            // uncover more race conditions between writing and reading
            boolean before = rand.nextBoolean();
            if (before) {
              lastId = id;
            }

            DocInfo info = model.get(id);

            long val = info.val;
            long nextVal = Math.abs(val)+1;

            // the version we set on the update should determine who wins
            // These versions are not derived from the actual leader update handler hand hence this
            // test may need to change depending on how we handle version numbers.
            long version = testVersion.incrementAndGet();

            // yield after getting the next version to increase the odds of updates happening out of order
            if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deletePercent) {
                verbose("deleting id",id,"val=",nextVal,"version",version);

                Long returnedVersion = deleteAndGetVersion(Integer.toString(id), params("_version_",Long.toString(-version), SEEN_LEADER,SEEN_LEADER_VAL));

                // TODO: returning versions for these types of updates is redundant
                // but if we do return, they had better be equal
                if (returnedVersion != null) {
                  assertEquals(-version, returnedVersion.longValue());
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

                verbose("deleting id", id, "val=",nextVal,"version",version,"DONE");
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {

              } else {
                verbose("adding id", id, "val=", nextVal,"version",version);

                Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal), "_version_",Long.toString(version)), params(SEEN_LEADER,SEEN_LEADER_VAL));
                if (returnedVersion != null) {
                  assertEquals(version, returnedVersion.longValue());
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (version > currInfo.version) {
                    model.put(id, new DocInfo(version, nextVal));
                  }
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"version",version,"DONE");
                }

              }
            // }   // end sync

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
            while (operations.get() > 0) {
              // throttle reads (don't completely stop)
              readPermission.tryAcquire(10, TimeUnit.MILLISECONDS);


              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              DocInfo info;

              if (realTime) {
                info = visibleModel.get(id);
              } else {
                synchronized(TestRealTimeGet.this) {
                  info = committedModel.get(id);
                }
              }


              if  (VERBOSE) {
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
                long foundVer = (Long)(((Map)doclist.get(0)).get("_version_"));
                if (foundVer < Math.abs(info.version)
                    || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                  verbose("ERROR, id=", id, "found=",response,"model",info);
                  assertTrue(false);
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

    int bufferedAddsApplied = 0;
    do {
      assertTrue(uLog.getState() == UpdateLog.State.ACTIVE);

      // before we start buffering updates, we want to point
      // visibleModel away from the live model.

      visibleModel = new ConcurrentHashMap<Integer, DocInfo>(model);

      synchronized (stateChangeLock) {
        uLog.bufferUpdates();
      }

      assertTrue(uLog.getState() == UpdateLog.State.BUFFERING);

      // sometimes wait for a second to allow time for writers to write something
      if (random.nextBoolean()) Thread.sleep(random.nextInt(10)+1);

      Future<UpdateLog.RecoveryInfo> recoveryInfoF = uLog.applyBufferedUpdates();
      if (recoveryInfoF != null) {
        UpdateLog.RecoveryInfo recInfo = null;

        int writeThreadNumber = 0;
        while (recInfo == null) {
          try {
            // wait a short period of time for recovery to complete (and to give a chance for more writers to concurrently add docs)
            recInfo = recoveryInfoF.get(random.nextInt(100/nWriteThreads), TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            // idle one more write thread
            verbose("Operation",operations.get(),"Draining permits for write thread",writeThreadNumber);
            writePermissions[writeThreadNumber++].drainPermits();
            if (writeThreadNumber >= nWriteThreads) {
              // if we hit the end, back up and give a few write permits
              writeThreadNumber--;
              writePermissions[writeThreadNumber].release(random.nextInt(2) + 1);
            }

            // throttle readers so they don't steal too much CPU from the recovery thread
            readPermission.drainPermits();
          }
        }

        bufferedAddsApplied += recInfo.adds;
      }

      // put all writers back at full blast
      for (Semaphore writePerm : writePermissions) {
        // I don't think semaphores check for overflow, so we need to check mow many remain
        int neededPermits = Integer.MAX_VALUE - writePerm.availablePermits();
        if (neededPermits > 0) writePerm.release( neededPermits );
      }

      // put back readers at full blast and point back to live model
      visibleModel = model;
      int neededPermits = Integer.MAX_VALUE - readPermission.availablePermits();
      if (neededPermits > 0) readPermission.release( neededPermits );

      verbose("ROUND=",operations.get());
    } while (operations.decrementAndGet() > 0);

    verbose("bufferedAddsApplied=",bufferedAddsApplied);

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
    final int deleteByQueryPercent = 1+random.nextInt(5);
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

    final AtomicLong operations = new AtomicLong(1000);  // number of query operations to perform in total - crank up if
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
    writer.setDoRandomForceMergeAssert(false);

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
                  Map<Integer,DocInfo> newCommittedModel;
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
                    newCommittedModel = new HashMap<Integer,DocInfo>(model);  // take a snapshot
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
                DocInfo info = model.get(id);
                long val = info.val;
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
                  model.put(id, new DocInfo(0,-nextVal));
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
                  model.put(id, new DocInfo(0,-nextVal));
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
        Random rand = new Random(random.nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              DocInfo info;
              synchronized(TestRealTimeGet.this) {
                info = committedModel.get(id);
              }
              long val = info.val;

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
    final TermsEnum termsEnum = terms.iterator(null);
    if (!termsEnum.seekExact(termBytes, false)) {
      return -1;
    }
    DocsEnum docs = termsEnum.docs(MultiFields.getLiveDocs(r), null, false);
    int id = docs.nextDoc();
    if (id != DocIdSetIterator.NO_MORE_DOCS) {
      int next = docs.nextDoc();
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
    }
    return id == DocIdSetIterator.NO_MORE_DOCS ? -1 : id;
  }

}
