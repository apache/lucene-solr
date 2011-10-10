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


import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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


  public static void verbose(Object... args) {
    if (!VERBOSE) return;
    StringBuilder sb = new StringBuilder("TEST:");
    sb.append(Thread.currentThread().getName());
    sb.append(':');
    for (Object o : args) {
      sb.append(' ');
      sb.append(o.toString());
    }
    System.out.println(sb.toString());
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


  @Test
  public void testStressGetRealtime() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(60); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // real-time get isn't currently supported with delete-by-query
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 60;
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
                  verbose("commit start");
                  assertU(commit());
                  verbose("commit end");
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
                Long version = deleteAndGetVersion(Integer.toString(id));

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
                Long version = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal)));
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



  @Test
  public void testStressGetRealtimeVersions() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random.nextInt(20);
    final int softCommitPercent = 30+random.nextInt(60); // what percent of the commits are soft
    final int deletePercent = 4+random.nextInt(25);
    final int deleteByQueryPercent = 0;  // real-time get isn't currently supported with delete-by-query
    final int ndocs = 5 + (random.nextBoolean() ? random.nextInt(25) : random.nextInt(200));
    int nWriteThreads = 5 + random.nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

        // query variables
    final int percentRealtimeQuery = 60;
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
                  verbose("commit start");
                  assertU(commit());
                  verbose("commit end");
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
           // synchronized (sync) {
              DocInfo info = model.get(id);

              long val = info.val;
              long nextVal = Math.abs(val)+1;

              if (oper < commitPercent + deletePercent) {
                if (VERBOSE) {
                  verbose("deleting id",id,"val=",nextVal);
                }

                // assertU("<delete><id>" + id + "</id></delete>");
                Long version = deleteAndGetVersion(Integer.toString(id));
                assertTrue(version < 0);

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

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
                Long version = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal)));
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


  private Long addAndGetVersion(SolrInputDocument sdoc) throws Exception {
    String response = updateJ(jsonAdd(sdoc), null);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    return (Long) ((List)rsp.get("adds")).get(1);
  }

  private Long deleteAndGetVersion(String id) throws Exception {
    String response = updateJ("{\"delete\":{\"id\":\""+id+"\"}}", null);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    return (Long) ((List)rsp.get("deletes")).get(1);
  }


}
