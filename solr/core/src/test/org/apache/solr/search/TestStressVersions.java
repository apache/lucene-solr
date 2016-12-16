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


import org.noggit.ObjectBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.core.SolrCore.verbose;

public class TestStressVersions extends TestRTGBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }


  // This version doesn't synchronize on id to tell what update won, but instead uses versions
  @Test
  public void testStressGetRealtimeVersions() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = 1 + random().nextInt(5);
    final int optimisticPercent = 1+random().nextInt(50);    // percent change that an update uses optimistic locking
    final int optimisticCorrectPercent = 25+random().nextInt(70);    // percent change that a version specified will be correct
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 5 + random().nextInt(25);

    final int maxConcurrentCommits = nWriteThreads;

    // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 5 + random().nextInt(25);



    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<>();

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

                  synchronized(globalLock) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                  }

                  if (rand.nextInt(100) < softCommitPercent) {
                    verbose("softCommit start");
                    assertU(TestHarness.commit("softCommit","true"));
                    verbose("softCommit end");
                  } else {
                    verbose("hardCommit start");
                    assertU(commit());
                    verbose("hardCommit end");
                  }

                  synchronized(globalLock) {
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
                synchronized(globalLock) {
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

  }


}
