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


import org.apache.lucene.util.Constants;
import org.noggit.ObjectBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.core.SolrCore.verbose;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class TestStressRecovery extends TestRTGBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema15.xml");
  }


  // This points to the live model when state is ACTIVE, but a snapshot of the
  // past when recovering.
  volatile ConcurrentHashMap<Integer,DocInfo> visibleModel;

  // This version simulates updates coming from the leader and sometimes being reordered
  // and tests the ability to buffer updates and apply them later
  @Test
  public void testStressRecovery() throws Exception {
    assumeFalse("FIXME: This test is horribly slow sometimes on Windows!", Constants.WINDOWS);
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random().nextInt(10);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = random().nextInt(5);
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 2 + random().nextInt(10);  // fewer write threads to give recovery thread more of a chance

    final int maxConcurrentCommits = nWriteThreads;

    // query variables
    final int percentRealtimeQuery = 75;
    final int percentGetLatestVersions = random().nextInt(4);
    final AtomicLong operations = new AtomicLong(atLeast(100));  // number of recovery loops to perform
    int nReadThreads = 2 + random().nextInt(10);  // fewer read threads to give writers more of a chance

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();

    List<Thread> threads = new ArrayList<>();


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
        Random rand = new Random(random().nextInt());
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

                  synchronized(globalLock) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                  }

                  synchronized (stateChangeLock) {
                    // These commits won't take affect if we are in recovery mode,
                    // so change the version to -1 so we won't update our model.
                    if (uLog.getState() != UpdateLog.State.ACTIVE) version = -1;
                    if (rand.nextInt(100) < softCommitPercent) {
                      verbose("softCommit start");
                      assertU(TestHarness.commit("softCommit","true"));
                      verbose("softCommit end");
                    } else {
                      verbose("hardCommit start");
                      assertU(commit());
                      verbose("hardCommit end");
                    }
                  }

                  synchronized(globalLock) {
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

                Long returnedVersion = deleteAndGetVersion(Integer.toString(id), params("_version_",Long.toString(-version), DISTRIB_UPDATE_PARAM,FROM_LEADER));

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

                verbose("deleteByQuery id",id,"val=",nextVal,"version",version);

                Long returnedVersion = deleteByQueryAndGetVersion("id:"+Integer.toString(id), params("_version_",Long.toString(-version), DISTRIB_UPDATE_PARAM,FROM_LEADER));

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

                verbose("deleteByQuery id", id, "val=",nextVal,"version",version,"DONE");

              } else {
                verbose("adding id", id, "val=", nextVal,"version",version);

                Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal), "_version_",Long.toString(version)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
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
                synchronized(globalLock) {
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


            if (rand.nextInt(100) < percentGetLatestVersions) {
              getLatestVersions();
              // TODO: some sort of validation that the latest version is >= to the latest version we added?
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

    int bufferedAddsApplied = 0;
    do {
      assertTrue(uLog.getState() == UpdateLog.State.ACTIVE);

      // before we start buffering updates, we want to point
      // visibleModel away from the live model.

      visibleModel = new ConcurrentHashMap<>(model);

      synchronized (stateChangeLock) {
        uLog.bufferUpdates();
      }

      assertTrue(uLog.getState() == UpdateLog.State.BUFFERING);

      // sometimes wait for a second to allow time for writers to write something
      if (random().nextBoolean()) Thread.sleep(random().nextInt(10)+1);

      Future<UpdateLog.RecoveryInfo> recoveryInfoF = uLog.applyBufferedUpdates();
      if (recoveryInfoF != null) {
        UpdateLog.RecoveryInfo recInfo = null;

        int writeThreadNumber = 0;
        while (recInfo == null) {
          try {
            // wait a short period of time for recovery to complete (and to give a chance for more writers to concurrently add docs)
            recInfo = recoveryInfoF.get(random().nextInt(100/nWriteThreads), TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            // idle one more write thread
            verbose("Operation",operations.get(),"Draining permits for write thread",writeThreadNumber);
            writePermissions[writeThreadNumber++].drainPermits();
            if (writeThreadNumber >= nWriteThreads) {
              // if we hit the end, back up and give a few write permits
              writeThreadNumber--;
              writePermissions[writeThreadNumber].release(random().nextInt(2) + 1);
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

}
