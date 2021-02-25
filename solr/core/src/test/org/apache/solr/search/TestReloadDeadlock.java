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


import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.TimeUnits;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

// This test takes approx 30 seconds on a 2012 MacBook Pro running in IntelliJ. There should be a bunch of
// update threads dumped out all waiting on DefaultSolrCoreState.getIndexWriter,
// DistributedUpdateProcessor.versionAdd(DistributedUpdateProcessor.java:1016)
// and the like in a "real" failure. If we have false=fails we should probably bump this timeout.
// See SOLR-7836
@TimeoutSuite(millis = 7 * TimeUnits.MINUTE)
@Nightly
public class TestReloadDeadlock extends TestRTGBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml", "schema15.xml");
  }

  public static void ifVerbose(Object... args) {
    if (VERBOSE) {
      // if (!log.isDebugEnabled()) return;
      StringBuilder sb = new StringBuilder("VERBOSE:");
      for (Object o : args) {
        sb.append(' ');
        sb.append(o == null ? "(null)" : o.toString());
      }
      log.info("{}", sb);
    }
  }

  @Test
  public void testReloadDeadlock() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random().nextInt(5);
    final int deleteByQueryPercent = 20 + random().nextInt(20);
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(50));
    int nWriteThreads = 5 + random().nextInt(10);

    // query variables
    final AtomicLong reloads = new AtomicLong(50);  // number of reloads. Increase this number to force failure.

    ifVerbose("commitPercent", commitPercent, "deleteByQueryPercent", deleteByQueryPercent
        , "ndocs", ndocs, "nWriteThreads", nWriteThreads, "reloads", reloads);

    initModel(ndocs);

    final AtomicBoolean areCommitting = new AtomicBoolean();

    List<Thread> threads = new ArrayList<>();

    final AtomicLong testVersion = new AtomicLong(0);

    for (int i = 0; i < nWriteThreads; i++) {
      Thread thread = new Thread("WRITER" + i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (reloads.get() > 0) {
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                if (areCommitting.compareAndSet(false, true)) {
                  Map<Integer, DocInfo> newCommittedModel;
                  long version;

                  synchronized (TestReloadDeadlock.this) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                  }

                  ifVerbose("hardCommit start");
                  assertU(commit());
                  ifVerbose("hardCommit end");

                  synchronized (TestReloadDeadlock.this) {
                    // install this model snapshot only if it's newer than the current one
                    if (version >= committedModelClock) {
                      ifVerbose("installing new committedModel version=" + committedModelClock);
                      committedModel = newCommittedModel;
                      committedModelClock = version;
                    }
                  }
                  areCommitting.set(false);
                }
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
              long nextVal = Math.abs(val) + 1;

              long version = testVersion.incrementAndGet();

              // yield after getting the next version to increase the odds of updates happening out of order
              if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deleteByQueryPercent) {
                deleteByQuery(id, nextVal, version);
              } else {
                addDoc(id, nextVal, version);
              }

              if (!before) {
                lastId = id;
              }
            }
          } catch (Throwable e) {
            reloads.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.start();
    }

    // The reload operation really doesn't need to happen from multiple threads, we just want it firing pretty often.
    while (reloads.get() > 0) {
      Thread.sleep(10 + random().nextInt(250));
      reloads.decrementAndGet();
      h.getCoreContainer().reload("collection1");
    }

    try {
      for (Thread thread : threads) {
        thread.join(10000); // Normally they'll all return immediately (or close to that).
      }
    } catch (InterruptedException ie) {
      fail("Shouldn't have sat around here this long waiting for the threads to join.");
    }
    for (Thread thread : threads) { // Probably a silly test, but what the heck.
      assertFalse("All threads should be dead, but at least thread " + thread.getName() + " is not", thread.isAlive());
    }
  }

  private void addDoc(int id, long nextVal, long version) throws Exception {
    ifVerbose("adding id", id, "val=", nextVal, "version", version);

    Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), FIELD, Long.toString(nextVal),
        "_version_", Long.toString(version)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
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

    ifVerbose("adding id", id, "val=", nextVal, "version", version, "DONE");
  }

  private void deleteByQuery(int id, long nextVal, long version) throws Exception {
    ifVerbose("deleteByQuery id", id, "val=", nextVal, "version", version);

    Long returnedVersion = deleteByQueryAndGetVersion("id:" + Integer.toString(id),
        params("_version_", Long.toString(-version), DISTRIB_UPDATE_PARAM, FROM_LEADER));

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

    ifVerbose("deleteByQuery id", id, "val=", nextVal, "version", version, "DONE");
  }
}
