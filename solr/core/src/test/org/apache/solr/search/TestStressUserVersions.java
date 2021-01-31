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


import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.TestHarness;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestStressUserVersions extends TestRTGBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-externalversionconstraint.xml","schema15.xml");
  }

  private static String vfield = "my_version_l";
  private static String lfield = "live_b";
  private static String dversion = "del_version";


  public static void verbose(Object... args) {
    // if (!log.isDebugEnabled()) return;
    StringBuilder sb = new StringBuilder("VERBOSE:");
    for (Object o : args) {
      sb.append(' ');
      sb.append(o==null ? "(null)" : o.toString());
    }
    log.info("{}", sb);
  }

  // This version simulates user versions sometimes being reordered.
  // It should fail (and currently does) if optimistic concurrency is disabled (cmd.setVersion(currVersion))
  // in DocBasedVersionConstraintsProcessor.
  @Test
  public void testStressReorderVersions() throws Exception {
    clearIndex();
    assertU(commit());

    final int commitPercent = 5 + random().nextInt(TEST_NIGHTLY ? 20 : 3);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = random().nextInt(8);
    final int ndocs = 5 + (TEST_NIGHTLY ? (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200)) : 7);
    int nWriteThreads = 1 + random().nextInt(TEST_NIGHTLY ? 12 : 2);

    final int maxConcurrentCommits = nWriteThreads;

    // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong( TEST_NIGHTLY ? 10000 : 75);  // number of query operations to perform in total - ramp up for a longer test
    int nReadThreads = 1 + random().nextInt(TEST_NIGHTLY ? 12 : 2);


    /** // testing
     final int commitPercent = 5;
     final int softCommitPercent = 100; // what percent of the commits are soft
     final int deletePercent = 0;
     final int deleteByQueryPercent = 50;
     final int ndocs = 1;
     int nWriteThreads = 2;

     final int maxConcurrentCommits = nWriteThreads;

     // query variables
     final int percentRealtimeQuery = 101;
     final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
     int nReadThreads = 1;
     **/


    verbose("commitPercent",commitPercent, "softCommitPercent",softCommitPercent, "deletePercent",deletePercent, "deleteByQueryPercent",deleteByQueryPercent
        , "ndocs",ndocs,"nWriteThreads",nWriteThreads,"percentRealtimeQuery",percentRealtimeQuery,"operations",operations, "nReadThreads",nReadThreads);

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();
    TimeOut timeout = new TimeOut(TEST_NIGHTLY ? 8 : 2, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    final AtomicLong testVersion = new AtomicLong(0);
    List<Callable<Object>> threads = new ArrayList<>();
    for (int i=0; i<nWriteThreads; i++) {
      Callable<Object> thread = new Callable<>() {
        Random rand = new Random(random().nextInt());

        @Override
        public Object call() {
          try {
            while (operations.get() > 0) {
              if (timeout.hasTimedOut()) {
                return null;
              }
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                  Map<Integer, DocInfo> newCommittedModel;
                  long version;

                  synchronized (TestStressUserVersions.this) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                  }

                  if (rand.nextInt(100) < softCommitPercent) {
                    verbose("softCommit start");
                    assertU(TestHarness.commit("softCommit", "true"));
                    verbose("softCommit end");
                  } else {
                    verbose("hardCommit start");
                    assertU(commit());
                    verbose("hardCommit end");
                  }

                  synchronized (TestStressUserVersions.this) {
                    // install this model snapshot only if it's newer than the current one
                    if (version >= committedModelClock) {
                      if (VERBOSE) {
                        verbose("installing new committedModel version=" + committedModelClock);
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
              long nextVal = Math.abs(val) + 1;

              // the version we set on the update should determine who wins
              // These versions are not derived from the actual leader update handler hand hence this
              // test may need to change depending on how we handle version numbers.
              long version = testVersion.incrementAndGet();

              if (oper < commitPercent + deletePercent) {
                verbose("deleting id", id, "val=", nextVal, "version", version);

                Long returnedVersion = deleteAndGetVersion(Integer.toString(id), params(dversion, Long.toString(version)));

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (Math.abs(version) > Math.abs(currInfo.version)) {
                    model.put(id, new DocInfo(version, -nextVal));
                  }
                }

                verbose("deleting id", id, "val=", nextVal, "version", version, "DONE");

              } else {
                verbose("adding id", id, "val=", nextVal, "version", version);

                Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), FIELD, Long.toString(nextVal), vfield, Long.toString(version)), null);

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (version > currInfo.version) {
                    model.put(id, new DocInfo(version, nextVal));
                  }
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal, "version", version, "DONE");
                }

              }
              // }   // end sync

              if (!before) {
                lastId = id;
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }

          return null;
        }

        ;
      };
      threads.add(thread);
    }


    for (int i=0; i<nReadThreads; i++) {
      Callable<Object> thread = new Callable<>() {
        Random rand = new Random(random().nextInt());

        @Override
        public Object call() {
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
                synchronized(TestStressUserVersions.this) {
                  info = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                verbose("querying id", id);
              }
              SolrQueryRequest sreq = null;
              String response;
              try {
                if (realTime) {
                  sreq = req("wt", "json", "qt", "/get", "ids", Integer.toString(id));
                } else {
                  sreq = req("wt", "json", "q", "id:" + Integer.toString(id), "omitHeader", "true");
                }

                response = h.query(sreq);
              } finally {
                IOUtils.closeQuietly(sreq);
              }
              Map rsp = (Map) Utils.fromJSONString(response);
              List doclist = (List)(((Map)rsp.get("response")).get("docs"));
              if (doclist.size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else {
                assertEquals(1, doclist.size());
                boolean isLive = (Boolean)(((Map)doclist.get(0)).get(lfield));
                long foundVer = (Long)(((Map)doclist.get(0)).get(vfield));

                if (isLive) {
                  long foundVal = (Long)(((Map)doclist.get(0)).get(FIELD));
                  if (foundVer < Math.abs(info.version)
                      || (foundVer == info.version && foundVal != info.val) ) {    // if the version matches, the val must
                    log.error("ERROR, id={} found={} model {}", id, response, info);
                    assertTrue("ERROR, id=" + id + " found=" + response + " model " + info, false);
                  }
                } else {
                  // if the doc is deleted (via tombstone), it shouldn't have a value on it.
                  assertNull( ((Map)doclist.get(0)).get(FIELD) );

                  if (foundVer < Math.abs(info.version)) {
                    log.error("ERROR, id={} found={} model {}", id, response, info);
                    assertTrue("ERROR, id=" + id + " found=" + response + " model " + info, false);
                  }
                }

              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("",e);
            throw new RuntimeException(e);
          }
          return null;
        }
      };

      threads.add(thread);
    }

    getTestExecutor().invokeAll(threads);
  }

}
