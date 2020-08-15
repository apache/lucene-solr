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


import java.io.File;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.search.TestRecovery.VersionProvider.getNextVersion;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class TestRecovery extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // means that we've seen the leader and have version info (i.e. we are a non-leader replica)
  private static String FROM_LEADER = DistribPhase.FROMLEADER.toString(); 

  private static int timeout=60;  // acquire timeout in seconds.  change this to a huge number when debugging to prevent threads from advancing.

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;


  @Before
  public void beforeTest() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    randomizeUpdateLogImpl();
    initCore("solrconfig-tlog.xml","schema15.xml");
    
    // validate that the schema was not changed to an unexpected state
    IndexSchema schema = h.getCore().getLatestSchema();
    assertTrue(schema.getFieldOrNull("_version_").hasDocValues() && !schema.getFieldOrNull("_version_").indexed()
        && !schema.getFieldOrNull("_version_").stored());

  }
  
  @After
  public void afterTest() {
    TestInjection.reset(); // do after every test, don't wait for AfterClass
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
    
    deleteCore();
  }

  private Map<String, Metric> getMetrics() {
    SolrMetricManager manager = h.getCoreContainer().getMetricManager();
    MetricRegistry registry = manager.registry(h.getCore().getCoreMetricManager().getRegistryName());
    return registry.getMetrics();
  }

  @Test
  public void stressLogReplay() throws Exception {
    final int NUM_UPDATES = 150;
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = logReplayFinish::release;
      clearIndex();
      assertU(commit());
      Map<Integer, Integer> docIdToVal = new HashMap<>();
      for (int i = 0; i < NUM_UPDATES; i++) {
        int kindOfUpdate = random().nextInt(100);
        if (docIdToVal.size() < 10) kindOfUpdate = 0;
        if (kindOfUpdate <= 50) {
          // add a new document update, may by duplicate with the current one
          int val = random().nextInt(1000);
          int docId = random().nextInt(10000);
          addAndGetVersion(sdoc("id", String.valueOf(docId), "val_i_dvo", val), null);
          docIdToVal.put(docId, val);
        } else if (kindOfUpdate <= 80) {
          // inc val of a document
          ArrayList<Integer> ids = new ArrayList<>(docIdToVal.keySet());
          int docId = ids.get(random().nextInt(ids.size()));
          int delta = random().nextInt(10);
          addAndGetVersion(sdoc("id", String.valueOf(docId), "val_i_dvo", map("inc", delta)), null);
          docIdToVal.put(docId, docIdToVal.get(docId) + delta);
        } else if (kindOfUpdate <= 85) {
          // set val of a document
          ArrayList<Integer> ids = new ArrayList<>(docIdToVal.keySet());
          int docId = ids.get(random().nextInt(ids.size()));
          int val = random().nextInt(1000);
          addAndGetVersion(sdoc("id", String.valueOf(docId), "val_i_dvo", map("set", val)), null);
          docIdToVal.put(docId, val);
        } else if (kindOfUpdate <= 90) {
          // delete by id
          ArrayList<Integer> vals = new ArrayList<>(docIdToVal.values());
          int val = vals.get(random().nextInt(vals.size()));
          deleteByQueryAndGetVersion("val_i_dvo:"+val, null);
          docIdToVal.entrySet().removeIf(integerIntegerEntry -> integerIntegerEntry.getValue() == val);
        } else {
          // delete by query
          ArrayList<Integer> ids = new ArrayList<>(docIdToVal.keySet());
          int docId = ids.get(random().nextInt(ids.size()));
          deleteAndGetVersion(String.valueOf(docId), null);
          docIdToVal.remove(docId);
        }
      }

      h.close();
      createCore();
      assertJQ(req("q","*:*") ,"/response/numFound==0");
      // unblock recovery
      logReplay.release(Integer.MAX_VALUE);
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertU(commit());
      assertJQ(req("q","*:*") ,"/response/numFound=="+docIdToVal.size());

      for (Map.Entry<Integer, Integer> entry : docIdToVal.entrySet()) {
        assertJQ(req("q","id:"+entry.getKey(), "fl", "val_i_dvo") ,
            "/response/numFound==1",
            "/response/docs==[{'val_i_dvo':"+entry.getValue()+"}]");
      }
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  @Test
  public void testLogReplay() throws Exception {
    
    try {

      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


      clearIndex();
      assertU(commit());

      Deque<Long> versions = new ArrayDeque<>();
      versions.addFirst(addAndGetVersion(sdoc("id", "A1"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A11"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A12"), null));
      versions.addFirst(deleteByQueryAndGetVersion("id:A11", null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A13"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A12", "val_i_dvo", map("set", 1)), null)); // atomic update
      versions.addFirst(addAndGetVersion(sdoc("id", "A12", "val_i_dvo", map("set", 2)), null)); // in-place update
      assertJQ(req("q","*:*"),"/response/numFound==0");

      assertJQ(req("qt","/get", "getVersions",""+versions.size()) ,"/versions==" + versions);

      h.close();
      createCore();

      Map<String, Metric> metrics = getMetrics(); // live map view

      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // verify that previous close didn't do a commit
      // recovery should be blocked by our hook
      assertJQ(req("q","*:*") ,"/response/numFound==0");

      // make sure we can still access versions after a restart
      assertJQ(req("qt","/get", "getVersions",""+versions.size()),"/versions==" + versions);

      assertEquals(UpdateLog.State.REPLAYING, h.getCore().getUpdateHandler().getUpdateLog().getState());
      // check metrics
      @SuppressWarnings({"unchecked"})
      Gauge<Integer> state = (Gauge<Integer>)metrics.get("TLOG.state");
      assertEquals(UpdateLog.State.REPLAYING.ordinal(), state.getValue().intValue());
      @SuppressWarnings({"unchecked"})
      Gauge<Integer> replayingLogs = (Gauge<Integer>)metrics.get("TLOG.replay.remaining.logs");
      assertTrue(replayingLogs.getValue().intValue() > 0);
      @SuppressWarnings({"unchecked"})
      Gauge<Long> replayingDocs = (Gauge<Long>)metrics.get("TLOG.replay.remaining.bytes");
      assertTrue(replayingDocs.getValue().longValue() > 0);
      Meter replayDocs = (Meter)metrics.get("TLOG.replay.ops");
      long initialOps = replayDocs.getCount();

      // unblock recovery
      logReplay.release(1000);

      // make sure we can still access versions during recovery
      assertJQ(req("qt","/get", "getVersions",""+versions.size()),"/versions==" + versions);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertJQ(req("q","val_i_dvo:2") ,"/response/numFound==1"); // assert that in-place update is retained

      assertJQ(req("q","*:*") ,"/response/numFound==3");

      assertEquals(7L, replayDocs.getCount() - initialOps);
      assertEquals(UpdateLog.State.ACTIVE.ordinal(), state.getValue().intValue());

      // make sure we can still access versions after recovery
      assertJQ(req("qt","/get", "getVersions",""+versions.size()) ,"/versions==" + versions);

      assertU(adoc("id","A2"));
      assertU(adoc("id","A3"));
      assertU(delI("A2"));
      assertU(adoc("id","A4"));

      assertJQ(req("q","*:*") ,"/response/numFound==3");
      assertJQ(req("q","val_i_dvo:2") ,"/response/numFound==1"); // assert that in-place update is retained

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertJQ(req("q","*:*") ,"/response/numFound==5");
      assertJQ(req("q","id:A2") ,"/response/numFound==0");

      // no updates, so insure that recovery does not run
      h.close();
      int permits = logReplay.availablePermits();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      assertJQ(req("q","*:*") ,"/response/numFound==5");
      assertJQ(req("q","val_i_dvo:2") ,"/response/numFound==1"); // assert that in-place update is retained
      Thread.sleep(100);
      assertEquals(permits, logReplay.availablePermits()); // no updates, so insure that recovery didn't run

      assertEquals(UpdateLog.State.ACTIVE, h.getCore().getUpdateHandler().getUpdateLog().getState());

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }

  }

  @Test
  public void testNewDBQAndDocMatchingOldDBQDuringLogReplay() throws Exception {
    try {

      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();

      clearIndex();
      assertU(commit());

      // because we're sending updates during log replay, we can't emulate replica logic -- we need to use
      // normal updates like a leader / single-node instance would get.
      //
      // (In SolrCloud mode, when a replica run recoverFromLog, replica in this time period will have state = DOWN,
      // so It won't receive any updates.)
      
      updateJ(jsonAdd(sdoc("id","B0")),params());
      updateJ(jsonAdd(sdoc("id","B1")),params()); // should be deleted by subsequent DBQ in tlog
      updateJ(jsonAdd(sdoc("id","B2")),params()); // should be deleted by DBQ that arives during tlog replay
      updateJ(jsonDelQ("id:B1 OR id:B3 OR id:B6"),params());
      updateJ(jsonAdd(sdoc("id","B3")),params()); // should *NOT* be deleted by previous DBQ in tlog
      updateJ(jsonAdd(sdoc("id","B4")),params()); // should be deleted by DBQ that arives during tlog replay
      updateJ(jsonAdd(sdoc("id","B5")),params());
      
      // sanity check no updates have been applied yet (just in tlog)
      assertJQ(req("q","*:*"),"/response/numFound==0");

      h.close();
      createCore(); // (Attempts to) kick off recovery (which is currently blocked by semaphore)

      // verify that previous close didn't do a commit & that recovery should be blocked by our hook
      assertJQ(req("q","*:*") ,"/response/numFound==0");

      // begin recovery (first few items)
      logReplay.release(TestUtil.nextInt(random(),1,6));
      // ... but before recover is completely unblocked/finished, have a *new* DBQ arrive
      // that should delete some items we either have just replayed, or are about to replay (or maybe both)...
      updateJ(jsonDelQ("id:B2 OR id:B4"),params());
      // ...and re-add a doc that would have matched a DBQ already in the tlog
      // (which may/may-not have been replayed yet)
      updateJ(jsonAdd(sdoc("id","B6")),params()); // should *NOT* be deleted by DBQ from tlog
      assertU(commit());

      // now completely unblock recovery
      logReplay.release(1000);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      // verify only the expected docs are found, even with out of order DBQ and DBQ that arived during recovery
      assertJQ(req("q", "*:*", "fl", "id", "sort", "id asc")
               , "/response/docs==[{'id':'B0'}, {'id':'B3'}, {'id':'B5'}, {'id':'B6'}]");
      
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }

  }

  @Test
  public void testLogReplayWithReorderedDBQ() throws Exception {
    testLogReplayWithReorderedDBQWrapper(() -> {
          String v1010 = getNextVersion();
          String v1015 = getNextVersion();
          String v1017_del = "-" + getNextVersion();
          String v1020 = getNextVersion();
          
          updateJ(jsonAdd(sdoc("id", "RDBQ1_1", "_version_", v1010)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          updateJ(jsonDelQ("id:RDBQ1_2"), params(DISTRIB_UPDATE_PARAM, FROM_LEADER, "_version_", v1017_del)); // This should've arrived after the ver2 update
          updateJ(jsonAdd(sdoc("id", "RDBQ1_2", "_version_", v1015)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          updateJ(jsonAdd(sdoc("id", "RDBQ1_3", "_version_", v1020)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
        },
        () -> assertJQ(req("q", "*:*"), "/response/numFound==2")
    );
  }

  @Test
  public void testLogReplayWithReorderedDBQByAsterixAndChildDocs() throws Exception {
    testLogReplayWithReorderedDBQWrapper(() -> {
          String v1010 = getNextVersion();
          String v1012 = getNextVersion();
          String v1017_del = "-" + getNextVersion();
          String v1018 = getNextVersion();
          String v1020 = getNextVersion();
          
          // 1010 - will be deleted
          updateJ(jsonAdd(sdocWithChildren("RDBQ2_1", v1010)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1018 - should be kept, including child docs
          updateJ(jsonAdd(sdocWithChildren("RDBQ2_2", v1018)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1017 - delete should affect only 1010
          updateJ(jsonDelQ("_root_:RDBQ2_1 _root_:RDBQ2_2 id:RDBQ2_3 _root_:RDBQ2_4"), params(DISTRIB_UPDATE_PARAM, FROM_LEADER, "_version_", v1017_del)); // This should've arrived after the ver2 update
          // 1012 - will be deleted
          updateJ(jsonAdd(sdoc("id", "RDBQ2_3", "_version_", v1012)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1020 - should be untouched
          updateJ(jsonAdd(sdocWithChildren("RDBQ2_4", v1020)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
        },
        () -> assertJQ(req("q", "*:*"), "/response/numFound==6")
    );
  }

  @Test
  public void testLogReplayWithReorderedDBQByIdAndChildDocs() throws Exception {
    testLogReplayWithReorderedDBQWrapper(() -> {
          String v1010 = getNextVersion();
          String v1012 = getNextVersion();
          String v1017_del = "-" + getNextVersion();
          String v1018 = getNextVersion();
          String v1020 = getNextVersion();
      
          // 1010 - will be deleted
          updateJ(jsonAdd(sdocWithChildren("RDBQ3_1", v1010)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1018 - should be kept, including child docs
          updateJ(jsonAdd(sdocWithChildren("RDBQ3_2", v1018)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1017 - delete should affect only 1010
          updateJ(jsonDelQ("id:RDBQ3_1 id:RDBQ3_2 id:RDBQ3_3 id:RDBQ3_4"), params(DISTRIB_UPDATE_PARAM, FROM_LEADER, "_version_", v1017_del)); // This should've arrived after the ver2 update
          // 1012 - will be deleted
          updateJ(jsonAdd(sdoc("id", "RDBQ3_3", "_version_", v1012)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1020 - should be untouched
          updateJ(jsonAdd(sdocWithChildren("RDBQ3_4", v1020)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
        },
        () -> assertJQ(req("q", "*:*"), "/response/numFound==8") // RDBQ3_2, RDBQ3_4 and 6 children docs (delete by id does not delete child docs)
    );
  }

  @Test
  public void testLogReplayWithReorderedDBQInsertingChildnodes() throws Exception {
    testLogReplayWithReorderedDBQWrapper(() -> {
          String v1013 = getNextVersion();
          String v1017_del = "-" + getNextVersion();
          
          updateJ(jsonDelQ("id:RDBQ4_2"), params(DISTRIB_UPDATE_PARAM, FROM_LEADER, "_version_", v1017_del));
          // test doc: B1
          // 1013 - will be inserted with 3 children
          updateJ(jsonAdd(sdocWithChildren("RDBQ4_1", v1013, 3)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
        },
        () -> assertJQ(req("q", "*:*"), "/response/numFound==4") // RDBQ4_1 and RDBQ4_2, plus 2x 3 children
    );
  }


  @Test
  public void testLogReplayWithReorderedDBQUpdateWithDifferentChildCount() throws Exception {
    testLogReplayWithReorderedDBQWrapper(() -> {
          String v1011 = getNextVersion();
          String v1012 = getNextVersion();
          String v1013 = getNextVersion();
          String v1018 = getNextVersion();
          String v1019_del = "-" + getNextVersion();
      
          // control
          // 1011 - will be inserted with 3 children as 1012
          updateJ(jsonAdd(sdocWithChildren("RDBQ5_1", v1011, 2)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          // 1012 - this should be the final
          updateJ(jsonAdd(sdocWithChildren("RDBQ5_1", v1012, 3)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));

          // 1013 - will be inserted with 3 children as 1018
          updateJ(jsonAdd(sdocWithChildren("RDBQ5_2", v1013, 2)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
          updateJ(jsonDelQ("id:RDBQ5_3"), params(DISTRIB_UPDATE_PARAM, FROM_LEADER, "_version_", v1019_del));
          // 1018 - this should be the final
          updateJ(jsonAdd(sdocWithChildren("RDBQ5_2", v1018, 3)), params(DISTRIB_UPDATE_PARAM, FROM_LEADER));
        },
        () -> assertJQ(req("q", "*:*"), "/response/numFound==8") // RDBQ5_1+3children+RDBQ5_2+3children
    );
  }

  private void testLogReplayWithReorderedDBQWrapper(ThrowingRunnable act, ThrowingRunnable assrt) throws Exception {

    try {

      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


      clearIndex();
      assertU(commit());

      // Adding some documents
      act.run();


      assertJQ(req("q", "*:*"), "/response/numFound==0");

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // verify that previous close didn't do a commit
      // recovery should be blocked by our hook
      assertJQ(req("q", "*:*"), "/response/numFound==0");

      // unblock recovery
      logReplay.release(1000);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      // Asserting
      assrt.run();

    } catch (Throwable thr) {
      throw new Exception(thr);
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }

  }

  @Test
  public void testBuffering() throws Exception {

    TestInjection.skipIndexWriterCommitOnClose = true;
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);

    UpdateLog.testing_logReplayHook = () -> {
      try {
        assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    UpdateLog.testing_logReplayFinishHook = logReplayFinish::release;


    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      clearIndex();
      assertU(commit());

      Map<String, Metric> metrics = getMetrics();

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture == null);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      @SuppressWarnings({"unchecked"})
      Gauge<Integer> state = (Gauge<Integer>)metrics.get("TLOG.state");
      assertEquals(UpdateLog.State.BUFFERING.ordinal(), state.getValue().intValue());
      @SuppressWarnings({"unchecked"})
      Gauge<Integer> bufferedOps = (Gauge<Integer>)metrics.get("TLOG.buffered.ops");
      int initialOps = bufferedOps.getValue();
      Meter applyingBuffered = (Meter)metrics.get("TLOG.applyingBuffered.ops");
      long initialApplyingOps = applyingBuffered.getCount();
      
      String v3 = getNextVersion();
      String v940_del = "-" + getNextVersion();
      String v950_del = "-" + getNextVersion();
      String v1010 = getNextVersion();
      String v1015 = getNextVersion();
      String v1017_del = "-" + getNextVersion();
      String v1020 = getNextVersion();
      String v1030 = getNextVersion();
      String v1040 = getNextVersion();
      String v1050 = getNextVersion();
      String v1060 = getNextVersion();
      String v1070 = getNextVersion();
      String v1080 = getNextVersion();
      String v2010_del = "-" + getNextVersion();
      String v2060_del = "-" + getNextVersion();
      String v3000_del = "-" + getNextVersion();

      String versionListFirstCheck = String.join(",", v2010_del, v1030, v1020, v1017_del, v1015, v1010);
      String versionListSecondCheck = String.join(",", v3000_del, v1080, v1050, v1060, v940_del, v1040 ,v3, v2010_del, v1030, v1020, v1017_del, v1015, v1010);

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","B1", "_version_",v1010)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B11", "_version_",v1015)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonDelQ("id:B1 id:B11 id:B2 id:B3"), params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v1017_del));
      updateJ(jsonAdd(sdoc("id","B2", "_version_",v1020)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B3", "_version_",v1030)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      deleteAndGetVersion("B1", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v2010_del));

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':["+versionListFirstCheck+"]}"
      );

      assertU(commit());

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':["+versionListFirstCheck+"]}"
      );

      // updates should be buffered, so we should not see any results yet.
      assertJQ(req("q", "*:*")
          , "/response/numFound==0"
      );

      // real-time get should also not show anything (this could change in the future,
      // but it's currently used for validating version numbers too, so it would
      // be bad for updates to be visible if we're just buffering.
      assertJQ(req("qt","/get", "id","B3")
          ,"=={'doc':null}"
      );

      assertEquals(6, bufferedOps.getValue().intValue() - initialOps);

      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);

      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      logReplay.release(1000);

      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      assertEquals(6L, applyingBuffered.getCount() - initialApplyingOps);

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':["+versionListFirstCheck+"]}"
      );


      assertJQ(req("q", "*:*")
          , "/response/numFound==2"
      );

      // move back to recovering
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      Long ver = getVer(req("qt","/get", "id","B3"));
      assertEquals(Long.valueOf(v1030), ver);

      // add a reordered doc that shouldn't overwrite one in the index
      updateJ(jsonAdd(sdoc("id","B3", "_version_",v3)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // reorder two buffered updates
      updateJ(jsonAdd(sdoc("id","B4", "_version_",v1040)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      deleteAndGetVersion("B4", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v940_del));   // this update should not take affect
      updateJ(jsonAdd(sdoc("id","B6", "_version_",v1060)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B5", "_version_",v1050)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B8", "_version_",v1080)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // test that delete by query is at least buffered along with everything else so it will delete the
      // currently buffered id:8 (even if it doesn't currently support versioning)
      updateJ("{\"delete\": { \"query\":\"id:B2 OR id:B8\" }}", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v3000_del));

      assertJQ(req("qt","/get", "getVersions","13")
          ,"=={'versions':[" + versionListSecondCheck + "]}"  // the "3" appears because versions aren't checked while buffering
      );

      logReplay.drainPermits();
      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);
      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      // apply a single update
      logReplay.release(1);

      // now add another update
      updateJ(jsonAdd(sdoc("id","B7", "_version_",v1070)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // a reordered update that should be dropped
      deleteAndGetVersion("B5", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v950_del));

      deleteAndGetVersion("B6", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_",v2060_del));

      logReplay.release(1000);
      UpdateLog.RecoveryInfo recInfo = rinfoFuture.get();

      assertJQ(req("q", "*:*", "sort","id asc", "fl","id,_version_")
          , "/response/docs==["
                           + "{'id':'B3','_version_':"+v1030+"}"
                           + ",{'id':'B4','_version_':"+v1040+"}"
                           + ",{'id':'B5','_version_':"+v1050+"}"
                           + ",{'id':'B7','_version_':"+v1070+"}"
                           +"]"
      );

      assertEquals(1, recInfo.deleteByQuery);

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state

      assertEquals(0, bufferedOps.getValue().intValue());
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }

  }


  @Test
  public void testDropBuffered() throws Exception {

    TestInjection.skipIndexWriterCommitOnClose = true;
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);

    UpdateLog.testing_logReplayHook = () -> {
      try {
        assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      String v101 = getNextVersion();
      String v102 = getNextVersion();
      String v103 = getNextVersion();
      String v104 = getNextVersion();
      String v105 = getNextVersion();
      String v200 = getNextVersion();
      String v201 = getNextVersion();
      String v203 = getNextVersion();
      String v204 = getNextVersion();
      String v205 = getNextVersion();
      String v206 = getNextVersion();
      String v301 = getNextVersion();
      String v302 = getNextVersion();
      String v998 = getNextVersion();
      String v999 = getNextVersion();
      
      clearIndex();
      assertU(commit());

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture == null);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","C1", "_version_",v101)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C2", "_version_",v102)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C3", "_version_",v103)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id", "C4", "_version_",v104)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id", "C5", "_version_",v105)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      logReplay.release(1000);
      rinfoFuture = ulog.applyBufferedUpdates();
      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':["+v105+","+v104+"]}"
      );

      // this time add some docs first before buffering starts (so tlog won't be at pos 0)
      updateJ(jsonAdd(sdoc("id","C100", "_version_",v200)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C101", "_version_",v201)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","C103", "_version_",v203)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C104", "_version_",v204)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","C105", "_version_",v205)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C106", "_version_",v206)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      rinfoFuture = ulog.applyBufferedUpdates();
      rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("q", "*:*", "sort","_version_ asc", "fl","id,_version_")
          , "/response/docs==["
          + "{'id':'C4','_version_':"+v104+"}"
          + ",{'id':'C5','_version_':"+v105+"}"
          + ",{'id':'C100','_version_':"+v200+"}"
          + ",{'id':'C101','_version_':"+v201+"}"
          + ",{'id':'C105','_version_':"+v205+"}"
          + ",{'id':'C106','_version_':"+v206+"}"
          +"]"
      );

      // Note that the v101->v103 are dropped, therefore it does not present in RTG
      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':["+String.join(",",v206,v205,v201,v200,v105,v104)+"]}"
      );

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      updateJ(jsonAdd(sdoc("id","C301", "_version_",v998)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C302", "_version_",v999)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      assertTrue(ulog.dropBufferedUpdates());

      // make sure we can overwrite with a lower version
      // TODO: is this functionality needed?
      updateJ(jsonAdd(sdoc("id","C301", "_version_",v301)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C302", "_version_",v302)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertU(commit());

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':["+v302+","+v301+"]}"
      );

      assertJQ(req("q", "*:*", "sort","_version_ desc", "fl","id,_version_", "rows","2")
          , "/response/docs==["
          + "{'id':'C302','_version_':"+v302+"}"
          + ",{'id':'C301','_version_':"+v301+"}"
          +"]"
      );


      updateJ(jsonAdd(sdoc("id","C2", "_version_",v302)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));




      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }

  }

  @Test
  public void testBufferedMultipleCalls() throws Exception {

    TestInjection.skipIndexWriterCommitOnClose = true;
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);

    UpdateLog.testing_logReplayHook = () -> {
      try {
        assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();
    Future<UpdateLog.RecoveryInfo> rinfoFuture;

    try {
      String v101 = getNextVersion();
      String v102 = getNextVersion();
      String v103 = getNextVersion();
      String v104 = getNextVersion();
      String v105 = getNextVersion();
      String v200 = getNextVersion();
      String v201 = getNextVersion();
      String v203 = getNextVersion();
      String v204 = getNextVersion();
      String v205 = getNextVersion();
      String v206 = getNextVersion();
      
      clearIndex();
      assertU(commit());
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","c1", "_version_",v101)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","c2", "_version_",v102)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","c3", "_version_",v103)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // call bufferUpdates again (this currently happens when recovery fails)... we should get a new starting point
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      updateJ(jsonAdd(sdoc("id", "c4", "_version_",v104)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id", "c5", "_version_",v105)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      logReplay.release(1000);
      rinfoFuture = ulog.applyBufferedUpdates();
      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':["+v105+","+v104+"]}"
      );

      updateJ(jsonAdd(sdoc("id","c100", "_version_",v200)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","c101", "_version_",v201)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","c103", "_version_",v203)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","c104", "_version_",v204)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // call bufferUpdates again (this currently happens when recovery fails)... we should get a new starting point
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","c105", "_version_",v205)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","c106", "_version_",v206)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      rinfoFuture = ulog.applyBufferedUpdates();
      rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("q", "*:*", "sort","_version_ asc", "fl","id,_version_")
          , "/response/docs==["
              + "{'id':'c4','_version_':"+v104+"}"
              + ",{'id':'c5','_version_':"+v105+"}"
              + ",{'id':'c100','_version_':"+v200+"}"
              + ",{'id':'c101','_version_':"+v201+"}"
              + ",{'id':'c105','_version_':"+v205+"}"
              + ",{'id':'c106','_version_':"+v206+"}"
+""              +"]"
      );

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':["+String.join(",",v206,v205,v201,v200,v105,v104)+"]}"
      );

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }

  }


  // we need to make sure that the log is informed of a core reload
  @Test
  public void testReload() throws Exception {
    long version = addAndGetVersion(sdoc("id","reload1") , null);

    h.reload();

    version = addAndGetVersion(sdoc("id","reload1", "_version_", Long.toString(version)), null);

    assertU(commit());

    // if we try the optimistic concurrency again, the tlog lookup maps should be clear
    // and we should go to the index to check the version.  This indirectly tests that
    // the update log was informed of the reload.  See SOLR-4858

    version = addAndGetVersion(sdoc("id","reload1", "_version_", Long.toString(version)), null);

    // a deleteByQuery currently forces open a new realtime reader via the update log.
    // This also tests that the update log was informed of the new update handler.

    deleteByQueryAndGetVersion("foo_t:hownowbrowncow", null);

    version = addAndGetVersion(sdoc("id","reload1", "_version_", Long.toString(version)), null);

    // if the update log was not informed of the new update handler, then the old core will
    // incorrectly be used for some of the operations above and opened searchers
    // will never be closed.  This used to cause the test framework to fail because of unclosed directory checks.
    // SolrCore.openNewSearcher was modified to throw an error if the core is closed, resulting in
    // a faster fail.
  }


  @Test
  public void testExistOldBufferLog() throws Exception {

    TestInjection.skipIndexWriterCommitOnClose = true;

    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      String v101 = getNextVersion();
      String v102 = getNextVersion();
      String v103 = getNextVersion();
      String v117 = getNextVersion();
      
      clearIndex();
      assertU(commit());

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","Q1", "_version_",v101)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","Q2", "_version_",v102)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","Q3", "_version_",v103)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      req.close();
      h.close();
      createCore();

      req = req();
      uhandler = req.getCore().getUpdateHandler();
      ulog = uhandler.getUpdateLog();

      // the core does not replay updates from buffer tlog on startup
      assertTrue(ulog.existOldBufferLog());   // since we died while buffering, we should see this last

      // buffer tlog won't be removed on restart
      req.close();
      h.close();
      createCore();

      req = req();
      uhandler = req.getCore().getUpdateHandler();
      ulog = uhandler.getUpdateLog();

      assertTrue(ulog.existOldBufferLog());

      ulog.bufferUpdates();
      ulog.applyBufferedUpdates();
      
      TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeout.waitFor("Timeout waiting for finish replay updates",
          () -> h.getCore().getUpdateHandler().getUpdateLog().getState() == UpdateLog.State.ACTIVE);
      
      updateJ(jsonAdd(sdoc("id","Q7", "_version_",v117)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER)); // do another add to make sure flags are back to normal

      req.close();
      h.close();
      createCore();

      req = req();
      uhandler = req.getCore().getUpdateHandler();
      
      UpdateLog updateLog = uhandler.getUpdateLog();

      // TODO this can fail
      // assertFalse(updateLog.existOldBufferLog());
      
      // Timeout for Q7 get replayed, because it was added on tlog, therefore it will be replayed on restart
      timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeout.waitFor("Timeout waiting for finish replay updates",
          () -> h.getCore().getUpdateHandler().getUpdateLog().getState() == UpdateLog.State.ACTIVE);
      
      assertJQ(req("qt","/get", "id", "Q7") ,"/doc/id==Q7");
    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }

  }



  // make sure that on a restart, versions don't start too low
  @Test
  public void testVersionsOnRestart() throws Exception {
    String v1 = getNextVersion();
    String v2 = getNextVersion();
    
    clearIndex();
    assertU(commit());

    assertU(adoc("id","D1", "val_i",v1));
    assertU(adoc("id","D2", "val_i",v1));
    assertU(commit());
    long D1Version1 = getVer(req("q","id:D1"));
    long D2Version1 = getVer(req("q","id:D2"));

    h.close();
    createCore();

    assertU(adoc("id","D1", "val_i",v2));
    assertU(commit());
    long D1Version2 = getVer(req("q","id:D1"));

    assert(D1Version2 > D1Version1);

    assertJQ(req("qt","/get", "getVersions","2")
        ,"/versions==[" + D1Version2 + "," + D2Version1 + "]"
    );

  }

  // make sure that log isn't needlessly replayed after a clean close
  @Test
  public void testCleanShutdown() throws Exception {
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);

    UpdateLog.testing_logReplayHook = () -> {
      try {
        assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      String v1 = getNextVersion();
      
      clearIndex();
      assertU(commit());

      assertU(adoc("id","E1", "val_i",v1));
      assertU(adoc("id","E2", "val_i",v1));

      // set to a high enough number so this test won't hang on a bug
      logReplay.release(10);

      h.close();
      createCore();

      // make sure the docs got committed
      assertJQ(req("q","*:*"),"/response/numFound==2");

      // make sure no replay happened
      assertEquals(10, logReplay.availablePermits());

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }
  }
  
  
  private void addDocs(int nDocs, int start, LinkedList<Long> versions) throws Exception {
    for (int i=0; i<nDocs; i++) {
      versions.addFirst( addAndGetVersion( sdoc("id",Integer.toString(start + nDocs)) , null) );
    }
  }

  @Test
  public void testRemoveOldLogs() throws Exception {
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


      clearIndex();
      assertU(commit());

      UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
      File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

      h.close();

      String[] files = ulog.getLogList(logDir);
      for (String file : files) {
        Files.delete(new File(logDir, file).toPath());
      }

      assertEquals(0, ulog.getLogList(logDir).length);

      createCore();

      int numIndexed = 0;
      int maxReq = 200;

      LinkedList<Long> versions = new LinkedList<>();

      int docsPerBatch = 3;
      // we don't expect to reach numRecordsToKeep as yet, so the bottleneck is still number of logs to keep
      int expectedToRetain = ulog.getMaxNumLogsToKeep() * docsPerBatch;
      int versExpected;

      for (int i = 1; i <= ulog.getMaxNumLogsToKeep() + 2; i ++) {
        addDocs(docsPerBatch, numIndexed, versions); numIndexed += docsPerBatch;
        versExpected = Math.min(numIndexed, expectedToRetain + docsPerBatch); // not yet committed, so one more tlog could slip in
        assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, versExpected)));
        assertU(commit());
        versExpected = Math.min(numIndexed, expectedToRetain);
        assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, versExpected)));
        assertEquals(Math.min(i, ulog.getMaxNumLogsToKeep()), ulog.getLogList(logDir).length);
      }

      docsPerBatch = ulog.getNumRecordsToKeep() + 20;
      // about to commit a lot of docs, so numRecordsToKeep becomes the bottleneck
      expectedToRetain = ulog.getNumRecordsToKeep();

      addDocs(docsPerBatch, numIndexed, versions);  numIndexed+=docsPerBatch;
      versExpected = Math.min(numIndexed, expectedToRetain);
      assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, versExpected)));
      assertU(commit());
      expectedToRetain = expectedToRetain - 1; // we lose a log entry due to the commit record
      versExpected = Math.min(numIndexed, expectedToRetain);
      assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, versExpected)));

      // previous logs should be gone now
      assertEquals(1, ulog.getLogList(logDir).length);

      addDocs(1, numIndexed, versions);  numIndexed+=1;
      h.close();
      createCore();      // trigger recovery, make sure that tlog reference handling is correct

      // test we can get versions while replay is happening
      assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, expectedToRetain)));

      logReplay.release(1000);
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      expectedToRetain = expectedToRetain - 1; // we lose a log entry due to the commit record made by recovery
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,expectedToRetain)));

      docsPerBatch = ulog.getNumRecordsToKeep() + 20;
      // about to commit a lot of docs, so numRecordsToKeep becomes the bottleneck
      expectedToRetain = ulog.getNumRecordsToKeep();

      addDocs(docsPerBatch, numIndexed, versions);  numIndexed+=docsPerBatch;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,expectedToRetain)));
      assertU(commit());
      expectedToRetain = expectedToRetain - 1; // we lose a log entry due to the commit record
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,expectedToRetain)));

      // previous logs should be gone now
      assertEquals(1, ulog.getLogList(logDir).length);

      //
      // test that a corrupt tlog file doesn't stop us from coming up, or seeing versions before that tlog file.
      //
      addDocs(1, numIndexed, new LinkedList<Long>()); // don't add this to the versions list because we are going to lose it...
      h.close();
      files = ulog.getLogList(logDir);
      Arrays.sort(files);
      try (RandomAccessFile raf = new RandomAccessFile(new File(logDir, files[files.length - 1]), "rw")) {
        raf.writeChars("This is a trashed log file that really shouldn't work at all, but we'll see...");
      }

      ignoreException("Failure to open existing");
      createCore();
      // we should still be able to get the list of versions (not including the trashed log file)
      assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, expectedToRetain)));
      resetExceptionIgnores();

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  //
  // test that a partially written last tlog entry (that will cause problems for both reverse reading and for
  // log replay) doesn't stop us from coming up, and from recovering the documents that were not cut off.
  //
  @Test
  public void testTruncatedLog() throws Exception {
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();

      UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
      File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

      clearIndex();
      assertU(commit());

      assertU(adoc("id","F1"));
      assertU(adoc("id","F2"));
      assertU(adoc("id","F3"));

      h.close();
      String[] files = ulog.getLogList(logDir);
      Arrays.sort(files);
      try (RandomAccessFile raf = new RandomAccessFile(new File(logDir, files[files.length - 1]), "rw")) {
        raf.seek(raf.length());  // seek to end
        raf.writeLong(0xffffffffffffffffL);
        raf.writeChars("This should be appended to a good log file, representing a bad partially written record.");
      }

      logReplay.release(1000);
      logReplayFinish.drainPermits();
      ignoreException("OutOfBoundsException");  // this is what the corrupted log currently produces... subject to change.
      createCore();
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      resetExceptionIgnores();
      assertJQ(req("q","*:*") ,"/response/numFound==3");

      //
      // Now test that the bad log file doesn't mess up retrieving latest versions
      //

      String v104 = getNextVersion();
      String v105 = getNextVersion();
      String v106 = getNextVersion();
      
      updateJ(jsonAdd(sdoc("id","F4", "_version_",v104)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","F5", "_version_",v105)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","F6", "_version_",v106)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // This currently skips the bad log file and also returns the version of the clearIndex (del *:*)
      // assertJQ(req("qt","/get", "getVersions","6"), "/versions==[106,105,104]");
      assertJQ(req("qt","/get", "getVersions","3"), "/versions==["+v106+","+v105+","+v104+"]");

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }


  //
  // test that a corrupt tlog doesn't stop us from coming up
  //
  @Test
  public void testCorruptLog() throws Exception {
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;

      UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
      File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

      clearIndex();
      assertU(commit());

      assertU(adoc("id","G1"));
      assertU(adoc("id","G2"));
      assertU(adoc("id","G3"));

      h.close();


      String[] files = ulog.getLogList(logDir);
      Arrays.sort(files);
      try (RandomAccessFile raf = new RandomAccessFile(new File(logDir, files[files.length - 1]), "rw")) {
        long len = raf.length();
        raf.seek(0);  // seek to start
        raf.write(new byte[(int) len]);  // zero out file
      }


      ignoreException("Failure to open existing log file");  // this is what the corrupted log currently produces... subject to change.
      createCore();
      resetExceptionIgnores();

      // just make sure it responds
      assertJQ(req("q","*:*") ,"/response/numFound==0");

      //
      // Now test that the bad log file doesn't mess up retrieving latest versions
      //
      String v104 = getNextVersion();
      String v105 = getNextVersion();
      String v106 = getNextVersion();

      updateJ(jsonAdd(sdoc("id","G4", "_version_",v104)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","G5", "_version_",v105)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","G6", "_version_",v106)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // This currently skips the bad log file and also returns the version of the clearIndex (del *:*)
      assertJQ(req("qt","/get", "getVersions","3"), "/versions==["+v106+","+v105+","+v104+"]");

      assertU(commit());

      assertJQ(req("q","*:*") ,"/response/numFound==3");

      // This messes up some other tests (on windows) if we don't remove the bad log.
      // This *should* hopefully just be because the tests are too fragile and not because of real bugs - but it should be investigated further.
      deleteLogs();

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }



  // in rare circumstances, two logs can be left uncapped (lacking a commit at the end signifying that all the content in the log was committed)
  @Test
  public void testRecoveryMultipleLogs() throws Exception {
    try {
      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();

      UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
      File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

      clearIndex();
      assertU(commit());

      assertU(adoc("id","AAAAAA"));
      assertU(adoc("id","BBBBBB"));
      assertU(adoc("id","CCCCCC"));

      h.close();
      String[] files = ulog.getLogList(logDir);
      Arrays.sort(files);
      String fname = files[files.length-1];
      byte[] content;
      try (RandomAccessFile raf = new RandomAccessFile(new File(logDir, fname), "rw")) {
        raf.seek(raf.length());  // seek to end
        raf.writeLong(0xffffffffffffffffL);
        raf.writeChars("This should be appended to a good log file, representing a bad partially written record.");

        content = new byte[(int) raf.length()];
        raf.seek(0);
        raf.readFully(content);
      }

      // Now make a newer log file with just the IDs changed.  NOTE: this may not work if log format changes too much!
      findReplace("AAAAAA".getBytes(StandardCharsets.UTF_8), "aaaaaa".getBytes(StandardCharsets.UTF_8), content);
      findReplace("BBBBBB".getBytes(StandardCharsets.UTF_8), "bbbbbb".getBytes(StandardCharsets.UTF_8), content);
      findReplace("CCCCCC".getBytes(StandardCharsets.UTF_8), "cccccc".getBytes(StandardCharsets.UTF_8), content);

      // WARNING... assumes format of .00000n where n is less than 9
      long logNumber = Long.parseLong(fname.substring(fname.lastIndexOf(".") + 1));
      String fname2 = String.format(Locale.ROOT,
          UpdateLog.LOG_FILENAME_PATTERN,
          UpdateLog.TLOG_NAME,
          logNumber + 1);
      try (RandomAccessFile raf = new RandomAccessFile(new File(logDir, fname2), "rw")) {
        raf.write(content);
      }

      logReplay.release(1000);
      logReplayFinish.drainPermits();
      ignoreException("OutOfBoundsException");  // this is what the corrupted log currently produces... subject to change.
      createCore();
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      resetExceptionIgnores();
      assertJQ(req("q","*:*") ,"/response/numFound==6");

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  @Test
  public void testLogReplayWithInPlaceUpdatesAndDeletes() throws Exception {

    try {

      TestInjection.skipIndexWriterCommitOnClose = true;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = () -> {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      UpdateLog.testing_logReplayFinishHook = () -> logReplayFinish.release();


      clearIndex();
      assertU(commit());

      Deque<Long> versions = new ArrayDeque<>();
      versions.addFirst(addAndGetVersion(sdoc("id", "A1"), null));
      
      // DBQ of updated document using id
      versions.addFirst(addAndGetVersion(sdoc("id", "A2", "val_i_dvo", "1"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A2", "val_i_dvo", map("set", 2)), null)); // in-place update
      versions.addFirst(deleteByQueryAndGetVersion("id:A2", null));

      // DBQ of updated document using updated value
      versions.addFirst(addAndGetVersion(sdoc("id", "A3", "val_i_dvo", "101"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A3", "val_i_dvo", map("set", 102)), null)); // in-place update
      versions.addFirst(deleteByQueryAndGetVersion("val_i_dvo:102", null));

      // DBQ using an intermediate update value (shouldn't delete anything)
      versions.addFirst(addAndGetVersion(sdoc("id", "A4", "val_i_dvo", "200"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A4", "val_i_dvo", map("inc", "1")), null)); // in-place update
      versions.addFirst(addAndGetVersion(sdoc("id", "A4", "val_i_dvo", map("inc", "1")), null)); // in-place update
      versions.addFirst(deleteByQueryAndGetVersion("val_i_dvo:201", null));

      // DBI of updated document
      versions.addFirst(addAndGetVersion(sdoc("id", "A5", "val_i_dvo", "300"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A5", "val_i_dvo", map("inc", "1")), null)); // in-place update
      versions.addFirst(addAndGetVersion(sdoc("id", "A5", "val_i_dvo", map("inc", "1")), null)); // in-place update
      versions.addFirst(deleteAndGetVersion("A5", null));
      
      assertJQ(req("q","*:*"),"/response/numFound==0");
      

      assertJQ(req("qt","/get", "getVersions",""+versions.size()) ,"/versions==" + versions);

      h.close();
      createCore();

      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // verify that previous close didn't do a commit
      // recovery should be blocked by our hook
      assertJQ(req("q","*:*") ,"/response/numFound==0");

      // make sure we can still access versions after a restart
      assertJQ(req("qt","/get", "getVersions",""+versions.size()),"/versions==" + versions);

      // unblock recovery
      logReplay.release(1000);

      // make sure we can still access versions during recovery
      assertJQ(req("qt","/get", "getVersions",""+versions.size()),"/versions==" + versions);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertJQ(req("q","val_i_dvo:202") ,"/response/numFound==1"); // assert that in-place update is retained

      assertJQ(req("q","*:*") ,"/response/numFound==2");
      assertJQ(req("q","id:A2") ,"/response/numFound==0");
      assertJQ(req("q","id:A3") ,"/response/numFound==0");
      assertJQ(req("q","id:A4") ,"/response/numFound==1");
      assertJQ(req("q","id:A5") ,"/response/numFound==0");

      // make sure we can still access versions after recovery
      assertJQ(req("qt","/get", "getVersions",""+versions.size()) ,"/versions==" + versions);

      assertU(adoc("id","A10"));

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertJQ(req("q","*:*") ,"/response/numFound==3");
      assertJQ(req("q","id:A2") ,"/response/numFound==0");
      assertJQ(req("q","id:A3") ,"/response/numFound==0");
      assertJQ(req("q","id:A4") ,"/response/numFound==1");
      assertJQ(req("q","id:A5") ,"/response/numFound==0");
      assertJQ(req("q","id:A10"),"/response/numFound==1");
      
      // no updates, so insure that recovery does not run
      h.close();
      int permits = logReplay.availablePermits();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      assertJQ(req("q","*:*") ,"/response/numFound==3");
      assertJQ(req("q","val_i_dvo:202") ,"/response/numFound==1"); // assert that in-place update is retained
      assertJQ(req("q","id:A2") ,"/response/numFound==0");
      assertJQ(req("q","id:A3") ,"/response/numFound==0");
      assertJQ(req("q","id:A4") ,"/response/numFound==1");
      assertJQ(req("q","id:A5") ,"/response/numFound==0");
      assertJQ(req("q","id:A10"),"/response/numFound==1");
      Thread.sleep(100);
      assertEquals(permits, logReplay.availablePermits()); // no updates, so insure that recovery didn't run

      assertEquals(UpdateLog.State.ACTIVE, h.getCore().getUpdateHandler().getUpdateLog().getState());

    } finally {
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }

  }

  // NOTE: replacement must currently be same size
  private static void findReplace(byte[] from, byte[] to, byte[] data) {
    int idx = -from.length;
    for(;;) {
      idx = indexOf(from, data, idx + from.length);  // skip over previous match
      if (idx < 0) break;
      for (int i=0; i<to.length; i++) {
        data[idx+i] = to[i];
      }
    }
  }
  
  private static int indexOf(byte[] target, byte[] data, int start) {
    outer: for (int i=start; i<data.length - target.length; i++) {
      for (int j=0; j<target.length; j++) {
        if (data[i+j] != target[j]) continue outer;
      }
      return i;
    }
    return -1;
  }

  // stops the core, removes the transaction logs, restarts the core.
  void deleteLogs() throws Exception {
    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    h.close();

    try {
      String[] files = ulog.getLogList(logDir);
      for (String file : files) {
        Files.delete(new File(logDir, file).toPath());
      }

      assertEquals(0, ulog.getLogList(logDir).length);
    } finally {
      // make sure we create the core again, even if the assert fails so it won't mess
      // up the next test.
      createCore();
      assertJQ(req("q","*:*") ,"/response/numFound==");   // ensure it works
    }
  }

  private static Long getVer(SolrQueryRequest req) throws Exception {
    String response = JQ(req);
    @SuppressWarnings({"rawtypes"})
    Map rsp = (Map) Utils.fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map doc = null;
    if (rsp.containsKey("doc")) {
      doc = (Map)rsp.get("doc");
    } else if (rsp.containsKey("docs")) {
      @SuppressWarnings({"rawtypes"})
      List lst = (List)rsp.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    } else if (rsp.containsKey("response")) {
      @SuppressWarnings({"rawtypes"})
      Map responseMap = (Map)rsp.get("response");
      @SuppressWarnings({"rawtypes"})
      List lst = (List)responseMap.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    }

    if (doc == null) return null;

    return (Long)doc.get("_version_");
  }
  
  static class VersionProvider{
    private static long version = 0;
    
    static String getNextVersion() {
      return Long.toString(version++);
    }
  }
}

