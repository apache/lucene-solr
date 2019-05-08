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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.HdfsUpdateLog;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
// TODO: longer term this should be combined with TestRecovery somehow ??
public class TestRecoveryHdfs extends SolrTestCaseJ4 {
  // means that we've seen the leader and have version info (i.e. we are a non-leader replica)
  private static String FROM_LEADER = DistribPhase.FROMLEADER.toString(); 

  private static int timeout=60;  // acquire timeout in seconds.  change this to a huge number when debugging to prevent threads from advancing.
  
  private static MiniDFSCluster dfsCluster;
  private static String hdfsUri;
  private static FileSystem fs;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
    hdfsUri = HdfsTestUtil.getURI(dfsCluster);
    
    try {
      URI uri = new URI(hdfsUri);
      Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
      fs = FileSystem.get(uri, conf);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }

    System.setProperty("solr.ulog.dir", hdfsUri + "/solr/shard1");
    
    initCore("solrconfig-tlog.xml","schema15.xml");
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.closeQuietly(fs);
    fs = null;
    try {
      deleteCore();
    } finally {
      try {
        HdfsTestUtil.teardownClass(dfsCluster);
      } finally {
        dfsCluster = null;
        hdfsDataDir = null;
        System.clearProperty("solr.ulog.dir");
        System.clearProperty("test.build.data");
        System.clearProperty("test.cache.data");
      }
    }
  }

  @Test
  public void testReplicationFactor() throws Exception {
    clearIndex(); 
    
    HdfsUpdateLog ulog = (HdfsUpdateLog) h.getCore().getUpdateHandler().getUpdateLog();
    
    assertU(commit());
    addAndGetVersion(sdoc("id", "REP1"), null);
    assertU(commit());

    String[] logList = ulog.getLogList(new Path(ulog.getLogDir()));
    boolean foundRep2 = false;
    for (String tl : logList) {
       FileStatus status = fs.getFileStatus(new Path(ulog.getLogDir(), tl));
       if (status.getReplication() == 2) {
         foundRep2 = true;
         break;
       }
    }
    
    assertTrue("Expected to find tlogs with a replication factor of 2", foundRep2);
  }
  
  @Test
  public void testLogReplay() throws Exception {
    try {
      DirectUpdateHandler2.commitOnClose = false;
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

      Deque<Long> versions = new ArrayDeque<>();
      versions.addFirst(addAndGetVersion(sdoc("id", "A1"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A11"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A12"), null));
      versions.addFirst(deleteByQueryAndGetVersion("id:A11", null));
      versions.addFirst(addAndGetVersion(sdoc("id", "A13"), null));

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

      assertJQ(req("q","*:*") ,"/response/numFound==3");

      // make sure we can still access versions after recovery
      assertJQ(req("qt","/get", "getVersions",""+versions.size()) ,"/versions==" + versions);

      assertU(adoc("id","A2"));
      assertU(adoc("id","A3"));
      assertU(delI("A2"));
      assertU(adoc("id","A4"));

      assertJQ(req("q","*:*") ,"/response/numFound==3");

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
      Thread.sleep(100);
      assertEquals(permits, logReplay.availablePermits()); // no updates, so insure that recovery didn't run

      assertEquals(UpdateLog.State.ACTIVE, h.getCore().getUpdateHandler().getUpdateLog().getState());

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  @Test
  public void testBuffering() throws Exception {
    DirectUpdateHandler2.commitOnClose = false;
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

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();
      assertNull(rinfoFuture);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","B1", "_version_","1010")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B11", "_version_","1015")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonDelQ("id:B1 id:B11 id:B2 id:B3"), params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-1017"));
      updateJ(jsonAdd(sdoc("id","B2", "_version_","1020")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B3", "_version_","1030")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      deleteAndGetVersion("B1", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-2010"));

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':[-2010,1030,1020,-1017,1015,1010]}"
      );

      assertU(commit());

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':[-2010,1030,1020,-1017,1015,1010]}"
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


      rinfoFuture = ulog.applyBufferedUpdates();
      assertNotNull(rinfoFuture);

      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      logReplay.release(1000);

      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());


      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':[-2010,1030,1020,-1017,1015,1010]}"
      );


      assertJQ(req("q", "*:*")
          , "/response/numFound==2"
      );

      // move back to recovering
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      Long ver = getVer(req("qt","/get", "id","B3"));
      assertEquals(1030L, ver.longValue());

      // add a reordered doc that shouldn't overwrite one in the index
      updateJ(jsonAdd(sdoc("id","B3", "_version_","3")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // reorder two buffered updates
      updateJ(jsonAdd(sdoc("id","B4", "_version_","1040")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      deleteAndGetVersion("B4", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-940"));   // this update should not take affect
      updateJ(jsonAdd(sdoc("id","B6", "_version_","1060")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B5", "_version_","1050")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","B8", "_version_","1080")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // test that delete by query is at least buffered along with everything else so it will delete the
      // currently buffered id:8 (even if it doesn't currently support versioning)
      updateJ("{\"delete\": { \"query\":\"id:B2 OR id:B8\" }}", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-3000"));

      assertJQ(req("qt","/get", "getVersions","13")
          ,"=={'versions':[-3000,1080,1050,1060,-940,1040,3,-2010,1030,1020,-1017,1015,1010]}"  // the "3" appears because versions aren't checked while buffering
      );

      logReplay.drainPermits();
      rinfoFuture = ulog.applyBufferedUpdates();
      assertNotNull(rinfoFuture);
      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      // apply a single update
      logReplay.release(1);

      // now add another update
      updateJ(jsonAdd(sdoc("id","B7", "_version_","1070")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // a reordered update that should be dropped
      deleteAndGetVersion("B5", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-950"));

      deleteAndGetVersion("B6", params(DISTRIB_UPDATE_PARAM,FROM_LEADER, "_version_","-2060"));

      logReplay.release(1000);
      UpdateLog.RecoveryInfo recInfo = rinfoFuture.get();

      assertJQ(req("q", "*:*", "sort","id asc", "fl","id,_version_")
          , "/response/docs==["
                           + "{'id':'B3','_version_':1030}"
                           + ",{'id':'B4','_version_':1040}"
                           + ",{'id':'B5','_version_':1050}"
                           + ",{'id':'B7','_version_':1070}"
                           +"]"
      );

      assertEquals(1, recInfo.deleteByQuery);

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }
  }

  @Test
  @Ignore("HDFS-3107: no truncate support yet")
  public void testDropBuffered() throws Exception {
    DirectUpdateHandler2.commitOnClose = false;
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

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();
      assertNull(rinfoFuture);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","C1", "_version_","101")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C2", "_version_","102")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C3", "_version_","103")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id", "C4", "_version_","104")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id", "C5", "_version_","105")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      logReplay.release(1000);
      rinfoFuture = ulog.applyBufferedUpdates();
      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':[105,104]}"
      );

      // this time add some docs first before buffering starts (so tlog won't be at pos 0)
      updateJ(jsonAdd(sdoc("id","C100", "_version_","200")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C101", "_version_","201")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","C103", "_version_","203")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C104", "_version_","204")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","C105", "_version_","205")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C106", "_version_","206")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      rinfoFuture = ulog.applyBufferedUpdates();
      rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("q", "*:*", "sort","_version_ asc", "fl","id,_version_")
          , "/response/docs==["
          + "{'id':'C4','_version_':104}"
          + ",{'id':'C5','_version_':105}"
          + ",{'id':'C100','_version_':200}"
          + ",{'id':'C101','_version_':201}"
          + ",{'id':'C105','_version_':205}"
          + ",{'id':'C106','_version_':206}"
          +"]"
      );

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':[206,205,201,200,105,104]}"
      );

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      updateJ(jsonAdd(sdoc("id","C301", "_version_","998")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C302", "_version_","999")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      assertTrue(ulog.dropBufferedUpdates());

      // make sure we can overwrite with a lower version
      // TODO: is this functionality needed?
      updateJ(jsonAdd(sdoc("id","C301", "_version_","301")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","C302", "_version_","302")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertU(commit());

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':[302,301]}"
      );

      assertJQ(req("q", "*:*", "sort","_version_ desc", "fl","id,_version_", "rows","2")
          , "/response/docs==["
          + "{'id':'C302','_version_':302}"
          + ",{'id':'C301','_version_':301}"
          +"]"
      );

      updateJ(jsonAdd(sdoc("id","C2", "_version_","302")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }
  }

  @Test
  public void testExistOldBufferLog() throws Exception {
    DirectUpdateHandler2.commitOnClose = false;

    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      clearIndex();
      assertU(commit());

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","Q1", "_version_","101")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","Q2", "_version_","102")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","Q3", "_version_","103")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      req.close();
      h.close();
      createCore();

      req = req();
      uhandler = req.getCore().getUpdateHandler();
      ulog = uhandler.getUpdateLog();

      // the core no longer replay updates from buffer tlog on startup
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
      // simulate receiving no updates
      ulog.applyBufferedUpdates();
      updateJ(jsonAdd(sdoc("id","Q7", "_version_","117")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER)); // do another add to make sure flags are back to normal

      req.close();
      h.close();
      createCore();

      req = req();
      uhandler = req.getCore().getUpdateHandler();
      ulog = uhandler.getUpdateLog();

      assertFalse(ulog.existOldBufferLog());
      // Timeout for Q7 get replayed, because it was added on tlog, therefore it will be replayed on restart
      TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      timeout.waitFor("Timeout waiting for finish replay updates",
          () -> h.getCore().getUpdateHandler().getUpdateLog().getState() == UpdateLog.State.ACTIVE);
      assertJQ(req("qt","/get", "id", "Q7") ,"/doc/id==Q7");
    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;

      req().close();
    }
  }

  // make sure that on a restart, versions don't start too low
  @Test
  public void testVersionsOnRestart() throws Exception {
    clearIndex();
    assertU(commit());

    assertU(adoc("id","D1", "val_i","1"));
    assertU(adoc("id","D2", "val_i","1"));
    assertU(commit());
    long v1 = getVer(req("q","id:D1"));
    long v1a = getVer(req("q","id:D2"));

    h.close();
    createCore();

    assertU(adoc("id","D1", "val_i","2"));
    assertU(commit());
    long v2 = getVer(req("q","id:D1"));

    assert(v2 > v1);

    assertJQ(req("qt","/get", "getVersions","2")
        ,"/versions==[" + v2 + "," + v1a + "]"
    );
  }

  // make sure that log isn't needlessly replayed after a clean close
  @Test
  public void testCleanShutdown() throws Exception {
    DirectUpdateHandler2.commitOnClose = true;
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
      clearIndex();
      assertU(commit());

      assertU(adoc("id","E1", "val_i","1"));
      assertU(adoc("id","E2", "val_i","1"));

      // set to a high enough number so this test won't hang on a bug
      logReplay.release(10);

      h.close();
      createCore();

      // make sure the docs got committed
      assertJQ(req("q","*:*"),"/response/numFound==2");

      // make sure no replay happened
      assertEquals(10, logReplay.availablePermits());

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
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
      DirectUpdateHandler2.commitOnClose = false;
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

      String logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();
      
      h.close();

      String[] files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      for (String file : files) {
        fs.delete(new Path(logDir, file), false);
      }

      assertEquals(0, HdfsUpdateLog.getLogList(fs, new Path(logDir)).length);

      createCore();

      int start = 0;
      int maxReq = 50;

      LinkedList<Long> versions = new LinkedList<>();
      addDocs(10, start, versions); start+=10;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      addDocs(10, start, versions);  start+=10;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      assertEquals(2, HdfsUpdateLog.getLogList(fs, new Path(logDir)).length);

      addDocs(105, start, versions);  start+=105;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      // previous two logs should be gone now
      assertEquals(1, HdfsUpdateLog.getLogList(fs, new Path(logDir)).length);

      addDocs(1, start, versions);  start+=1;
      h.close();
      createCore();      // trigger recovery, make sure that tlog reference handling is correct

      // test we can get versions while replay is happening
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      logReplay.release(1000);
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));

      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      addDocs(105, start, versions);  start+=105;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      // previous logs should be gone now
      assertEquals(1, HdfsUpdateLog.getLogList(fs, new Path(logDir)).length);

      //
      // test that a corrupt tlog file doesn't stop us from coming up, or seeing versions before that tlog file.
      //
      addDocs(1, start, new LinkedList<Long>()); // don't add this to the versions list because we are going to lose it...
      h.close();
      files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      Arrays.sort(files);

      FSDataOutputStream dos = fs.create(new Path(new Path(logDir), files[files.length-1]), (short)1);
      dos.writeUTF("This is a trashed log file that really shouldn't work at all, but we'll see..");
      dos.close();

      ignoreException("Failure to open existing");
      createCore();
      // we should still be able to get the list of versions (not including the trashed log file)
      assertJQ(req("qt", "/get", "getVersions", "" + maxReq), "/versions==" + versions.subList(0, Math.min(maxReq, start)));
      resetExceptionIgnores();

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
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
      DirectUpdateHandler2.commitOnClose = false;
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

      String logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();

      clearIndex();
      assertU(commit());

      assertU(adoc("id","F1"));
      assertU(adoc("id","F2"));
      assertU(adoc("id","F3"));
      
      h.close();
      

      
      String[] files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      Arrays.sort(files);

      FSDataOutputStream dos = fs.append(new Path(logDir, files[files.length-1]));
    
      dos.writeLong(0xffffffffffffffffL);
      dos.writeChars("This should be appended to a good log file, representing a bad partially written record.");
      dos.close();

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

      updateJ(jsonAdd(sdoc("id","F4", "_version_","104")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","F5", "_version_","105")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","F6", "_version_","106")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // This currently skips the bad log file and also returns the version of the clearIndex (del *:*)
      // assertJQ(req("qt","/get", "getVersions","6"), "/versions==[106,105,104]");
      assertJQ(req("qt","/get", "getVersions","3"), "/versions==[106,105,104]");

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
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
      DirectUpdateHandler2.commitOnClose = false;

      String logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();
 
      clearIndex();
      assertU(commit());

      assertU(adoc("id","G1"));
      assertU(adoc("id","G2"));
      assertU(adoc("id","G3"));

      h.close();

      String[] files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      Arrays.sort(files);

      FSDataOutputStream dos = fs.create(new Path(logDir, files[files.length-1]), (short)1);
      dos.write(new byte[800]);  // zero out file
      dos.close();


      ignoreException("Failure to open existing log file");  // this is what the corrupted log currently produces... subject to change.
      createCore();
      resetExceptionIgnores();

      // just make sure it responds
      assertJQ(req("q","*:*") ,"/response/numFound==0");

      //
      // Now test that the bad log file doesn't mess up retrieving latest versions
      //

      updateJ(jsonAdd(sdoc("id","G4", "_version_","104")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","G5", "_version_","105")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      updateJ(jsonAdd(sdoc("id","G6", "_version_","106")), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      // This currently skips the bad log file and also returns the version of the clearIndex (del *:*)
      // assertJQ(req("qt","/get", "getVersions","6"), "/versions==[106,105,104]");
      assertJQ(req("qt","/get", "getVersions","3"), "/versions==[106,105,104]");

      assertU(commit());

      assertJQ(req("q","*:*") ,"/response/numFound==3");

      // This messes up some other tests (on windows) if we don't remove the bad log.
      // This *should* hopefully just be because the tests are too fragile and not because of real bugs - but it should be investigated further.
      deleteLogs();

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }

  // in rare circumstances, two logs can be left uncapped (lacking a commit at the end signifying that all the content in the log was committed)
  @Test
  public void testRecoveryMultipleLogs() throws Exception {
    try {
      DirectUpdateHandler2.commitOnClose = false;
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

      String logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();

      clearIndex();
      assertU(commit());

      assertU(adoc("id","AAAAAA"));
      assertU(adoc("id","BBBBBB"));
      assertU(adoc("id","CCCCCC"));

      h.close();
      String[] files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      Arrays.sort(files);
      String fname = files[files.length-1];

      try(FSDataOutputStream dos = fs.append(new Path(logDir, files[files.length-1]))) {
        dos.writeLong(0xffffffffffffffffL);
        dos.writeChars("This should be appended to a good log file, representing a bad partially written record.");
      }

      try(FSDataInputStream dis = fs.open(new Path(logDir, files[files.length-1]))) {
        byte[] content = new byte[dis.available()];
        dis.readFully(content);

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

        try(FSDataOutputStream dos = fs.create(new Path(logDir, fname2), (short)1)) {
          dos.write(content);
        }
      }

      logReplay.release(1000);
      logReplayFinish.drainPermits();
      ignoreException("OutOfBoundsException");  // this is what the corrupted log currently produces... subject to change.
      createCore();
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      resetExceptionIgnores();
      assertJQ(req("q","*:*") ,"/response/numFound==6");

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
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
      System.arraycopy(to, 0, data, idx, to.length);
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
    String logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();

    h.close();

    try {
      String[] files = HdfsUpdateLog.getLogList(fs, new Path(logDir));
      for (String file : files) {
        //new File(logDir, file).delete();
        fs.delete(new Path(logDir, file), false);
      }

      assertEquals(0, HdfsUpdateLog.getLogList(fs, new Path(logDir)).length);
    } finally {
      // make sure we create the core again, even if the assert fails so it won't mess
      // up the next test.
      createCore();
      assertJQ(req("q","*:*") ,"/response/numFound==");   // ensure it works
    }
  }

  private static Long getVer(SolrQueryRequest req) throws Exception {
    String response = JQ(req);
    Map rsp = (Map) Utils.fromJSONString(response);
    Map doc = null;
    if (rsp.containsKey("doc")) {
      doc = (Map)rsp.get("doc");
    } else if (rsp.containsKey("docs")) {
      List lst = (List)rsp.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    } else if (rsp.containsKey("response")) {
      Map responseMap = (Map)rsp.get("response");
      List lst = (List)responseMap.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    }

    if (doc == null) return null;

    return (Long)doc.get("_version_");
  }
}

