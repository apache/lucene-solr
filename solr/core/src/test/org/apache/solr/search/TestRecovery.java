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


import org.apache.noggit.JSONUtil;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.SEEN_LEADER;

public class TestRecovery extends SolrTestCaseJ4 {
  private static String SEEN_LEADER_VAL="true"; // value that means we've seen the leader and have version info (i.e. we are a non-leader replica)
  private static int timeout=60;  // acquire timeout in seconds.  change this to a huge number when debugging to prevent threads from advancing.
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-tlog.xml","schema12.xml");
  }

  @Test
  public void testLogReplay() throws Exception {
    try {

      DirectUpdateHandler2.commitOnClose = false;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      UpdateLog.testing_logReplayHook = new Runnable() {
        @Override
        public void run() {
          try {
            assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      UpdateLog.testing_logReplayFinishHook = new Runnable() {
        @Override
        public void run() {
          logReplayFinish.release();
        }
      };


      clearIndex();
      assertU(commit());

      Deque<Long> versions = new ArrayDeque<Long>();
      versions.addFirst(addAndGetVersion(sdoc("id", "1"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "11"), null));
      versions.addFirst(addAndGetVersion(sdoc("id", "12"), null));
      versions.addFirst(deleteByQueryAndGetVersion("id:11", null));
      versions.addFirst(addAndGetVersion(sdoc("id", "13"), null));

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

      assertU(adoc("id","2"));
      assertU(adoc("id","3"));
      assertU(delI("2"));
      assertU(adoc("id","4"));

      assertJQ(req("q","*:*") ,"/response/numFound==3");

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(timeout, TimeUnit.SECONDS));
      assertJQ(req("q","*:*") ,"/response/numFound==5");
      assertJQ(req("q","id:2") ,"/response/numFound==0");

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

    UpdateLog.testing_logReplayHook = new Runnable() {
      @Override
      public void run() {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };

    UpdateLog.testing_logReplayFinishHook = new Runnable() {
      @Override
      public void run() {
        logReplayFinish.release();
      }
    };


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
      assertTrue(rinfoFuture == null);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","1", "_version_","1010")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","11", "_version_","1015")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonDelQ("id:1 id:11 id:2 id:3"), params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-1017"));
      updateJ(jsonAdd(sdoc("id","2", "_version_","1020")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","3", "_version_","1030")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      deleteAndGetVersion("1", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-2010"));

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
      assertJQ(req("qt","/get", "id","3")
          ,"=={'doc':null}"
      );


      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);

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

      Long ver = getVer(req("qt","/get", "id","3"));
      assertEquals(1030L, ver.longValue());

      // add a reordered doc that shouldn't overwrite one in the index
      updateJ(jsonAdd(sdoc("id","3", "_version_","3")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // reorder two buffered updates
      updateJ(jsonAdd(sdoc("id","4", "_version_","1040")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      deleteAndGetVersion("4", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-940"));   // this update should not take affect
      updateJ(jsonAdd(sdoc("id","6", "_version_","1060")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","5", "_version_","1050")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","8", "_version_","1080")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // test that delete by query is at least buffered along with everything else so it will delete the
      // currently buffered id:8 (even if it doesn't currently support versioning)
      updateJ("{\"delete\": { \"query\":\"id:2 OR id:8\" }}", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-3000"));

      assertJQ(req("qt","/get", "getVersions","13")
          ,"=={'versions':[-3000,1080,1050,1060,-940,1040,3,-2010,1030,1020,-1017,1015,1010]}"  // the "3" appears because versions aren't checked while buffering
      );

      logReplay.drainPermits();
      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);
      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      // apply a single update
      logReplay.release(1);

      // now add another update
      updateJ(jsonAdd(sdoc("id","7", "_version_","1070")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // a reordered update that should be dropped
      deleteAndGetVersion("5", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-950"));

      deleteAndGetVersion("6", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-2060"));

      logReplay.release(1000);
      UpdateLog.RecoveryInfo recInfo = rinfoFuture.get();

      assertJQ(req("q", "*:*", "sort","id asc", "fl","id,_version_")
          , "/response/docs==["
                           + "{'id':'3','_version_':1030}"
                           + ",{'id':'4','_version_':1040}"
                           + ",{'id':'5','_version_':1050}"
                           + ",{'id':'7','_version_':1070}"
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
  public void testDropBuffered() throws Exception {

    DirectUpdateHandler2.commitOnClose = false;
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);

    UpdateLog.testing_logReplayHook = new Runnable() {
      @Override
      public void run() {
        try {
          assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };

    UpdateLog.testing_logReplayFinishHook = new Runnable() {
      @Override
      public void run() {
        logReplayFinish.release();
      }
    };


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
      assertTrue(rinfoFuture == null);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // simulate updates from a leader
      updateJ(jsonAdd(sdoc("id","1", "_version_","101")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","2", "_version_","102")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","3", "_version_","103")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id", "4", "_version_","104")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id", "5", "_version_","105")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      logReplay.release(1000);
      rinfoFuture = ulog.applyBufferedUpdates();
      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("qt","/get", "getVersions","2")
          ,"=={'versions':[105,104]}"
      );

      // this time add some docs first before buffering starts (so tlog won't be at pos 0)
      updateJ(jsonAdd(sdoc("id","100", "_version_","200")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","101", "_version_","201")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","103", "_version_","203")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","104", "_version_","204")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      assertTrue(ulog.dropBufferedUpdates());
      ulog.bufferUpdates();
      updateJ(jsonAdd(sdoc("id","105", "_version_","205")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","106", "_version_","206")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      rinfoFuture = ulog.applyBufferedUpdates();
      rinfo = rinfoFuture.get();
      assertEquals(2, rinfo.adds);

      assertJQ(req("q", "*:*", "sort","_version_ asc", "fl","id,_version_")
          , "/response/docs==["
          + "{'id':'4','_version_':104}"
          + ",{'id':'5','_version_':105}"
          + ",{'id':'100','_version_':200}"
          + ",{'id':'101','_version_':201}"
          + ",{'id':'105','_version_':205}"
          + ",{'id':'106','_version_':206}"
          +"]"
      );

      assertJQ(req("qt","/get", "getVersions","6")
          ,"=={'versions':[206,205,201,200,105,104]}"
      );


      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
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

    assertU(adoc("id","1", "val_i","1"));
    assertU(adoc("id","2", "val_i","1"));
    assertU(commit());
    long v1 = getVer(req("q","id:1"));
    long v1a = getVer(req("q","id:2"));

    h.close();
    createCore();

    assertU(adoc("id","1", "val_i","2"));
    assertU(commit());
    long v2 = getVer(req("q","id:1"));

    assert(v2 > v1);

    assertJQ(req("qt","/get", "getVersions","2")
        ,"/versions==[" + v2 + "," + v1a + "]"
    );

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

      UpdateLog.testing_logReplayHook = new Runnable() {
        @Override
        public void run() {
          try {
            assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      UpdateLog.testing_logReplayFinishHook = new Runnable() {
        @Override
        public void run() {
          logReplayFinish.release();
        }
      };


      clearIndex();
      assertU(commit());

      File logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();

      h.close();

      String[] files = UpdateLog.getLogList(logDir);
      for (String file : files) {
        new File(logDir, file).delete();
      }

      assertEquals(0, UpdateLog.getLogList(logDir).length);

      createCore();

      int start = 0;
      int maxReq = 50;

      LinkedList<Long> versions = new LinkedList<Long>();
      addDocs(10, start, versions); start+=10;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      addDocs(10, start, versions);  start+=10;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      assertEquals(2, UpdateLog.getLogList(logDir).length);

      addDocs(105, start, versions);  start+=105;
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
      assertU(commit());
      assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

      // previous two logs should be gone now
      assertEquals(1, UpdateLog.getLogList(logDir).length);

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
      assertEquals(1, UpdateLog.getLogList(logDir).length);

      //
      // test that a corrupt tlog file doesn't stop us from coming up, or seeing versions before that tlog file.
      //
      addDocs(1, start, new LinkedList<Long>()); // don't add this to the versions list because we are going to lose it...
      h.close();
      files = UpdateLog.getLogList(logDir);
      Arrays.sort(files);
      RandomAccessFile raf = new RandomAccessFile(new File(logDir, files[files.length-1]), "rw");
      raf.writeChars("This is a trashed log file that really shouldn't work at all, but we'll see...");
      raf.close();

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

      UpdateLog.testing_logReplayHook = new Runnable() {
        @Override
        public void run() {
          try {
            assertTrue(logReplay.tryAcquire(timeout, TimeUnit.SECONDS));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      UpdateLog.testing_logReplayFinishHook = new Runnable() {
        @Override
        public void run() {
          logReplayFinish.release();
        }
      };

      File logDir = h.getCore().getUpdateHandler().getUpdateLog().getLogDir();

      clearIndex();
      assertU(commit());

      assertU(adoc("id","1"));
      assertU(adoc("id","2"));
      assertU(adoc("id","3"));

      h.close();
      String[] files = UpdateLog.getLogList(logDir);
      Arrays.sort(files);
      RandomAccessFile raf = new RandomAccessFile(new File(logDir, files[files.length-1]), "rw");
      raf.seek(raf.length());  // seek to end
      raf.writeLong(0xffffffffffffffffL);
      raf.writeChars("This should be appended to a good log file, representing a bad partially written record.");
      raf.close();

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

      updateJ(jsonAdd(sdoc("id","4", "_version_","104")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","5", "_version_","105")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","6", "_version_","106")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // This currently skips the bad log file and also returns the version of the clearIndex (del *:*)
      // assertJQ(req("qt","/get", "getVersions","6"), "/versions==[106,105,104]");
      assertJQ(req("qt","/get", "getVersions","3"), "/versions==[106,105,104]");

    } finally {
      DirectUpdateHandler2.commitOnClose = true;
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
    }
  }



  private static Long getVer(SolrQueryRequest req) throws Exception {
    String response = JQ(req);
    Map rsp = (Map) ObjectBuilder.fromJSON(response);
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

