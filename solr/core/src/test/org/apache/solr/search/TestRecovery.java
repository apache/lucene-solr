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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.SEEN_LEADER;

public class TestRecovery extends SolrTestCaseJ4 {
  private static String SEEN_LEADER_VAL="true"; // value that means we've seen the leader and have version info (i.e. we are a non-leader replica)

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
            logReplay.acquire();
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

      assertU(adoc("id","1"));
      assertJQ(req("q","id:1")
          ,"/response/numFound==0"
      );

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // verify that previous close didn't do a commit
      // recovery should be blocked by our hook
      assertJQ(req("q","id:1") ,"/response/numFound==0");

      // unblock recovery
      logReplay.release(1000);

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(60, TimeUnit.SECONDS));

      assertJQ(req("q", "id:1")
          , "/response/numFound==1"
      );

      assertU(adoc("id","2"));
      assertU(adoc("id","3"));
      assertU(delI("2"));
      assertU(adoc("id","4"));

      assertJQ(req("q","*:*") ,"/response/numFound==1");

      h.close();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(60, TimeUnit.SECONDS));
      assertJQ(req("q","*:*") ,"/response/numFound==3");
      assertJQ(req("q","id:2") ,"/response/numFound==0");

      // no updates, so insure that recovery does not run
      h.close();
      int permits = logReplay.availablePermits();
      createCore();
      // Solr should kick this off now
      // h.getCore().getUpdateHandler().getUpdateLog().recoverFromLog();

      assertJQ(req("q","*:*") ,"/response/numFound==3");
      Thread.sleep(100);
      assertEquals(permits, logReplay.availablePermits()); // no updates, so insure that recovery didn't run

      assertEquals(UpdateLog.State.ACTIVE, h.getCore().getUpdateHandler().getUpdateLog().getState());

    } finally {
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
          logReplay.acquire();
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
      deleteAndGetVersion("1", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-201"));

      assertJQ(req("qt","/get", "getVersions","4")
          ,"=={'versions':[-201,103,102,101]}"
      );

      assertU(commit());

      assertJQ(req("qt","/get", "getVersions","4")
          ,"=={'versions':[-201,103,102,101]}"
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


      assertJQ(req("qt","/get", "getVersions","4")
          ,"=={'versions':[-201,103,102,101]}"
      );


      assertJQ(req("q", "*:*")
          , "/response/numFound==2"
      );

      // move back to recovering
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      Long ver = getVer(req("qt","/get", "id","3"));
      assertEquals(103L, ver.longValue());

      // add a reordered doc that shouldn't overwrite one in the index
      updateJ(jsonAdd(sdoc("id","3", "_version_","3")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // reorder two buffered updates
      updateJ(jsonAdd(sdoc("id","4", "_version_","104")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      deleteAndGetVersion("4", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-94"));   // this update should not take affect
      updateJ(jsonAdd(sdoc("id","6", "_version_","106")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","5", "_version_","105")), params(SEEN_LEADER,SEEN_LEADER_VAL));
      updateJ(jsonAdd(sdoc("id","8", "_version_","108")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // test that delete by query is at least buffered along with everything else so it will delete the
      // currently buffered id:8 (even if it doesn't currently support versioning)
      updateJ("{\"delete\": { \"query\":\"id:2 OR id:8\" }}", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-300"));

      assertJQ(req("qt","/get", "getVersions","10")
          ,"=={'versions':[-300,108,105,106,-94,104,3,-201,103,102]}"  // the "3" appears because versions aren't checked while buffering
      );

      logReplay.drainPermits();
      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);
      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      // apply a single update
      logReplay.release(1);

      // now add another update
      updateJ(jsonAdd(sdoc("id","7", "_version_","107")), params(SEEN_LEADER,SEEN_LEADER_VAL));

      // a reordered update that should be dropped
      deleteAndGetVersion("5", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-95"));

      deleteAndGetVersion("6", params(SEEN_LEADER,SEEN_LEADER_VAL, "_version_","-206"));

      logReplay.release(1000);
      UpdateLog.RecoveryInfo recInfo = rinfoFuture.get();

      assertJQ(req("q", "*:*", "sort","id asc", "fl","id,_version_")
          , "/response/docs==["
                           + "{'id':'3','_version_':103}"
                           + ",{'id':'4','_version_':104}"
                           + ",{'id':'5','_version_':105}"
                           + ",{'id':'7','_version_':107}"
                           +"]"
      );

      assertEquals(1, recInfo.deleteByQuery);

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState()); // leave each test method in a good state
    } finally {
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

    h.close();
    createCore();

    assertU(adoc("id","1", "val_i","2"));
    assertU(commit());
    long v2 = getVer(req("q","id:1"));

    assert(v2 > v1);
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

