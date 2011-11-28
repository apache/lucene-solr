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


import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.FSUpdateLog;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class TestRecovery extends SolrTestCaseJ4 {

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

      FSUpdateLog.testing_logReplayHook = new Runnable() {
        @Override
        public void run() {
          try {
            logReplay.acquire();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      FSUpdateLog.testing_logReplayFinishHook = new Runnable() {
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

      // wait until recovery has finished
      assertTrue(logReplayFinish.tryAcquire(60, TimeUnit.SECONDS));
      assertJQ(req("q","*:*") ,"/response/numFound==3");
      assertJQ(req("q","id:2") ,"/response/numFound==0");

      // no updates, so insure that recovery does not run
      h.close();
      int permits = logReplay.availablePermits();
      createCore();
      assertJQ(req("q","*:*") ,"/response/numFound==3");
      Thread.sleep(100);
      assertEquals(permits, logReplay.availablePermits()); // no updates, so insure that recovery didn't run


    } finally {
      FSUpdateLog.testing_logReplayHook = null;
      FSUpdateLog.testing_logReplayFinishHook = null;
    }

  }

  @Test
  public void testBuffering() throws Exception {
    try {

      DirectUpdateHandler2.commitOnClose = false;
      final Semaphore logReplay = new Semaphore(0);
      final Semaphore logReplayFinish = new Semaphore(0);

      FSUpdateLog.testing_logReplayHook = new Runnable() {
        @Override
        public void run() {
          try {
            logReplay.acquire();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

      FSUpdateLog.testing_logReplayFinishHook = new Runnable() {
        @Override
        public void run() {
          logReplayFinish.release();
        }
      };


      clearIndex();
      assertU(commit());

      SolrQueryRequest req = req();
      UpdateHandler uhandler = req.getCore().getUpdateHandler();
      UpdateLog ulog = uhandler.getUpdateLog();

      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());
      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());
      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture == null);
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      assertU(adoc("id","1"));
      assertU(adoc("id","2"));
      assertU(adoc("id","3"));
      assertU(delI("1"));
      assertU(commit());

      // updates should be buffered, so we should not see any results yet.
      assertJQ(req("q", "*:*")
          , "/response/numFound==0"
      );

      rinfoFuture = ulog.applyBufferedUpdates();
      assertTrue(rinfoFuture != null);

      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      logReplay.release(1000);

      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      assertJQ(req("q", "*:*")
          , "/response/numFound==2"
      );


    } finally {
      FSUpdateLog.testing_logReplayHook = null;
      FSUpdateLog.testing_logReplayFinishHook = null;
    }

  }



}

/**

 - update processor directly log, or pass through to update handler?
  - only updates with versions (from leader) get logged?
    - could also log user updates and assign versions later - but couldn't tell them it if failed!
  - processor needs to know not to check versions
    - this suggests processor should just directly log.

 - we shouldn't be syncing when temporarily logging (this is called via run update processor currently, so we're ok)

 Transition from "recovering" to "active" - need to ensure that no updates are lost.

 - grab global write lock
 - change state to active
 - release global write lock

 - UpdateHandler.setState(BUFFER_UPDATES)  BUFFER_UPDATES, REPLAY_BUFFERED_UPDATES, ACTIVE

   ulog.bufferUpdates
   ulog.replayBufferedUpdates
   ulog.makeActive

   recoverFrom
   bufferUpdates()
   replayUpdates()
   makeActive()

   bufferUpdates()
   replayBufferedUpdates()  // block or provide a callback function when active?
     - TODO: what if there are failures while replaying buffered updates?
       - perhaps just provide functions to return stats about the last recovery?
           - number of updates buffered
           - number of updates errored
           - time?

 -

**/