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

package org.apache.solr.schema;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.processor.DistributedUpdateProcessorFactory;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;

public class TestSchemalessBufferedUpdates extends SolrTestCaseJ4 {

  // means that we've seen the leader and have version info (i.e. we are a non-leader replica)
  private static final String FROM_LEADER = DistribPhase.FROMLEADER.toString();
  private static final String UPDATE_CHAIN = "add-unknown-fields-to-the-schema";
  private static final int TIMEOUT = 10;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @BeforeClass
  public static void beforeClass() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    File tmpConfDir = new File(tmpSolrHome, confDir);
    File testHomeConfDir = new File(TEST_HOME(), confDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig-schemaless.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "schema-add-schema-fields-update-processor.xml"), tmpConfDir);
    FileUtils.copyFileToDirectory(new File(testHomeConfDir, "solrconfig.snippet.randomindexconfig.xml"), tmpConfDir);
    initCore("solrconfig-schemaless.xml", "schema-add-schema-fields-update-processor.xml", tmpSolrHome.getPath());
  }

  @Test
  public void test() throws Exception {
    TestInjection.skipIndexWriterCommitOnClose = true;
    final Semaphore logReplay = new Semaphore(0);
    final Semaphore logReplayFinish = new Semaphore(0);
    UpdateLog.testing_logReplayHook = () -> {
      try {
        assertTrue(logReplay.tryAcquire(TIMEOUT, TimeUnit.SECONDS));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    UpdateLog.testing_logReplayFinishHook = logReplayFinish::release;

    SolrQueryRequest req = req();
    UpdateHandler uhandler = req.getCore().getUpdateHandler();
    UpdateLog ulog = uhandler.getUpdateLog();

    try {
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      // Invalid date will be normalized by ParseDateField URP
      updateJ(jsonAdd(processAdd(sdoc("id","1", "f_dt","2017-01-04"))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
      assertU(commit());
      assertJQ(req("q", "*:*"), "/response/numFound==1");

      ulog.bufferUpdates();
      assertEquals(UpdateLog.State.BUFFERING, ulog.getState());

      // If the ParseDateField URP isn't ahead of the DUP, then the date won't be normalized in the buffered tlog entry,
      // and the doc won't be indexed on the replaying replica - a warning is logged as follows:
      // WARN [...] o.a.s.u.UpdateLog REYPLAY_ERR: IOException reading log
      //            org.apache.solr.common.SolrException: Invalid Date String:'2017-01-05'
      //              at org.apache.solr.util.DateMathParser.parseMath(DateMathParser.java:234)
      updateJ(jsonAdd(processAdd(sdoc("id","2", "f_dt","2017-01-05"))), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));

      Future<UpdateLog.RecoveryInfo> rinfoFuture = ulog.applyBufferedUpdates();

      assertTrue(rinfoFuture != null);

      assertEquals(UpdateLog.State.APPLYING_BUFFERED, ulog.getState());

      logReplay.release(1000);

      UpdateLog.RecoveryInfo rinfo = rinfoFuture.get();
      assertEquals(UpdateLog.State.ACTIVE, ulog.getState());

      assertU(commit());
      assertJQ(req("q", "*:*"), "/response/numFound==2");
    } finally {
      TestInjection.reset();
      UpdateLog.testing_logReplayHook = null;
      UpdateLog.testing_logReplayFinishHook = null;
      req().close();
    }
  }

  private SolrInputDocument processAdd(final SolrInputDocument docIn) throws IOException {
    UpdateRequestProcessorChain processorChain = h.getCore().getUpdateProcessingChain(UPDATE_CHAIN);
    assertNotNull("Undefined URP chain '" + UPDATE_CHAIN + "'", processorChain);
    List <UpdateRequestProcessorFactory> factoriesUpToDUP = new ArrayList<>();
    for (UpdateRequestProcessorFactory urpFactory : processorChain.getProcessors()) {
      factoriesUpToDUP.add(urpFactory);
      if (urpFactory.getClass().equals(DistributedUpdateProcessorFactory.class)) 
        break;
    }
    UpdateRequestProcessorChain chainUpToDUP = new UpdateRequestProcessorChain(factoriesUpToDUP, h.getCore());
    assertNotNull("URP chain '" + UPDATE_CHAIN + "'", chainUpToDUP);
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = req();
    try {
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.solrDoc = docIn;
      UpdateRequestProcessor processor = chainUpToDUP.createProcessor(req, rsp);
      processor.processAdd(cmd);
      if (cmd.solrDoc.get("f_dt").getValue() instanceof Date) {
        // Non-JSON types (Date in this case) aren't handled properly in noggit-0.6.  Although this is fixed in
        // https://github.com/yonik/noggit/commit/ec3e732af7c9425e8f40297463cbe294154682b1 to call obj.toString(), 
        // Date::toString produces a Date representation that Solr doesn't like, so we convert using Instant::toString
        cmd.solrDoc.get("f_dt").setValue(((Date) cmd.solrDoc.get("f_dt").getValue()).toInstant().toString());
      }
      return cmd.solrDoc;
    } finally {
      SolrRequestInfo.clearRequestInfo();
      req.close();
    }
  }
}
