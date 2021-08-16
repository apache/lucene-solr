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

package org.apache.solr.update;

import java.lang.invoke.MethodHandles;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxSizeAutoCommitTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Given an ID, returns an XML string for an "add document" request
  private static String addDoc(int id) {
    return adoc("id", Integer.toString(id));
  }
  // Given an ID, returns an XML string for a "delete document" request
  private static String delDoc(int id) {
    return delI(Integer.toString(id));
  }
  // How long to sleep while checking for commits
  private static final int COMMIT_CHECKING_SLEEP_TIME_MS = 50;
  // max TLOG file size
  private static final int MAX_FILE_SIZE = 1000;
  
  private SolrCore core;
  private DirectUpdateHandler2 updateHandler;
  private CommitTracker hardCommitTracker;
  private UpdateRequestHandler updateRequestHandler;
  private MockEventListener monitor;

  @Before
  public void setup() throws Exception {
    System.setProperty("solr.ulog", "solr.UpdateLog");
    initCore("solrconfig-tlog.xml", "schema.xml");
    core = h.getCore();
    updateHandler = (DirectUpdateHandler2) core.getUpdateHandler();

    // we don't care about auto-commit's opening a new Searcher in this test, just skip it.
    updateHandler.softCommitTracker.setOpenSearcher(false);
    updateHandler.commitTracker.setOpenSearcher(false);

    // we don't care about soft commit's at all
    updateHandler.softCommitTracker.setTimeUpperBound(-1);
    updateHandler.softCommitTracker.setDocsUpperBound(-1);
    updateHandler.softCommitTracker.setTLogFileSizeUpperBound(-1);
    
    hardCommitTracker = updateHandler.commitTracker;
    // Only testing file-size based auto hard commits - disable other checks
    hardCommitTracker.setTimeUpperBound(-1);
    hardCommitTracker.setDocsUpperBound(-1);
    hardCommitTracker.setTLogFileSizeUpperBound(MAX_FILE_SIZE);

    monitor = new MockEventListener();
    updateHandler.registerCommitCallback(monitor);
    
    updateRequestHandler = new UpdateRequestHandler();
    updateRequestHandler.init( null );
  }

  @After
  public void tearDown() throws Exception {
    if (null != monitor) {
      monitor.assertSaneOffers();
      monitor.clear();
    }
    super.tearDown();
    System.clearProperty("solr.ulog");
    deleteCore();
  }

  @Test
  public void testAdds() throws Exception {

    Assert.assertEquals("There have been no updates yet, so there shouldn't have been any commits", 0,
                        hardCommitTracker.getCommitCount());

    long tlogSizePreUpdates = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertEquals("There have been no updates yet, so tlog should be empty", 0, tlogSizePreUpdates);

    // Add a large number of docs - should trigger a commit
    int numDocsToAdd = 500;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    
    monitor.doStuffAndExpectAtLeastOneCommit(hardCommitTracker, updateHandler, () -> {
        updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
      });
  }

  @Test
  public void testRedundantDeletes() throws Exception {

    Assert.assertEquals("There have been no updates yet, so there shouldn't have been any commits", 0,
                        hardCommitTracker.getCommitCount());

    long tlogSizePreUpdates = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertEquals("There have been no updates yet, so tlog should be empty", 0, tlogSizePreUpdates);
    
    // Add docs
    int numDocsToAdd = 150;
    SolrQueryResponse updateResp = new SolrQueryResponse();

    monitor.doStuffAndExpectAtLeastOneCommit(hardCommitTracker, updateHandler, () -> {
        updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
      });
    

    // Send a bunch of redundant deletes
    int numDeletesToSend = 500;
    int docIdToDelete = 100;

    SolrQueryRequestBase batchSingleDeleteRequest = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap<>())) {};
    List<String> docs = new ArrayList<>();
    for (int i = 0; i < numDeletesToSend; i++) {
      docs.add(delI(Integer.toString(docIdToDelete)));
    }
    batchSingleDeleteRequest.setContentStreams(toContentStreams(docs));
    
    monitor.doStuffAndExpectAtLeastOneCommit(hardCommitTracker, updateHandler, () -> {
        updateRequestHandler.handleRequest(batchSingleDeleteRequest, updateResp);
      });
    
  }

  @Test
  public void testDeletes() throws Exception {

    Assert.assertEquals("There have been no updates yet, so there shouldn't have been any commits", 0,
                        hardCommitTracker.getCommitCount());

    long tlogSizePreUpdates = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertEquals("There have been no updates yet, so tlog should be empty", 0, tlogSizePreUpdates);
    
    // Add docs
    int numDocsToAdd = 500;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    
    monitor.doStuffAndExpectAtLeastOneCommit(hardCommitTracker, updateHandler, () -> {
        updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
      });
    
    // Delete all documents - should trigger a commit
    
    monitor.doStuffAndExpectAtLeastOneCommit(hardCommitTracker, updateHandler, () -> {
        updateRequestHandler.handleRequest(constructBatchDeleteDocRequest(0, numDocsToAdd), updateResp);
      });
    
  }

  /**
   * Construct a batch add document request with a series of very simple Solr docs with increasing IDs.
   * @param startId the document ID to begin with
   * @param batchSize the number of documents to include in the batch
   * @return a SolrQueryRequestBase
   */
  private SolrQueryRequestBase constructBatchAddDocRequest(int startId, int batchSize) {
    return constructBatchRequestHelper(startId, batchSize, MaxSizeAutoCommitTest::addDoc);
  }

  /**
   * Construct a batch delete document request, with IDs incrementing from startId
   * @param startId the document ID to begin with
   * @param batchSize the number of documents to include in the batch
   * @return a SolrQueryRequestBase
   */
  private SolrQueryRequestBase constructBatchDeleteDocRequest(int startId, int batchSize) {
    return constructBatchRequestHelper(startId, batchSize, MaxSizeAutoCommitTest::delDoc);
  }

  /**
   * Helper for constructing a batch update request
   * @param startId the document ID to begin with
   * @param batchSize the number of documents to include in the batch
   * @param requestFn a function that takes an (int) ID and returns an XML string of the request to add to the batch request
   * @return a SolrQueryRequestBase
   */
  private SolrQueryRequestBase constructBatchRequestHelper(int startId, int batchSize, Function<Integer, String> requestFn) {
    SolrQueryRequestBase updateReq = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap<>())) {};
    List<String> docs = new ArrayList<>();
    for (int i = startId; i < startId + batchSize; i++) {
      docs.add(requestFn.apply(i));
    }
    updateReq.setContentStreams(toContentStreams(docs));
    return updateReq;
  }

  /**
   * Convert the given list of strings into a list of streams, for Solr update requests
   * @param strs strings to convert into streams
   * @return list of streams
   */
  private List<ContentStream> toContentStreams(List<String> strs) {
    ArrayList<ContentStream> streams = new ArrayList<>();
    for (String str : strs) {
      streams.addAll(ClientUtils.toContentStreams(str, "text/xml"));
    }
    return streams;
  }

  private static final class MockEventListener implements SolrEventListener {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    
    public MockEventListener() {
      /* No-Op */
    }
    
    // use capacity bound Queue just so we're sure we don't OOM 
    public final BlockingQueue<Long> hard = new LinkedBlockingQueue<>(1000);
    
    // if non enpty, then at least one offer failed (queues full)
    private StringBuffer fail = new StringBuffer();
    
    @Override
    public void init(@SuppressWarnings({"rawtypes"})NamedList args) {}
    
    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      // No-Op
    }
    
    @Override
    public void postCommit() {
      Long now = System.nanoTime();
      if (!hard.offer(now)) fail.append(", hardCommit @ " + now);
    }
    
    @Override
    public void postSoftCommit() {
      // No-Op
    }
    
    public void clear() {
      hard.clear();
      fail.setLength(0);
    }

    public void doStuffAndExpectAtLeastOneCommit(final CommitTracker commitTracker,
                                                 final DirectUpdateHandler2 updateHandler,
                                                 final Runnable stuff) throws InterruptedException {
      assertSaneOffers();
      
      final int POLL_TIME = 5;
      final TimeUnit POLL_UNIT = TimeUnit.SECONDS;
      
      final int preAutoCommitCount = commitTracker.getCommitCount();
      log.info("Auto-Commit count prior to doing work: {}", preAutoCommitCount);
      stuff.run();
      log.info("Work Completed");
      
      int numIters = 0;
      Long lastPostCommitTimeStampSeen = null;
      final long startTimeNanos = System.nanoTime();
      final long cutOffTime = startTimeNanos + TimeUnit.SECONDS.toNanos(300);
      while (System.nanoTime() < cutOffTime) {
        numIters++;
        log.info("Polling at most {} {} for expected (post-)commit#{}", POLL_TIME, POLL_UNIT, numIters);
        lastPostCommitTimeStampSeen = hard.poll(POLL_TIME, POLL_UNIT);
        assertNotNull("(post-)commit#" + numIters + " didn't occur in allowed time frame",
                      lastPostCommitTimeStampSeen);

        synchronized (commitTracker) {
          final int currentAutoCommitCount = commitTracker.getCommitCount() - preAutoCommitCount;
          final long currentFileSize = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
          if ((currentFileSize < MAX_FILE_SIZE) &&
              (currentAutoCommitCount == numIters) &&
              ( ! commitTracker.hasPending() )) {
            // if all of these condiions are met, then we should be completely done
            assertSaneOffers(); // last minute sanity check
            return;
          }
          // else: log & loop...
          log.info("(Auto-)commits triggered: {}; (post-)commits seen: {}; current tlog file size: {}",
                   currentAutoCommitCount, numIters, currentFileSize);
        }
      }
      
      // if we didn't return already, then we ran out of time
      fail("Exhausted cut off time polling for post-commit events (got " + numIters + ")");
    }
    
    public void assertSaneOffers() {
      assertEquals("Failure of MockEventListener" + fail.toString(), 
                   0, fail.length());
    }
  }  
  
}

