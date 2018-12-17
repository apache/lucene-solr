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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaxSizeAutoCommitTest extends SolrTestCaseJ4 {

  // Given an ID, returns an XML string for an "add document" request
  private static final Function<Integer, String> ADD_DOC_FN = (id) -> adoc("id", Integer.toString(id));
  // Given an ID, returns an XML string for a "delete document" request
  private static final Function<Integer, String> DELETE_DOC_FN = (id) -> delI(Integer.toString(id));
  // How long to sleep while checking for commits
  private static final int COMMIT_CHECKING_SLEEP_TIME_MS = 50;

  private SolrCore core;
  private DirectUpdateHandler2 updateHandler;
  private CommitTracker hardCommitTracker;
  private UpdateRequestHandler updateRequestHandler;

  @Before
  public void setup() throws Exception {
    System.setProperty("solr.ulog", "solr.UpdateLog");
    initCore("solrconfig-tlog.xml", "schema.xml");
    core = h.getCore();
    updateHandler = (DirectUpdateHandler2) core.getUpdateHandler();
    hardCommitTracker = updateHandler.commitTracker;
    // Only testing file-size based auto hard commits - disable other checks
    hardCommitTracker.setTimeUpperBound(-1);
    hardCommitTracker.setDocsUpperBound(-1);
    updateRequestHandler = new UpdateRequestHandler();
    updateRequestHandler.init( null );
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("solr.ulog");
    deleteCore();
  }

  @Test
  public void testAdds() throws Exception {
    int maxFileSizeBound = 1000;
    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Add a large number of docs - should trigger a commit
    int numDocsToAdd = 500;
    SolrQueryResponse updateResp = new SolrQueryResponse();

    Assert.assertEquals("There have been no updates yet, so there shouldn't have been any commits", 0,
        hardCommitTracker.getCommitCount());

    long tlogSizePreUpdates = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertEquals("There have been no updates yet, so tlog should be empty", 0, tlogSizePreUpdates);

    updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);

    // The long sleep is to allow for the triggered commit to finish
    waitForCommit(1000);

    // Verify commit information
    Assert.assertTrue("At least one commit should have occurred", hardCommitTracker.getCommitCount() > 0);
    long tlogSizePostUpdates = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertTrue("Current tlog size is larger than the max bound", tlogSizePostUpdates < maxFileSizeBound);
  }

  @Test
  public void testRedundantDeletes() throws Exception {
    int maxFileSizeBound = 1000;
    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Add docs
    int numDocsToAdd = 150;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
    waitForCommit(1000);

    // Get the current commit info
    int commitCountPreDeletes = hardCommitTracker.getCommitCount();

    // Send a bunch of redundant deletes
    int numDeletesToSend = 500;
    int docIdToDelete = 100;

    SolrQueryRequestBase batchSingleDeleteRequest = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap<>())) {};
    List<String> docs = new ArrayList<>();
    for (int i = 0; i < numDeletesToSend; i++) {
      docs.add(delI(Integer.toString(docIdToDelete)));
    }
    batchSingleDeleteRequest.setContentStreams(toContentStreams(docs));

    updateRequestHandler.handleRequest(batchSingleDeleteRequest, updateResp);

    // The long sleep is to allow for the expected triggered commit to finish
    waitForCommit(1000);

    // Verify commit information
    Assert.assertTrue("At least one commit should have occurred",
        hardCommitTracker.getCommitCount() > commitCountPreDeletes);
    long tlogSizePostDeletes = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertTrue("Current tlog size is larger than the max bound", tlogSizePostDeletes < maxFileSizeBound);
  }

  @Test
  public void testDeletes() throws Exception {
    int maxFileSizeBound = 1000;

    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Add docs
    int numDocsToAdd = 500;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
    waitForCommit(1000);

    // Get the current commit info
    int commitCountPreDeletes = hardCommitTracker.getCommitCount();

    // Delete all documents - should trigger a commit
    updateRequestHandler.handleRequest(constructBatchDeleteDocRequest(0, numDocsToAdd), updateResp);

    // The long sleep is to allow for the expected triggered commit to finish
    waitForCommit(1000);

    // Verify commit information
    Assert.assertTrue("At least one commit should have occurred",
        hardCommitTracker.getCommitCount() > commitCountPreDeletes);
    long tlogSizePostDeletes = updateHandler.getUpdateLog().getCurrentLogSizeFromStream();
    Assert.assertTrue("Current tlog size is larger than the max bound", tlogSizePostDeletes < maxFileSizeBound);
  }

  /**
   * Sleeps in increments of COMMIT_CHECKING_SLEEP_TIME_MS while checking to see if a commit completed. If it did,
   * then return. If not, continue this cycle for at most the amount of time specified
   * @param maxTotalWaitTimeMillis the max amount of time (in ms) to wait/check for a commit
   */
  private void waitForCommit(long maxTotalWaitTimeMillis) throws Exception {
    long startTimeNanos = System.nanoTime();
    long maxTotalWaitTimeNanos = TimeUnit.MILLISECONDS.toNanos(maxTotalWaitTimeMillis);
    while (System.nanoTime() - startTimeNanos < maxTotalWaitTimeNanos) {
      Thread.sleep(COMMIT_CHECKING_SLEEP_TIME_MS);
      if (!updateHandler.getUpdateLog().hasUncommittedChanges()) {
        return;
      }
    }
  }

  /**
   * Construct a batch add document request with a series of very simple Solr docs with increasing IDs.
   * @param startId the document ID to begin with
   * @param batchSize the number of documents to include in the batch
   * @return a SolrQueryRequestBase
   */
  private SolrQueryRequestBase constructBatchAddDocRequest(int startId, int batchSize) {
    return constructBatchRequestHelper(startId, batchSize, ADD_DOC_FN);
  }

  /**
   * Construct a batch delete document request, with IDs incrementing from startId
   * @param startId the document ID to begin with
   * @param batchSize the number of documents to include in the batch
   * @return a SolrQueryRequestBase
   */
  private SolrQueryRequestBase constructBatchDeleteDocRequest(int startId, int batchSize) {
    return constructBatchRequestHelper(startId, batchSize, DELETE_DOC_FN);
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
}
