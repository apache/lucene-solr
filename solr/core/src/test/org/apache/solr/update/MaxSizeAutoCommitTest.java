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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

//commented 2-Aug-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
public class MaxSizeAutoCommitTest extends SolrTestCaseJ4 {

  // Given an ID, returns an XML string for an "add document" request
  private static final Function<Integer, String> ADD_DOC_FN = (id) -> adoc("id", Integer.toString(id));
  // Given an ID, returns an XML string for a "delete document" request
  private static final Function<Integer, String> DELETE_DOC_FN = (id) -> delI(Integer.toString(id));

  private ObjectMapper objectMapper; // for JSON parsing
  private SolrCore core;
  private DirectUpdateHandler2 updateHandler;
  private CommitTracker hardCommitTracker;
  private UpdateRequestHandler updateRequestHandler;
  private String tlogDirPath;

  @Before
  public void setup() throws Exception {
    objectMapper = new ObjectMapper();
    System.setProperty("solr.autoCommit.maxSize", "5k");
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
    tlogDirPath = core.getDataDir() + "/tlog";
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("solr.autoCommit.maxSize");
    System.clearProperty("solr.ulog");
    deleteCore();
  }

  @Test
  public void simpleTest() throws Exception {
    int maxFileSizeBound = 1000;
    int maxFileSizeBoundWithBuffer = (int) (maxFileSizeBound * 1.25);
    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Adding these docs will place the tlog size just under the threshold
    int numDocs = 27;
    int batchSize = 3;
    int numBatches = numDocs / batchSize;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    int numTlogs = -1;
    TreeMap<String, Long> tlogsInfo = null;

    for (int batchCounter = 0; batchCounter < numBatches; batchCounter++) {
      int docStartId = batchSize * batchCounter;

      // Send batch update request
      updateRequestHandler.handleRequest(constructBatchAddDocRequest(docStartId, batchSize), updateResp);

      // The sleep is to allow existing commits to finish (or at least mostly finish) before querying/submitting more documents
      waitForCommit(200);

      // There should just be 1 tlog and its size should be within the (buffered) file size bound
      tlogsInfo = getTlogFileSizes(tlogDirPath, maxFileSizeBoundWithBuffer);
      numTlogs = parseTotalNumTlogs(tlogsInfo);
      Assert.assertEquals(1, numTlogs);
    }

    // Now that the core's tlog size is just under the threshold, one more update should induce a commit
    int docStartId = batchSize * numBatches;
    updateRequestHandler.handleRequest(constructBatchAddDocRequest(docStartId, batchSize), updateResp);
    waitForCommit(200);

    // Verify that a commit happened. There should now be 2 tlogs, both of which are < maxFileSizeBound.
    TreeMap<String, Long> tlogsInfoPostCommit = getTlogFileSizes(tlogDirPath, maxFileSizeBoundWithBuffer);
    Assert.assertEquals(2, parseTotalNumTlogs(tlogsInfoPostCommit));

    // And the current tlog's size should be less than the previous tlog's size
    Assert.assertTrue(tlogsInfoPostCommit.lastEntry().getValue() < tlogsInfo.lastEntry().getValue());
  }

  @Test
  public void testRedundantDeletes() throws Exception {
    int maxFileSizeBound = 1000;
    int maxFileSizeBoundWithBuffer = (int) (maxFileSizeBound * 1.25);

    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Add docs
    int numDocsToAdd = 150;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
    waitForCommit(200);

    // Get the tlog file info
    TreeMap<String, Long> tlogsInfoPreDeletes = getTlogFileSizes(tlogDirPath);

    // Send a bunch of redundant deletes
    int numDeletesToSend = 5000;
    int docIdToDelete = 100;

    SolrQueryRequestBase requestWithOneDelete = new SolrQueryRequestBase(core, new MapSolrParams(new HashMap<String, String>())) {};
    List<String> docs = new ArrayList<>();
    docs.add(delI(Integer.toString(docIdToDelete)));

    requestWithOneDelete.setContentStreams(toContentStreams(docs));

    for (int i = 0; i < numDeletesToSend; i++) {
      if (i % 50 == 0) {
        // Wait periodically to allow existing commits to finish before
        // sending more delete requests
        waitForCommit(200);
      }
      updateRequestHandler.handleRequest(requestWithOneDelete, updateResp);
    }

    // Verify that new tlogs have been created, and that their sizes are as expected
    TreeMap<String, Long> tlogsInfoPostDeletes = getTlogFileSizes(tlogDirPath, maxFileSizeBoundWithBuffer);
    Assert.assertTrue(parseTotalNumTlogs(tlogsInfoPreDeletes) < parseTotalNumTlogs(tlogsInfoPostDeletes));
  }

  @Test
  //commented 2-Aug-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void deleteTest() throws Exception {
    int maxFileSizeBound = 1000;
    int maxFileSizeBoundWithBuffer = (int) (maxFileSizeBound * 1.25);

    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Add docs
    int numDocsToAdd = 150;
    SolrQueryResponse updateResp = new SolrQueryResponse();
    updateRequestHandler.handleRequest(constructBatchAddDocRequest(0, numDocsToAdd), updateResp);
    waitForCommit(200);

    // Get the tlog file info
    TreeMap<String, Long> tlogsInfoPreDeletes = getTlogFileSizes(tlogDirPath);

    // Delete documents (in batches, so we can allow commits to finish and new tlog files to be created)
    int batchSize = 15;
    int numBatches = numDocsToAdd / batchSize;
    for (int batchCounter = 0; batchCounter < numBatches; batchCounter++) {
      int docStartId = batchSize * batchCounter;

      // Send batch delete doc request
      updateRequestHandler.handleRequest(constructBatchDeleteDocRequest(docStartId, batchSize), updateResp);

      // The sleep is to allow existing commits to finish before deleting more documents
      waitForCommit(200);
    }

    // Verify that the commit happened by seeing if a new tlog file was opened
    TreeMap<String, Long> tlogsInfoPostDeletes = getTlogFileSizes(tlogDirPath, maxFileSizeBoundWithBuffer);
    Assert.assertTrue(parseTotalNumTlogs(tlogsInfoPreDeletes) < parseTotalNumTlogs(tlogsInfoPostDeletes));
  }
  
  @Test
  @Repeat(iterations = 5)
  //commented 2-Aug-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
  public void endToEndTest() throws Exception {
    int maxFileSizeBound = 5000;
    // Set max size bound
    hardCommitTracker.setTLogFileSizeUpperBound(maxFileSizeBound);

    // Giving a 10% buffer for the max size bound
    int maxFileSizeBoundWithBuffer = (int) (maxFileSizeBound * 1.1);

    SolrQueryRequest selectQuery = req("*:*");
    List<Integer> docCounts = new ArrayList<>();

    int numDocs = 1000;
    int batchSize = 20;
    int numBatches = numDocs / batchSize;
    for (int batchCounter = 0; batchCounter < numBatches; batchCounter++) {
      SolrQueryResponse updateResp = new SolrQueryResponse();
      int docStartId = batchSize * batchCounter;

      // Send batch add doc request
      updateRequestHandler.handleRequest(constructBatchAddDocRequest(docStartId, batchSize), updateResp);

      // The sleep is to allow existing commits to finish before querying/submitting more documents
      waitForCommit(200);

      // Check tlog file sizes
      getTlogFileSizes(tlogDirPath, maxFileSizeBoundWithBuffer);

      // See how many documents are currently visible. This should increase as more commits occur.
      docCounts.add(queryCore(selectQuery));
    }

    // One final commit, after which all documents should be visible
    CommitUpdateCommand commitUpdateCommand = new CommitUpdateCommand(req(), false);
    updateHandler.commit(commitUpdateCommand);
    waitForCommit(200);
    docCounts.add(queryCore(selectQuery));

    // Evaluate the document counts
    checkNumFoundDocuments(docCounts, numDocs);
  }

  /**
   * Sleeps in increments of 50 ms while checking to see if a commit completed. If it did, then return. If not, continue
   * this cycle for at most the amount of time specified
   * @param maxTotalWaitTimeMillis the max amount of time (in ms) to wait/check for a commit
   */
  private void waitForCommit(long maxTotalWaitTimeMillis) throws Exception {
    long startTimeNanos = System.nanoTime();
    long maxTotalWaitTimeNanos = TimeUnit.MILLISECONDS.toNanos(maxTotalWaitTimeMillis);
    while (System.nanoTime() - startTimeNanos < maxTotalWaitTimeNanos) {
      Thread.sleep(50);
      if (!updateHandler.getUpdateLog().hasUncommittedChanges()) {
        return;
      }
    }
  }

  /**
   * Returns the total number of tlogs that have been created for the core.
   *
   * The tlogs in a core's tlog directory are named: tlog.0000000000000000000, tlog.0000000000000000001, tlog.0000000000000000002, etc.
   * Because old tlogs are periodically deleted, we can't just count the number of existing files. Instead, we take the
   * highest ordering tlog file name (which would be the newest) and parse the extension.
   *
   * e.g if the most recently created tlog file is tlog.0000000000000000003, we know that this core has had 4 tlogs.
   *
   * @param tlogsInfo TreeMap of (tlog file name, tlog file size (in bytes)) pairs
   * @return total number of tlogs created for this core
   */
  private int parseTotalNumTlogs(TreeMap<String, Long> tlogsInfo) {
    String mostRecentFileName = tlogsInfo.lastKey();
    int extensionDelimiterIndex = mostRecentFileName.lastIndexOf(".");
    if (extensionDelimiterIndex == -1) {
      throw new RuntimeException("Invalid tlog filename: " + mostRecentFileName);
    }
    String extension = mostRecentFileName.substring(extensionDelimiterIndex + 1);
    try {
      return Integer.parseInt(extension) + 1;
    } catch (NumberFormatException e) {
      throw new RuntimeException("Could not parse tlog filename: " + mostRecentFileName, e);
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
   * Executes the given query
   * @param query the query to execute
   * @return the number of documents found
   */
  public int queryCore(SolrQueryRequest query) throws Exception {
    String responseStr = h.query(query);
    try {
      Map<String, Object> root = (Map<String, Object>) objectMapper.readValue(responseStr, Object.class);
      Map<String, Object> rootResponse = (Map<String, Object>) root.get("response");
      return (int) rootResponse.get("numFound");
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse Solr query response", e);
    }
  }

  /**
   * Checks the given list of document counts to make sure that they are increasing (as commits occur).
   * @param numDocList list of the number of documents found in a given core. Ascending from oldest to newest
   */
  private void checkNumFoundDocuments(List<Integer> numDocList, int finalValue) {
    long currentTotal = 0;
    for (Integer count : numDocList) {
      Assert.assertTrue(count >= currentTotal);
      currentTotal = count;
    }
    Assert.assertEquals(finalValue, numDocList.get(numDocList.size() - 1).intValue());
  }


  /**
   * Goes through the given tlog directory and inspects each tlog.
   * @param tlogDirPath tlog directory path
   * @return a TreeMap of (tlog file name, tlog file size (in bytes)) pairs
   */
  private TreeMap<String, Long> getTlogFileSizes(String tlogDirPath) {
    return getTlogFileSizes(tlogDirPath, Integer.MAX_VALUE);
  }

  /**
   * Goes through the given tlog directory and inspects each tlog. Asserts that each tlog's size is <= the given max size bound.
   * @param tlogDirPath tlog directory path
   * @param maxSizeBound the max tlog size
   * @return a TreeMap of (tlog file name, tlog file size (in bytes)) pairs
   */
  private TreeMap<String, Long> getTlogFileSizes(String tlogDirPath, int maxSizeBound) {
    File tlogDir = new File(tlogDirPath);
    File[] tlogs = tlogDir.listFiles();
    TreeMap<String, Long> tlogInfo = new TreeMap<>();
    if (tlogs != null) {
      for (File tlog : tlogs) {
        String message = String.format(Locale.getDefault(), "Tlog size exceeds the max size bound. Tlog path: %s, tlog size: %d",
            tlog.getPath(), tlog.length());
        Assert.assertTrue(message, tlog.length() <= maxSizeBound);
        tlogInfo.put(tlog.getName(), tlog.length());
      }
    }
    return tlogInfo;
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
