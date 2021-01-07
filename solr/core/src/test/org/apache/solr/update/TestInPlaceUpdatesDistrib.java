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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Field;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.index.NoMergePolicyFactory;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests the in-place updates (docValues updates) for a one shard, three replica cluster.
 */
@Slow
public class TestInPlaceUpdatesDistrib extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean onlyLeaderIndexes = random().nextBoolean();

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-inplace-updates.xml";
    configString = "solrconfig-tlog.xml";

    // we need consistent segments that aren't re-ordered on merge because we're
    // asserting inplace updates happen by checking the internal [docid]
    systemSetPropertySolrTestsMergePolicyFactory(NoMergePolicyFactory.class.getName());

    randomizeUpdateLogImpl();

    initCore(configString, schemaString);
    
    // sanity check that autocommits are disabled
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);
    
    // assert that NoMergePolicy was chosen
    RefCounted<IndexWriter> iw = h.getCore().getSolrCoreState().getIndexWriter(h.getCore());
    try {
      IndexWriter writer = iw.get();
      assertTrue("Actual merge policy is: " + writer.getConfig().getMergePolicy(),
          writer.getConfig().getMergePolicy() instanceof NoMergePolicy); 
    } finally {
      iw.decref();
    }
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  public TestInPlaceUpdatesDistrib() throws Exception {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  private SolrClient LEADER = null;
  private List<SolrClient> NONLEADERS = null;
  
  @Test
  @ShardsFixed(num = 3)
  @SuppressWarnings("unchecked")
  //28-June-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void test() throws Exception {
    waitForRecoveriesToFinish(true);

    resetDelays();
    
    mapReplicasToClients();
    
    clearIndex();
    commit();
    
    // sanity check no one broke the assumptions we make about our schema
    checkExpectedSchemaField(map("name", "inplace_updatable_int",
        "type","int",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE));
    checkExpectedSchemaField(map("name", "inplace_updatable_float",
        "type","float",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE));
    checkExpectedSchemaField(map("name", "_version_",
        "type","long",
        "stored",Boolean.FALSE,
        "indexed",Boolean.FALSE,
        "docValues",Boolean.TRUE));

    // Do the tests now:
    
    // AwaitsFix this test fails easily
    // delayedReorderingFetchesMissingUpdateFromLeaderTest();
    
    resetDelays();
    docValuesUpdateTest();
    resetDelays();
    ensureRtgWorksWithPartialUpdatesTest();
    resetDelays();
    outOfOrderUpdatesIndividualReplicaTest();
    resetDelays();
    updatingDVsInAVeryOldSegment();
    resetDelays();
    updateExistingThenNonExistentDoc();
    resetDelays();
    // TODO Should we combine all/some of these into a single test, so as to cut down on execution time?
    reorderedDBQIndividualReplicaTest();
    resetDelays();
    reorderedDeletesTest();
    resetDelays();
    reorderedDBQsSimpleTest();
    resetDelays();
    reorderedDBQsResurrectionTest();
    resetDelays();
    setNullForDVEnabledField();
    resetDelays();
    
    // AwaitsFix this test fails easily
    // reorderedDBQsUsingUpdatedValueFromADroppedUpdate();
  }

  private void resetDelays() {
    for (JettySolrRunner j   : jettys  ) {
      j.getDebugFilter().unsetDelay();
    }
  }
  
  private void mapReplicasToClients() throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = null;
    Slice shard1 = clusterState.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1);
    leader = shard1.getLeader();

    String leaderBaseUrl = zkStateReader.getBaseUrlForNodeName(leader.getNodeName());
    for (int i=0; i<clients.size(); i++) {
      if (((HttpSolrClient)clients.get(i)).getBaseURL().startsWith(leaderBaseUrl))
        LEADER = clients.get(i);
    }
    
    NONLEADERS = new ArrayList<>();
    for (Replica rep: shard1.getReplicas()) {
      if (rep.equals(leader)) {
        continue;
      }
      String baseUrl = zkStateReader.getBaseUrlForNodeName(rep.getNodeName());
      for (int i=0; i<clients.size(); i++) {
        if (((HttpSolrClient)clients.get(i)).getBaseURL().startsWith(baseUrl))
          NONLEADERS.add(clients.get(i));
      }
    }
    
    assertNotNull(LEADER);
    assertEquals(2, NONLEADERS.size());
  }

  private void setNullForDVEnabledField() throws Exception {
    // to test set=null
    // should this test be here? As set null would be an atomic update
    clearIndex();
    commit();

    buildRandomIndex(0);
    float inplace_updatable_float = 1;

    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    for (SolrClient client : NONLEADERS) {
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertEquals(inplace_updatable_float, doc.get("inplace_updatable_float"));
      assertEquals(version0, doc.get("_version_"));
    }

    index("id", 0, "inplace_updatable_float", map("set", null));
    LEADER.commit();

    sdoc = LEADER.getById("0");  // RTG straight from the index
    assertNull(sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version1 = (long) sdoc.get("_version_");

    for (SolrClient client : NONLEADERS) {
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNull(doc.get("inplace_updatable_float"));
      assertEquals(version1, doc.get("_version_"));
    }
  }

  final int NUM_RETRIES = 100, WAIT_TIME = 50;

  // The following should work: full update to doc 0, in-place update for doc 0, delete doc 0
  private void reorderedDBQsSimpleTest() throws Exception {
    
    clearIndex();
    commit();
    
    buildRandomIndex(0);

    float inplace_updatable_float = 1;

    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "inplace_updatable_float", newinplace_updatable_float + 1, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(0, version0 + 3));

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      if (log.isInfoEnabled()) {
        log.info("Issuing well ordered update: {}", update.getDocuments());
      }
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, random());
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));
    
    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
    }

    log.info("reorderedDBQsSimpleTest: This test passed fine...");
  }

  private void reorderedDBQIndividualReplicaTest() throws Exception {
    if (onlyLeaderIndexes) {
      log.info("RTG with DBQs are not working in tlog replicas");
      return;
    }
    clearIndex();
    commit();

    // put replica out of sync
    float newinplace_updatable_float = 100;
    long version0 = 2000;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float",
        newinplace_updatable_float, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "inplace_updatable_float",
        newinplace_updatable_float + 1, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest("inplace_updatable_float:"+(newinplace_updatable_float + 1), version0 + 3));

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool =
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));

    // re-order the updates by swapping the last two
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    reorderedUpdates.set(1, updates.get(2));
    reorderedUpdates.set(2, updates.get(1));

    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(100);
    }

    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    SolrDocument doc = NONLEADERS.get(0).getById(String.valueOf(0), params("distrib", "false"));
    assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);

    log.info("reorderedDBQIndividualReplicaTest: This test passed fine...");
    clearIndex();
    commit();
  }

  private void docValuesUpdateTest() throws Exception {
    // number of docs we're testing (0 <= id), index may contain additional random docs (id < 0)
    int numDocs = atLeast(100);
    if (onlyLeaderIndexes) numDocs = TestUtil.nextInt(random(), 10, 50);
    log.info("Trying num docs = {}", numDocs);
    final List<Integer> ids = new ArrayList<Integer>(numDocs);
    for (int id = 0; id < numDocs; id++) {
      ids.add(id);
    }
      
    buildRandomIndex(101.0F, ids);
    
    List<Integer> luceneDocids = new ArrayList<>(numDocs);
    List<Number> valuesList = new ArrayList<>(numDocs);
    SolrParams params = params("q", "id:[0 TO *]", "fl", "*,[docid]", "rows", String.valueOf(numDocs), "sort", "id_i asc");
    SolrDocumentList results = LEADER.query(params).getResults();
    assertEquals(numDocs, results.size());
    for (SolrDocument doc : results) {
      luceneDocids.add((Integer) doc.get("[docid]"));
      valuesList.add((Float) doc.get("inplace_updatable_float"));
    }
    log.info("Initial results: {}", results);
    
    // before we do any atomic operations, sanity check our results against all clients
    assertDocIdsAndValuesAgainstAllClients("sanitycheck", params, luceneDocids, "inplace_updatable_float", valuesList);

    // now we're going to overwrite the value for all of our testing docs
    // giving them a value between -5 and +5
    for (int id : ids) {
      // NOTE: in rare cases, this may be setting the value to 0, on a doc that
      // already had an init value of 0 -- which is an interesting edge case, so we don't exclude it
      final float multiplier = r.nextBoolean() ? -5.0F : 5.0F;
      final float value = r.nextFloat() * multiplier;
      assert -5.0F <= value && value <= 5.0F;
      valuesList.set(id, value);
    }
    log.info("inplace_updatable_float: {}", valuesList);
    
    // update doc w/ set
    Collections.shuffle(ids, r); // so updates aren't applied in index order
    for (int id : ids) {
      index("id", id, "inplace_updatable_float", map("set", valuesList.get(id)));
    }

    commit();

    assertDocIdsAndValuesAgainstAllClients
      ("set", SolrParams.wrapDefaults(params("q", "inplace_updatable_float:[-5.0 TO 5.0]",
                                             "fq", "id:[0 TO *]"),
                                      // existing sort & fl that we want...
                                      params),
       luceneDocids, "inplace_updatable_float", valuesList);
      
    // update doc, w/increment
    log.info("Updating the documents...");
    Collections.shuffle(ids, r); // so updates aren't applied in the same order as our 'set'
    for (int id : ids) {
      // all incremements will use some value X such that 20 < abs(X)
      // thus ensuring that after all incrememnts are done, there should be
      // 0 test docs matching the query inplace_updatable_float:[-10 TO 10]
      final float inc = (r.nextBoolean() ? -1.0F : 1.0F) * (r.nextFloat() + (float)atLeast(20));
      assert 20 < Math.abs(inc);
      final float value = (float)valuesList.get(id) + inc;
      assert value < -10 || 10 < value;
        
      valuesList.set(id, value);
      index("id", id, "inplace_updatable_float", map("inc", inc));
    }
    commit();
    
    assertDocIdsAndValuesAgainstAllClients
      ("inc", SolrParams.wrapDefaults(params("q", "-inplace_updatable_float:[-10.0 TO 10.0]",
                                             "fq", "id:[0 TO *]"),
                                      // existing sort & fl that we want...
                                      params),
       luceneDocids, "inplace_updatable_float", valuesList);

    log.info("Updating the documents with new field...");
    Collections.shuffle(ids, r);
    for (int id : ids) {
      final int val = random().nextInt(20);
      valuesList.set(id, val);
      index("id", id, "inplace_updatable_int", map((random().nextBoolean()?"inc": "set"), val));
    }
    commit();

    assertDocIdsAndValuesAgainstAllClients
        ("inplace_for_first_field_update", SolrParams.wrapDefaults(params("q", "inplace_updatable_int:[* TO *]",
            "fq", "id:[0 TO *]"),
            params),
            luceneDocids, "inplace_updatable_int", valuesList);
    log.info("docValuesUpdateTest: This test passed fine...");
  }

  /**
   * Ingest many documents, keep committing. Then update a document from a very old segment.
   */
  private void updatingDVsInAVeryOldSegment() throws Exception {
    clearIndex();
    commit();

    String id = String.valueOf(Integer.MAX_VALUE);
    index("id", id, "inplace_updatable_float", "1", "title_s", "newtitle");

    // create 10 more segments
    for (int i=0; i<10; i++) {
      buildRandomIndex(101.0F, Collections.emptyList());
    }

    index("id", id, "inplace_updatable_float", map("inc", "1"));

    for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), NONLEADERS.get(1)}) {
      assertEquals("newtitle", client.getById(id).get("title_s"));
      assertEquals(2.0f, client.getById(id).get("inplace_updatable_float"));
    }
    commit();
    for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), NONLEADERS.get(1)}) {
      assertEquals("newtitle", client.getById(id).get("title_s"));
      assertEquals(2.0f, client.getById(id).get("inplace_updatable_float"));
    }

    log.info("updatingDVsInAVeryOldSegment: This test passed fine...");
  }


  /**
   * Test scenario:
   * <ul>
   *   <li>Send a batch of documents to one node</li>
   *   <li>Batch consist of an update for document which is existed and an update for documents which is not existed </li>
   *   <li>Assumption which is made is that both updates will be applied: field for existed document will be updated,
   *   new document will be created for a non existed one</li>
   * </ul>
   *
   */
  private void updateExistingThenNonExistentDoc() throws Exception {
    clearIndex();
    index("id", 1, "inplace_updatable_float", "1", "title_s", "newtitle");
    commit();
    SolrInputDocument existingDocUpdate = new SolrInputDocument();
    existingDocUpdate.setField("id", 1);
    existingDocUpdate.setField("inplace_updatable_float", map("set", "50"));

    SolrInputDocument nonexistentDocUpdate = new SolrInputDocument();
    nonexistentDocUpdate.setField("id", 2);
    nonexistentDocUpdate.setField("inplace_updatable_float", map("set", "50"));
    
    SolrInputDocument docs[] = new SolrInputDocument[] {existingDocUpdate, nonexistentDocUpdate};

    SolrClient solrClient = clients.get(random().nextInt(clients.size()));
    add(solrClient, null, docs);
    commit();
    for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), NONLEADERS.get(1)}) {
      for (SolrInputDocument expectDoc : docs) {
        String docId = expectDoc.getFieldValue("id").toString();
        SolrDocument actualDoc = client.getById(docId);
        assertNotNull("expected to get doc by id:" + docId, actualDoc);
        assertEquals("expected to update "+actualDoc, 
            50.0f, actualDoc.get("inplace_updatable_float"));
      }
    }
  }

  /**
   * Retries the specified 'req' against each SolrClient in "clients" until the expected number of 
   * results are returned, at which point the results are verified using assertDocIdsAndValuesInResults
   *
   * @param debug used in log and assertion messages
   * @param req the query to execut, should include rows &amp; sort params such that the results can be compared to luceneDocids and valuesList
   * @param luceneDocids a list of "[docid]" values to be tested against each doc in the req results (in order)
   * @param fieldName used to get value from the doc to validate with valuesList
   * @param valuesList a list of given fieldName values to be tested against each doc in results (in order)
   */
  private void assertDocIdsAndValuesAgainstAllClients(final String debug,
                                                      final SolrParams req,
                                                      final List<Integer> luceneDocids,
                                                      final String fieldName,
                                                      final List<Number> valuesList) throws Exception {
    assert luceneDocids.size() == valuesList.size();
    final long numFoundExpected = luceneDocids.size();
    
    CLIENT: for (SolrClient client : clients) {
      final String clientDebug = client.toString() + (LEADER.equals(client) ? " (leader)" : " (not leader)");
      final String msg = "'"+debug+"' results against client: " + clientDebug;
      SolrDocumentList results = null;
      // For each client, do a (sorted) sanity check query to confirm searcher has been re-opened
      // after our update -- if the numFound matches our expectations, then verify the inplace float
      // value and [docid] of each result doc against our expecations to ensure that the values were
      // updated properly w/o the doc being completley re-added internally. (ie: truly inplace)
      RETRY: for (int attempt = 0; attempt <= NUM_RETRIES; attempt++) {
        log.info("Attempt #{} checking {}", attempt, msg);
        results = client.query(req).getResults();
        if (numFoundExpected == results.getNumFound()) {
          break RETRY;
        }
        if (attempt == NUM_RETRIES) {
          fail("Repeated retry for "+msg+"; Never got numFound="+numFoundExpected+"; results=> "+results);
        }
        log.info("numFound missmatch, searcher may not have re-opened yet.  Will sleep an retry...");
        Thread.sleep(WAIT_TIME);          
      }
      
      assertDocIdsAndValuesInResults(msg, results, luceneDocids, fieldName, valuesList);
    }
  }
  
  /**
   * Given a result list sorted by "id", asserts that the "[docid] and "inplace_updatable_float" values 
   * for each document match in order.
   *
   * @param msgPre used as a prefix for assertion messages
   * @param results the sorted results of some query, such that all matches are included (ie: rows = numFound)
   * @param luceneDocids a list of "[docid]" values to be tested against each doc in results (in order)
   * @param fieldName used to get value from the doc to validate with valuesList
   * @param valuesList a list of given fieldName values to be tested against each doc in results (in order)
   */
  private void assertDocIdsAndValuesInResults(final String msgPre,
                                              final SolrDocumentList results,
                                              final List<Integer> luceneDocids,
                                              final String fieldName,
                                              final List<Number> valuesList) {

    assert luceneDocids.size() == valuesList.size();
    assertEquals(msgPre + ": rows param wasn't big enough, we need to compare all results matching the query",
                 results.getNumFound(), results.size());
    assertEquals(msgPre + ": didn't get a result for every known docid",
                 luceneDocids.size(), results.size());
    
    for (SolrDocument doc : results) {
      final int id = Integer.parseInt(doc.get("id").toString());
      final Object val = doc.get(fieldName);
      final Object docid = doc.get("[docid]");
      assertEquals(msgPre + " wrong val for " + doc.toString(), valuesList.get(id), val);
      assertEquals(msgPre + " wrong [docid] for " + doc.toString(), luceneDocids.get(id), docid);
    }
  }
  
  
  private void ensureRtgWorksWithPartialUpdatesTest() throws Exception {
    clearIndex();
    commit();

    float inplace_updatable_float = 1;
    String title = "title100";
    long version = 0, currentVersion;

    currentVersion = buildRandomIndex(100).get(0);
    assertTrue(currentVersion > version);

    // do an initial (non-inplace) update to ensure both the float & int fields we care about have (any) value
    // that way all subsequent atomic updates will be inplace
    currentVersion = addDocAndGetVersion("id", 100,
                                         "inplace_updatable_float", map("set", r.nextFloat()),
                                         "inplace_updatable_int", map("set", r.nextInt()));
    LEADER.commit();
    
    // get the internal docids of id=100 document from the three replicas
    List<Integer> docids = getInternalDocIds("100");

    // update doc, set
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("set", inplace_updatable_float));
    assertTrue(currentVersion > version);
    version = currentVersion;
    LEADER.commit();
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));
    
    SolrDocument sdoc = LEADER.getById("100");  // RTG straight from the index
    assertEquals(sdoc.toString(), inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals(sdoc.toString(), title, sdoc.get("title_s"));
    assertEquals(sdoc.toString(), version, sdoc.get("_version_"));

    if(r.nextBoolean()) {
      title = "newtitle100";
      currentVersion = addDocAndGetVersion("id", 100, "title_s", title, "inplace_updatable_float", inplace_updatable_float); // full indexing
      assertTrue(currentVersion > version);
      version = currentVersion;

      sdoc = LEADER.getById("100");  // RTG from the tlog
      assertEquals(sdoc.toString(), inplace_updatable_float, sdoc.get("inplace_updatable_float"));
      assertEquals(sdoc.toString(), title, sdoc.get("title_s"));
      assertEquals(sdoc.toString(), version, sdoc.get("_version_"));

      // we've done a full index, so we need to update the [docid] for each replica
      LEADER.commit(); // can't get (real) [docid] from the tlogs, need to force a commit
      docids = getInternalDocIds("100");
    }

    inplace_updatable_float++;
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("inc", 1));
    assertTrue(currentVersion > version);
    version = currentVersion;
    LEADER.commit();
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));
    
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_int", map("set", "100"));
    assertTrue(currentVersion > version);
    version = currentVersion;

    inplace_updatable_float++;
    currentVersion = addDocAndGetVersion("id", 100, "inplace_updatable_float", map("inc", 1));
    assertTrue(currentVersion > version);
    version = currentVersion;

    // set operation with invalid value for field
    SolrException e = expectThrows(SolrException.class,
        () -> addDocAndGetVersion( "id", 100, "inplace_updatable_float", map("set", "NOT_NUMBER")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("For input string: \"NOT_NUMBER\""));

    // inc operation with invalid inc value
    e = expectThrows(SolrException.class,
        () -> addDocAndGetVersion( "id", 100, "inplace_updatable_int", map("inc", "NOT_NUMBER")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    MatcherAssert.assertThat(e.getMessage(), containsString("For input string: \"NOT_NUMBER\""));

    // RTG from tlog(s)
    for (SolrClient client : clients) {
      final String clientDebug = client.toString() + (LEADER.equals(client) ? " (leader)" : " (not leader)");
      sdoc = client.getById("100", params("distrib", "false"));

      assertEquals(clientDebug + " => "+ sdoc, 100, sdoc.get("inplace_updatable_int"));
      assertEquals(clientDebug + " => "+ sdoc, inplace_updatable_float, sdoc.get("inplace_updatable_float"));
      assertEquals(clientDebug + " => "+ sdoc, title, sdoc.get("title_s"));
      assertEquals(clientDebug + " => "+ sdoc, version, sdoc.get("_version_"));
    }
    
    // assert that the internal docid for id=100 document remains same, in each replica, as before
    LEADER.commit(); // can't get (real) [docid] from the tlogs, need to force a commit
    assertTrue("Earlier: "+docids+", now: "+getInternalDocIds("100"), docids.equals(getInternalDocIds("100")));

    log.info("ensureRtgWorksWithPartialUpdatesTest: This test passed fine...");
  }

  /**
   * Returns the "[docid]" value(s) returned from a non-distrib RTG to each of the clients used 
   * in this test (in the same order as the clients list)
   */
  private List<Integer> getInternalDocIds(String id) throws SolrServerException, IOException {
    List<Integer> ret = new ArrayList<>(clients.size());
    for (SolrClient client : clients) {
      SolrDocument doc = client.getById(id, params("distrib", "false", "fl", "[docid]"));
      Object docid = doc.get("[docid]");
      assertNotNull(docid);
      assertEquals(Integer.class, docid.getClass());
      ret.add((Integer) docid);
    }
    assert clients.size() == ret.size();
    return ret;
  }

  private void outOfOrderUpdatesIndividualReplicaTest() throws Exception {
    clearIndex();
    commit();

    buildRandomIndex(0);

    float inplace_updatable_float = 1;
    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    for (int i=1; i<atLeast(3); i++) {
      updates.add(simulatedUpdateRequest(version0 + i, "id", 0, "inplace_updatable_float", newinplace_updatable_float + i, "_version_", version0 + i + 1));
    }

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      if (log.isInfoEnabled()) {
        log.info("Issuing well ordered update: {}", update.getDocuments());
      }
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads, since some of these updates will
    // be blocking calls, waiting for some previous updates to arrive on which it depends.
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, r);
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      if (log.isInfoEnabled()) {
        log.info("Testing client: {}", ((HttpSolrClient) client).getBaseURL());
      }
      assertReplicaValue(client, 0, "inplace_updatable_float", (newinplace_updatable_float + (float)(updates.size() - 1)), 
          "inplace_updatable_float didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
      assertReplicaValue(client, 0, "title_s", "title0_new", 
          "Title didn't match for replica at client: " + ((HttpSolrClient)client).getBaseURL());
      assertEquals(version0 + updates.size(), getReplicaValue(client, 0, "_version_"));
    }

    log.info("outOfOrderUpdatesIndividualReplicaTest: This test passed fine...");
  }
  
  // The following should work: full update to doc 0, in-place update for doc 0, delete doc 0
  private void reorderedDeletesTest() throws Exception {
    
    clearIndex();
    commit();

    buildRandomIndex(0);

    float inplace_updatable_float = 1;
    // update doc, set
    index("id", 0, "inplace_updatable_float", map("set", inplace_updatable_float));

    LEADER.commit();
    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    assertEquals(inplace_updatable_float, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    // put replica out of sync
    float newinplace_updatable_float = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", "inplace_updatable_float", newinplace_updatable_float, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "inplace_updatable_float", newinplace_updatable_float + 1, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(0, version0 + 3));

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      if (log.isInfoEnabled()) {
        log.info("Issuing well ordered update: {}", update.getDocuments());
      }
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));

    // re-order the updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, r);
    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0), random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

    // assert all requests were successful
    for (Future<UpdateResponse> resp: updateResponses) {
      assertEquals(0, resp.get().getStatus());
    }

    // assert both replicas have same effect
    for (SolrClient client : NONLEADERS) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
    }

    log.info("reorderedDeletesTest: This test passed fine...");
  }

  /* Test for a situation when a document requiring in-place update cannot be "resurrected"
   * when the original full indexed document has been deleted by an out of order DBQ.
   * Expected behaviour in this case should be to throw the replica into LIR (since this will
   * be rare). Here's an example of the situation:
        ADD(id=x, val=5, ver=1)
        UPD(id=x, val=10, ver = 2)
        DBQ(q=val:10, v=4)
        DV(id=x, val=5, ver=3)
   */
  private void reorderedDBQsResurrectionTest() throws Exception {
    if (onlyLeaderIndexes) {
      log.info("RTG with DBQs are not working in tlog replicas");
      return;
    }
    clearIndex();
    commit();

    buildRandomIndex(0);

    SolrDocument sdoc = LEADER.getById("0");  // RTG straight from the index
    //assertEquals(value, sdoc.get("inplace_updatable_float"));
    assertEquals("title0", sdoc.get("title_s"));
    long version0 = (long) sdoc.get("_version_");

    String field = "inplace_updatable_int";
    
    // put replica out of sync
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, "title_s", "title0_new", field, 5, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, field, 10, "_version_", version0 + 2)); // inplace_updatable_float=101
    updates.add(simulatedUpdateRequest(version0 + 2, "id", 0, field, 5, "_version_", version0 + 3)); // inplace_updatable_float=101
    updates.add(simulatedDeleteRequest(field+":10", version0 + 4)); // supposed to not delete anything

    // order the updates correctly for NONLEADER 1
    for (UpdateRequest update : updates) {
      if (log.isInfoEnabled()) {
        log.info("Issuing well ordered update: {}", update.getDocuments());
      }
      NONLEADERS.get(1).request(update);
    }

    // Reordering needs to happen using parallel threads
    ExecutorService threadpool = 
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));
    // re-order the last two updates for NONLEADER 0
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.swap(reorderedUpdates, 2, 3);

    List<Future<UpdateResponse>> updateResponses = new ArrayList<>();
    for (UpdateRequest update : reorderedUpdates) {
      // pretend as this update is coming from the other non-leader, so that
      // the resurrection can happen from there (instead of the leader)
      update.setParam(DistributedUpdateProcessor.DISTRIB_FROM, ((HttpSolrClient)NONLEADERS.get(1)).getBaseURL());
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, NONLEADERS.get(0),
                                                                         random().nextLong());
      updateResponses.add(threadpool.submit(task));
      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(10);
    }
    
    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

    int successful = 0;
    for (Future<UpdateResponse> resp: updateResponses) {
      try {
        UpdateResponse r = resp.get();
        if (r.getStatus() == 0) {
          successful++;
        }
      } catch (Exception ex) {
        // reordered DBQ should trigger an error, thus throwing the replica into LIR.
        // the cause of the error is that the full document was deleted by mistake due to the
        // out of order DBQ, and the in-place update that arrives after the DBQ (but was supposed to 
        // arrive before) cannot be applied, since the full document can't now be "resurrected".

        if (!ex.getMessage().contains("Tried to fetch missing update"
            + " from the leader, but missing wasn't present at leader.")) {
          throw ex;
        }
      }
    }
    // All should succeed, i.e. no LIR
    assertEquals(updateResponses.size(), successful);

    if (log.isInfoEnabled()) {
      log.info("Non leader 0: {}", ((HttpSolrClient) NONLEADERS.get(0)).getBaseURL());
      log.info("Non leader 1: {}", ((HttpSolrClient) NONLEADERS.get(1)).getBaseURL()); // nowarn
    }
    
    SolrDocument doc0 = NONLEADERS.get(0).getById(String.valueOf(0), params("distrib", "false"));
    SolrDocument doc1 = NONLEADERS.get(1).getById(String.valueOf(0), params("distrib", "false"));

    log.info("Doc in both replica 0: {}", doc0);
    log.info("Doc in both replica 1: {}", doc1);
    // assert both replicas have same effect
    for (int i=0; i<NONLEADERS.size(); i++) { // 0th is re-ordered replica, 1st is well-ordered replica
      SolrClient client = NONLEADERS.get(i);
      SolrDocument doc = client.getById(String.valueOf(0), params("distrib", "false"));
      assertNotNull("Client: "+((HttpSolrClient)client).getBaseURL(), doc);
      assertEquals("Client: "+((HttpSolrClient)client).getBaseURL(), 5, doc.getFieldValue(field));
    }

    log.info("reorderedDBQsResurrectionTest: This test passed fine...");
    clearIndex();
    commit();
  }
  
  private void delayedReorderingFetchesMissingUpdateFromLeaderTest() throws Exception {
    clearIndex();
    commit();
    
    float inplace_updatable_float = 1F;
    buildRandomIndex(inplace_updatable_float, Collections.singletonList(1));

    float newinplace_updatable_float = 100F;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(regularUpdateRequest("id", 1, "title_s", "title1_new", "id_i", 1, "inplace_updatable_float", newinplace_updatable_float));
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1)));
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1)));

    // The next request to replica2 will be delayed (timeout is 5s)
    shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay(
        "Waiting for dependant update to timeout", 1, 6000);

    ExecutorService threadpool =
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));
    for (UpdateRequest update : updates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, cloudClient,
                                                                         random().nextLong());
      threadpool.submit(task);

      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(100); 
    }

    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

    commit();

    // TODO: Could try checking ZK for LIR flags to ensure LIR has not kicked in
    // Check every 10ms, 100 times, for a replica to go down (& assert that it doesn't)
    for (int i=0; i<100; i++) {
      Thread.sleep(10);
      cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
      ClusterState state = cloudClient.getZkStateReader().getClusterState();

      int numActiveReplicas = 0;
      for (Replica rep: state.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1).getReplicas())
        if (rep.getState().equals(Replica.State.ACTIVE))
          numActiveReplicas++;

      assertEquals("The replica receiving reordered updates must not have gone down", 3, numActiveReplicas);
    }
    
    for (SolrClient client : clients) {
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      try {
        timeout.waitFor("Timeout", () -> {
          try {
            return (float) getReplicaValue(client, 1, "inplace_updatable_float") == newinplace_updatable_float + 2.0f;
          } catch (SolrServerException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      } catch (TimeoutException e) {

      }
    }
    
    for (SolrClient client : clients) {
      if (log.isInfoEnabled()) {
        log.info("Testing client (Fetch missing test): {}", ((HttpSolrClient) client).getBaseURL());
        log.info("Version at {} is: {}"
            , ((HttpSolrClient) client).getBaseURL(), getReplicaValue(client, 1, "_version_")); // nowarn
      }
      assertReplicaValue(client, 1, "inplace_updatable_float", (newinplace_updatable_float + 2.0f),
          "inplace_updatable_float didn't match for replica at client: " + ((HttpSolrClient) client).getBaseURL());
      assertReplicaValue(client, 1, "title_s", "title1_new",
          "Title didn't match for replica at client: " + ((HttpSolrClient) client).getBaseURL());
    }
    
    // Try another round of these updates, this time with a delete request at the end.
    // This is to ensure that the fetch missing update from leader doesn't bomb out if the 
    // document has been deleted on the leader later on
    {
      clearIndex();
      commit();
      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().unsetDelay();
      
      updates.add(regularDeleteRequest(1));

      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay("Waiting for dependant update to timeout", 1, 5999); // the first update
      shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay("Waiting for dependant update to timeout", 4, 5998); // the delete update

      threadpool =
          ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));
      for (UpdateRequest update : updates) {
        AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, cloudClient,
                                                                           random().nextLong());
        threadpool.submit(task);
        
        // while we can't guarantee/trust what order the updates are executed in, since multiple threads
        // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
        Thread.sleep(100);
      }

      threadpool.shutdown();
      assertTrue("Thread pool didn't terminate within 15 secs", threadpool.awaitTermination(15, TimeUnit.SECONDS));

      commit();

      try (ZkShardTerms zkShardTerms = new ZkShardTerms(DEFAULT_COLLECTION, SHARD1, cloudClient.getZkStateReader().getZkClient())) {
        for (int i=0; i<100; i++) {
          Thread.sleep(10);
          cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
          ClusterState state = cloudClient.getZkStateReader().getClusterState();

          int numActiveReplicas = 0;
          for (Replica rep: state.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1).getReplicas()) {
            assertTrue(zkShardTerms.canBecomeLeader(rep.getName()));
            if (rep.getState().equals(Replica.State.ACTIVE))
              numActiveReplicas++;
          }
          assertEquals("The replica receiving reordered updates must not have gone down", 3, numActiveReplicas);
        }
      }

      for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), 
          NONLEADERS.get(1)}) { // nonleader 0 re-ordered replica, nonleader 1 well-ordered replica
        SolrDocument doc = client.getById(String.valueOf(1), params("distrib", "false"));
        assertNull("This doc was supposed to have been deleted, but was: " + doc, doc);
      }

    }
    log.info("delayedReorderingFetchesMissingUpdateFromLeaderTest: This test passed fine...");
  }

  /**
   * Use the schema API to verify that the specified expected Field exists with those exact attributes. 
   */
  public void checkExpectedSchemaField(Map<String,Object> expected) throws Exception {
    String fieldName = (String) expected.get("name");
    assertNotNull("expected contains no name: " + expected, fieldName);
    FieldResponse rsp = new Field(fieldName).process(this.cloudClient);
    assertNotNull("Field Null Response: " + fieldName, rsp);
    assertEquals("Field Status: " + fieldName + " => " + rsp.toString(), 0, rsp.getStatus());
    assertEquals("Field: " + fieldName, expected, rsp.getField());
  }

  private class AsyncUpdateWithRandomCommit implements Callable<UpdateResponse> {
    UpdateRequest update;
    SolrClient solrClient;
    final Random rnd;
    int commitBound = onlyLeaderIndexes ? 50 : 3;

    public AsyncUpdateWithRandomCommit (UpdateRequest update, SolrClient solrClient, long seed) {
      this.update = update;
      this.solrClient = solrClient;
      this.rnd = new Random(seed);
    }

    @Override
    public UpdateResponse call() throws Exception {
      UpdateResponse resp = update.process(solrClient); //solrClient.request(update);
      if (rnd.nextInt(commitBound) == 0)
        solrClient.commit();
      return resp;
    }
  }
  
  Object getReplicaValue(SolrClient client, int doc, String field) throws SolrServerException, IOException {
    SolrDocument sdoc = client.getById(String.valueOf(doc), params("distrib", "false"));
    return sdoc==null? null: sdoc.get(field);
  }

  void assertReplicaValue(SolrClient client, int doc, String field, Object expected,
      String message) throws SolrServerException, IOException {
    assertEquals(message, expected, getReplicaValue(client, doc, field));
  }

  // This returns an UpdateRequest with the given fields that represent a document.
  // This request is constructed such that it is a simulation of a request coming from
  // a leader to a replica.
  UpdateRequest simulatedUpdateRequest(Long prevVersion, Object... fields) throws SolrServerException, IOException {
    SolrInputDocument doc = sdoc(fields);
    
    // get baseUrl of the leader
    String baseUrl = getBaseUrl(doc.get("id").toString());

    UpdateRequest ur = new UpdateRequest();
    ur.add(doc);
    ur.setParam("update.distrib", "FROMLEADER");
    if (prevVersion != null) {
      ur.setParam("distrib.inplace.prevversion", String.valueOf(prevVersion));
      ur.setParam("distrib.inplace.update", "true");
    }
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  UpdateRequest simulatedDeleteRequest(int id, long version) throws SolrServerException, IOException {
    String baseUrl = getBaseUrl(""+id);

    UpdateRequest ur = new UpdateRequest();
    if (random().nextBoolean() || onlyLeaderIndexes) {
      ur.deleteById(""+id);
    } else {
      ur.deleteByQuery("id:"+id);
    }
    ur.setParam("_version_", ""+version);
    ur.setParam("update.distrib", "FROMLEADER");
    ur.setParam("distrib.from", baseUrl);
    return ur;
  }

  UpdateRequest simulatedDeleteRequest(String query, long version) throws SolrServerException, IOException {
    String baseUrl = getBaseUrl((HttpSolrClient)LEADER);

    UpdateRequest ur = new UpdateRequest();
    ur.deleteByQuery(query);
    ur.setParam("_version_", ""+version);
    ur.setParam("update.distrib", "FROMLEADER");
    ur.setParam("distrib.from", baseUrl + DEFAULT_COLLECTION + "/");
    return ur;
  }

  private String getBaseUrl(String id) {
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    Slice slice = collection.getRouter().getTargetSlice(id, null, null, null, collection);
    String baseUrl = slice.getLeader().getCoreUrl();
    return baseUrl;
  }

  UpdateRequest regularUpdateRequest(Object... fields) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    SolrInputDocument doc = sdoc(fields);
    ur.add(doc);
    return ur;
  }

  UpdateRequest regularDeleteRequest(int id) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    ur.deleteById(""+id);
    return ur;
  }

  UpdateRequest regularDeleteByQueryRequest(String q) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    ur.deleteByQuery(q);
    return ur;
  }

  @SuppressWarnings("rawtypes")
  protected long addDocAndGetVersion(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParam("versions", "true");
    ureq.add(doc);
    UpdateResponse resp;
    
    // send updates to leader, to avoid SOLR-8733
    resp = ureq.process(LEADER);
    
    long returnedVersion = Long.parseLong(((NamedList)resp.getResponse().get("adds")).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion > 0);
    return returnedVersion;
  }

  /**
   * Convinience method variant that never uses <code>initFloat</code>
   * @see #buildRandomIndex(Float,List)
   */
  protected List<Long> buildRandomIndex(Integer... specialIds) throws Exception {
    return buildRandomIndex(null, Arrays.asList(specialIds));
  }
                                        
  /** 
   * Helper method to build a randomized index with the fields needed for all test methods in this class.
   * At a minimum, this index will contain 1 doc per "special" (non-negative) document id.  These special documents will be added with the <code>initFloat</code> specified in the "inplace_updatable_float" field.
   *
   * A random number of documents (with negative ids) will be indexed in between each of the 
   * "special" documents, as well as before/after the first/last special document.
   *
   * @param initFloat Value to use in the "inplace_updatable_float" for the special documents; will never be used if null
   * @param specialIds The ids to use for the special documents, all values must be non-negative
   * @return the versions of each of the specials document returned when indexing it
   */
  protected List<Long> buildRandomIndex(Float initFloat, List<Integer> specialIds) throws Exception {

    int id = -1; // used for non special docs
    final int numPreDocs = rarely() || onlyLeaderIndexes ? TestUtil.nextInt(random(),0,9) : atLeast(10);
    for (int i = 1; i <= numPreDocs; i++) {
      addDocAndGetVersion("id", id, "title_s", "title" + id, "id_i", id);
      id--;
    }
    final List<Long> versions = new ArrayList<>(specialIds.size());
    for (int special : specialIds) {
      if (null == initFloat) {
        versions.add(addDocAndGetVersion("id", special, "title_s", "title" + special, "id_i", special));
      } else {
        versions.add(addDocAndGetVersion("id", special, "title_s", "title" + special, "id_i", special,
                                         "inplace_updatable_float", initFloat));
      }
      final int numPostDocs = rarely() || onlyLeaderIndexes ? TestUtil.nextInt(random(),0,2) : atLeast(10);
      for (int i = 1; i <= numPostDocs; i++) {
        addDocAndGetVersion("id", id, "title_s", "title" + id, "id_i", id);
        id--;
      }
    }
    LEADER.commit();
    
    assert specialIds.size() == versions.size();
    return versions;
  }

  /*
   * Situation:
   * add(id=1,inpfield=12,title=mytitle,version=1)
   * inp(id=1,inpfield=13,prevVersion=1,version=2) // timeout indefinitely
   * inp(id=1,inpfield=14,prevVersion=2,version=3) // will wait till timeout, and then fetch a "not found" from leader
   * dbq("inp:14",version=4)
   */
  private void reorderedDBQsUsingUpdatedValueFromADroppedUpdate() throws Exception {
    if (onlyLeaderIndexes) {
      log.info("RTG with DBQs are not working in tlog replicas");
      return;
    }
    clearIndex();
    commit();
    
    float inplace_updatable_float = 1F;
    buildRandomIndex(inplace_updatable_float, Collections.singletonList(1));

    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(regularUpdateRequest("id", 1, "id_i", 1, "inplace_updatable_float", 12, "title_s", "mytitle"));
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1))); // delay indefinitely
    updates.add(regularUpdateRequest("id", 1, "inplace_updatable_float", map("inc", 1)));
    updates.add(regularDeleteByQueryRequest("inplace_updatable_float:14"));

    // The second request will be delayed very very long, so that the next update actually gives up waiting for this
    // and fetches a full update from the leader.
    shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().addDelay(
        "Waiting for dependant update to timeout", 2, 8000);

    ExecutorService threadpool =
        ExecutorUtil.newMDCAwareFixedThreadPool(updates.size() + 1, new SolrNamedThreadFactory(getTestName()));
    for (UpdateRequest update : updates) {
      AsyncUpdateWithRandomCommit task = new AsyncUpdateWithRandomCommit(update, cloudClient,
                                                                         random().nextLong());
      threadpool.submit(task);

      // while we can't guarantee/trust what order the updates are executed in, since multiple threads
      // are involved, but we're trying to bias the thread scheduling to run them in the order submitted
      Thread.sleep(100); 
    }

    threadpool.shutdown();
    assertTrue("Thread pool didn't terminate within 12 secs", threadpool.awaitTermination(12, TimeUnit.SECONDS));

    commit();

    // TODO: Could try checking ZK for LIR flags to ensure LIR has not kicked in
    // Check every 10ms, 100 times, for a replica to go down (& assert that it doesn't)
    for (int i=0; i<100; i++) {
      Thread.sleep(10);
      cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
      ClusterState state = cloudClient.getZkStateReader().getClusterState();

      int numActiveReplicas = 0;
      for (Replica rep: state.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1).getReplicas())
        if (rep.getState().equals(Replica.State.ACTIVE))
          numActiveReplicas++;

      assertEquals("The replica receiving reordered updates must not have gone down", 3, numActiveReplicas);
    }

    for (SolrClient client : clients) {
      if (log.isInfoEnabled()) {
        log.info("Testing client (testDBQUsingUpdatedFieldFromDroppedUpdate): {}", ((HttpSolrClient) client).getBaseURL());
        log.info("Version at {} is: {}", ((HttpSolrClient) client).getBaseURL(),
            getReplicaValue(client, 1, "_version_")); // nowarn
      }
      assertNull(client.getById("1", params("distrib", "false")));
    }

    log.info("reorderedDBQsUsingUpdatedValueFromADroppedUpdate: This test passed fine...");
  }

  @Override
  public void clearIndex() {
    super.clearIndex();
    try {
      for (SolrClient client: new SolrClient[] {LEADER, NONLEADERS.get(0), NONLEADERS.get(1)}) {
        if (client != null) {
          client.request(simulatedDeleteRequest("*:*", -Long.MAX_VALUE));
          client.commit();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
