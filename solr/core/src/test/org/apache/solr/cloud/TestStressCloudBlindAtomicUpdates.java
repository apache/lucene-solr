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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Field;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.FieldType;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse.FieldTypeResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress test of Atomic Updates in a MinCloud Cluster.
 * 
 * Focus of test is parallel threads hammering updates on diff docs using random clients/nodes, 
 * Optimistic Concurrency is not used here because of SOLR-8733, instead we just throw lots of 
 * "inc" operations at a numeric field and check that the math works out at the end.
 */
@Slow
@SuppressSSL(bugUrl="SSL overhead seems to cause OutOfMemory when stress testing")
public class TestStressCloudBlindAtomicUpdates extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String COLLECTION_NAME = "test_col";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** One client per node */
  private static final ArrayList<HttpSolrClient> CLIENTS = new ArrayList<>(5);

  /** Service to execute all parallel work 
   * @see #NUM_THREADS
   */
  private static ExecutorService EXEC_SERVICE;

  /** num parallel threads in use by {@link #EXEC_SERVICE} */
  private static int NUM_THREADS;

  /** 
   * Used as an increment and multiplier when deciding how many docs should be in
   * the test index.  1 means every doc in the index is a candidate for updates, bigger numbers mean a
   * larger index is used (so tested docs are more likeely to be spread out in multiple segments)
   */
  private static int DOC_ID_INCR;

  /**
   * The TestInjection configuration to be used for the current test method.
   *
   * Value is set by {@link #clearCloudCollection}, and used by {@link #startTestInjection} -- but only once 
   * initial index seeding has finished (we're focusing on testing atomic updates, not basic indexing).
   */
  private String testInjection = null;

  @BeforeClass
  @SuppressWarnings({"unchecked"})
  private static void createMiniSolrCloudCluster() throws Exception {
    // NOTE: numDocsToCheck uses atLeast, so nightly & multiplier are alreayd a factor in index size
    // no need to redundently factor them in here as well
    DOC_ID_INCR = TestUtil.nextInt(random(), 1, 7);
    
    NUM_THREADS = atLeast(3);
    EXEC_SERVICE = ExecutorUtil.newMDCAwareFixedThreadPool
      (NUM_THREADS, new SolrNamedThreadFactory(DEBUG_LABEL));
    
    // at least 2, but don't go crazy on nightly/test.multiplier with "atLeast()"
    final int numShards = TEST_NIGHTLY ? 5 : 2; 
    final int repFactor = 2; 
    final int numNodes = numShards * repFactor;
   
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = Paths.get(TEST_HOME(), "collection1", "conf");
    
    configureCluster(numNodes).addConfig(configName, configDir).configure();

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .withProperty("config", "solrconfig-tlog.xml")
        .withProperty("schema", "schema-minimal-atomic-stress.xml")
        .process(CLOUD_CLIENT);

    waitForRecoveriesToFinish(CLOUD_CLIENT);

    CLIENTS.clear();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      assertNotNull("Cluster contains null jetty?", jetty);
      final URL baseUrl = jetty.getBaseUrl();
      assertNotNull("Jetty has null baseUrl: " + jetty.toString(), baseUrl);
      CLIENTS.add(getHttpSolrClient(baseUrl + "/" + COLLECTION_NAME + "/"));
    }

    // sanity check no one broke the assumptions we make about our schema
    checkExpectedSchemaType( map("name","long",
                                 "class", RANDOMIZED_NUMERIC_FIELDTYPES.get(Long.class),
                                 "multiValued",Boolean.FALSE,
                                 "indexed",Boolean.FALSE,
                                 "stored",Boolean.FALSE,
                                 "docValues",Boolean.FALSE) );
  }
  
  @AfterClass
  private static void afterClass() throws Exception {
    TestInjection.reset();
    if (null != EXEC_SERVICE) {
      ExecutorUtil.shutdownAndAwaitTermination(EXEC_SERVICE);
      EXEC_SERVICE = null;
    }
    if (null != CLOUD_CLIENT) {
      IOUtils.closeQuietly(CLOUD_CLIENT);
      CLOUD_CLIENT = null;
    }
    for (HttpSolrClient client : CLIENTS) {
      if (null == client) {
        log.error("CLIENTS contains a null SolrClient???");
      }
      IOUtils.closeQuietly(client);
    }
    CLIENTS.clear();
  }
  
  @Before
  private void clearCloudCollection() throws Exception {
    TestInjection.reset();
    waitForRecoveriesToFinish(CLOUD_CLIENT);
    
    assertEquals(0, CLOUD_CLIENT.deleteByQuery("*:*").getStatus());
    assertEquals(0, CLOUD_CLIENT.optimize().getStatus());
    
    assertEquals("Collection should be empty!",
                 0, CLOUD_CLIENT.query(params("q", "*:*")).getResults().getNumFound());

    final int injectionPercentage = (int)Math.ceil(atLeast(1) / 2);
    testInjection = usually() ? "false:0" : ("true:" + injectionPercentage);
  }

  /**
   * Assigns {@link #testInjection} to various TestInjection variables.  Calling this 
   * method multiple times in the same method should always result in the same setting being applied 
   * (even if {@link TestInjection#reset} was called in between.
   *
   * NOTE: method is currently a No-Op pending SOLR-13189
   */
  private void startTestInjection() {
    log.info("TODO: TestInjection disabled pending solution to SOLR-13189");
    //log.info("TestInjection: fail replica, update pause, tlog pauses: " + testInjection);
    //TestInjection.failReplicaRequests = testInjection;
    //TestInjection.updateLogReplayRandomPause = testInjection;
    //TestInjection.updateRandomPause = testInjection;
  }


  @Test
  @SuppressWarnings({"unchecked"})
  public void test_dv() throws Exception {
    String field = "long_dv";
    checkExpectedSchemaField(map("name", field,
                                 "type","long",
                                 "stored",Boolean.FALSE,
                                 "indexed",Boolean.FALSE,
                                 "docValues",Boolean.TRUE));
    
    checkField(field);
  }
  
  @Test
  @SuppressWarnings({"unchecked"})
  public void test_dv_stored() throws Exception {
    String field = "long_dv_stored";
    checkExpectedSchemaField(map("name", field,
                                 "type","long",
                                 "stored",Boolean.TRUE,
                                 "indexed",Boolean.FALSE,
                                 "docValues",Boolean.TRUE));
    
    checkField(field);

  }
  @SuppressWarnings({"unchecked"})
  public void test_dv_stored_idx() throws Exception {
    String field = "long_dv_stored_idx";
    checkExpectedSchemaField(map("name", field,
                                 "type","long",
                                 "stored",Boolean.TRUE,
                                 "indexed",Boolean.TRUE,
                                 "docValues",Boolean.TRUE));
    
    checkField(field);
  }

  @SuppressWarnings({"unchecked"})
  public void test_dv_idx() throws Exception {
    String field = "long_dv_idx";
    checkExpectedSchemaField(map("name", field,
                                 "type","long",
                                 "stored",Boolean.FALSE,
                                 "indexed",Boolean.TRUE,
                                 "docValues",Boolean.TRUE));
    
    checkField(field);
  }
  @SuppressWarnings({"unchecked"})
  public void test_stored_idx() throws Exception {
    String field = "long_stored_idx";
    checkExpectedSchemaField(map("name", field,
                                 "type","long",
                                 "stored",Boolean.TRUE,
                                 "indexed",Boolean.TRUE,
                                 "docValues",Boolean.FALSE));
    
    checkField(field);
  }
  
  public void checkField(final String numericFieldName) throws Exception {

    final CountDownLatch abortLatch = new CountDownLatch(1);

    final int numDocsToCheck = atLeast(37);
    final int numDocsInIndex = (numDocsToCheck * DOC_ID_INCR);
    final AtomicLong[] expected = new AtomicLong[numDocsToCheck];

    log.info("Testing {}: numDocsToCheck={}, numDocsInIndex={}, incr={}"
        , numericFieldName,  numDocsToCheck, numDocsInIndex, DOC_ID_INCR);
    
    // seed the index & keep track of what docs exist and with what values
    for (int id = 0; id < numDocsInIndex; id++) {
      // NOTE: the field we're mutating is a long, but we seed with a random int,
      // and we will inc/dec by random smaller ints, to ensure we never over/under flow
      final int initValue = random().nextInt();
      SolrInputDocument doc = doc(f("id",""+id), f(numericFieldName, initValue));
      UpdateResponse rsp = update(doc).process(CLOUD_CLIENT);
      assertEquals(doc.toString() + " => " + rsp.toString(), 0, rsp.getStatus());
      if (0 == id % DOC_ID_INCR) {
        expected[id / DOC_ID_INCR] = new AtomicLong(initValue);
      }
    }
    assertNotNull("Sanity Check no off-by-one in expected init: ", expected[expected.length-1]);
    
    
    // sanity check index contents
    waitForRecoveriesToFinish(CLOUD_CLIENT);
    assertEquals(0, CLOUD_CLIENT.commit().getStatus());
    assertEquals(numDocsInIndex,
                 CLOUD_CLIENT.query(params("q", "*:*")).getResults().getNumFound());

    startTestInjection();
    
    // spin up parallel workers to hammer updates
    List<Future<Worker>> results = new ArrayList<Future<Worker>>(NUM_THREADS);
    for (int workerId = 0; workerId < NUM_THREADS; workerId++) {
      Worker worker = new Worker(workerId, expected, abortLatch, new Random(random().nextLong()),
                                 numericFieldName);
      // ask for the Worker to be returned in the Future so we can inspect it
      results.add(EXEC_SERVICE.submit(worker, worker));
    }
    // check the results of all our workers
    for (Future<Worker> r : results) {
      try {
        Worker w = r.get();
        if (! w.getFinishedOk() ) {
          // quick and dirty sanity check if any workers didn't succeed, but didn't throw an exception either
          abortLatch.countDown();
          log.error("worker={} didn't finish ok, but didn't throw exception?", w.workerId);
        }
      } catch (ExecutionException ee) {
        Throwable rootCause = ee.getCause();
        if (rootCause instanceof Error) {
          // low level error, or test assertion failure - either way don't leave it wrapped
          log.error("Worker exec Error, throwing root cause", ee);
          throw (Error) rootCause;
        } else { 
          log.error("Worker ExecutionException, re-throwing", ee);
          throw ee;
        }
      }
    }

    assertEquals("Abort latch has changed, why didn't we get an exception from a worker?",
                 1L, abortLatch.getCount());
    
    TestInjection.reset();
    waitForRecoveriesToFinish(CLOUD_CLIENT);

    // check all the final index contents match our expectations
    int incorrectDocs = 0;
    for (int id = 0; id < numDocsInIndex; id += DOC_ID_INCR) {
      assert 0 == id % DOC_ID_INCR : "WTF? " + id;
      
      final long expect = expected[id / DOC_ID_INCR].longValue();
      
      final String docId = "" + id;
      
      // sometimes include an fq on the expected value to ensure the updated values
      // are "visible" for searching
      final SolrParams p = (0 != TestUtil.nextInt(random(), 0,15))
        ? params() : params("fq",numericFieldName + ":\"" + expect + "\"");
      SolrDocument doc = getRandClient(random()).getById(docId, p);
      
      final boolean foundWithFilter = (null != doc);
      if (! foundWithFilter) {
        // try again w/o fq to see what it does have
        doc = getRandClient(random()).getById(docId);
      }
      
      Long actual = (null == doc) ? null : (Long) doc.getFirstValue(numericFieldName);
      if (actual == null || expect != actual.longValue() || ! foundWithFilter) {
        log.error("docId={}, foundWithFilter={}, expected={}, actual={}",
                  docId, foundWithFilter, expect, actual);
        incorrectDocs++;
      }
      
    }
    assertEquals("Some docs had errors -- check logs", 0, incorrectDocs);
  }

  
  public static final class Worker implements Runnable {
    public final int workerId;
    final AtomicLong[] expected;
    final CountDownLatch abortLatch;
    final Random rand;
    final String updateField; 
    final int numDocsToUpdate;
    boolean ok = false; // set to true only on successful completion
    public Worker(int workerId, AtomicLong[] expected, CountDownLatch abortLatch, Random rand,
                  String updateField) {
      this.workerId = workerId;
      this.expected = expected;
      this.abortLatch = abortLatch;
      this.rand = rand;
      this.updateField = updateField;
      this.numDocsToUpdate = atLeast(rand, 25);
    }
    public boolean getFinishedOk() {
      return ok;
    }
    private void doRandomAtomicUpdate(int docId) throws Exception {
      assert 0 == docId % DOC_ID_INCR : "WTF? " + docId;
      
      final int delta = TestUtil.nextInt(rand, -1000, 1000);
      log.info("worker={}, docId={}, delta={}", workerId, docId, delta);

      SolrClient client = getRandClient(rand);
      SolrInputDocument doc = doc(f("id",""+docId),
                                  f(updateField,Collections.singletonMap("inc",delta)));
      UpdateResponse rsp = update(doc).process(client);
      assertEquals(doc + " => " + rsp, 0, rsp.getStatus());
      
      AtomicLong counter = expected[docId / DOC_ID_INCR];
      assertNotNull("null counter for " + docId + "/" + DOC_ID_INCR, counter);
      counter.getAndAdd(delta);

    }
    
    public void run() {
      final String origThreadName = Thread.currentThread().getName();
      try {
        Thread.currentThread().setName(origThreadName + "-w" + workerId);
        final int maxDocMultiplier = expected.length-1;
        for (int docIter = 0; docIter < numDocsToUpdate; docIter++) {

          final int docId = DOC_ID_INCR * TestUtil.nextInt(rand, 0, maxDocMultiplier);

          // tweak our thread name to keep track of what we're up to
          Thread.currentThread().setName(origThreadName + "-w" + workerId + "-d" + docId);

          // no matter how random the doc selection may be per thread, ensure
          // every doc that is selected by *a* thread gets at least a couple rapid fire updates
          final int itersPerDoc = atLeast(rand, 2);
          
          for (int updateIter = 0; updateIter < itersPerDoc; updateIter++) {
            if (0 == abortLatch.getCount()) {
              return;
            }
            doRandomAtomicUpdate(docId);
          }
          if (rand.nextBoolean()) { Thread.yield(); }
        }
        
      } catch (Error err) {
        log.error(Thread.currentThread().getName(), err);
        abortLatch.countDown();
        throw err;
      } catch (Exception ex) {
        log.error(Thread.currentThread().getName(), ex);
        abortLatch.countDown();
        throw new RuntimeException(ex.getMessage(), ex);
      } finally {
        Thread.currentThread().setName(origThreadName);
      }
      ok = true;
    }
  }
  
  
  public static UpdateRequest update(SolrInputDocument... docs) {
    return update(null, docs);
  }
  public static UpdateRequest update(SolrParams params, SolrInputDocument... docs) {
    UpdateRequest r = new UpdateRequest();
    if (null != params) {
      r.setParams(new ModifiableSolrParams(params));
    }
    r.add(Arrays.asList(docs));
    return r;
  }
  
  public static SolrInputDocument doc(SolrInputField... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    for (SolrInputField f : fields) {
      doc.put(f.getName(), f);
    }
    return doc;
  }
  
  public static SolrInputField f(String fieldName, Object... values) {
    SolrInputField f = new SolrInputField(fieldName);
    f.setValue(values);
    // TODO: soooooooooo stupid (but currently neccessary because atomic updates freak out
    // if the Map with the "inc" operation is inside of a collection - even if it's the only "value") ...
    if (1 == values.length) {
      f.setValue(values[0]);
    } else {
      f.setValue(values);
    }
    return f;
  }
  
  public static SolrClient getRandClient(Random rand) {
    int numClients = CLIENTS.size();
    int idx = TestUtil.nextInt(rand, 0, numClients);
    return (idx == numClients) ? CLOUD_CLIENT : CLIENTS.get(idx);
  }

  public static void waitForRecoveriesToFinish(CloudSolrClient client) throws Exception {
    assert null != client.getDefaultCollection();
    client.getZkStateReader().forceUpdateCollection(client.getDefaultCollection());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(client.getDefaultCollection(),
                                                        client.getZkStateReader(),
                                                        true, true, 330);
  }

  /**
   * Use the schema API to verify that the specified expected Field exists with those exact attributes. 
   * @see #CLOUD_CLIENT
   */
  public static void checkExpectedSchemaField(Map<String,Object> expected) throws Exception {
    String fieldName = (String) expected.get("name");
    assertNotNull("expected contains no name: " + expected, fieldName);
    FieldResponse rsp = new Field(fieldName).process(CLOUD_CLIENT);
    assertNotNull("Field Null Response: " + fieldName, rsp);
    assertEquals("Field Status: " + fieldName + " => " + rsp.toString(), 0, rsp.getStatus());
    assertEquals("Field: " + fieldName, expected, rsp.getField());
  }
  
  /**
   * Use the schema API to verify that the specified expected FieldType exists with those exact attributes. 
   * @see #CLOUD_CLIENT
   */
  public static void checkExpectedSchemaType(Map<String,Object> expected) throws Exception {
    
    String typeName = (String) expected.get("name");
    assertNotNull("expected contains no type: " + expected, typeName);
    FieldTypeResponse rsp = new FieldType(typeName).process(CLOUD_CLIENT);
    assertNotNull("FieldType Null Response: " + typeName, rsp);
    assertEquals("FieldType Status: " + typeName + " => " + rsp.toString(), 0, rsp.getStatus());
    assertEquals("FieldType: " + typeName, expected, rsp.getFieldType().getAttributes());
    
  }
}
