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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.ExpectedErr;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.addErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.assertUpdateTolerantErrors;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.delIErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.delQErr;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.f;
import static org.apache.solr.cloud.TestTolerantUpdateProcessorCloud.update;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_PARAM;
import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;

/**
 * Test of TolerantUpdateProcessor using a randomized MiniSolrCloud.
 * Reuses some utility methods in {@link TestTolerantUpdateProcessorCloud}
 *
 * <p>
 * <b>NOTE:</b> This test sets up a static instance of MiniSolrCloud with a single collection 
 * and several clients pointed at specific nodes. These are all re-used across multiple test methods, 
 * and assumes that the state of the cluster is healthy between tests.
 * </p>
 *
 */
@SuppressSSL(bugUrl="https://issues.apache.org/jira/browse/SOLR-9182 - causes OOM")
public class TestTolerantUpdateProcessorRandomCloud extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "test_col";
  
  /** A basic client for operations at the cloud level, default collection will be set */
  private static CloudSolrClient CLOUD_CLIENT;
  /** one HttpSolrClient for each server */
  private static List<HttpSolrClient> NODE_CLIENTS;

  @BeforeClass
  public static void createMiniSolrCloudCluster() throws Exception {
    
    final String configName = "solrCloudCollectionConfig";
    final File configDir = new File(TEST_HOME() + File.separator + "collection1" + File.separator + "conf");

    final int numShards = TestUtil.nextInt(random(), 2, TEST_NIGHTLY ? 5 : 3);
    final int repFactor = TestUtil.nextInt(random(), 2, TEST_NIGHTLY ? 5 : 3);
    // at least one server won't have any replicas
    final int numServers = 1 + (numShards * repFactor);

    log.info("Configuring cluster: servers={}, shards={}, repfactor={}", numServers, numShards, repFactor);
    configureCluster(numServers)
      .addConfig(configName, configDir.toPath())
      .configure();
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put("config", "solrconfig-distrib-update-processor-chains.xml");
    collectionProperties.put("schema", "schema15.xml"); // string id 

    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.setDefaultCollection(COLLECTION_NAME);

    CollectionAdminRequest.createCollection(COLLECTION_NAME, configName, numShards, repFactor)
        .setProperties(collectionProperties)
        .process(CLOUD_CLIENT);

    cluster.waitForActiveCollection(COLLECTION_NAME, numShards, numShards * repFactor);
    
    if (NODE_CLIENTS != null) {
      for (HttpSolrClient client : NODE_CLIENTS) {
        client.close();
      }
    }
    NODE_CLIENTS = new ArrayList<HttpSolrClient>(numServers);
    
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      URL jettyURL = jetty.getBaseUrl();
      NODE_CLIENTS.add(getHttpSolrClient(jettyURL.toString() + "/" + COLLECTION_NAME + "/"));
    }
    assertEquals(numServers, NODE_CLIENTS.size());
    
  }
  
  @Before
  private void deleteAllDocs() throws Exception {
    assertEquals(0, update(params("commit","true")).deleteByQuery("*:*").process(CLOUD_CLIENT).getStatus());
    assertEquals("index should be empty", 0L, countDocs(CLOUD_CLIENT));
  }
  
  @AfterClass
  public static void afterClass() throws IOException {
    if (NODE_CLIENTS != null) {
      for (HttpSolrClient client : NODE_CLIENTS) {
        client.close();
      }
    }
    NODE_CLIENTS = null;
    if (CLOUD_CLIENT != null) {
      CLOUD_CLIENT.close();
    }
    CLOUD_CLIENT = null;
  }
  
  public void testRandomUpdates() throws Exception {
    final int maxDocId = atLeast(10000);
    final BitSet expectedDocIds = new BitSet(maxDocId+1);
    
    final int numIters = atLeast(50);
    for (int i = 0; i < numIters; i++) {

      log.info("BEGIN ITER #{}", i);
      
      final UpdateRequest req = update(params("maxErrors","-1",
                                              "update.chain", "tolerant-chain-max-errors-10"));
      final int numCmds = TestUtil.nextInt(random(), 1, 20);
      final List<ExpectedErr> expectedErrors = new ArrayList<ExpectedErr>(numCmds);
      int expectedErrorsCount = 0;
      // it's ambigious/confusing which order mixed DELQ + ADD  (or ADD and DELI for the same ID)
      // in the same request wll be processed by various clients, so we keep things simple
      // and ensure that no single doc Id is affected by more then one command in the same request
      final BitSet docsAffectedThisRequest = new BitSet(maxDocId+1);
      for (int cmdIter = 0; cmdIter < numCmds; cmdIter++) {
        if ((maxDocId / 2) < docsAffectedThisRequest.cardinality()) {
          // we're already mucking with more then half the docs in the index
          break;
        }

        final boolean causeError = random().nextBoolean();
        if (causeError) {
          expectedErrorsCount++;
        }
        
        if (random().nextBoolean()) {
          // add a doc
          String id = null;
          SolrInputDocument doc = null;
          if (causeError && (0 == TestUtil.nextInt(random(), 0, 21))) {
            doc = doc(f("foo_s","no unique key"));
            expectedErrors.add(addErr("(unknown)"));
          } else {
            final int id_i = randomUnsetBit(random(), docsAffectedThisRequest, maxDocId);
            docsAffectedThisRequest.set(id_i);
            id = "id_"+id_i;
            if (causeError) {
              expectedErrors.add(addErr(id));
            } else {
              expectedDocIds.set(id_i);
            }
            final String val = causeError ? "bogus_val" : (""+TestUtil.nextInt(random(), 42, 666));
            doc = doc(f("id",id),
                      f("id_i", id_i),
                      f("foo_i", val));
          }
          req.add(doc);
          log.info("ADD: {} = {}", id, doc);
        } else {
          // delete something
          if (random().nextBoolean()) {
            // delete by id
            final int id_i = randomUnsetBit(random(), docsAffectedThisRequest, maxDocId);
            final String id = "id_"+id_i;
            final boolean docExists = expectedDocIds.get(id_i);
            docsAffectedThisRequest.set(id_i);
            long versionConstraint = docExists ? 1 : -1;
            if (causeError) {
              versionConstraint = -1 * versionConstraint;
              expectedErrors.add(delIErr(id));
            } else {
              // if doc exists it will legitimately be deleted
              expectedDocIds.clear(id_i);
            }
            req.deleteById(id, versionConstraint);
            log.info("DEL: {} = {}", id, causeError ? "ERR" : "OK" );
          } else {
            // delete by query
            final String q;
            if (causeError) {
              // even though our DBQ is gibberish that's going to fail, record a docId as affected
              // so that we don't generate the same random DBQ and get redundent errors
              // (problematic because of how DUP forwarded DBQs have to have their errors deduped by TUP)
              final int id_i = randomUnsetBit(random(), docsAffectedThisRequest, maxDocId);
              docsAffectedThisRequest.set(id_i);
              q = "foo_i:["+id_i+" TO ....giberish";
              expectedErrors.add(delQErr(q));
            } else {
              // ensure our DBQ is only over a range of docs not already affected
              // by any other cmds in this request
              final int rangeAxis = randomUnsetBit(random(), docsAffectedThisRequest, maxDocId);
              final int loBound = docsAffectedThisRequest.previousSetBit(rangeAxis);
              final int hiBound = docsAffectedThisRequest.nextSetBit(rangeAxis);
              final int lo = TestUtil.nextInt(random(), loBound+1, rangeAxis);
              final int hi = TestUtil.nextInt(random(), rangeAxis,
                                              // bound might be negative if no set bits above axis
                                              (hiBound < 0) ? maxDocId : hiBound-1);

              if (lo != hi) {
                assert lo < hi : "lo="+lo+" hi="+hi;
                // NOTE: clear & set are exclusive of hi, so we use "}" in range query accordingly
                q = "id_i:[" + lo + " TO " + hi + "}";
                expectedDocIds.clear(lo, hi);
                docsAffectedThisRequest.set(lo, hi);
              } else {
                // edge case: special case DBQ of one doc
                assert (lo == rangeAxis && hi == rangeAxis) : "lo="+lo+" axis="+rangeAxis+" hi="+hi;
                q = "id_i:[" + lo + " TO " + lo + "]"; // have to be inclusive of both ends
                expectedDocIds.clear(lo);
                docsAffectedThisRequest.set(lo);
              }
            }
            req.deleteByQuery(q);
            log.info("DEL: {}", q);
          }
        }
      }
      assertEquals("expected error count sanity check: " + req.toString(),
                   expectedErrorsCount, expectedErrors.size());
        
      final SolrClient client = random().nextBoolean() ? CLOUD_CLIENT
        : NODE_CLIENTS.get(TestUtil.nextInt(random(), 0, NODE_CLIENTS.size()-1));
      
      final UpdateResponse rsp = req.process(client);
      assertUpdateTolerantErrors(client.toString() + " => " + expectedErrors.toString(), rsp,
                                 expectedErrors.toArray(new ExpectedErr[expectedErrors.size()]));

      if (log.isInfoEnabled()) {
        log.info("END ITER #{}, expecting #docs: {}", i, expectedDocIds.cardinality());
      }

      assertEquals("post update commit failed?", 0, CLOUD_CLIENT.commit().getStatus());
      
      for (int j = 0; j < 5; j++) {
        if (expectedDocIds.cardinality() == countDocs(CLOUD_CLIENT)) {
          break;
        }
        log.info("sleeping to give searchers a chance to re-open #{}", j);
        Thread.sleep(200);
      }

      // check the index contents against our expectations
      final BitSet actualDocIds = allDocs(CLOUD_CLIENT, maxDocId);
      if ( expectedDocIds.cardinality() != actualDocIds.cardinality() ) {
        log.error("cardinality mismatch: expected {} BUT actual {}",
                  expectedDocIds.cardinality(),
                  actualDocIds.cardinality());
      }
      final BitSet x = (BitSet) actualDocIds.clone();
      x.xor(expectedDocIds);
      for (int b = x.nextSetBit(0); 0 <= b; b = x.nextSetBit(b+1)) {
        final boolean expectedBit = expectedDocIds.get(b);
        final boolean actualBit = actualDocIds.get(b);
        log.error("bit #{} mismatch: expected {} BUT actual {}", b, expectedBit, actualBit);
      }
      assertEquals(x.cardinality() + " mismatched bits",
                   expectedDocIds.cardinality(), actualDocIds.cardinality());
    }
  }

  /** sanity check that randomUnsetBit works as expected 
   * @see #randomUnsetBit
   */
  public void testSanityRandomUnsetBit() {
    final int max = atLeast(100);
    BitSet bits = new BitSet(max+1);
    for (int i = 0; i <= max; i++) {
      assertFalse("how is bitset already full? iter="+i+" card="+bits.cardinality()+"/max="+max,
                  bits.cardinality() == max+1);
      final int nextBit = randomUnsetBit(random(), bits, max);
      assertTrue("nextBit shouldn't be negative yet: " + nextBit,
                 0 <= nextBit);
      assertTrue("nextBit can't exceed max: " + nextBit,
                 nextBit <= max);
      assertFalse("expect unset: " + nextBit, bits.get(nextBit));
      bits.set(nextBit);
    }
    
    assertEquals("why isn't bitset full?", max+1, bits.cardinality());

    final int firstClearBit = bits.nextClearBit(0);
    assertTrue("why is there a clear bit? = " + firstClearBit,
               max < firstClearBit);
    assertEquals("why is a bit set above max?",
                 -1, bits.nextSetBit(max+1));
    
    assertEquals("wrong nextBit at end of all iters", -1,
                 randomUnsetBit(random(), bits, max));
    assertEquals("wrong nextBit at redundant end of all iters", -1,
                 randomUnsetBit(random(), bits, max));
  }
  
  public static SolrInputDocument doc(SolrInputField... fields) {
    // SolrTestCaseJ4 has same method name, prevents static import from working
    return TestTolerantUpdateProcessorCloud.doc(fields);
  }

  /**
   * Given a BitSet, returns a random bit that is currently false, or -1 if all bits are true.
   * NOTE: this method is not fair.
   */
  public static final int randomUnsetBit(Random r, BitSet bits, final int max) {
    // NOTE: don't forget, BitSet will grow automatically if not careful
    if (bits.cardinality() == max+1) {
      return -1;
    }
    final int candidate = TestUtil.nextInt(r, 0, max);
    if (bits.get(candidate)) {
      final int lo = bits.previousClearBit(candidate);
      final int hi = bits.nextClearBit(candidate);
      if (lo < 0 && max < hi) {
        fail("how the hell did we not short circut out? card="+bits.cardinality()+"/size="+bits.size());
      } else if (lo < 0) {
        return hi;
      } else if (max < hi) {
        return lo;
      } // else...
      return ((candidate - lo) < (hi - candidate)) ? lo : hi;
    }
    return candidate;
  }

  /** returns the numFound from a *:* query */
  public static final long countDocs(SolrClient c) throws Exception {
    return c.query(params("q","*:*","rows","0")).getResults().getNumFound();
  }

  /** uses a Cursor to iterate over every doc in the index, recording the 'id_i' value in a BitSet */
  private static final BitSet allDocs(final SolrClient c, final int maxDocIdExpected) throws Exception {
    BitSet docs = new BitSet(maxDocIdExpected+1);
    String cursorMark = CURSOR_MARK_START;
    int docsOnThisPage = Integer.MAX_VALUE;
    while (0 < docsOnThisPage) {
      final SolrParams p = params("q","*:*",
                                  "rows","100",
                                  // note: not numeric, but we don't actual care about the order
                                  "sort", "id asc",
                                  CURSOR_MARK_PARAM, cursorMark);
      QueryResponse rsp = c.query(p);
      cursorMark = rsp.getNextCursorMark();
      docsOnThisPage = 0;
      for (SolrDocument doc : rsp.getResults()) {
        docsOnThisPage++;
        int id_i = ((Integer)doc.get("id_i")).intValue();
        assertTrue("found id_i bigger then expected "+maxDocIdExpected+": " + id_i,
                   id_i <= maxDocIdExpected);
        docs.set(id_i);
      }
      cursorMark = rsp.getNextCursorMark();
    }
    return docs;
  }
}
