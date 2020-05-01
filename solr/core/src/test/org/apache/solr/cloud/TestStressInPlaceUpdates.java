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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.primes.Primes;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class TestStressInPlaceUpdates extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema-inplace-updates.xml";
    configString = "solrconfig-tlog.xml";

    // sanity check that autocommits are disabled
    initCore(configString, schemaString);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxTime);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoCommmitMaxDocs);
    assertEquals(-1, h.getCore().getSolrConfig().getUpdateHandlerInfo().autoSoftCommmitMaxDocs);
  }

  public TestStressInPlaceUpdates() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }

  protected final ConcurrentHashMap<Integer, DocInfo> model = new ConcurrentHashMap<>();
  protected Map<Integer, DocInfo> committedModel = new HashMap<>();
  protected long snapshotCount;
  protected long committedModelClock;
  protected int clientIndexUsedForCommit;
  protected volatile int lastId;
  protected final String field = "val_l";

  private void initModel(int ndocs) {
    for (int i = 0; i < ndocs; i++) {
      // seed versions w/-1 so "from scratch" adds/updates will fail optimistic concurrency checks
      // if some other thread beats us to adding the id
      model.put(i, new DocInfo(-1L, 0, 0));
    }
    committedModel.putAll(model);
  }

  SolrClient leaderClient = null;

  @Test
  @ShardsFixed(num = 3)
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void stressTest() throws Exception {
    waitForRecoveriesToFinish(true);

    this.leaderClient = getClientForLeader();
    assertNotNull("Couldn't obtain client for the leader of the shard", this.leaderClient);

    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30 + random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4 + random().nextInt(25);
    final int deleteByQueryPercent = random().nextInt(8);
    final int ndocs = atLeast(5);
    int nWriteThreads = 5 + random().nextInt(12);
    int fullUpdatePercent = 5 + random().nextInt(50);

    // query variables
    final int percentRealtimeQuery = 75;
    // number of cumulative read/write operations by all threads
    final AtomicLong operations = new AtomicLong(5000);  
    int nReadThreads = 5 + random().nextInt(12);


    /** // testing
     final int commitPercent = 5;
     final int softCommitPercent = 100; // what percent of the commits are soft
     final int deletePercent = 0;
     final int deleteByQueryPercent = 50;
     final int ndocs = 10;
     int nWriteThreads = 10;

     final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

     // query variables
     final int percentRealtimeQuery = 101;
     final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
     int nReadThreads = 10;

     int fullUpdatePercent = 20;
     **/

    if (log.isInfoEnabled()) {
      log.info("{}", Arrays.asList
          ("commitPercent", commitPercent, "softCommitPercent", softCommitPercent,
              "deletePercent", deletePercent, "deleteByQueryPercent", deleteByQueryPercent,
              "ndocs", ndocs, "nWriteThreads", nWriteThreads, "percentRealtimeQuery", percentRealtimeQuery,
              "operations", operations, "nReadThreads", nReadThreads));
    }

    initModel(ndocs);

    List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < nWriteThreads; i++) {
      Thread thread = new Thread("WRITER" + i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() > 0) {
              int oper = rand.nextInt(50);

              if (oper < commitPercent) {
                Map<Integer, DocInfo> newCommittedModel;
                long version;

                synchronized (TestStressInPlaceUpdates.this) {
                  // take a snapshot of the model
                  // this is safe to do w/o synchronizing on the model because it's a ConcurrentHashMap
                  newCommittedModel = new HashMap<>(model);  
                  version = snapshotCount++;

                  int chosenClientIndex = rand.nextInt(clients.size());

                  if (rand.nextInt(100) < softCommitPercent) {
                    log.info("softCommit start");
                    clients.get(chosenClientIndex).commit(true, true, true);
                    log.info("softCommit end");
                  } else {
                    log.info("hardCommit start");
                    clients.get(chosenClientIndex).commit();
                    log.info("hardCommit end");
                  }

                  // install this model snapshot only if it's newer than the current one
                  if (version >= committedModelClock) {
                    if (VERBOSE) {
                      log.info("installing new committedModel version={}", committedModelClock);
                    }
                    clientIndexUsedForCommit = chosenClientIndex;
                    committedModel = newCommittedModel;
                    committedModelClock = version;
                  }
                }
                continue;
              }

              int id;

              if (rand.nextBoolean()) {
                id = rand.nextInt(ndocs);
              } else {
                id = lastId;  // reuse the last ID half of the time to force more race conditions
              }

              // set the lastId before we actually change it sometimes to try and
              // uncover more race conditions between writing and reading
              boolean before = rand.nextBoolean();
              if (before) {
                lastId = id;
              }

              DocInfo info = model.get(id);

              // yield after getting the next version to increase the odds of updates happening out of order
              if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                final boolean dbq = (oper >= commitPercent + deletePercent);
                final String delType = dbq ? "DBI": "DBQ";
                log.info("{} id {}: {}", delType, id, info);
                
                Long returnedVersion = null;

                try {
                  returnedVersion = deleteDocAndGetVersion(Integer.toString(id), params("_version_", Long.toString(info.version)), dbq);
                  log.info("{}: Deleting id={}, version={}. Returned version={}"
                      , delType, id, info.version, returnedVersion);
                } catch (RuntimeException e) {
                  if (e.getMessage() != null && e.getMessage().contains("version conflict")
                      || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                    // Its okay for a leader to reject a concurrent request
                    log.warn("Conflict during {}, rejected id={}, {}", delType, id, e);
                    returnedVersion = null;
                  } else {
                    throw e;
                  }
                }

                // only update model if update had no conflict & the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (null != returnedVersion &&
                      (Math.abs(returnedVersion.longValue()) > Math.abs(currInfo.version))) {
                    model.put(id, new DocInfo(returnedVersion.longValue(), 0, 0));
                  }
                }
                
              } else {
                int val1 = info.intFieldValue;
                long val2 = info.longFieldValue;
                int nextVal1 = val1;
                long nextVal2 = val2;

                int addOper = rand.nextInt(30);
                Long returnedVersion;
                if (addOper < fullUpdatePercent || info.version <= 0) { // if document was never indexed or was deleted
                  // FULL UPDATE
                  nextVal1 = Primes.nextPrime(val1 + 1);
                  nextVal2 = nextVal1 * 1000000000l;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "title_s", "title" + id, "val1_i_dvo", nextVal1, "val2_l_dvo", nextVal2, "_version_", info.version);
                    log.info("FULL: Writing id={}, val=[{},{}], version={}, Prev was=[{},{}].  Returned version={}"
                        ,id, nextVal1, nextVal2, info.version, val1, val2, returnedVersion);

                  } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("version conflict")
                        || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict during full update, rejected id={}, {}", id, e);
                      returnedVersion = null;
                    } else {
                      throw e;
                    }
                  }
                } else {
                  // PARTIAL
                  nextVal2 = val2 + val1;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "val2_l_dvo", map("inc", String.valueOf(val1)), "_version_", info.version);
                    log.info("PARTIAL: Writing id={}, val=[{},{}], version={}, Prev was=[{},{}].  Returned version={}"
                        ,id, nextVal1, nextVal2, info.version, val1, val2,  returnedVersion);
                  } catch (RuntimeException e) {
                    if (e.getMessage() != null && e.getMessage().contains("version conflict")
                        || e.getMessage() != null && e.getMessage().contains("Conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict during partial update, rejected id={}, {}", id, e);
                    } else if (e.getMessage() != null && e.getMessage().contains("Document not found for update.") 
                               && e.getMessage().contains("id="+id)) {
                      log.warn("Attempted a partial update for a recently deleted document, rejected id={}, {}", id, e);
                    } else {
                      throw e;
                    }
                    returnedVersion = null;
                  }
                }

                // only update model if update had no conflict & the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (null != returnedVersion &&
                      (Math.abs(returnedVersion.longValue()) > Math.abs(currInfo.version))) {
                    model.put(id, new DocInfo(returnedVersion.longValue(), nextVal1, nextVal2));
                  }

                }
              }

              if (!before) {
                lastId = id;
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);

    }

    // Read threads
    for (int i = 0; i < nReadThreads; i++) {
      Thread thread = new Thread("READER" + i) {
        Random rand = new Random(random().nextInt());

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              DocInfo expected;

              if (realTime) {
                expected = model.get(id);
              } else {
                synchronized (TestStressInPlaceUpdates.this) {
                  expected = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                log.info("querying id {}", id);
              }
              ModifiableSolrParams params = new ModifiableSolrParams();
              if (realTime) {
                params.set("wt", "json");
                params.set("qt", "/get");
                params.set("ids", Integer.toString(id));
              } else {
                params.set("wt", "json");
                params.set("q", "id:" + Integer.toString(id));
                params.set("omitHeader", "true");
              }

              int clientId = rand.nextInt(clients.size());
              if (!realTime) clientId = clientIndexUsedForCommit;

              QueryResponse response = clients.get(clientId).query(params);
              if (response.getResults().size() == 0) {
                // there's no info we can get back with a delete, so not much we can check without further synchronization
              } else if (response.getResults().size() == 1) {
                final SolrDocument actual = response.getResults().get(0);
                final String msg = "Realtime=" + realTime + ", expected=" + expected + ", actual=" + actual;
                assertNotNull(msg, actual);

                final Long foundVersion = (Long) actual.getFieldValue("_version_");
                assertNotNull(msg, foundVersion);
                assertTrue(msg + "... solr doc has non-positive version???",
                           0 < foundVersion.longValue());
                final Integer intVal = (Integer) actual.getFieldValue("val1_i_dvo");
                assertNotNull(msg, intVal);
                
                final Long longVal = (Long) actual.getFieldValue("val2_l_dvo");
                assertNotNull(msg, longVal);

                assertTrue(msg + " ...solr returned older version then model. " +
                           "should not be possible given the order of operations in writer threads",
                           Math.abs(expected.version) <= foundVersion.longValue());

                if (foundVersion.longValue() == expected.version) {
                  assertEquals(msg, expected.intFieldValue, intVal.intValue());
                  assertEquals(msg, expected.longFieldValue, longVal.longValue());
                }

                // Some things we can assert about any Doc returned from solr,
                // even if it's newer then our (expected) model information...

                assertTrue(msg + " ...how did a doc in solr get a non positive intVal?",
                           0 < intVal);
                assertTrue(msg + " ...how did a doc in solr get a non positive longVal?",
                           0 < longVal);
                assertEquals(msg + " ...intVal and longVal in solr doc are internally (modulo) inconsistent w/eachother",
                             0, (longVal % intVal));

                // NOTE: when foundVersion is greater then the version read from the model,
                // it's not possible to make any assertions about the field values in solr relative to the
                // field values in the model -- ie: we can *NOT* assert expected.longFieldVal <= doc.longVal
                //
                // it's tempting to think that this would be possible if we changed our model to preserve the
                // "old" valuess when doing a delete, but that's still no garuntee because of how oportunistic
                // concurrency works with negative versions:  When adding a doc, we can assert that it must not
                // exist with version<0, but we can't assert that the *reason* it doesn't exist was because of
                // a delete with the specific version of "-42".
                // So a wrtier thread might (1) prep to add a doc for the first time with "intValue=1,_version_=-1",
                // and that add may succeed and (2) return some version X which is put in the model.  but
                // inbetween #1 and #2 other threads may have added & deleted the doc repeatedly, updating
                // the model with intValue=7,_version_=-42, and a reader thread might meanwhile read from the
                // model before #2 and expect intValue=5, but get intValue=1 from solr (with a greater version)
                
              } else {
                fail(String.format(Locale.ENGLISH, "There were more than one result: {}", response));
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("", e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }
    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    { // final pass over uncommitted model with RTG

      for (SolrClient client : clients) {
        for (Map.Entry<Integer,DocInfo> entry : model.entrySet()) {
          final Integer id = entry.getKey();
          final DocInfo expected = entry.getValue();
          final SolrDocument actual = client.getById(id.toString());

          String msg = "RTG: " + id + "=" + expected;
          if (null == actual) {
            // a deleted or non-existent document
            // sanity check of the model agrees...
            assertTrue(msg + " is deleted/non-existent in Solr, but model has non-neg version",
                       expected.version < 0);
            assertEquals(msg + " is deleted/non-existent in Solr", expected.intFieldValue, 0);
            assertEquals(msg + " is deleted/non-existent in Solr", expected.longFieldValue, 0);
          } else {
            msg = msg + " <==VS==> " + actual;
            assertEquals(msg, expected.intFieldValue, actual.getFieldValue("val1_i_dvo"));
            assertEquals(msg, expected.longFieldValue, actual.getFieldValue("val2_l_dvo"));
            assertEquals(msg, expected.version, actual.getFieldValue("_version_"));
            assertTrue(msg + " doc exists in solr, but version is negative???",
                       0 < expected.version);
          }
        }
      }
    }
    
    { // do a final search and compare every result with the model

      // because commits don't provide any sort of concrete versioning (or optimistic concurrency constraints)
      // there's no way to garuntee that our committedModel matches what was in Solr at the time of the last commit.
      // It's possible other threads made additional writes to solr before the commit was processed, but after
      // the committedModel variable was assigned it's new value.
      //
      // what we can do however, is commit all completed updates, and *then* compare solr search results
      // against the (new) committed model....
      
      waitForThingsToLevelOut(30); // NOTE: this does an automatic commit for us & ensures replicas are up to date
      committedModel = new HashMap<>(model);

      // first, prune the model of any docs that have negative versions
      // ie: were never actually added, or were ultimately deleted.
      for (int i = 0; i < ndocs; i++) {
        DocInfo info = committedModel.get(i);
        if (info.version < 0) {
          // first, a quick sanity check of the model itself...
          assertEquals("Inconsistent int value in model for deleted doc" + i + "=" + info,
                       0, info.intFieldValue);
          assertEquals("Inconsistent long value in model for deleted doc" + i + "=" + info,
                       0L, info.longFieldValue);

          committedModel.remove(i);
        }
      }

      for (SolrClient client : clients) {
        QueryResponse rsp = client.query(params("q","*:*", "sort", "id asc", "rows", ndocs+""));
        for (SolrDocument actual : rsp.getResults()) {
          final Integer id = Integer.parseInt(actual.getFieldValue("id").toString());
          final DocInfo expected = committedModel.get(id); 
          
          assertNotNull("Doc found but missing/deleted from model: " + actual, expected);
          
          final String msg = "Search: " + id + "=" + expected + " <==VS==> " + actual;
          assertEquals(msg, expected.intFieldValue, actual.getFieldValue("val1_i_dvo"));
          assertEquals(msg, expected.longFieldValue, actual.getFieldValue("val2_l_dvo"));
          assertEquals(msg, expected.version, actual.getFieldValue("_version_"));
          assertTrue(msg + " doc exists in solr, but version is negative???",
                     0 < expected.version);

          // also sanity check the model (which we already know matches the doc)
          assertEquals("Inconsistent (modulo) values in model for id " + id + "=" + expected,
                       0, (expected.longFieldValue % expected.intFieldValue));
        }
        assertEquals(committedModel.size(), rsp.getResults().getNumFound());
      }
    }
  }

  /**
   * Used for storing the info for a document in an in-memory model.
   */
  private static class DocInfo {
    long version;
    int intFieldValue;
    long longFieldValue;

    public DocInfo(long version, int val1, long val2) {
      assert version != 0; // must either be real positive version, or negative deleted version/indicator
      this.version = version;
      this.intFieldValue = val1;
      this.longFieldValue = val2;
    }

    @Override
    public String toString() {
      return "[version=" + version + ", intValue=" + intFieldValue + ",longValue=" + longFieldValue + "]";
    }
  }

  @SuppressWarnings("rawtypes")
  protected long addDocAndGetVersion(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("versions", "true");

    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(params);
    ureq.add(doc);
    UpdateResponse resp;

    // send updates to leader, to avoid SOLR-8733
    resp = ureq.process(leaderClient);

    long returnedVersion = Long.parseLong(((NamedList) resp.getResponse().get("adds")).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion > 0);
    return returnedVersion;
  }

  @SuppressWarnings("rawtypes")
  protected long deleteDocAndGetVersion(String id, ModifiableSolrParams params, boolean deleteByQuery) throws Exception {
    params.add("versions", "true");
   
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(params);
    if (deleteByQuery) {
      ureq.deleteByQuery("id:"+id);
    } else {
      ureq.deleteById(id);
    }
    UpdateResponse resp;
    // send updates to leader, to avoid SOLR-8733
    resp = ureq.process(leaderClient);
    
    String key = deleteByQuery? "deleteByQuery": "deletes";
    long returnedVersion = Long.parseLong(((NamedList) resp.getResponse().get(key)).getVal(0).toString());
    assertTrue("Due to SOLR-8733, sometimes returned version is 0. Let us assert that we have successfully"
        + " worked around that problem here.", returnedVersion < 0);
    return returnedVersion;
  }

  /**
   * Method gets the SolrClient for the leader replica. This is needed for a workaround for SOLR-8733.
   */
  public SolrClient getClientForLeader() throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    cloudClient.getZkStateReader().forceUpdateCollection(DEFAULT_COLLECTION);
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = null;
    Slice shard1 = clusterState.getCollection(DEFAULT_COLLECTION).getSlice(SHARD1);
    leader = shard1.getLeader();

    for (int i = 0; i < clients.size(); i++) {
      String leaderBaseUrl = zkStateReader.getBaseUrlForNodeName(leader.getNodeName());
      if (((HttpSolrClient) clients.get(i)).getBaseURL().startsWith(leaderBaseUrl))
        return clients.get(i);
    }

    return null;
  }
}
