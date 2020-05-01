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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class ChaosMonkeyNothingIsSafeWithPullReplicasTest extends AbstractFullDistribZkTestBase {
  private static final int FAIL_TOLERANCE = 100;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  private final boolean useTlogReplicas = random().nextBoolean();
  
  private final int numPullReplicas;
  private final int numRealtimeOrTlogReplicas;
  
  protected int getPullReplicaCount() {
    return numPullReplicas;
  }

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
    if (usually()) {
      System.setProperty("solr.autoCommit.maxTime", "15000");
    }
    System.clearProperty("solr.httpclient.retries");
    System.clearProperty("solr.retries.on.forward");
    System.clearProperty("solr.retries.to.followers"); 
    setErrorHook();
  }
  
  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    clearErrorHook();
    TestInjection.reset();
  }
  
  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};

  private int clientSoTimeout;

  private volatile FullThrottleStoppableIndexingThread ftIndexThread;

  private final boolean runFullThrottle;
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    // can help to hide this when testing and looking at logs
    //ignoreException("shard update error");
    useFactory("solr.StandardDirectoryFactory");
  }
  
  @Override
  public void distribTearDown() throws Exception {
    try {
      ftIndexThread.safeStop();
    } catch (NullPointerException e) {
      // okay
    }
    super.distribTearDown();
  }
  
  public ChaosMonkeyNothingIsSafeWithPullReplicasTest() {
    super();
    numPullReplicas = random().nextInt(TEST_NIGHTLY ? 2 : 1) + 1;
    numRealtimeOrTlogReplicas = random().nextInt(TEST_NIGHTLY ? 4 : 3) + 1;
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    }

    int numNodes = sliceCount * (numRealtimeOrTlogReplicas + numPullReplicas);
    fixShardCount(numNodes);
    log.info("Starting ChaosMonkey test with {} shards and {} nodes", sliceCount, numNodes);

    runFullThrottle = random().nextBoolean();
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }
  
  @Override
  protected CloudSolrClient createCloudClient(String defaultCollection) {
    return this.createCloudClient(defaultCollection, this.clientSoTimeout);
  }
  
  protected CloudSolrClient createCloudClient(String defaultCollection, int socketTimeout) {
    CloudSolrClient client = getCloudSolrClient(zkServer.getZkAddress(), random().nextBoolean(), 30000, socketTimeout);
    if (defaultCollection != null) client.setDefaultCollection(defaultCollection);
    return client;
  }


  @Test
  public void test() throws Exception {
    // None of the operations used here are particularly costly, so this should work.
    // Using this low timeout will also help us catch index stalling.
    clientSoTimeout = 8000;

    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    assertEquals(this.sliceCount, docCollection.getSlices().size());
    Slice s = docCollection.getSlice("shard1");
    assertNotNull(s);
    assertEquals("Unexpected number of replicas. Collection: " + docCollection, numRealtimeOrTlogReplicas + numPullReplicas, s.getReplicas().size());
    assertEquals("Unexpected number of pull replicas. Collection: " + docCollection, numPullReplicas, s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    assertEquals(useTlogReplicas()?0:numRealtimeOrTlogReplicas, s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(useTlogReplicas()?numRealtimeOrTlogReplicas:0, s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    
    boolean testSuccessful = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      // make sure we have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
      }      // make sure we again have leaders for each shard
      
      waitForRecoveriesToFinish(false);

      del("*:*");
      
      List<StoppableThread> threads = new ArrayList<>();
      List<StoppableIndexingThread> indexTreads = new ArrayList<>();
      int threadCount = TEST_NIGHTLY ? 3 : 1;
      int i = 0;
      for (i = 0; i < threadCount; i++) {
        StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, 35, 1, true);
        threads.add(indexThread);
        indexTreads.add(indexThread);
        indexThread.start();
      }
      
      threadCount = 1;
      i = 0;
      for (i = 0; i < threadCount; i++) {
        StoppableSearchThread searchThread = new StoppableSearchThread(cloudClient);
        threads.add(searchThread);
        searchThread.start();
      }
      
      if (usually()) {
        StoppableCommitThread commitThread = new StoppableCommitThread(cloudClient, 1000, false);
        threads.add(commitThread);
        commitThread.start();
      }
      
      if (runFullThrottle) {
        ftIndexThread = 
            new FullThrottleStoppableIndexingThread(cloudClient.getHttpClient(), controlClient, cloudClient, clients, "ft1", true, this.clientSoTimeout);
        ftIndexThread.start();
      }
      
      chaosMonkey.startTheMonkey(true, 10000);
      try {
        long runLength;
        if (RUN_LENGTH != -1) {
          runLength = RUN_LENGTH;
        } else {
          int[] runTimes;
          if (TEST_NIGHTLY) {
            runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
                30000, 45000, 90000, 120000};
          } else {
            runTimes = new int[] {5000, 7000, 10000};
          }
          runLength = runTimes[random().nextInt(runTimes.length - 1)];
        }
        ChaosMonkey.wait(runLength, DEFAULT_COLLECTION, zkStateReader);
      } finally {
        chaosMonkey.stopTheMonkey();
      }

      // ideally this should go into chaosMonkey
      restartZk(1000 * (5 + random().nextInt(4)));

      if (runFullThrottle) {
        ftIndexThread.safeStop();
      }
      
      for (StoppableThread indexThread : threads) {
        indexThread.safeStop();
      }
      
      // start any downed jetties to be sure we still will end up with a leader per shard...
      
      // wait for stop...
      for (StoppableThread indexThread : threads) {
        indexThread.join();
      }
      
      // try and wait for any replications and what not to finish...
      
      ChaosMonkey.wait(2000, DEFAULT_COLLECTION, zkStateReader);
      
      // wait until there are no recoveries...
      waitForThingsToLevelOut(Integer.MAX_VALUE);//Math.round((runLength / 1000.0f / 3.0f)));
      
      // make sure we again have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 30000);
      }
      
      commit();
      
      // TODO: assert we didnt kill everyone
      
      zkStateReader.updateLiveNodes();
      assertTrue(zkStateReader.getClusterState().getLiveNodes().size() > 0);
      
      
      // we expect full throttle fails, but cloud client should not easily fail
      for (StoppableThread indexThread : threads) {
        if (indexThread instanceof StoppableIndexingThread && !(indexThread instanceof FullThrottleStoppableIndexingThread)) {
          int failCount = ((StoppableIndexingThread) indexThread).getFailCount();
          assertFalse("There were too many update fails (" + failCount + " > " + FAIL_TOLERANCE
              + ") - we expect it can happen, but shouldn't easily", failCount > FAIL_TOLERANCE);
        }
      }
      
      waitForReplicationFromReplicas(DEFAULT_COLLECTION, zkStateReader, new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME));
//      waitForAllWarmingSearchers();
      
      Set<String> addFails = getAddFails(indexTreads);
      Set<String> deleteFails = getDeleteFails(indexTreads);
      // full throttle thread can
      // have request fails
      checkShardConsistency(!runFullThrottle, true, addFails, deleteFails);      
      
      long ctrlDocs = controlClient.query(new SolrQuery("*:*")).getResults()
      .getNumFound(); 
      
      // ensure we have added more than 0 docs
      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*"))
          .getResults().getNumFound();
      
      assertTrue("Found " + ctrlDocs + " control docs", cloudClientDocs > 0);

      if (log.isInfoEnabled()) {
        log.info("collection state: {}", printClusterStateInfo(DEFAULT_COLLECTION));
      }
      
      if (VERBOSE) System.out.println("control docs:"
          + controlClient.query(new SolrQuery("*:*")).getResults()
              .getNumFound() + "\n\n");
      
      // try and make a collection to make sure the overseer has survived the expiration and session loss

      // sometimes we restart zookeeper as well
      if (random().nextBoolean()) {
        restartZk(1000 * (5 + random().nextInt(4)));
      }

      try (CloudSolrClient client = createCloudClient("collection1", 30000)) {
        // We don't really know how many live nodes we have at this point, so "maxShardsPerNode" needs to be > 1
        createCollection(null, "testcollection",
              1, 1, 10, client, null, "conf1"); 
      }
      List<Integer> numShardsNumReplicas = new ArrayList<>(2);
      numShardsNumReplicas.add(1);
      numShardsNumReplicas.add(1 + getPullReplicaCount());
      checkForCollection("testcollection", numShardsNumReplicas, null);
      
      testSuccessful = true;
    } finally {
      if (!testSuccessful) {
        logReplicaTypesReplicationInfo(DEFAULT_COLLECTION, cloudClient.getZkStateReader());
        printLayout();
      }
    }
  }

  private Set<String> getAddFails(List<StoppableIndexingThread> threads) {
    Set<String> addFails = new HashSet<String>();
    for (StoppableIndexingThread thread : threads)   {
      addFails.addAll(thread.getAddFails());
//      addFails.addAll(thread.getAddFailsMinRf());
    }
    return addFails;
  }
  
  private Set<String> getDeleteFails(List<StoppableIndexingThread> threads) {
    Set<String> deleteFails = new HashSet<String>();
    for (StoppableIndexingThread thread : threads)   {
      deleteFails.addAll(thread.getDeleteFails());
//      deleteFails.addAll(thread.getDeleteFailsMinRf());
    }
    return deleteFails;
  }

  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }

}
