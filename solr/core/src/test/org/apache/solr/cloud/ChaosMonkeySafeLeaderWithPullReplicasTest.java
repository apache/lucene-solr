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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class ChaosMonkeySafeLeaderWithPullReplicasTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));
  
  private final boolean useTlogReplicas = random().nextBoolean();
  
  private final int numPullReplicas;
  private final int numRealtimeOrTlogReplicas;
  
  @Override
  protected int getPullReplicaCount() {
    return numPullReplicas;
  }
  
  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
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
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    super.distribSetUp();
  }
  
  public ChaosMonkeySafeLeaderWithPullReplicasTest() {
    super();
    numPullReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    numRealtimeOrTlogReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    }

    int numNodes = sliceCount * (numRealtimeOrTlogReplicas + numPullReplicas);
    fixShardCount(numNodes);
    log.info("Starting ChaosMonkey test with {} shards and {} nodes", sliceCount, numNodes);
  }
  
  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void test() throws Exception {
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    assertEquals(this.sliceCount, docCollection.getSlices().size());
    Slice s = docCollection.getSlice("shard1");
    assertNotNull(s);
    assertEquals("Unexpected number of replicas. Collection: " + docCollection, numRealtimeOrTlogReplicas + numPullReplicas, s.getReplicas().size());
    assertEquals("Unexpected number of pull replicas. Collection: " + docCollection, numPullReplicas, s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    assertEquals(useTlogReplicas()?0:numRealtimeOrTlogReplicas, s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(useTlogReplicas()?numRealtimeOrTlogReplicas:0, s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 1 seconds 'soft' commit
    randomlyEnableAutoSoftCommit();

    tryDelete();
    
    List<StoppableThread> threads = new ArrayList<>();
    int threadCount = 2;
    int batchSize = 1;
    if (random().nextBoolean()) {
      batchSize = random().nextInt(98) + 2;
    }
    
    boolean pauseBetweenUpdates = TEST_NIGHTLY ? random().nextBoolean() : true;
    int maxUpdates = -1;
    if (!pauseBetweenUpdates) {
      maxUpdates = 1000 + random().nextInt(1000);
    } else {
      maxUpdates = 15000;
    }
    
    for (int i = 0; i < threadCount; i++) {
      StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, maxUpdates, batchSize, pauseBetweenUpdates); // random().nextInt(999) + 1
      threads.add(indexThread);
      indexThread.start();
    }
    
    StoppableCommitThread commitThread = new StoppableCommitThread(cloudClient, 1000, false);
    threads.add(commitThread);
    commitThread.start();
    
    chaosMonkey.startTheMonkey(false, 500);
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
          runTimes = new int[] {5000, 7000, 15000};
        }
        runLength = runTimes[random().nextInt(runTimes.length - 1)];
      }
      
      ChaosMonkey.wait(runLength, DEFAULT_COLLECTION, cloudClient.getZkStateReader());
    } finally {
      chaosMonkey.stopTheMonkey();
    }
    
    for (StoppableThread thread : threads) {
      thread.safeStop();
    }
    
    // wait for stop...
    for (StoppableThread thread : threads) {
      thread.join();
    }
    
    for (StoppableThread thread : threads) {
      if (thread instanceof StoppableIndexingThread) {
        assertEquals(0, ((StoppableIndexingThread)thread).getFailCount());
      }
    }
    
    // try and wait for any replications and what not to finish...

    Thread.sleep(2000);

    waitForThingsToLevelOut(180000);
    
    // even if things were leveled out, a jetty may have just been stopped or something
    // we wait again and wait to level out again to make sure the system is not still in flux
    
    Thread.sleep(3000);

    waitForThingsToLevelOut(180000);

    if (log.isInfoEnabled()) {
      log.info("control docs:{}\n\n", controlClient.query(new SolrQuery("*:*")).getResults().getNumFound());
      log.info("collection state: {}", printClusterStateInfo(DEFAULT_COLLECTION)); // nowarn
    }
    
    waitForReplicationFromReplicas(DEFAULT_COLLECTION, cloudClient.getZkStateReader(), new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME));
//    waitForAllWarmingSearchers();

    checkShardConsistency(batchSize == 1, true);
    
    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
    if (random().nextBoolean()) {
      zkServer.shutdown();
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run(false);
    }

    try (CloudSolrClient client = createCloudClient("collection1")) {
        createCollection(null, "testcollection", 1, 1, 100, client, null, "conf1");

    }
    List<Integer> numShardsNumReplicas = new ArrayList<>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1 + getPullReplicaCount());
    checkForCollection("testcollection",numShardsNumReplicas, null);
  }

  private void tryDelete() throws Exception {
    long start = System.nanoTime();
    long timeout = start + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      try {
        del("*:*");
        break;
      } catch (SolrServerException | SolrException e) {
        // cluster may not be up yet
        e.printStackTrace();
      }
      Thread.sleep(100);
    }
  }
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
