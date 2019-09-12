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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slow
public class ChaosMonkeySafeLeaderTest extends AbstractFullDistribZkTestBase {
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
    System.setProperty("solr.autoCommit.maxTime", "15000");
    System.clearProperty("solr.httpclient.retries");
    System.clearProperty("solr.retries.on.forward");
    System.clearProperty("solr.retries.to.followers"); 
    setErrorHook();
  }
  
  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    clearErrorHook();
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
  
  public ChaosMonkeySafeLeaderTest() {
    super();
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 5 : 3) + 1;
    }

    int numShards = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "-1"));
    if (numShards == -1) {
      // we make sure that there's at least one shard with more than one replica
      // so that the ChaosMonkey has something to kill
      numShards = sliceCount + random().nextInt(TEST_NIGHTLY ? 12 : 2) + 1;
    }
    fixShardCount(numShards);
  }

  @Test
  public void test() throws Exception {
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 1 seconds 'soft' commit
    randomlyEnableAutoSoftCommit();

    tryDelete();
    
    List<StoppableIndexingThread> threads = new ArrayList<>();
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
      
      Thread.sleep(runLength);
    } finally {
      chaosMonkey.stopTheMonkey();
    }
    
    for (StoppableIndexingThread indexThread : threads) {
      indexThread.safeStop();
    }
    
    // wait for stop...
    for (StoppableIndexingThread indexThread : threads) {
      indexThread.join();
    }
    
    for (StoppableIndexingThread indexThread : threads) {
      assertEquals(0, indexThread.getFailCount());
    }
    
    // try and wait for any replications and what not to finish...

    Thread.sleep(2000);

    waitForThingsToLevelOut(180000);
    
    // even if things were leveled out, a jetty may have just been stopped or something
    // we wait again and wait to level out again to make sure the system is not still in flux
    
    Thread.sleep(3000);

    waitForThingsToLevelOut(180000);

    checkShardConsistency(batchSize == 1, true);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    
    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
    if (random().nextBoolean()) {
      zkServer.shutdown();
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run(false);
    }

    try (CloudSolrClient client = createCloudClient("collection1")) {
        createCollection(null, "testcollection", 1, 1, 1, client, null, "conf1");

    }
    List<Integer> numShardsNumReplicas = new ArrayList<>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1);
    checkForCollection("testcollection",numShardsNumReplicas, null);
  }

  private void tryDelete() throws Exception {
    long start = System.nanoTime();
    long timeout = start + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      try {
        del("*:*");
        break;
      } catch (SolrServerException e) {
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
