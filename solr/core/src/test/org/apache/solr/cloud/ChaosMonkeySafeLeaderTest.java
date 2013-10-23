package org.apache.solr.cloud;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.update.SolrCmdDistributor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

@Slow
public class ChaosMonkeySafeLeaderTest extends AbstractFullDistribZkTestBase {
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  @BeforeClass
  public static void beforeSuperClass() {
    SolrCmdDistributor.testing_errorHook = new Diagnostics.Callable() {
      @Override
      public void call(Object... data) {
        Exception e = (Exception) data[0];
        if (e == null) return;
        if (e.getMessage().contains("Timeout")) {
          Diagnostics.logThreadDumps("REQUESTING THREAD DUMP DUE TO TIMEOUT: " + e.getMessage());
        }
      }
    };
  }
  
  @AfterClass
  public static void afterSuperClass() {
    SolrCmdDistributor.testing_errorHook = null;
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    useFactory("solr.StandardDirectoryFactory");

    super.setUp();
    
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("numShards");
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public ChaosMonkeySafeLeaderTest() {
    super();
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "3"));
    shardCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "12"));
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 1 seconds 'soft' commit
    randomlyEnableAutoSoftCommit();

    del("*:*");
    
    List<StopableIndexingThread> threads = new ArrayList<StopableIndexingThread>();
    int threadCount = 2;
    for (int i = 0; i < threadCount; i++) {
      StopableIndexingThread indexThread = new StopableIndexingThread(10000 + i*50000, true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    chaosMonkey.startTheMonkey(false, 500);
    long runLength;
    if (RUN_LENGTH != -1) {
      runLength = RUN_LENGTH;
    } else {
      int[] runTimes = new int[] {5000,6000,10000,15000,15000,30000,30000,45000,90000,120000};
      runLength = runTimes[random().nextInt(runTimes.length - 1)];
    }
    try {
      Thread.sleep(runLength);
    } finally {
      chaosMonkey.stopTheMonkey();
    }
    
    for (StopableIndexingThread indexThread : threads) {
      indexThread.safeStop();
    }
    
    // wait for stop...
    for (StopableIndexingThread indexThread : threads) {
      indexThread.join();
    }
    
    for (StopableIndexingThread indexThread : threads) {
      assertEquals(0, indexThread.getFails());
    }
    
    // try and wait for any replications and what not to finish...

    Thread.sleep(2000);

    waitForThingsToLevelOut(180000);

    checkShardConsistency(true, true);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    
    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
    if (random().nextBoolean()) {
      zkServer.shutdown();
      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
      zkServer.run();
    }
    
    CloudSolrServer client = createCloudClient("collection1");
    try {
        createCollection(null, "testcollection",
            1, 1, 1, client, null, "conf1");

    } finally {
      client.shutdown();
    }
    List<Integer> numShardsNumReplicas = new ArrayList<Integer>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1);
    checkForCollection("testcollection",numShardsNumReplicas, null);
  }

  private void randomlyEnableAutoSoftCommit() {
    if (r.nextBoolean()) {
      enableAutoSoftCommit(1000);
    } else {
      log.info("Not turning on auto soft commit");
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
