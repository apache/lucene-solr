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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@Nightly
@SuppressSSL
public class TlogReplayBufferedWhileIndexingTest extends AbstractFullDistribZkTestBase {

  private List<StoppableIndexingThread> threads;
  
  public TlogReplayBufferedWhileIndexingTest() throws Exception {
    super();
    sliceCount = 1;
    fixShardCount(2);
    schemaString = "schema15.xml";      // we need a string id
  }
  
  @BeforeClass
  public static void beforeRestartWhileUpdatingTest() throws Exception {
    System.setProperty("leaderVoteWait", "300000");
    System.setProperty("solr.autoCommit.maxTime", "10000");
    System.setProperty("solr.autoSoftCommit.maxTime", "3000");
    TestInjection.updateLogReplayRandomPause = "true:10";
    TestInjection.updateRandomPause = "true:10";
    if (System.getProperty("solr.hdfs.home") != null) useFactory("solr.StandardDirectoryFactory");
  }
  
  @AfterClass
  public static void afterRestartWhileUpdatingTest() {
    System.clearProperty("leaderVoteWait");
    System.clearProperty("solr.autoCommit.maxTime");
    System.clearProperty("solr.autoSoftCommit.maxTime");
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    waitForRecoveriesToFinish(false);
    
    int numThreads = 3;
    
    threads = new ArrayList<>(numThreads);
    
    ArrayList<JettySolrRunner> allJetty = new ArrayList<>();
    allJetty.addAll(jettys);
    allJetty.remove(shardToLeaderJetty.get("shard1").jetty);
    assert allJetty.size() == 1 : allJetty.size();
    allJetty.get(0).stop();
    
    StoppableIndexingThread indexThread;
    for (int i = 0; i < numThreads; i++) {
      boolean pauseBetweenUpdates = random().nextBoolean();
      int batchSize = random().nextInt(4) + 1;
      indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, 900, batchSize, pauseBetweenUpdates);
      threads.add(indexThread);
      indexThread.start();
    }

    Thread.sleep(2000);
    
    allJetty.get(0).start();
    
    Thread.sleep(45000);
  
    waitForThingsToLevelOut(600); // we can insert random update delays, so this can take a while, especially when beasting this test
    
    Thread.sleep(2000);
    
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, cloudClient.getZkStateReader(), false, true);
    
    for (StoppableIndexingThread thread : threads) {
      thread.safeStop();
    }
    
    waitForThingsToLevelOut(30);

    checkShardConsistency(false, false);

  }

  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException,
      SolrServerException {
    cloudClient.add(doc);
  }

  
  @Override
  public void distribTearDown() throws Exception {
    // make sure threads have been stopped...
    if (threads != null) {
      for (StoppableIndexingThread thread : threads) {
        thread.safeStop();
      }
      
      for (StoppableIndexingThread thread : threads) {
        thread.join();
      }
    }

    super.distribTearDown();
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
