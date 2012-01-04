package org.apache.solr.cloud;

/**
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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ChaosMonkeySolrCloudTest extends FullSolrCloudTest {
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    // we expect this time of exception as shards go up and down...
    ignoreException("shard update error ");
    ignoreException("Connection refused");
  }
  
  @AfterClass
  public static void afterSuperClass() throws Exception {
    resetExceptionIgnores();
  }
  
  public ChaosMonkeySolrCloudTest() {
    super();
    shardCount = atLeast(9);
    sliceCount = atLeast(3);
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    List<StopableIndexingThread> threads = new ArrayList<StopableIndexingThread>();
    for (int i = 0; i < atLeast(2); i++) {
      StopableIndexingThread indexThread = new StopableIndexingThread(i * 50000, true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    chaosMonkey.startTheMonkey();
    
    Thread.sleep(atLeast(12000));
    
    chaosMonkey.stopTheMonkey();
    
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

    Thread.sleep(1000);
    
    // wait until there are no recoveries...
    waitForRecoveriesToFinish(VERBOSE);
    
    commit();
    
    //assertEquals(chaosMonkey.getStarts(), getNumberOfRecoveryAttempts() - shardCount - sliceCount);
    
    
    checkShardConsistency(false);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  // skip the randoms - they can deadlock...
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
