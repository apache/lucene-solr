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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore("SOLR-3126")
public class ChaosMonkeySafeLeaderTest extends AbstractFullDistribZkTestBase {
  
  private static final int BASE_RUN_LENGTH = 120000;

  @BeforeClass
  public static void beforeSuperClass() {

  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");
    
    // sometimes we cannot get the same port
    ignoreException("java\\.net\\.BindException: Address already in use");
    
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
    sliceCount = 3;//atLeast(2);
    shardCount = 12;//atLeast(sliceCount*2);
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 5 seconds 'soft' commit
    randomlyEnableAutoSoftCommit();

    del("*:*");
    
    List<StopableIndexingThread> threads = new ArrayList<StopableIndexingThread>();
    int threadCount = 2;
    for (int i = 0; i < threadCount; i++) {
      StopableIndexingThread indexThread = new StopableIndexingThread(i * 50000, true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    chaosMonkey.startTheMonkey(false, 500);
    int runLength = atLeast(BASE_RUN_LENGTH);
    Thread.sleep(runLength);
    
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
    
    waitForThingsToLevelOut(Integer.MAX_VALUE);//Math.round((runLength / 1000.0f / 3.0f)));

    checkShardConsistency(true, true);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
  }

  private void randomlyEnableAutoSoftCommit() {
    if (r.nextBoolean()) {
      log.info("Turning on auto soft commit");
      for (CloudJettyRunner jetty : shardToJetty.get("shard1")) {
        SolrCore core = ((SolrDispatchFilter) jetty.jetty.getDispatchFilter()
            .getFilter()).getCores().getCore("collection1");
        try {
          ((DirectUpdateHandler2) core.getUpdateHandler()).getCommitTracker()
              .setTimeUpperBound(5000);
        } finally {
          core.close();
        }
      }
    } else {
      log.info("Not turning on auto soft commit");
    }
  }
  
  // skip the randoms - they can deadlock...
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
