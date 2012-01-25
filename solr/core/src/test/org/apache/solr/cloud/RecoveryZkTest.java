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

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryZkTest extends FullSolrCloudTest {

  //private static final String DISTRIB_UPDATE_CHAIN = "distrib-update-chain";
  private static Logger log = LoggerFactory.getLogger(RecoveryZkTest.class);
  private StopableIndexingThread indexThread;
  private StopableIndexingThread indexThread2;
  @BeforeClass
  public static void beforeSuperClass() throws Exception {

  }
  
  @AfterClass
  public static void afterSuperClass() throws Exception {

  }
  
  public RecoveryZkTest() {
    super();
    sliceCount = 1;
    shardCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // start a couple indexing threads
    
    indexThread = new StopableIndexingThread(0, true);
    indexThread.start();
    
    indexThread2 = new StopableIndexingThread(10000, true);
    
    indexThread2.start();

    // give some time to index...
    Thread.sleep(atLeast(2000));   
    
    // bring shard replica down
    JettySolrRunner replica = chaosMonkey.stopShard("shard1", 1);

    
    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(atLeast(2000));
    
    // bring shard replica up
    replica.start();
    
    // make sure replication can start
    Thread.sleep(1500);
    
    waitForRecoveriesToFinish(false);
    
    // stop indexing threads
    indexThread.safeStop();
    indexThread2.safeStop();
    
    indexThread.join();
    indexThread2.join();
    
    Thread.sleep(5000);
    
    commit();

    // test that leader and replica have same doc count
    
    checkShardConsistency("shard1", false); 
    
    long client1Docs = shardToClient.get("shard1").get(0).query(new SolrQuery("*:*")).getResults().getNumFound();
    long client2Docs = shardToClient.get("shard1").get(1).query(new SolrQuery("*:*")).getResults().getNumFound();
    
    assertTrue(client1Docs > 0);
    assertEquals(client1Docs, client2Docs);
 
    // won't always pass yet...
    //query("q", "*:*", "sort", "id desc");
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException,
      SolrServerException {
    controlClient.add(doc);
    
    // UpdateRequest ureq = new UpdateRequest();
    // ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    // ureq.process(cloudClient);
    cloudClient.add(doc);
  }

  
  @Override
  public void tearDown() throws Exception {
    // make sure threads have been stopped...
    indexThread.safeStop();
    indexThread2.safeStop();
    
    indexThread.join();
    indexThread2.join();
    
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
