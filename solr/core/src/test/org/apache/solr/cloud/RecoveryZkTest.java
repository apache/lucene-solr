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

import java.io.IOException;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class RecoveryZkTest extends AbstractFullDistribZkTestBase {

  //private static final String DISTRIB_UPDATE_CHAIN = "distrib-update-chain";
  private static Logger log = LoggerFactory.getLogger(RecoveryZkTest.class);
  private StopableIndexingThread indexThread;
  private StopableIndexingThread indexThread2;

  public RecoveryZkTest() {
    super();
    sliceCount = 1;
    shardCount = 2;
    schemaString = "schema15.xml";      // we need a string id
  }
  
  public static String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  public static RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};
  
  protected String[] getFieldNames() {
    return fieldNames;
  }

  protected RandVal[] getRandValues() {
    return randVals;
  }

  @Override
  public void doTest() throws Exception {
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // start a couple indexing threads
    
    int[] maxDocList = new int[] {300, 700, 1200, 1350, 5000, 15000};
    
    int maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];
    
    indexThread = new StopableIndexingThread("1", true, maxDoc);
    indexThread.start();
    
    indexThread2 = new StopableIndexingThread("2", true, maxDoc);
    
    indexThread2.start();

    // give some time to index...
    int[] waitTimes = new int[] {200, 2000, 3000};
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);
     
    // bring shard replica down
    JettySolrRunner replica = chaosMonkey.stopShard("shard1", 1).jetty;

    
    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);
    
    // bring shard replica up
    replica.start();
    
    // make sure replication can start
    Thread.sleep(3000);
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    
    // stop indexing threads
    indexThread.safeStop();
    indexThread2.safeStop();
    
    indexThread.join();
    indexThread2.join();
    
    Thread.sleep(1000);
  
    waitForThingsToLevelOut(45);
    
    Thread.sleep(2000);
    
    waitForThingsToLevelOut(30);
    
    Thread.sleep(5000);
    
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, false, true);

    // test that leader and replica have same doc count
    
    String fail = checkShardConsistency("shard1", false, false);
    if (fail != null) {
      fail(fail);
    }
    
    SolrQuery query = new SolrQuery("*:*");
    query.setParam("distrib", "false");
    long client1Docs = shardToJetty.get("shard1").get(0).client.solrClient.query(query).getResults().getNumFound();
    long client2Docs = shardToJetty.get("shard1").get(1).client.solrClient.query(query).getResults().getNumFound();
    
    assertTrue(client1Docs > 0);
    assertEquals(client1Docs, client2Docs);
 
    // won't always pass yet...
    //query("q", "*:*", "sort", "id desc");
  }
  
  @Override
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
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }
}
