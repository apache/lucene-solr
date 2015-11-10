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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.SnapShooter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class CleanupOldIndexTest extends AbstractFullDistribZkTestBase {

  private static Logger log = LoggerFactory.getLogger(CleanupOldIndexTest.class);
  private StoppableIndexingThread indexThread;

  public CleanupOldIndexTest() {
    super();
    sliceCount = 1;
    fixShardCount(2);
    schemaString = "schema15.xml";
  }
  
  public static String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  public static RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};
  
  protected String[] getFieldNames() {
    return fieldNames;
  }

  protected RandVal[] getRandValues() {
    return randVals;
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    int[] maxDocList = new int[] {300, 700, 1200};
    int maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];

    indexThread = new StoppableIndexingThread(controlClient, cloudClient, "1", true, maxDoc, 1, true);
    indexThread.start();

    // give some time to index...
    int[] waitTimes = new int[] {200, 2000, 3000};
    Thread.sleep(waitTimes[random().nextInt(waitTimes.length - 1)]);

    // create some "old" index directories
    JettySolrRunner jetty = chaosMonkey.getShard("shard1", 1);
    CoreContainer coreContainer = jetty.getCoreContainer();
    File dataDir = null;
    try (SolrCore solrCore = coreContainer.getCore("collection1")) {
      dataDir = new File(solrCore.getDataDir());
    }
    assertTrue(dataDir.isDirectory());

    long msInDay = 60*60*24L;
    String timestamp1 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date(1*msInDay));
    String timestamp2 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date(2*msInDay));
    File oldIndexDir1 = new File(dataDir, "index."+timestamp1);
    FileUtils.forceMkdir(oldIndexDir1);
    File oldIndexDir2 = new File(dataDir, "index."+timestamp2);
    FileUtils.forceMkdir(oldIndexDir2);

    // verify the "old" index directories exist
    assertTrue(oldIndexDir1.isDirectory());
    assertTrue(oldIndexDir2.isDirectory());

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
    indexThread.join();

    Thread.sleep(1000);
  
    waitForThingsToLevelOut(120);
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, false, true);

    // test that leader and replica have same doc count
    
    String fail = checkShardConsistency("shard1", false, false);
    if (fail != null)
      fail(fail);

    SolrQuery query = new SolrQuery("*:*");
    query.setParam("distrib", "false");
    long client1Docs = shardToJetty.get("shard1").get(0).client.solrClient.query(query).getResults().getNumFound();
    long client2Docs = shardToJetty.get("shard1").get(1).client.solrClient.query(query).getResults().getNumFound();
    
    assertTrue(client1Docs > 0);
    assertEquals(client1Docs, client2Docs);

    assertTrue(!oldIndexDir1.isDirectory());
    assertTrue(!oldIndexDir2.isDirectory());
  }
  
  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    controlClient.add(doc);
    cloudClient.add(doc);
  }

  
  @Override
  public void distribTearDown() throws Exception {
    // make sure threads have been stopped...
    indexThread.safeStop();
    indexThread.join();
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
