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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

@Slow
@Nightly
public class RestartWhileUpdatingTest extends AbstractFullDistribZkTestBase {

  //private static final String DISTRIB_UPDATE_CHAIN = "distrib-update-chain";
  private List<StoppableIndexingThread> threads;

  public RestartWhileUpdatingTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);
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

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    // start a couple indexing threads
    
    int[] maxDocList = new int[] {5000, 10000};
 
    
    int maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];
    
    int numThreads = random().nextInt(4) + 1;
    
    threads = new ArrayList<>(2);
    
    StoppableIndexingThread indexThread;
    for (int i = 0; i < numThreads; i++) {
      indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, maxDoc, 1, true);
      threads.add(indexThread);
      indexThread.start();
    }

    Thread.sleep(2000);
    
    int restartTimes = random().nextInt(4) + 1;;
    for (int i = 0; i < restartTimes; i++) {
      stopAndStartAllReplicas();
    }
    
    Thread.sleep(2000);
    
    // stop indexing threads
    for (StoppableIndexingThread thread : threads) {
      thread.safeStop();
      thread.safeStop();
    }
    
    Thread.sleep(1000);
  
    waitForThingsToLevelOut(120);
    
    Thread.sleep(2000);
    
    waitForThingsToLevelOut(30);
    
    Thread.sleep(5000);
    
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, cloudClient.getZkStateReader(), false, true);

    
    checkShardConsistency(false, false);
  }

  public void stopAndStartAllReplicas() throws Exception, InterruptedException {
    chaosMonkey.stopAll(random().nextInt(2000));
    
    Thread.sleep(1000);
    
    chaosMonkey.startAll();
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
        thread.safeStop();
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
