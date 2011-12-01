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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.RecoveryStrat.RecoveryListener;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 *
 */
@Ignore("this test still fails sometimes - it seems usually due to replay failing")
public class RecoveryZkTest extends FullDistributedZkTest {

  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    System.setProperty("mockdir.checkindex", "false");
  }
  
  @AfterClass
  public static void afterSuperClass() throws Exception {
    System.clearProperty("mockdir.checkindex");
  }
  
  public RecoveryZkTest() {
    super();
    sliceCount = 1;
    shardCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    // nocommit: remove the need for this
    Thread.sleep(5000);
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    del("*:*");
    
    // start a couple indexing threads
    
    class StopableThread extends Thread {
      private volatile boolean stop = false;
      private int startI;
      
      
      public StopableThread(int startI) {
        this.startI = startI;
        setDaemon(true);
      }
      
      @Override
      public void run() {
        int i = startI;
        while (true && !stop) {
          try {
            indexr(id, i++, i1, 50, tlong, 50, t1,
                "to come to the aid of their country.");
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
      
      public void safeStop() {
        stop = true;
      }
      
    };
    
    StopableThread indexThread = new StopableThread(0);
    indexThread.start();
    
    StopableThread indexThread2 = new StopableThread(10000);
    
    indexThread2.start();

    // give some time to index...
    Thread.sleep(4000);   
    
    // bring shard replica down
    System.out.println("bring shard down");
    JettySolrRunner replica = chaosMonkey.killShard("shard1", 1);

    
    // wait a moment - lets allow some docs to be indexed so replication time is non 0
    Thread.sleep(4000);

    final CountDownLatch recoveryLatch = new CountDownLatch(1);

    
    // bring shard replica up
    replica.start();
    
    RecoveryStrat recoveryStrat = ((SolrDispatchFilter) replica.getDispatchFilter().getFilter()).getCores()
        .getZkController().getRecoveryStrat();
    
    recoveryStrat.setRecoveryListener(new RecoveryListener() {
      
      @Override
      public void startRecovery() {}
      
      @Override
      public void finishedReplication() {}
      
      @Override
      public void finishedRecovery() {
        recoveryLatch.countDown();
      }
    });
    
    
    // wait for recovery to finish
    // if it takes over n seconds, assume we didnt get our listener attached before
    // recover started - it should be done before n though
    recoveryLatch.await(30, TimeUnit.SECONDS);
    
    // stop indexing threads
    indexThread.safeStop();
    indexThread2.safeStop();
    
    indexThread.join();
    indexThread2.join();
    
    
    System.out.println("commit");
    commit();

    // test that leader and replica have same doc count
    
    long client1Docs = shardToClient.get("shard1").get(0).query(new SolrQuery("*:*")).getResults().getNumFound();
    long client2Docs = shardToClient.get("shard1").get(1).query(new SolrQuery("*:*")).getResults().getNumFound();
    
    assertTrue(client1Docs > 0);
    assertEquals(client1Docs, client2Docs);
 
    // TODO: right now the control and distrib are usually off by a few docs...
    //query("q", "*:*", "distrib", true, "sort", i1 + " desc");
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    controlClient.add(doc);

    // nocommit: look into why cloudClient.addDoc returns NPE
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ureq.setParam("update.chain", "distrib-update-chain");
    ureq.process(cloudClient);
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
}
