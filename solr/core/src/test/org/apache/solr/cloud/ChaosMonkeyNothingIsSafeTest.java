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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@Ignore("ignore while investigating jenkins fails")
public class ChaosMonkeyNothingIsSafeTest extends AbstractFullDistribZkTestBase {
  public static Logger log = LoggerFactory.getLogger(ChaosMonkeyNothingIsSafeTest.class);
  
  private static final int BASE_RUN_LENGTH = 20000;

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
    // can help to hide this when testing and looking at logs
    //ignoreException("shard update error");
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("numShards");
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public ChaosMonkeyNothingIsSafeTest() {
    super();
    sliceCount = 1;
    shardCount = 7;
  }
  
  @Override
  public void doTest() throws Exception {
    boolean testsSuccesful = false;
    try {
      handle.clear();
      handle.put("QTime", SKIPVAL);
      handle.put("timestamp", SKIPVAL);
      
      // make sure we have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderProps(DEFAULT_COLLECTION, "shard" + j, 10000);
      }      // make sure we again have leaders for each shard
      
      waitForRecoveriesToFinish(false);
      
      // we cannot do delete by query
      // as it's not supported for recovery
       del("*:*");
      
      List<StopableThread> threads = new ArrayList<StopableThread>();
      int threadCount = 1;
      int i = 0;
      for (i = 0; i < threadCount; i++) {
        StopableIndexingThread indexThread = new StopableIndexingThread(
            i * 50000, true);
        threads.add(indexThread);
        indexThread.start();
      }
      
      threadCount = 1;
      i = 0;
      for (i = 0; i < threadCount; i++) {
        StopableSearchThread searchThread = new StopableSearchThread();
        threads.add(searchThread);
        searchThread.start();
      }
      
      // TODO: only do this randomly - if we don't do it, compare against control below
      FullThrottleStopableIndexingThread ftIndexThread = new FullThrottleStopableIndexingThread(
          clients, i * 50000, true);
      threads.add(ftIndexThread);
      ftIndexThread.start();
      
      chaosMonkey.startTheMonkey(true, 1500);
      int runLength = atLeast(BASE_RUN_LENGTH);
      try {
        Thread.sleep(runLength);
      } finally {
        chaosMonkey.stopTheMonkey();
      }
      
      for (StopableThread indexThread : threads) {
        indexThread.safeStop();
      }
      
      // wait for stop...
      for (StopableThread indexThread : threads) {
        indexThread.join();
      }
      
      // fails will happen...
      // for (StopableIndexingThread indexThread : threads) {
      // assertEquals(0, indexThread.getFails());
      // }
      
      // try and wait for any replications and what not to finish...
      
      Thread.sleep(2000);
      
      // wait until there are no recoveries...
      waitForThingsToLevelOut(Integer.MAX_VALUE);//Math.round((runLength / 1000.0f / 3.0f)));
      
      // make sure we again have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderProps(DEFAULT_COLLECTION, "shard" + j, 10000);
      }
      
      commit();
      
      // TODO: assert we didnt kill everyone
      
      zkStateReader.updateClusterState(true);
      assertTrue(zkStateReader.getClusterState().getLiveNodes().size() > 0);
      
      
      // we dont't current check vs control because the full throttle thread can
      // have request fails
      checkShardConsistency(false, true);
      
      // ensure we have added more than 0 docs
      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*"))
          .getResults().getNumFound();
      
      assertTrue(cloudClientDocs > 0);
      
      if (VERBOSE) System.out.println("control docs:"
          + controlClient.query(new SolrQuery("*:*")).getResults()
              .getNumFound() + "\n\n");
      testsSuccesful = true;
    } finally {
      if (!testsSuccesful) {
        printLayout();
      }
    }
  }
  
  // skip the randoms - they can deadlock...
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }

  private SolrInputDocument getDoc(Object... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    return doc;
  }

  class FullThrottleStopableIndexingThread extends StopableIndexingThread {
    private HttpClient httpClient = HttpClientUtil.createClient(null);
    private volatile boolean stop = false;
    int clientIndex = 0;
    private ConcurrentUpdateSolrServer suss;
    private List<SolrServer> clients;  
    
    public FullThrottleStopableIndexingThread(List<SolrServer> clients,
        int startI, boolean doDeletes) {
      super(startI, doDeletes);
      setName("FullThrottleStopableIndexingThread");
      setDaemon(true);
      this.clients = clients;
      suss = new ConcurrentUpdateSolrServer(
          ((HttpSolrServer) clients.get(0)).getBaseURL(), httpClient, 8,
          2) {
        public void handleError(Throwable ex) {
          log.warn("suss error", ex);
        }
      };
    }
    
    @Override
    public void run() {
      int i = startI;
      int numDeletes = 0;
      int numAdds = 0;

      while (true && !stop) {
        ++i;
        
        if (doDeletes && random().nextBoolean() && deletes.size() > 0) {
          Integer delete = deletes.remove(0);
          try {
            numDeletes++;
            suss.deleteById(Integer.toString(delete));
          } catch (Exception e) {
            changeUrlOnError(e);
            //System.err.println("REQUEST FAILED:");
            //e.printStackTrace();
            fails.incrementAndGet();
          }
        }
        
        try {
          numAdds++;
          if (numAdds > 4000)
            continue;
          SolrInputDocument doc = getDoc(
              id,
              i,
              i1,
              50,
              tlong,
              50,
              t1,
              "Saxon heptarchies that used to rip around so in old times and raise Cain.  My, you ought to seen old Henry the Eight when he was in bloom.  He WAS a blossom.  He used to marry a new wife every day, and chop off her head next morning.  And he would do it just as indifferent as if ");
          suss.add(doc);
        } catch (Exception e) {
          changeUrlOnError(e);
          //System.err.println("REQUEST FAILED:");
          //e.printStackTrace();
          fails.incrementAndGet();
        }
        
        if (doDeletes && random().nextBoolean()) {
          deletes.add(i);
        }
        
      }
      
      System.err.println("FT added docs:" + numAdds + " with " + fails + " fails" + " deletes:" + numDeletes);
    }

    private void changeUrlOnError(Exception e) {
      if (e instanceof ConnectException) {
        clientIndex++;
        if (clientIndex > clients.size() - 1) {
          clientIndex = 0;
        }
        suss.shutdownNow();
        suss = new ConcurrentUpdateSolrServer(
            ((HttpSolrServer) clients.get(clientIndex)).getBaseURL(),
            httpClient, 30, 3) {
          public void handleError(Throwable ex) {
            log.warn("suss error", ex);
          }
        };
      }
    }
    
    public void safeStop() {
      stop = true;
      suss.shutdownNow();
      httpClient.getConnectionManager().shutdown();
    }

    public int getFails() {
      return fails.get();
    }
    
  };
  
}
