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

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.update.SolrCmdDistributor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
public class ChaosMonkeyNothingIsSafeTest extends AbstractFullDistribZkTestBase {
  public static Logger log = LoggerFactory.getLogger(ChaosMonkeyNothingIsSafeTest.class);
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  @BeforeClass
  public static void beforeSuperClass() {
    SolrCmdDistributor.testing_errorHook = new Diagnostics.Callable() {
      @Override
      public void call(Object... data) {
        SolrCmdDistributor.Request sreq = (SolrCmdDistributor.Request)data[1];
        if (sreq.exception == null) return;
        if (sreq.exception.getMessage().contains("Timeout")) {
          Diagnostics.logThreadDumps("REQUESTING THREAD DUMP DUE TO TIMEOUT: " + sreq.exception.getMessage());
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
    super.setUp();
    // can help to hide this when testing and looking at logs
    //ignoreException("shard update error");
    System.setProperty("numShards", Integer.toString(sliceCount));
    useFactory("solr.StandardDirectoryFactory");
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
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "2"));
    shardCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "7"));
  }
  
  @Override
  public void doTest() throws Exception {
    boolean testsSuccesful = false;
    try {
      handle.clear();
      handle.put("QTime", SKIPVAL);
      handle.put("timestamp", SKIPVAL);
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      // make sure we have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
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
            (i+1) * 50000, true);
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
      
      // TODO: only do this sometimes so that we can sometimes compare against control
      boolean runFullThrottle = random().nextBoolean();
      if (runFullThrottle) {
        FullThrottleStopableIndexingThread ftIndexThread = new FullThrottleStopableIndexingThread(
            clients, (i+1) * 50000, true);
        threads.add(ftIndexThread);
        ftIndexThread.start();
      }
      
      chaosMonkey.startTheMonkey(true, 10000);

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
      
      for (StopableThread indexThread : threads) {
        indexThread.safeStop();
      }
      
      // wait for stop...
      for (StopableThread indexThread : threads) {
        indexThread.join();
      }
      
       // we expect full throttle fails, but cloud client should not easily fail
       // but it's allowed to fail and sometimes does, so commented out for now
//       for (StopableThread indexThread : threads) {
//         if (indexThread instanceof StopableIndexingThread && !(indexThread instanceof FullThrottleStopableIndexingThread)) {
//           assertEquals(0, ((StopableIndexingThread) indexThread).getFails());
//         }
//       }
      
      // try and wait for any replications and what not to finish...
      
      Thread.sleep(2000);
      
      // wait until there are no recoveries...
      waitForThingsToLevelOut(Integer.MAX_VALUE);//Math.round((runLength / 1000.0f / 3.0f)));
      
      // make sure we again have leaders for each shard
      for (int j = 1; j < sliceCount; j++) {
        zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 30000);
      }
      
      commit();
      
      // TODO: assert we didnt kill everyone
      
      zkStateReader.updateClusterState(true);
      assertTrue(zkStateReader.getClusterState().getLiveNodes().size() > 0);
      
      
      // full throttle thread can
      // have request fails 
      checkShardConsistency(!runFullThrottle, true);
      
      long ctrlDocs = controlClient.query(new SolrQuery("*:*")).getResults()
      .getNumFound(); 
      
      // ensure we have added more than 0 docs
      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*"))
          .getResults().getNumFound();
      
      assertTrue("Found " + ctrlDocs + " control docs", cloudClientDocs > 0);
      
      if (VERBOSE) System.out.println("control docs:"
          + controlClient.query(new SolrQuery("*:*")).getResults()
              .getNumFound() + "\n\n");
      
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
      
      testsSuccesful = true;
    } finally {
      if (!testsSuccesful) {
        printLayout();
      }
    }
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
      HttpClientUtil.setConnectionTimeout(httpClient, 15000);
      HttpClientUtil.setSoTimeout(httpClient, 15000);
      suss = new ConcurrentUpdateSolrServer(
          ((HttpSolrServer) clients.get(0)).getBaseURL(), httpClient, 8,
          2) {
        @Override
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
          @Override
          public void handleError(Throwable ex) {
            log.warn("suss error", ex);
          }
        };
      }
    }
    
    @Override
    public void safeStop() {
      stop = true;
      suss.shutdownNow();
      httpClient.getConnectionManager().shutdown();
    }

    @Override
    public int getFails() {
      return fails.get();
    }
    
  };
  
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
  
}
