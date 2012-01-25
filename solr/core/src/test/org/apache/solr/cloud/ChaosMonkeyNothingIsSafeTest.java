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
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

@Ignore
public class ChaosMonkeyNothingIsSafeTest extends FullSolrCloudTest {

  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    
  }
  
  @AfterClass
  public static void afterSuperClass() throws Exception {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // TODO use @Noisy annotation as we expect lots of exceptions
    ignoreException(".*");
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public ChaosMonkeyNothingIsSafeTest() {
    super();
    shardCount = atLeast(3);
    sliceCount = 2;
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    // we cannot do delete by query
    // as it's not supported for recovery
    //del("*:*");
    
    List<StopableIndexingThread> threads = new ArrayList<StopableIndexingThread>();
    int threadCount = 1;
    int i = 0;
    for (i = 0; i < threadCount; i++) {
      StopableIndexingThread indexThread = new StopableIndexingThread(i * 50000, true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    FullThrottleStopableIndexingThread ftIndexThread = new FullThrottleStopableIndexingThread(
        clients, i * 50000, true);
    threads.add(ftIndexThread);
    ftIndexThread.start();
    
    chaosMonkey.startTheMonkey(true, 1500);
    try {
      Thread.sleep(atLeast(6000));
    } finally {
      chaosMonkey.stopTheMonkey();
    }
    
    for (StopableIndexingThread indexThread : threads) {
      indexThread.safeStop();
    }
    
    // wait for stop...
    for (StopableIndexingThread indexThread : threads) {
      indexThread.join();
    }
    
    
    // fails will happen...
//    for (StopableIndexingThread indexThread : threads) {
//      assertEquals(0, indexThread.getFails());
//    }
    
    // try and wait for any replications and what not to finish...
    
    Thread.sleep(2000);
    
    // wait until there are no recoveries...
    waitForThingsToLevelOut();
    
    // make sure we again have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderProps(DEFAULT_COLLECTION, "shard" + j, 10000);
    }

    commit();
    
    // TODO: assert we didnt kill everyone
    
    zkStateReader.updateCloudState(true);
    assertTrue(zkStateReader.getCloudState().getLiveNodes().size() > 0);
    
    checkShardConsistency(false, false);
    
    // ensure we have added more than 0 docs
    long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();

    assertTrue(cloudClientDocs > 0);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
  }

  private void waitForThingsToLevelOut() throws KeeperException,
      InterruptedException, Exception, IOException, URISyntaxException {
    int cnt = 0;
    boolean retry = false;
    do {
      waitForRecoveriesToFinish(VERBOSE);
      
      commit();
      
      updateMappingsFromZk(jettys, clients);
      
      Set<String> theShards = shardToClient.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        failMessage = checkShardConsistency(shard, false);
      }
      
      if (failMessage != null) {
        retry  = true;
      }
      cnt++;
      if (cnt > 10) break;
      Thread.sleep(4000);
    } while (retry);
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
    MultiThreadedHttpConnectionManager cm = new MultiThreadedHttpConnectionManager();
    private HttpClient httpClient = new HttpClient(cm) ;
    private volatile boolean stop = false;
    int clientIndex = 0;
    private StreamingUpdateSolrServer suss;
    private List<SolrServer> clients;  
    
    public FullThrottleStopableIndexingThread(List<SolrServer> clients,
        int startI, boolean doDeletes) throws MalformedURLException {
      super(startI, doDeletes);
      setName("FullThrottleStopableIndexingThread");
      setDaemon(true);
      this.clients = clients;
      suss = new StreamingUpdateSolrServer(
          ((CommonsHttpSolrServer) clients.get(0)).getBaseURL(), httpClient, 8,
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
        
        if (doDeletes && random.nextBoolean() && deletes.size() > 0) {
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
        
        if (doDeletes && random.nextBoolean()) {
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
        try {
          suss.shutdownNow();
          suss = new StreamingUpdateSolrServer(
              ((CommonsHttpSolrServer) clients.get(clientIndex)).getBaseURL(),
              httpClient, 30, 3) {
            public void handleError(Throwable ex) {
              log.warn("suss error", ex);
            }
          };
        } catch (MalformedURLException e1) {
          e1.printStackTrace();
        }
      }
    }
    
    public void safeStop() {
      stop = true;
      suss.shutdownNow();
      cm.shutdown();
    }

    public int getFails() {
      return fails.get();
    }
    
  };
  
}
