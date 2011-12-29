package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrConfig;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderElectionTest extends SolrTestCaseJ4 {
  
  static final int TIMEOUT = 10000;
  private ZkTestServer server;
  private SolrZkClient zkClient;
  
  private Map<Integer,Thread> seqToThread;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    createTempDir();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
    zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
    seqToThread = new HashMap<Integer,Thread>();
  }
  
  class ClientThread extends Thread {
    SolrZkClient zkClient;
    private int nodeNumber;
    private int seq = -1;
    private volatile boolean stop;
    private volatile boolean electionDone = false;
    private final ZkNodeProps props;
    
    public ClientThread(int nodeNumber) throws Exception {
      super("Thread-" + nodeNumber);
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      this.nodeNumber = nodeNumber;
      props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, Integer.toString(nodeNumber), ZkStateReader.CORE_PROP, "");
    }
    
    @Override
    public void run() {
      
        LeaderElector elector = new LeaderElector(zkClient);
        
        ElectionContext context = new ShardLeaderElectionContext("shard1",
            "collection1", Integer.toString(nodeNumber), props, zkClient);
        
        try {
          elector.setup(context);
          seq = elector.joinElection(context);
          electionDone = true;
          seqToThread.put(seq, this);
        } catch (Throwable e) {
          e.printStackTrace();
        }
        
      while (!stop) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // nothing
        }
      }
      
    }
    
    public void close() throws InterruptedException {
      if (!zkClient.isClosed()) {
        zkClient.close();
      }
      this.stop = true;
    }
  }

  @Test
  public void testBasic() throws Exception {
    LeaderElector elector = new LeaderElector(zkClient);
    ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr/", ZkStateReader.CORE_PROP, "");
    ElectionContext context = new ShardLeaderElectionContext("shard2", "collection1", "dummynode1", props, zkClient);
    elector.setup(context);
    elector.joinElection(context);
    assertEquals("http://127.0.0.1/solr/", getLeaderUrl("collection1", "shard2"));
  }
  
  private String getLeaderUrl(String collection, String slice) throws KeeperException, InterruptedException {
    int iterCount=30;
    while (iterCount-- > 0)
      try {
      byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null);
      ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
      return leaderProps.getCoreUrl();
    } catch (NoNodeException e) {
      Thread.sleep(100);
    }
    throw new RuntimeException("Could not get leader props");
  }

  @Test
  public void testElection() throws Exception {
    
    List<ClientThread> threads = new ArrayList<ClientThread>();
    
    for (int i = 0; i < 15; i++) {
      ClientThread thread = new ClientThread(i);
      
      threads.add(thread);
    }
    
    for (Thread thread : threads) {
      thread.start();
    }
    
    
    while(true) { //wait for election to complete
      int doneCount = 0;
      for (ClientThread thread : threads) {
        if(thread.electionDone) {
          doneCount++;
        }
      }
      if(doneCount==15) {
        break;
      }
      Thread.sleep(100);
    }
    
    int leaderThread = getLeaderThread();
    
    // whoever the leader is, should be the n_0 seq
    assertEquals(0, threads.get(leaderThread).seq);
    
    // kill n_0, 1, 3 and 4
    ((ClientThread) seqToThread.get(0)).close();
    ((ClientThread) seqToThread.get(4)).close();
    ((ClientThread) seqToThread.get(1)).close();
    ((ClientThread) seqToThread.get(3)).close();
    
    leaderThread = getLeaderThread();
    
    // whoever the leader is, should be the n_2 seq
    assertEquals(2, threads.get(leaderThread).seq);
    
    // kill n_5, 2, 6, 7, and 8
    ((ClientThread) seqToThread.get(5)).close();
    ((ClientThread) seqToThread.get(2)).close();
    ((ClientThread) seqToThread.get(6)).close();
    ((ClientThread) seqToThread.get(7)).close();
    ((ClientThread) seqToThread.get(8)).close();
    
    leaderThread = getLeaderThread();
    
    // whoever the leader is, should be the n_9 seq
    assertEquals(9, threads.get(leaderThread).seq);
    
    // cleanup any threads still running
    for (ClientThread thread : threads) {
      thread.close();
    }
    
    for (Thread thread : threads) {
      thread.join();
    }
    
    //printLayout(server.getZkAddress());
  }

  private int getLeaderThread() throws KeeperException, InterruptedException {
    return Integer.parseInt(getLeaderUrl("collection1", "shard1").replaceAll("/", ""));
  }
  
  @Test
  public void testStressElection() throws Exception {
    //TODO add assertions
    final ScheduledExecutorService scheduler = Executors
        .newScheduledThreadPool(100);
    final List<ClientThread> threads = Collections
        .synchronizedList(new ArrayList<ClientThread>());
    
    Thread scheduleThread = new Thread() {
      @Override
      public void run() {
        for (int i = 0; i < 20; i++) {
          int launchIn = random.nextInt(2000);
          ClientThread thread;
          try {
            thread = new ClientThread(i);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          threads.add(thread);
          scheduler.schedule(thread, launchIn, TimeUnit.MILLISECONDS);
        }
      }
    };
    
    scheduleThread.start();
    
    Thread killThread = new Thread() {
      @Override
      public void run() {
        
        for (int i = 0; i < 1000; i++) {
          try {
            int j;
            try {
              j = random.nextInt(threads.size());
            } catch(IllegalArgumentException e) {
              continue;
            }
            try {
              threads.get(j).close();
            } catch (Exception e) {
              
            }
            threads.remove(j);
            Thread.sleep(10);
            
          } catch (Exception e) {

          }
        }
      }
    };
    
    killThread.start();
    
    scheduleThread.join();
    killThread.join();
    
    Thread.sleep(1000);
    
    scheduler.shutdownNow();
    
    // cleanup any threads still running
    for (ClientThread thread : threads) {
      thread.close();
    }
    
    for (Thread thread : threads) {
      thread.join();
    }

    //printLayout(server.getZkAddress());
    
  }
  
  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    server.shutdown();
    SolrConfig.severeErrors.clear();
    super.tearDown();
  }
  
  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
