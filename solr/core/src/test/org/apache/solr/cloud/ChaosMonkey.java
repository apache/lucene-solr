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

import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.FullSolrCloudTest.CloudJettyRunner;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The monkey can stop random or specific jetties used with SolrCloud.
 * 
 * It can also run in a background thread and start and stop jetties
 * randomly.
 *
 */
public class ChaosMonkey {
  private static Logger log = LoggerFactory.getLogger(ChaosMonkey.class);
  
  private static final int CONLOSS_PERCENT = 3; //30%
  private static final int EXPIRE_PERCENT = 4; //40%
  private Map<String,List<CloudJettyRunner>> shardToJetty;
  
  private ZkTestServer zkServer;
  private ZkStateReader zkStateReader;
  private String collection;
  private volatile boolean stop = false;
  private AtomicInteger stops = new AtomicInteger();
  private AtomicInteger starts = new AtomicInteger();
  private AtomicInteger expires = new AtomicInteger();
  private AtomicInteger connloss = new AtomicInteger();
  
  private Map<String,List<SolrServer>> shardToClient;
  private boolean expireSessions;
  private boolean causeConnectionLoss;
  private boolean aggressivelyKillLeaders;
  private Map<String,CloudJettyRunner> shardToLeaderJetty;
  private long startTime;
  
  public ChaosMonkey(ZkTestServer zkServer, ZkStateReader zkStateReader,
      String collection, Map<String,List<CloudJettyRunner>> shardToJetty,
      Map<String,CloudJettyRunner> shardToLeaderJetty) {
    this.shardToJetty = shardToJetty;
    this.shardToLeaderJetty = shardToLeaderJetty;
    this.zkServer = zkServer;
    this.zkStateReader = zkStateReader;
    this.collection = collection;
    Random random = LuceneTestCase.random();
    expireSessions = random.nextBoolean();
    causeConnectionLoss = random.nextBoolean();
    monkeyLog("init - expire sessions:" + expireSessions
        + " cause connection loss:" + causeConnectionLoss);
  }
  
  public void expireSession(JettySolrRunner jetty) {
    monkeyLog("expire session for " + jetty.getLocalPort() + " !");
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) jetty.getDispatchFilter().getFilter();
    if (solrDispatchFilter != null) {
      CoreContainer cores = solrDispatchFilter.getCores();
      if (cores != null) {
        long sessionId = cores.getZkController().getZkClient().getSolrZooKeeper().getSessionId();
        zkServer.expire(sessionId);
      }
    }
  }
  
  public void expireRandomSession() throws KeeperException, InterruptedException {
    String sliceName = getRandomSlice();
    
    CloudJettyRunner jetty = getRandomJetty(sliceName, aggressivelyKillLeaders);
    if (jetty != null) {
      expireSession(jetty.jetty);
      expires.incrementAndGet();
    }
  }
  
  public void randomConnectionLoss() throws KeeperException, InterruptedException {
    monkeyLog("cause connection loss!");
    
    String sliceName = getRandomSlice();
    CloudJettyRunner jetty = getRandomJetty(sliceName, aggressivelyKillLeaders);
    if (jetty != null) {
      causeConnectionLoss(jetty.jetty);
      connloss.incrementAndGet();
    }
  }
  
  private void causeConnectionLoss(JettySolrRunner jetty) {
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) jetty
        .getDispatchFilter().getFilter();
    if (solrDispatchFilter != null) {
      CoreContainer cores = solrDispatchFilter.getCores();
      if (cores != null) {
        SolrZkClient zkClient = cores.getZkController().getZkClient();
        // must be at least double tick time...
        zkClient.getSolrZooKeeper().pauseCnxn(ZkTestServer.TICK_TIME * 2);
      }
    }
  }

  public CloudJettyRunner stopShard(String slice, int index) throws Exception {
    CloudJettyRunner cjetty = shardToJetty.get(slice).get(index);
    stopJetty(cjetty);
    return cjetty;
  }

  public void stopJetty(CloudJettyRunner cjetty) throws Exception {
    stop(cjetty.jetty);
    stops.incrementAndGet();
  }

  public void killJetty(CloudJettyRunner cjetty) throws Exception {
    kill(cjetty);
    stops.incrementAndGet();
  }
  
  public void stopJetty(JettySolrRunner jetty) throws Exception {
    stops.incrementAndGet();
    stopJettySolrRunner(jetty);
  }
  
  private static void stopJettySolrRunner(JettySolrRunner jetty) throws Exception {
    
    monkeyLog("stop shard! " + jetty.getLocalPort());
    // get a clean shutdown so that no dirs are left open...
    FilterHolder fh = jetty.getDispatchFilter();
    if (fh != null) {
      SolrDispatchFilter sdf = (SolrDispatchFilter) fh.getFilter();
      if (sdf != null) {
        sdf.destroy();
      }
    }
    jetty.stop();
    
    if (!jetty.isStopped()) {
      throw new RuntimeException("could not stop jetty");
    }
  }
  
  public static void kill(CloudJettyRunner cjetty) throws Exception {
    JettySolrRunner jetty = cjetty.jetty;
    monkeyLog("kill shard! " + jetty.getLocalPort());
    FilterHolder fh = jetty.getDispatchFilter();
    SolrDispatchFilter sdf = null;
    if (fh != null) {
      sdf = (SolrDispatchFilter) fh.getFilter();
    }
    jetty.stop();
    
    if (sdf != null) {
      sdf.destroy();
    }
    
    if (!jetty.isStopped()) {
      throw new RuntimeException("could not kill jetty");
    }
  }
  
  public void stopShard(String slice) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      stopJetty(jetty);
    }
  }
  
  public void stopShardExcept(String slice, String shardName) throws Exception {
    List<CloudJettyRunner> jetties = shardToJetty.get(slice);
    for (CloudJettyRunner jetty : jetties) {
      if (!jetty.nodeName.equals(shardName)) {
        stopJetty(jetty);
      }
    }
  }
  
  public JettySolrRunner getShard(String slice, int index) throws Exception {
    JettySolrRunner jetty = shardToJetty.get(slice).get(index).jetty;
    return jetty;
  }
  
  public CloudJettyRunner stopRandomShard() throws Exception {
    String sliceName = getRandomSlice();
    
    return stopRandomShard(sliceName);
  }
  
  public CloudJettyRunner stopRandomShard(String slice) throws Exception {
    CloudJettyRunner cjetty = getRandomJetty(slice, aggressivelyKillLeaders);
    if (cjetty != null) {
      stopJetty(cjetty);
    }
    return cjetty;
  }
  
  
  public CloudJettyRunner killRandomShard() throws Exception {
    // add all the shards to a list
    String sliceName = getRandomSlice();
    
    return killRandomShard(sliceName);
  }

  private String getRandomSlice() {
    Map<String,Slice> slices = zkStateReader.getCloudState().getSlices(collection);
    
    List<String> sliceKeyList = new ArrayList<String>(slices.size());
    sliceKeyList.addAll(slices.keySet());
    String sliceName = sliceKeyList.get(LuceneTestCase.random().nextInt(sliceKeyList.size()));
    return sliceName;
  }
  
  public CloudJettyRunner killRandomShard(String slice) throws Exception {
    CloudJettyRunner cjetty = getRandomJetty(slice, aggressivelyKillLeaders);
    if (cjetty != null) {
      killJetty(cjetty);
    }
    return cjetty;
  }
  
  public CloudJettyRunner getRandomJetty(String slice, boolean aggressivelyKillLeaders) throws KeeperException, InterruptedException {
    

    int numRunning = 0;
    int numRecovering = 0;
    int numActive = 0;
    
    for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
      boolean running = true;
      
      // get latest cloud state
      zkStateReader.updateCloudState(true);
      
      Slice theShards = zkStateReader.getCloudState().getSlices(collection)
          .get(slice);
      
      ZkNodeProps props = theShards.getShards().get(cloudJetty.coreNodeName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.coreNodeName + " not found in " + theShards.getShards().keySet());
      }
      
      String state = props.get(ZkStateReader.STATE_PROP);
      String nodeName = props.get(ZkStateReader.NODE_NAME_PROP);
      
      
      if (!cloudJetty.jetty.isRunning()
          || !state.equals(ZkStateReader.ACTIVE)
          || !zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        running = false;
      }
      
      if (cloudJetty.jetty.isRunning()
          && state.equals(ZkStateReader.RECOVERING)
          && zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        numRecovering++;
      }
      
      if (cloudJetty.jetty.isRunning()
          && state.equals(ZkStateReader.ACTIVE)
          && zkStateReader.getCloudState().liveNodesContain(nodeName)) {
        numActive++;
      }
      
      if (running) {
        numRunning++;
      }
    }
    
    if (numActive < 2) {
      // we cannot kill anyone
      monkeyLog("only one active node in shard - monkey cannot kill :(");
      return null;
    }
    Random random = LuceneTestCase.random();
    int chance = random.nextInt(10);
    CloudJettyRunner cjetty;
    if (chance <= 5 && aggressivelyKillLeaders) {
      // if killLeader, really aggressively go after leaders
      cjetty = shardToLeaderJetty.get(slice);
    } else {
      // get random shard
      List<CloudJettyRunner> jetties = shardToJetty.get(slice);
      int index = random.nextInt(jetties.size());
      cjetty = jetties.get(index);
      
      ZkNodeProps leader = zkStateReader.getLeaderProps(collection, slice);
      boolean isLeader = leader.get(ZkStateReader.NODE_NAME_PROP).equals(jetties.get(index).nodeName);
      if (!aggressivelyKillLeaders && isLeader) {
        // we don't kill leaders...
        monkeyLog("abort! I don't kill leaders");
        return null;
      } 
    }

    if (cjetty.jetty.getLocalPort() == -1) {
      // we can't kill the dead
      monkeyLog("abort! This guy is already dead");
      return null;
    }
    
    //System.out.println("num active:" + numActive + " for " + slice + " sac:" + jetty.getLocalPort());
    monkeyLog("chose a victim! " + cjetty.jetty.getLocalPort());
  
    return cjetty;
  }
  
  public SolrServer getRandomClient(String slice) throws KeeperException, InterruptedException {
    // get latest cloud state
    zkStateReader.updateCloudState(true);

    // get random shard
    List<SolrServer> clients = shardToClient.get(slice);
    int index = LuceneTestCase.random().nextInt(clients.size() - 1);
    SolrServer client = clients.get(index);

    return client;
  }
  
  // synchronously starts and stops shards randomly, unless there is only one
  // active shard up for a slice or if there is one active and others recovering
  public void startTheMonkey(boolean killLeaders, final int roundPause) {
    monkeyLog("starting");
    this.aggressivelyKillLeaders = killLeaders;
    startTime = System.currentTimeMillis();
    // TODO: when kill leaders is on, lets kill a higher percentage of leaders
    
    stop = false;
    new Thread() {
      private List<CloudJettyRunner> deadPool = new ArrayList<CloudJettyRunner>();

      @Override
      public void run() {
        while (!stop) {
          try {
            Thread.sleep(roundPause);
            Random random = LuceneTestCase.random();
            if (random.nextBoolean()) {
             if (!deadPool.isEmpty()) {
               int index = random.nextInt(deadPool.size());
               JettySolrRunner jetty = deadPool.get(index).jetty;
               if (!ChaosMonkey.start(jetty)) {
                 continue;
               }
               //System.out.println("started on port:" + jetty.getLocalPort());
               deadPool.remove(index);
               starts.incrementAndGet();
               continue;
             }
            }
            
            int rnd = random.nextInt(10);

            if (expireSessions && rnd < EXPIRE_PERCENT) {
              expireRandomSession();
            } 
            
            if (causeConnectionLoss && rnd < CONLOSS_PERCENT) {
              randomConnectionLoss();
              randomConnectionLoss();
            }
            
            CloudJettyRunner cjetty;
            if (random.nextBoolean()) {
              cjetty = stopRandomShard();
            } else {
              cjetty = killRandomShard();
            }
            if (cjetty == null) {
              // we cannot kill
            } else {
              deadPool.add(cjetty);
            }
            
          } catch (InterruptedException e) {
            //
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        monkeyLog("finished");
        monkeyLog("I ran for " + (System.currentTimeMillis() - startTime)/1000.0f + "sec. I stopped " + stops + " and I started " + starts
            + ". I also expired " + expires.get() + " and caused " + connloss
            + " connection losses");
      }
    }.start();
  }
  
  public static void monkeyLog(String msg) {
    log.info("monkey: " + msg);
  }
  
  public void stopTheMonkey() {
    stop = true;
  }

  public int getStarts() {
    return starts.get();
  }

  public static void stop(JettySolrRunner jetty) throws Exception {
    stopJettySolrRunner(jetty);
  }
  
  public static boolean start(JettySolrRunner jetty) throws Exception {
    try {
      jetty.start();
    } catch (BindException e) {
      jetty.stop();
      Thread.sleep(2000);
      try {
        jetty.start();
      } catch (BindException e2) {
        jetty.stop();
        Thread.sleep(5000);
        try {
          jetty.start();
        } catch (BindException e3) {
          // we coud not get the port
          jetty.stop();
          return false;
        }
      }
    }
    return true;
  }

}