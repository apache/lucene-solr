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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase.CloudJettyRunner;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The monkey can stop random or specific jetties used with SolrCloud.
 * 
 * It can also run in a background thread and start and stop jetties
 * randomly.
 * TODO: expire multiple sessions / connectionloss at once
 * TODO: kill multiple jetties at once
 * TODO: ? add random headhunter mode that always kills the leader
 * TODO: chaosmonkey should be able to do cluster stop/start tests
 */
public class ChaosMonkey {
  private static Logger log = LoggerFactory.getLogger(ChaosMonkey.class);
  
  private static final int CONLOSS_PERCENT = 10; // 0 - 10 = 0 - 100%
  private static final int EXPIRE_PERCENT = 10; // 0 - 10 = 0 - 100%
  private Map<String,List<CloudJettyRunner>> shardToJetty;
  
  private static final Boolean MONKEY_ENABLED = Boolean.valueOf(System.getProperty("solr.tests.cloud.cm.enabled", "true"));
  private static final Boolean CONN_LOSS = Boolean.valueOf(System.getProperty("solr.tests.cloud.cm.connloss", null));
  private static final Boolean EXP = Boolean.valueOf(System.getProperty("solr.tests.cloud.cm.exp", null));
  
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
  private volatile long startTime;
  
  private List<CloudJettyRunner> deadPool = new ArrayList<>();

  private Thread monkeyThread;
  
  public ChaosMonkey(ZkTestServer zkServer, ZkStateReader zkStateReader,
      String collection, Map<String,List<CloudJettyRunner>> shardToJetty,
      Map<String,CloudJettyRunner> shardToLeaderJetty) {
    this.shardToJetty = shardToJetty;
    this.shardToLeaderJetty = shardToLeaderJetty;
    this.zkServer = zkServer;
    this.zkStateReader = zkStateReader;
    this.collection = collection;
    
    if (!MONKEY_ENABLED) {
      monkeyLog("The Monkey is Disabled and will not run");
      return;
    }
    
    Random random = LuceneTestCase.random();
    if (EXP != null) {
      expireSessions = EXP; 
    } else {
      expireSessions = random.nextBoolean();
    }
    if (CONN_LOSS != null) {
      causeConnectionLoss = CONN_LOSS;
    } else {
      causeConnectionLoss = random.nextBoolean();
    }
    
    
    monkeyLog("init - expire sessions:" + expireSessions
        + " cause connection loss:" + causeConnectionLoss);
  }
  
  // TODO: expire all clients at once?
  public void expireSession(final JettySolrRunner jetty) {
    monkeyLog("expire session for " + jetty.getLocalPort() + " !");
    
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) jetty
        .getDispatchFilter().getFilter();
    if (solrDispatchFilter != null) {
      CoreContainer cores = solrDispatchFilter.getCores();
      if (cores != null) {
        causeConnectionLoss(jetty, cores.getZkController().getClientTimeout() + 200);
      }
    }
    

//    Thread thread = new Thread() {
//      {
//        setDaemon(true);
//      }
//      public void run() {
//        SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) jetty.getDispatchFilter().getFilter();
//        if (solrDispatchFilter != null) {
//          CoreContainer cores = solrDispatchFilter.getCores();
//          if (cores != null) {
//            try {
//              Thread.sleep(ZkTestServer.TICK_TIME * 2 + 800);
//            } catch (InterruptedException e) {
//              // we act as only connection loss
//              return;
//            }
//            long sessionId = cores.getZkController().getZkClient().getSolrZooKeeper().getSessionId();
//            zkServer.expire(sessionId);
//          }
//        }
//      }
//    };
//    thread.start();

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
  
  public static void causeConnectionLoss(JettySolrRunner jetty) {
    causeConnectionLoss(jetty, ZkTestServer.TICK_TIME * 2 + 200);
  }
  
  public static void causeConnectionLoss(JettySolrRunner jetty, int pauseTime) {
    SolrDispatchFilter solrDispatchFilter = (SolrDispatchFilter) jetty
        .getDispatchFilter().getFilter();
    if (solrDispatchFilter != null) {
      CoreContainer cores = solrDispatchFilter.getCores();
      if (cores != null) {
        SolrZkClient zkClient = cores.getZkController().getZkClient();
        // must be at least double tick time...
        zkClient.getSolrZooKeeper().pauseCnxn(pauseTime);
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
    assert(jetty != null);
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
    FilterHolder filterHolder = cjetty.jetty.getDispatchFilter();
    if (filterHolder != null) {
      Filter filter = filterHolder.getFilter();
      if (filter != null) {
        CoreContainer cores = ((SolrDispatchFilter) filter).getCores();
        if (cores != null) {
          int zklocalport = ((InetSocketAddress) cores.getZkController()
              .getZkClient().getSolrZooKeeper().getSocketAddress()).getPort();
          IpTables.blockPort(zklocalport);
        }
      }
    }

    IpTables.blockPort(cjetty.jetty.getLocalPort());
    
    JettySolrRunner jetty = cjetty.jetty;
    monkeyLog("kill shard! " + jetty.getLocalPort());
    
    jetty.stop();
    
    stop(jetty);
    
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
    Map<String,Slice> slices = zkStateReader.getClusterState().getSlicesMap(collection);
    
    List<String> sliceKeyList = new ArrayList<>(slices.size());
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
    
    int numActive = 0;
    
    numActive = checkIfKillIsLegal(slice, numActive);
    
    // TODO: stale state makes this a tough call
    if (numActive < 2) {
      // we cannot kill anyone
      monkeyLog("only one active node in shard - monkey cannot kill :(");
      return null;
    }
    
    // let's check the deadpool count
    int numRunning = 0;
    for (CloudJettyRunner cjetty : shardToJetty.get(slice)) {
      if (!deadPool.contains(cjetty)) {
        numRunning++;
      }
    }
    
    if (numRunning < 2) {
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
      
      ZkNodeProps leader = null;
      try {
        leader = zkStateReader.getLeaderRetry(collection, slice);
      } catch (Throwable t) {
        log.error("Could not get leader", t);
        return null;
      }
      
      FilterHolder fh = cjetty.jetty.getDispatchFilter();
      if (fh == null) {
        monkeyLog("selected jetty not running correctly - skip");
        return null;
      }
      SolrDispatchFilter df = ((SolrDispatchFilter) fh.getFilter());
      if (df == null) {
        monkeyLog("selected jetty not running correctly - skip");
        return null;
      }
      CoreContainer cores = df.getCores();
      if (cores == null) {
        monkeyLog("selected jetty not running correctly - skip");
        return null;
      }

      // cluster state can be stale - also go by our 'near real-time' is leader prop
      boolean rtIsLeader;
      try (SolrCore core = cores.getCore(leader.getStr(ZkStateReader.CORE_NAME_PROP))) {
        if (core == null) {
          monkeyLog("selected jetty not running correctly - skip");
          return null;
        }
        rtIsLeader = core.getCoreDescriptor().getCloudDescriptor().isLeader();
      }

      boolean isLeader = leader.getStr(ZkStateReader.NODE_NAME_PROP).equals(jetties.get(index).nodeName)
          || rtIsLeader;
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

  private int checkIfKillIsLegal(String slice, int numActive)
      throws KeeperException, InterruptedException {
    for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
      
      // get latest cloud state
      zkStateReader.updateClusterState(true);
      
      Slice theShards = zkStateReader.getClusterState().getSlicesMap(collection)
          .get(slice);
      
      ZkNodeProps props = theShards.getReplicasMap().get(cloudJetty.coreNodeName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.coreNodeName + " not found in " + theShards.getReplicasMap().keySet());
      }
      
      String state = props.getStr(ZkStateReader.STATE_PROP);
      String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      
      if (cloudJetty.jetty.isRunning()
          && state.equals(ZkStateReader.ACTIVE)
          && zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        numActive++;
      }
    }
    return numActive;
  }
  
  public SolrServer getRandomClient(String slice) throws KeeperException, InterruptedException {
    // get latest cloud state
    zkStateReader.updateClusterState(true);

    // get random shard
    List<SolrServer> clients = shardToClient.get(slice);
    int index = LuceneTestCase.random().nextInt(clients.size() - 1);
    SolrServer client = clients.get(index);

    return client;
  }
  
  // synchronously starts and stops shards randomly, unless there is only one
  // active shard up for a slice or if there is one active and others recovering
  public void startTheMonkey(boolean killLeaders, final int roundPauseUpperLimit) {
    if (!MONKEY_ENABLED) {
      monkeyLog("The Monkey is disabled and will not start");
      return;
    }
    monkeyLog("starting");
    
    
    if (LuceneTestCase.random().nextBoolean()) {
      monkeyLog("Jetty will not commit on shutdown");
      DirectUpdateHandler2.commitOnClose = false;
    }
    
    this.aggressivelyKillLeaders = killLeaders;
    startTime = System.currentTimeMillis();
    // TODO: when kill leaders is on, lets kill a higher percentage of leaders
    
    stop = false;
    monkeyThread = new Thread() {

      @Override
      public void run() {
        while (!stop) {
          try {
    
            Random random = LuceneTestCase.random();
            Thread.sleep(random.nextInt(roundPauseUpperLimit));
            if (random.nextBoolean()) {
             if (!deadPool.isEmpty()) {
               int index = random.nextInt(deadPool.size());
               JettySolrRunner jetty = deadPool.get(index).jetty;
               if (jetty.isStopped() && !ChaosMonkey.start(jetty)) {
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
    };
    monkeyThread.start();
  }
  
  public static void monkeyLog(String msg) {
    log.info("monkey: " + msg);
  }
  
  public void stopTheMonkey() {
    stop = true;
    try {
      monkeyThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    DirectUpdateHandler2.commitOnClose = true;
    
    float runtime = (System.currentTimeMillis() - startTime)/1000.0f;
    if (runtime > 20 && stops.get() == 0) {
      LuceneTestCase.fail("The Monkey ran for over 20 seconds and no jetties were stopped - this is worth investigating!");
    }
  }

  public int getStarts() {
    return starts.get();
  }

  public static void stop(JettySolrRunner jetty) throws Exception {
    stopJettySolrRunner(jetty);
  }
  
  public static boolean start(JettySolrRunner jetty) throws Exception {

    IpTables.unblockPort(jetty.getLocalPort());
    try {
      jetty.start();
    } catch (Exception e) {
      jetty.stop();
      Thread.sleep(2000);
      try {
        jetty.start();
      } catch (Exception e2) {
        jetty.stop();
        Thread.sleep(5000);
        try {
          jetty.start();
        } catch (Exception e3) {
          log.error("Could not get the port to start jetty again", e3);
          // we coud not get the port
          jetty.stop();
          return false;
        }
      }
    }
    FilterHolder filterHolder = jetty.getDispatchFilter();
    if (filterHolder != null) {
      Filter filter = filterHolder.getFilter();
      if (filter != null) {
        CoreContainer cores = ((SolrDispatchFilter) filter).getCores();
        if (cores != null) {
          int zklocalport = ((InetSocketAddress) cores.getZkController()
              .getZkClient().getSolrZooKeeper().getSocketAddress()).getPort();
          IpTables.unblockPort(zklocalport);
        }
      }
    }
    return true;
  }

}