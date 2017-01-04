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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase.CloudJettyRunner;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.RTimer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
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
  
  private boolean expireSessions;
  private boolean causeConnectionLoss;
  private boolean aggressivelyKillLeaders;
  private Map<String,CloudJettyRunner> shardToLeaderJetty;
  private volatile RTimer runTimer;
  
  private List<CloudJettyRunner> deadPool = new ArrayList<>();

  private Thread monkeyThread;

  /**
   * Our own Random, seeded from LuceneTestCase on init, so that we can produce a consistent sequence 
   * of random chaos regardless of if/how othe threads access the test randomness in other threads
   * @see LuceneTestCase#random()
   */
  private final Random chaosRandom;
  
  public ChaosMonkey(ZkTestServer zkServer, ZkStateReader zkStateReader,
      String collection, Map<String,List<CloudJettyRunner>> shardToJetty,
      Map<String,CloudJettyRunner> shardToLeaderJetty) {
    this.shardToJetty = shardToJetty;
    this.shardToLeaderJetty = shardToLeaderJetty;
    this.zkServer = zkServer;
    this.zkStateReader = zkStateReader;
    this.collection = collection;
    this.chaosRandom = new Random(LuceneTestCase.random().nextLong());
    
    if (!MONKEY_ENABLED) {
      monkeyLog("The Monkey is Disabled and will not run");
      return;
    }
    
    if (EXP != null) {
      expireSessions = EXP; 
    } else {
      expireSessions = chaosRandom.nextBoolean();
    }
    if (CONN_LOSS != null) {
      causeConnectionLoss = CONN_LOSS;
    } else {
      causeConnectionLoss = chaosRandom.nextBoolean();
    }
    
    
    monkeyLog("init - expire sessions:" + expireSessions
        + " cause connection loss:" + causeConnectionLoss);
  }
  
  // TODO: expire all clients at once?
  public void expireSession(final JettySolrRunner jetty) {
    monkeyLog("expire session for " + jetty.getLocalPort() + " !");

    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      causeConnectionLoss(jetty);
      long sessionId = cores.getZkController().getZkClient()
          .getSolrZooKeeper().getSessionId();
      zkServer.expire(sessionId);
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
  
  public static void causeConnectionLoss(JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      SolrZkClient zkClient = cores.getZkController().getZkClient();
      zkClient.getSolrZooKeeper().closeCnxn();
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
    SolrDispatchFilter sdf = jetty.getSolrDispatchFilter();
    if (sdf != null) {
      try {
        sdf.destroy();
      } catch (Throwable t) {
        log.error("", t);
      }
    }
    try {
      jetty.stop();
    } catch (InterruptedException e) {
      log.info("Jetty stop interrupted - should be a test caused interruption, we will try again to be sure we shutdown");
    } 
    
    if (!jetty.isStopped()) {
      jetty.stop();
    }

    if (!jetty.isStopped()) {
      throw new RuntimeException("could not stop jetty");
    }
  }
  

  public static void kill(List<JettySolrRunner> jettys) throws Exception {
    for (JettySolrRunner jetty : jettys) {
      kill(jetty);
    }
  }
  
  public static void kill(JettySolrRunner jetty) throws Exception {

    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      if (cores.isZooKeeperAware()) {
        int zklocalport = ((InetSocketAddress) cores.getZkController()
            .getZkClient().getSolrZooKeeper().getSocketAddress()).getPort();
        IpTables.blockPort(zklocalport);
      }
    }

    IpTables.blockPort(jetty.getLocalPort());
    
    monkeyLog("kill shard! " + jetty.getLocalPort());
    
    jetty.stop();
    
    stop(jetty);
    
    if (!jetty.isStopped()) {
      throw new RuntimeException("could not kill jetty");
    }
  }
  
  public static void kill(CloudJettyRunner cjetty) throws Exception {
    kill(cjetty.jetty);
  }
  
  public void stopAll(int pauseBetweenMs) throws Exception {
    Set<String> keys = shardToJetty.keySet();
    List<Thread> jettyThreads = new ArrayList<>(keys.size());
    for (String key : keys) {
      List<CloudJettyRunner> jetties = shardToJetty.get(key);
      for (CloudJettyRunner jetty : jetties) {
        Thread.sleep(pauseBetweenMs);
        Thread thread = new Thread() {
          public void run() {
            try {
              stopJetty(jetty);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
        jettyThreads.add(thread);
        thread.start();

      }
    }
    for (Thread thread : jettyThreads) {
      thread.join();
    }
  }
  
  public void startAll() throws Exception {
    Set<String> keys = shardToJetty.keySet();
    for (String key : keys) {
      List<CloudJettyRunner> jetties = shardToJetty.get(key);
      for (CloudJettyRunner jetty : jetties) {
        start(jetty.jetty);
      }
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
    String sliceName = sliceKeyList.get(chaosRandom.nextInt(sliceKeyList.size()));
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
    
    int chance = chaosRandom.nextInt(10);
    CloudJettyRunner cjetty;
    if (chance <= 5 && aggressivelyKillLeaders) {
      // if killLeader, really aggressively go after leaders
      cjetty = shardToLeaderJetty.get(slice);
    } else {
      // get random shard
      List<CloudJettyRunner> jetties = shardToJetty.get(slice);
      int index = chaosRandom.nextInt(jetties.size());
      cjetty = jetties.get(index);
      
      ZkNodeProps leader = null;
      try {
        leader = zkStateReader.getLeaderRetry(collection, slice);
      } catch (Throwable t) {
        log.error("Could not get leader", t);
        return null;
      }

      // cluster state can be stale - also go by our 'near real-time' is leader prop
      boolean rtIsLeader;
      CoreContainer cc = cjetty.jetty.getCoreContainer();
      if (cc != null) {
        try (SolrCore core = cc.getCore(leader.getStr(ZkStateReader.CORE_NAME_PROP))) {
          if (core == null) {
            monkeyLog("selected jetty not running correctly - skip");
            return null;
          }
          rtIsLeader = core.getCoreDescriptor().getCloudDescriptor().isLeader();
        }
      } else {
        return null;
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

  private int checkIfKillIsLegal(String slice, int numActive) throws KeeperException, InterruptedException {
    for (CloudJettyRunner cloudJetty : shardToJetty.get(slice)) {
      
      // get latest cloud state
      zkStateReader.forceUpdateCollection(collection);
      
      Slice theShards = zkStateReader.getClusterState().getSlicesMap(collection)
          .get(slice);
      
      ZkNodeProps props = theShards.getReplicasMap().get(cloudJetty.coreNodeName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.coreNodeName + " not found in " + theShards.getReplicasMap().keySet());
      }
      
      final Replica.State state = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP));
      final String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      
      if (cloudJetty.jetty.isRunning()
          && state == Replica.State.ACTIVE
          && zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        numActive++;
      }
    }
    return numActive;
  }
  
  // synchronously starts and stops shards randomly, unless there is only one
  // active shard up for a slice or if there is one active and others recovering
  public void startTheMonkey(boolean killLeaders, final int roundPauseUpperLimit) {
    if (!MONKEY_ENABLED) {
      monkeyLog("The Monkey is disabled and will not start");
      return;
    }
    monkeyLog("starting");
    
    
    if (chaosRandom.nextBoolean()) {
      monkeyLog("Jetty will not commit on close");
      DirectUpdateHandler2.commitOnClose = false;
    }

    this.aggressivelyKillLeaders = killLeaders;
    runTimer = new RTimer();
    // TODO: when kill leaders is on, lets kill a higher percentage of leaders
    
    stop = false;
    monkeyThread = new Thread() {

      @Override
      public void run() {
        while (!stop) {
          try {
    
            Thread.sleep(chaosRandom.nextInt(roundPauseUpperLimit));

            causeSomeChaos();
            
          } catch (InterruptedException e) {
            //
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        monkeyLog("finished");
        monkeyLog("I ran for " + runTimer.getTime() / 1000 + "s. I stopped " + stops + " and I started " + starts
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
    runTimer.stop();

    DirectUpdateHandler2.commitOnClose = true;

    double runtime = runTimer.getTime()/1000.0f;
    if (runtime > 45 && stops.get() == 0) {
      LuceneTestCase.fail("The Monkey ran for over 45 seconds and no jetties were stopped - this is worth investigating!");
    }
  }

  /**
   * causes some randomly selected chaos
   */
  public void causeSomeChaos() throws Exception {
    if (chaosRandom.nextBoolean()) {
      if (!deadPool.isEmpty()) {
        int index = chaosRandom.nextInt(deadPool.size());
        JettySolrRunner jetty = deadPool.get(index).jetty;
        if (jetty.isStopped() && !ChaosMonkey.start(jetty)) {
          return;
        }
        deadPool.remove(index);
        starts.incrementAndGet();
        return;
      }
    }
    
    int rnd = chaosRandom.nextInt(10);
    
    if (expireSessions && rnd < EXPIRE_PERCENT) {
      expireRandomSession();
    } 
    
    if (causeConnectionLoss && rnd < CONLOSS_PERCENT) {
      randomConnectionLoss();
    }
    
    CloudJettyRunner cjetty;
    if (chaosRandom.nextBoolean()) {
      cjetty = stopRandomShard();
    } else {
      cjetty = killRandomShard();
    }
    if (cjetty == null) {
      // we cannot kill
    } else {
      deadPool.add(cjetty);
    }
  }
  
  public int getStarts() {
    return starts.get();
  }

  public static void stop(List<JettySolrRunner> jettys) throws Exception {
    for (JettySolrRunner jetty : jettys) {
      stop(jetty);
    }
  }
  
  public static void stop(JettySolrRunner jetty) throws Exception {
    stopJettySolrRunner(jetty);
  }
  
  public static void start(List<JettySolrRunner> jettys) throws Exception {
    for (JettySolrRunner jetty : jettys) {
      start(jetty);
    }
  }
  
  public static boolean start(JettySolrRunner jetty) throws Exception {

    IpTables.unblockPort(jetty.getLocalPort());
    try {
      jetty.start();
    } catch (Exception e) {
      jetty.stop();
      Thread.sleep(3000);
      try {
        jetty.start();
      } catch (Exception e2) {
        jetty.stop();
        Thread.sleep(10000);
        try {
          jetty.start();
        } catch (Exception e3) {
          jetty.stop();
          Thread.sleep(30000);
          try {
            jetty.start();
          } catch (Exception e4) {
            log.error("Could not get the port to start jetty again", e4);
            // we coud not get the port
            jetty.stop();
            return false;
          }
        }
      }
    }
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      if (cores.isZooKeeperAware()) {
        int zklocalport = ((InetSocketAddress) cores.getZkController()
            .getZkClient().getSolrZooKeeper().getSocketAddress()).getPort();
        IpTables.unblockPort(zklocalport);
      }
    }

    return true;
  }

}
