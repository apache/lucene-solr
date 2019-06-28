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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase.CloudJettyRunner;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.TimeOut;
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
  
  private static final int NO_STOP_WARN_TIME = 60;
  private static final int CONLOSS_PERCENT = 10; // 0 - 10 = 0 - 100%
  private static final int EXPIRE_PERCENT = 10; // 0 - 10 = 0 - 100%
  private Map<String,List<CloudJettyRunner>> shardToJetty;
  
  private static final Boolean MONKEY_ENABLED = Boolean.valueOf(System.getProperty("solr.tests.cloud.cm.enabled", "true"));
  // NOTE: CONN_LOSS and EXP are currently being set to "false" intentionally here. Remove the default value once we know tests pass reliably under those conditions
  private static final String CONN_LOSS = System.getProperty("solr.tests.cloud.cm.connloss", "false");
  private static final String EXP = System.getProperty("solr.tests.cloud.cm.exp", "false");
  
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
      expireSessions = Boolean.parseBoolean(EXP); 
    } else {
      expireSessions = chaosRandom.nextBoolean();
    }
    if (CONN_LOSS != null) {
      causeConnectionLoss = Boolean.parseBoolean(CONN_LOSS);
    } else {
      causeConnectionLoss = chaosRandom.nextBoolean();
    }
    
    
    monkeyLog("init - expire sessions:" + expireSessions
        + " cause connection loss:" + causeConnectionLoss);
  }
  
  // TODO: expire all clients at once?
  public void expireSession(final JettySolrRunner jetty) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores != null) {
      monkeyLog("expire session for " + jetty.getLocalPort() + " !");
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
    monkeyLog("Will cause connection loss!");
    
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
      monkeyLog("Will cause connection loss on " + jetty.getLocalPort());
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
    cjetty.jetty.stop();
    stops.incrementAndGet();
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
        jetty.jetty.start();
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
    Map<String,Slice> slices = zkStateReader.getClusterState().getCollection(collection).getSlicesMap();
    
    List<String> sliceKeyList = new ArrayList<>(slices.size());
    sliceKeyList.addAll(slices.keySet());
    String sliceName = sliceKeyList.get(chaosRandom.nextInt(sliceKeyList.size()));
    return sliceName;
  }
  
  public CloudJettyRunner killRandomShard(String slice) throws Exception {
    CloudJettyRunner cjetty = getRandomJetty(slice, aggressivelyKillLeaders);
    if (cjetty != null) {
      stopJetty(cjetty);
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
    int numRunning = getNumRunning(slice);
    
    if (numRunning < 2) {
      // we cannot kill anyone
      monkeyLog("only one active node in shard - monkey cannot kill :(");
      return null;
    }
    
    if (numActive == 2) {
      // we are careful
      Thread.sleep(1000);
      
      numActive = checkIfKillIsLegal(slice, numActive);
      
      if (numActive < 2) {
        // we cannot kill anyone
        monkeyLog("only one active node in shard - monkey cannot kill :(");
        return null;
      }
      
      numRunning = getNumRunning(slice);
      
      if (numRunning < 2) {
        // we cannot kill anyone
        monkeyLog("only one active node in shard - monkey cannot kill :(");
        return null;
      }
    }
    
    boolean canKillIndexer = canKillIndexer(slice);
    
    if (!canKillIndexer) {
      monkeyLog("Number of indexer nodes (nrt or tlog replicas) is not enough to kill one of them, Will only choose a pull replica to kill");
    }
    
    int chance = chaosRandom.nextInt(10);
    CloudJettyRunner cjetty = null;
    if (chance <= 5 && aggressivelyKillLeaders && canKillIndexer) {
      // if killLeader, really aggressively go after leaders
      cjetty = shardToLeaderJetty.get(slice);
    } else {
      List<CloudJettyRunner> jetties = shardToJetty.get(slice);
      // get random node
      int attempt = 0;
      while (true) {
        attempt++;
        int index = chaosRandom.nextInt(jetties.size());
        cjetty = jetties.get(index);
        if (canKillIndexer || getTypeForJetty(slice, cjetty) == Replica.Type.PULL) {
          break;
        } else if (attempt > 20) {
          monkeyLog("Can't kill indexer nodes (nrt or tlog replicas) and couldn't find a random pull node after 20 attempts - monkey cannot kill :(");
          return null;
        }
      }
      
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
          rtIsLeader = core != null && core.getCoreDescriptor().getCloudDescriptor().isLeader();
        }
      } else {
        return null;
      }

      boolean isLeader = leader.getStr(ZkStateReader.NODE_NAME_PROP).equals(cjetty.nodeName)
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

  private int getNumRunning(String slice) {
    int numRunning = 0;
    for (CloudJettyRunner cjetty : shardToJetty.get(slice)) {
      if (!deadPool.contains(cjetty)) {
        numRunning++;
      }
    }
    return numRunning;
  }

  private Type getTypeForJetty(String sliceName, CloudJettyRunner cjetty) {
    DocCollection docCollection = zkStateReader.getClusterState().getCollection(collection);
    
    Slice slice = docCollection.getSlice(sliceName);
    
    ZkNodeProps props = slice.getReplicasMap().get(cjetty.coreNodeName);
    if (props == null) {
      throw new RuntimeException("shard name " + cjetty.coreNodeName + " not found in " + slice.getReplicasMap().keySet());
    }
    return Replica.Type.valueOf(props.getStr(ZkStateReader.REPLICA_TYPE));
  }

  private boolean canKillIndexer(String sliceName) throws KeeperException, InterruptedException {
    int numIndexersFoundInShard = 0;
    for (CloudJettyRunner cloudJetty : shardToJetty.get(sliceName)) {
      
      // get latest cloud state
      zkStateReader.forceUpdateCollection(collection);
      
      DocCollection docCollection = zkStateReader.getClusterState().getCollection(collection);
      
      Slice slice = docCollection.getSlice(sliceName);
      
      ZkNodeProps props = slice.getReplicasMap().get(cloudJetty.coreNodeName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.coreNodeName + " not found in " + slice.getReplicasMap().keySet());
      }
      
      final Replica.State state = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP));
      final Replica.Type replicaType = Replica.Type.valueOf(props.getStr(ZkStateReader.REPLICA_TYPE));
      final String nodeName = props.getStr(ZkStateReader.NODE_NAME_PROP);
      
      if (cloudJetty.jetty.isRunning()
          && state == Replica.State.ACTIVE
          && (replicaType == Replica.Type.TLOG || replicaType == Replica.Type.NRT) 
          && zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        numIndexersFoundInShard++;
      }
    }
    return numIndexersFoundInShard > 1;
  }

  private int checkIfKillIsLegal(String sliceName, int numActive) throws KeeperException, InterruptedException {
    for (CloudJettyRunner cloudJetty : shardToJetty.get(sliceName)) {
      
      // get latest cloud state
      zkStateReader.forceUpdateCollection(collection);
      
      DocCollection docCollection = zkStateReader.getClusterState().getCollection(collection);
      
      Slice slice = docCollection.getSlice(sliceName);
      
      ZkNodeProps props = slice.getReplicasMap().get(cloudJetty.coreNodeName);
      if (props == null) {
        throw new RuntimeException("shard name " + cloudJetty.coreNodeName + " not found in " + slice.getReplicasMap().keySet());
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
  
  public static void monkeyLog(String msg, Object...logParams) {
    log.info("monkey: " + msg, logParams);
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
    if (runtime > NO_STOP_WARN_TIME && stops.get() == 0) {
      LuceneTestCase.fail("The Monkey ran for over " + NO_STOP_WARN_TIME +" seconds and no jetties were stopped - this is worth investigating!");
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
        if (jetty.isStopped()) {
          jetty.start();
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
    ExecutorService executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        15, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new DefaultSolrThreadFactory("ChaosMonkey"),
        false);
    for (JettySolrRunner jetty : jettys) {
      executor.submit(() -> {
        try {
          jetty.stop();
        } catch (Exception e) {
          log.error("error stopping jetty", e);
          throw new RuntimeException(e);
        }
      });
    }
    ExecutorUtil.shutdownAndAwaitTermination(executor);
  }

  public static void start(List<JettySolrRunner> jettys) throws Exception {
    ExecutorService executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        15, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new DefaultSolrThreadFactory("ChaosMonkey"),
        false);
    for (JettySolrRunner jetty : jettys) {
      executor.submit(() -> {
        try {
          jetty.start();
        } catch (Exception e) {
          log.error("error starting jetty", e);
          throw new RuntimeException(e);
        }
      });
    }
    ExecutorUtil.shutdownAndAwaitTermination(executor);
  }

  /**
   * You can call this method to wait while the ChaosMonkey is running, it waits approximately the specified time, and periodically
   * logs the status of the collection
   * @param runLength The time in ms to wait
   * @param collectionName The main collection being used for the ChaosMonkey
   * @param zkStateReader current state reader
   */
  public static void wait(long runLength, String collectionName, ZkStateReader zkStateReader) throws InterruptedException {
    TimeOut t = new TimeOut(runLength, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!t.hasTimedOut()) {
      Thread.sleep(Math.min(1000, t.timeLeft(TimeUnit.MILLISECONDS)));
      logCollectionStateSummary(collectionName, zkStateReader);
    }
  }

  private static void logCollectionStateSummary(String collectionName, ZkStateReader zkStateReader) {
    Pattern portPattern = Pattern.compile(".*:([0-9]*).*");
    DocCollection docCollection = zkStateReader.getClusterState().getCollection(collectionName);
    if (docCollection == null) {
      monkeyLog("Could not find collection {}", collectionName);
    }
    StringBuilder builder = new StringBuilder();
    builder.append("Collection status: {");
    for (Slice slice:docCollection.getSlices()) {
      builder.append(slice.getName()).append(": {");
      for (Replica replica:slice.getReplicas()) {
        log.info(replica.toString());
        java.util.regex.Matcher m = portPattern.matcher(replica.getBaseUrl());
        m.find();
        String jettyPort = m.group(1);
        builder.append(String.format(Locale.ROOT, "%s(%s): {state: %s, type: %s, leader: %s, Live: %s}, ", 
            replica.getName(), jettyPort, replica.getState(), replica.getType(), (replica.get("leader")!= null), zkStateReader.getClusterState().liveNodesContain(replica.getNodeName())));
      }
      if (slice.getReplicas().size() > 0) {
        builder.setLength(builder.length() - 2);
      }
      builder.append("}, ");
    }
    if (docCollection.getSlices().size() > 0) {
      builder.setLength(builder.length() - 2);
    }
    builder.append("}");
    monkeyLog(builder.toString());
  }

}
