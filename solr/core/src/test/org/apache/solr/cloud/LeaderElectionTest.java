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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

@Slow
public class LeaderElectionTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int TIMEOUT = 30000;
  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private Map<Integer,Thread> seqToThread;

  private volatile boolean stopStress = false;

  @BeforeClass
  public static void beforeClass() {

  }

  @AfterClass
  public static void afterClass() {

  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Path zkDir = createTempDir("zkData");

    server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    server.run();

    zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
    zkStateReader = new ZkStateReader(zkClient);
    seqToThread = Collections.synchronizedMap(new HashMap<Integer,Thread>());
    zkClient.makePath("/collections/collection1", true);
    zkClient.makePath("/collections/collection2", true);
  }

  class TestLeaderElectionContext extends ShardLeaderElectionContextBase {
    private long runLeaderDelay = 0;

    public TestLeaderElectionContext(LeaderElector leaderElector,
        String shardId, String collection, String coreNodeName, ZkNodeProps props,
        ZkController zkController, long runLeaderDelay) {
      super (leaderElector, shardId, collection, coreNodeName, props, zkController);
      this.runLeaderDelay = runLeaderDelay;
    }

    @Override
    void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
        throws KeeperException, InterruptedException, IOException {
      super.runLeaderProcess(weAreReplacement, pauseBeforeStartMs);
      if (runLeaderDelay > 0) {
        log.info("Sleeping for {}ms to simulate leadership takeover delay", runLeaderDelay);
        Thread.sleep(runLeaderDelay);
      }
    }
  }

  class ElectorSetup {
    SolrZkClient zkClient;
    ZkStateReader zkStateReader;
    ZkController zkController;
    LeaderElector elector;

    public ElectorSetup(OnReconnect onReconnect) {
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT, TIMEOUT, onReconnect);
      zkStateReader = new ZkStateReader(zkClient);
      elector = new LeaderElector(zkClient);
      zkController = MockSolrSource.makeSimpleMock(null, zkStateReader, null);
    }

    public void close() {
      if (!zkClient.isClosed()) {
        zkClient.close();
      }
      zkStateReader.close();
    }
  }

  class ClientThread extends Thread {
    ElectorSetup es;
    private String shard;
    private String nodeName;
    private long runLeaderDelay = 0;
    private volatile int seq = -1;
    private volatile boolean stop;
    private volatile boolean electionDone = false;
    private final ZkNodeProps props;

    public ClientThread(String shard, int nodeNumber) throws Exception {
      this(null, shard, nodeNumber, 0);
    }

    public ClientThread(ElectorSetup es, String shard, int nodeNumber, long runLeaderDelay) throws Exception {
      super("Thread-" + shard + nodeNumber);
      this.shard = shard;
      this.nodeName = shard + nodeNumber + ":80_solr";
      this.runLeaderDelay = runLeaderDelay;

      props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, this.nodeName, ZkStateReader.BASE_URL_PROP, Integer.toString(nodeNumber), ZkStateReader.CORE_NAME_PROP, "");

      this.es = es;
      if (this.es == null) {
        this.es = new ElectorSetup(() -> {
          try {
            setupOnConnect();
          } catch (Throwable t) {
          }
        });
      }
    }

    private void setupOnConnect() throws InterruptedException, KeeperException,
        IOException {
      assertNotNull(es);
      TestLeaderElectionContext context = new TestLeaderElectionContext(
          es.elector, shard, "collection1", nodeName,
          props, es.zkController, runLeaderDelay);
      es.elector.setup(context);
      seq = es.elector.joinElection(context, false);
      electionDone = true;
      seqToThread.put(seq, this);
    }

    @Override
    public void run() {
      try {
        setupOnConnect();
      } catch (Throwable e) {
        log.error("setup failed", e);
        es.close();
        return;
      }

      while (!stop) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }

    }

    public void close() {
      es.close();
      this.stop = true;
    }

    public int getSeq() {
      return seq;
    }
  }

  @Test
  public void testBasic() throws Exception {
    LeaderElector elector = new LeaderElector(zkClient);
    ZkController zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
    String nodeName = "127.0.0.1:80_solr";
    ZkNodeProps props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, nodeName, ZkStateReader.BASE_URL_PROP,
        zkStateReader.getBaseUrlForNodeName(nodeName), ZkStateReader.CORE_NAME_PROP, "");

    ElectionContext context = new ShardLeaderElectionContextBase(elector,
        "shard2", "collection1", "dummynode1", props, zkController);
    elector.setup(context);
    elector.joinElection(context, false);
    String urlScheme = zkStateReader.getClusterProperty(URL_SCHEME, "http");
    assertEquals(urlScheme + "://127.0.0.1:80/solr/", getLeaderUrl("collection1", "shard2"));
  }

  @Test
  public void testCancelElection() throws Exception {
    LeaderElector first = new LeaderElector(zkClient);
    String nodeName = "127.0.0.1:80_solr";

    ZkNodeProps props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, nodeName, ZkStateReader.BASE_URL_PROP,
        zkStateReader.getBaseUrlForNodeName(nodeName), ZkStateReader.CORE_NAME_PROP, "1");
    ZkController zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
    ElectionContext firstContext = new ShardLeaderElectionContextBase(first,
        "slice1", "collection2", "dummynode1", props, zkController);
    first.setup(firstContext);
    first.joinElection(firstContext, false);

    Thread.sleep(1000);

    String urlScheme = zkStateReader.getClusterProperty(URL_SCHEME, "http");
    String url1 = Utils.getBaseUrlForNodeName("127.0.0.1:80_solr/1", urlScheme) + "/";
    String url2 = Utils.getBaseUrlForNodeName("127.0.0.1:80_solr/2", urlScheme) + "/";

    assertEquals("original leader was not registered", url1, getLeaderUrl("collection2", "slice1"));

    LeaderElector second = new LeaderElector(zkClient);
    props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, nodeName, ZkStateReader.BASE_URL_PROP,
        zkStateReader.getBaseUrlForNodeName(nodeName), ZkStateReader.CORE_NAME_PROP, "2");
    zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
    ElectionContext context = new ShardLeaderElectionContextBase(second,
        "slice1", "collection2", "dummynode2", props, zkController);
    second.setup(context);
    second.joinElection(context, false);
    Thread.sleep(1000);
    assertEquals("original leader should have stayed leader", url1, getLeaderUrl("collection2", "slice1"));
    firstContext.cancelElection();
    Thread.sleep(1000);
    assertEquals("new leader was not registered", url2, getLeaderUrl("collection2", "slice1"));
  }

  private String getLeaderUrl(final String collection, final String slice)
      throws KeeperException, InterruptedException {
    int iterCount = 60;
    while (iterCount-- > 0) {
      try {
        byte[] data = zkClient.getData(
            ZkStateReader.getShardLeadersPath(collection, slice), null, null,
            true);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(
            ZkNodeProps.load(data));
        return leaderProps.getCoreUrl();
      } catch (NoNodeException | SessionExpiredException e) {
        Thread.sleep(500);
      }
    }
    zkClient.printLayoutToStdOut();
    throw new RuntimeException("Could not get leader props for " + collection + " " + slice);
  }

  private static void startAndJoinElection (List<ClientThread> threads) throws InterruptedException {
    for (Thread thread : threads) {
      thread.start();
    }

    while (true) { // wait for election to complete
      int doneCount = 0;
      for (ClientThread thread : threads) {
        if (thread.electionDone) {
          doneCount++;
        }
      }
      if (doneCount == threads.size()) {
        break;
      }
      Thread.sleep(100);
    }
  }

  @Test
  public void testElection() throws Exception {

    List<ClientThread> threads = new ArrayList<>();

    for (int i = 0; i < 15; i++) {
      ClientThread thread = new ClientThread("shard1", i);
      threads.add(thread);
    }
    try {
      startAndJoinElection(threads);

      int leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_0 seq
      assertEquals(0, threads.get(leaderThread).seq);

      // kill n_0, 1, 3 and 4
      ((ClientThread) seqToThread.get(0)).close();

      waitForLeader(threads, 1);

      leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_1 seq

      assertEquals(1, threads.get(leaderThread).seq);

      ((ClientThread) seqToThread.get(4)).close();
      ((ClientThread) seqToThread.get(1)).close();
      ((ClientThread) seqToThread.get(3)).close();

      // whoever the leader is, should be the n_2 seq

      waitForLeader(threads, 2);

      leaderThread = getLeaderThread();
      assertEquals(2, threads.get(leaderThread).seq);

      // kill n_5, 2, 6, 7, and 8
      ((ClientThread) seqToThread.get(5)).close();
      ((ClientThread) seqToThread.get(2)).close();
      ((ClientThread) seqToThread.get(6)).close();
      ((ClientThread) seqToThread.get(7)).close();
      ((ClientThread) seqToThread.get(8)).close();

      waitForLeader(threads, 9);
      leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_9 seq
      assertEquals(9, threads.get(leaderThread).seq);

    } finally {
      // cleanup any threads still running
      for (ClientThread thread : threads) {
        thread.close();
        thread.interrupt();

      }

      for (Thread thread : threads) {
        thread.join();
      }
    }

  }

  @Test
  public void testParallelElection() throws Exception {
    final int numShards = 2 + random().nextInt(18);
    log.info("Testing parallel election across {} shards", numShards);

    List<ClientThread> threads = new ArrayList<>();

    try {
      List<ClientThread> replica1s = new ArrayList<>();
      ElectorSetup es1 = new ElectorSetup(null);
      for (int i = 1; i <= numShards; i++) {
        ClientThread thread = new ClientThread(es1, "parshard" + i, 1, 0 /* don't delay */);
        threads.add(thread);
        replica1s.add(thread);
      }
      startAndJoinElection(replica1s);
      log.info("First replicas brought up and registered");

      // bring up second in line
      List<ClientThread> replica2s = new ArrayList<>();
      ElectorSetup es2 = new ElectorSetup(null);
      for (int i = 1; i <= numShards; i++) {
        ClientThread thread = new ClientThread(es2, "parshard" + i, 2, 40000 / (numShards - 1) /* delay enough to timeout or expire */);
        threads.add(thread);
        replica2s.add(thread);
      }
      startAndJoinElection(replica2s);
      log.info("Second replicas brought up and registered");

      // disconnect the leaders
      es1.close();

      for (int i = 1; i <= numShards; i ++) {
        // if this test fails, getLeaderUrl will more likely throw an exception and fail the test,
        // but add an assertEquals as well for good measure
        String leaderUrl = getLeaderUrl("collection1", "parshard" + i);
        int at = leaderUrl.indexOf("://");
        if (at != -1) {
          leaderUrl = leaderUrl.substring(at + 3);
        }
        assertEquals("2/", leaderUrl);
      }
    } finally {
      // cleanup any threads still running
      for (ClientThread thread : threads) {
        thread.close();
        thread.interrupt();
      }
      for (Thread thread : threads) {
        thread.join();
      }
    }
  }

  private void waitForLeader(List<ClientThread> threads, int seq)
      throws KeeperException, InterruptedException {
    int leaderThread;
    int tries = 0;
    leaderThread = getLeaderThread();
    while (threads.get(leaderThread).seq < seq) {
      leaderThread = getLeaderThread();
      if (tries++ > 50) {
        break;
      }
      Thread.sleep(200);
    }
  }

  private int getLeaderThread() throws KeeperException, InterruptedException {
    String leaderUrl = getLeaderUrl("collection1", "shard1");
    // strip off the scheme
    final int at = leaderUrl.indexOf("://");
    if (at != -1) {
      leaderUrl = leaderUrl.substring(at + 3);
    }
    return Integer.parseInt(leaderUrl.replaceAll("/", ""));
  }

  @Test
  public void testStressElection() throws Exception {
    final ScheduledExecutorService scheduler = Executors
        .newScheduledThreadPool(15, new SolrNamedThreadFactory("stressElection"));
    final List<ClientThread> threads = Collections
        .synchronizedList(new ArrayList<ClientThread>());

    // start with a leader
    ClientThread thread1 = null;
    thread1 = new ClientThread("shard1", 0);
    threads.add(thread1);
    scheduler.schedule(thread1, 0, TimeUnit.MILLISECONDS);



    Thread scheduleThread = new Thread() {
      @Override
      public void run() {
        int count = atLeast(5);
        for (int i = 1; i < count; i++) {
          int launchIn = random().nextInt(500);
          ClientThread thread = null;
          try {
            thread = new ClientThread("shard1", i);
          } catch (Exception e) {
            //
          }
          if (thread != null) {
            threads.add(thread);
            scheduler.schedule(thread, launchIn, TimeUnit.MILLISECONDS);
          }
        }
      }
    };

    Thread killThread = new Thread() {
      @Override
      public void run() {

        while (!stopStress) {
          try {
            int j;
            try {
              // always 1 we won't kill...
              j = random().nextInt(threads.size() - 2);
            } catch(IllegalArgumentException e) {
              continue;
            }
            try {
              threads.get(j).close();
            } catch (Exception e) {
            }

            Thread.sleep(10);
          } catch (Exception e) {
          }
        }
      }
    };

    Thread connLossThread = new Thread() {
      @Override
      public void run() {

        while (!stopStress) {
          try {
            Thread.sleep(50);
            int j;
            j = random().nextInt(threads.size());
            try {
              threads.get(j).es.zkClient.getSolrZooKeeper().closeCnxn();
              if (random().nextBoolean()) {
                long sessionId = zkClient.getSolrZooKeeper().getSessionId();
                server.expire(sessionId);
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
            Thread.sleep(500);

          } catch (Exception e) {

          }
        }
      }
    };

    scheduleThread.start();
    connLossThread.start();
    killThread.start();

    Thread.sleep(4000);

    stopStress = true;

    scheduleThread.interrupt();
    connLossThread.interrupt();
    killThread.interrupt();

    scheduleThread.join();
    scheduler.shutdownNow();

    connLossThread.join();
    killThread.join();

    int seq = threads.get(getLeaderThread()).getSeq();

    // we have a leader we know, TODO: lets check some other things

    // cleanup any threads still running
    for (ClientThread thread : threads) {
      thread.es.zkClient.getSolrZooKeeper().close();
      thread.close();
    }

    for (Thread thread : threads) {
      thread.join();
    }


  }

  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    zkStateReader.close();
    server.shutdown();
    super.tearDown();
  }

  private void printLayout() throws Exception {
    zkClient.printLayoutToStdOut();
  }
}
