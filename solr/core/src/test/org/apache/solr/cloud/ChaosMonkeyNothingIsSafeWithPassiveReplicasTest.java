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
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressObjectReleaseTracker;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@ThreadLeakLingering(linger = 60000)
@SuppressObjectReleaseTracker(bugUrl="Testing purposes")
public class ChaosMonkeyNothingIsSafeWithPassiveReplicasTest extends AbstractFullDistribZkTestBase {
  private static final int FAIL_TOLERANCE = 100;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  private final boolean useAppendReplicas = random().nextBoolean();
  
  private final int numPassiveReplicas;
  private final int numRealtimeOrAppendReplicas;
  
  protected int getPassiveReplicaCount() {
    return numPassiveReplicas;
  }

  @BeforeClass
  public static void beforeSuperClass() {
    schemaString = "schema15.xml";      // we need a string id
    System.setProperty("solr.autoCommit.maxTime", "15000");
    setErrorHook();
  }
  
  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    clearErrorHook();
  }
  
  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};

  private int clientSoTimeout;
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    // can help to hide this when testing and looking at logs
    //ignoreException("shard update error");
    useFactory("solr.StandardDirectoryFactory");
  }
  
  public ChaosMonkeyNothingIsSafeWithPassiveReplicasTest() {
    super();
    numPassiveReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;;
    numRealtimeOrAppendReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;;
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    }

    int numNodes = sliceCount * (numRealtimeOrAppendReplicas + numPassiveReplicas);
    fixShardCount(numNodes);
    log.info("Starting ChaosMonkey test with {} shards and {} nodes", sliceCount, numNodes);

    // None of the operations used here are particularly costly, so this should work.
    // Using this low timeout will also help us catch index stalling.
    clientSoTimeout = 5000;
  }

  @Override
  protected boolean useAppendReplicas() {
    return useAppendReplicas;
  }

  @Test
  public void test() throws Exception {
    cloudClient.setSoTimeout(clientSoTimeout);
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION);
    assertEquals(this.sliceCount, docCollection.getSlices().size());
    Slice s = docCollection.getSlice("shard1");
    assertNotNull(s);
    assertEquals("Unexpected number of replicas. Collection: " + docCollection, numRealtimeOrAppendReplicas + numPassiveReplicas, s.getReplicas().size());
    assertEquals("Unexpected number of passive replicas. Collection: " + docCollection, numPassiveReplicas, s.getReplicas(EnumSet.of(Replica.Type.PASSIVE)).size());
    assertEquals(useAppendReplicas()?0:numRealtimeOrAppendReplicas, s.getReplicas(EnumSet.of(Replica.Type.REALTIME)).size());
    assertEquals(useAppendReplicas()?numRealtimeOrAppendReplicas:0, s.getReplicas(EnumSet.of(Replica.Type.APPEND)).size());
    
    boolean testSuccessful = false;
    try {
      handle.clear();
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
      
      List<StoppableThread> threads = new ArrayList<>();
      List<StoppableIndexingThread> indexTreads = new ArrayList<>();
      int threadCount = TEST_NIGHTLY ? 3 : 1;
      int i = 0;
      for (i = 0; i < threadCount; i++) {
        StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true);
        threads.add(indexThread);
        indexTreads.add(indexThread);
        indexThread.start();
      }
      
      threadCount = 1;
      i = 0;
      for (i = 0; i < threadCount; i++) {
        StoppableSearchThread searchThread = new StoppableSearchThread(cloudClient);
        threads.add(searchThread);
        searchThread.start();
      }
      
      // TODO: we only do this sometimes so that we can sometimes compare against control,
      // it's currently hard to know what requests failed when using ConcurrentSolrUpdateServer
      boolean runFullThrottle = random().nextBoolean();
      if (runFullThrottle) {
        FullThrottleStoppableIndexingThread ftIndexThread = new FullThrottleStoppableIndexingThread(
            clients, "ft1", true);
        threads.add(ftIndexThread);
        ftIndexThread.start();
      }
      
      chaosMonkey.startTheMonkey(true, 10000);
      try {
        long runLength;
        if (RUN_LENGTH != -1) {
          runLength = RUN_LENGTH;
        } else {
          int[] runTimes;
          if (TEST_NIGHTLY) {
            runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
                30000, 45000, 90000, 120000};
          } else {
            runTimes = new int[] {5000, 7000, 15000};
          }
          runLength = runTimes[random().nextInt(runTimes.length - 1)];
        }
        ChaosMonkey.wait(runLength, DEFAULT_COLLECTION, zkStateReader);
      } finally {
        chaosMonkey.stopTheMonkey();
      }

      // ideally this should go into chaosMonkey
      restartZk(1000 * (5 + random().nextInt(4)));

      for (StoppableThread indexThread : threads) {
        indexThread.safeStop();
      }
      
      // start any downed jetties to be sure we still will end up with a leader per shard...
      
      // wait for stop...
      for (StoppableThread indexThread : threads) {
        indexThread.join();
      }
      
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
      
      zkStateReader.updateLiveNodes();
      assertTrue(zkStateReader.getClusterState().getLiveNodes().size() > 0);
      
      
      // we expect full throttle fails, but cloud client should not easily fail
      for (StoppableThread indexThread : threads) {
        if (indexThread instanceof StoppableIndexingThread && !(indexThread instanceof FullThrottleStoppableIndexingThread)) {
          int failCount = ((StoppableIndexingThread) indexThread).getFailCount();
          assertFalse("There were too many update fails (" + failCount + " > " + FAIL_TOLERANCE
              + ") - we expect it can happen, but shouldn't easily", failCount > FAIL_TOLERANCE);
        }
      }
      
      waitForReplicationFromReplicas(DEFAULT_COLLECTION, zkStateReader, new TimeOut(30, TimeUnit.SECONDS));
      
      Set<String> addFails = getAddFails(indexTreads);
      Set<String> deleteFails = getDeleteFails(indexTreads);
      // full throttle thread can
      // have request fails
      checkShardConsistency(!runFullThrottle, true, addFails, deleteFails);      
      
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
        restartZk(1000 * (5 + random().nextInt(4)));
      }

      try (CloudSolrClient client = createCloudClient("collection1")) {
        // We don't really know how many live nodes we have at this point, so "maxShardsPerNode" needs to be > 1
        createCollection(null, "testcollection",
              1, 1, 10, client, null, "conf1"); 
      }
      List<Integer> numShardsNumReplicas = new ArrayList<>(2);
      numShardsNumReplicas.add(1);
      numShardsNumReplicas.add(1 + getPassiveReplicaCount());
      checkForCollection("testcollection", numShardsNumReplicas, null);
      
      testSuccessful = true;
    } finally {
      if (!testSuccessful) {
        logReplicaTypesReplicationInfo(DEFAULT_COLLECTION, cloudClient.getZkStateReader());
        printLayout();
      }
    }
  }

  private void logReplicaTypesReplicationInfo(String collectionName, ZkStateReader zkStateReader) throws KeeperException, InterruptedException, IOException {
    log.info("## Extra Replica.Type information of the cluster");
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    for(Slice s:collection.getSlices()) {
      Replica leader = s.getLeader();
      for (Replica r:s.getReplicas()) {
        if (!zkStateReader.getClusterState().liveNodesContain(r.getNodeName())) {
          log.info("Replica {} not in liveNodes", r.getName());
          continue;
        }
        if (r.equals(leader)) {
          log.info("Replica {} is leader", r.getName());
        }
        logReplicationDetails(r);
      }
    }
  }

  private void waitForReplicationFromReplicas(String collectionName, ZkStateReader zkStateReader, TimeOut timeout) throws KeeperException, InterruptedException, IOException {
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    for(Slice s:collection.getSlices()) {
      Replica leader = s.getLeader();
      long leaderIndexVersion = -1;
      while (leaderIndexVersion == -1 && !timeout.hasTimedOut()) {
        leaderIndexVersion = getIndexVersion(leader);
        if (leaderIndexVersion < 0) {
          Thread.sleep(1000);
        }
      }
      for (Replica passiveReplica:s.getReplicas(EnumSet.of(Replica.Type.PASSIVE,Replica.Type.APPEND))) {
        if (!zkStateReader.getClusterState().liveNodesContain(passiveReplica.getNodeName())) {
          continue;
        }
        while (true) {
          long replicaIndexVersion = getIndexVersion(passiveReplica);
          if (leaderIndexVersion == replicaIndexVersion) {
            log.debug("Leader replica's version ({}) in sync with replica({}): {} == {}", leader.getName(), passiveReplica.getName(), leaderIndexVersion, replicaIndexVersion);
            break;
          } else {
            if (timeout.hasTimedOut()) {
              logReplicaTypesReplicationInfo(collectionName, zkStateReader);
              fail(String.format(Locale.ROOT, "Timed out waiting for replica %s (%d) to replicate from leader %s (%d)", passiveReplica.getName(), replicaIndexVersion, leader.getName(), leaderIndexVersion));
            }
            if (leaderIndexVersion > replicaIndexVersion) {
              log.debug("{} version is {} and leader's is {}, will wait for replication", passiveReplica.getName(), replicaIndexVersion, leaderIndexVersion);
            } else {
              log.debug("Leader replica's version ({}) is lower than passive replica({}): {} < {}", leader.getName(), passiveReplica.getName(), leaderIndexVersion, replicaIndexVersion);
            }
            Thread.sleep(1000);
          }
        }
      }
    }
  }

  private long getIndexVersion(Replica replica) throws IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_SHOW_COMMITS);
      try {
        QueryResponse response = client.query(params);
        @SuppressWarnings("unchecked")
        List<NamedList<Object>> commits = (List<NamedList<Object>>)response.getResponse().get(ReplicationHandler.CMD_SHOW_COMMITS);
        return (Long)commits.get(commits.size() - 1).get("indexVersion");
      } catch (SolrServerException e) {
        log.warn("Exception getting version from {}, will return an invalid version to retry.", replica.getName(), e);
        return -1;
      }
    }
  }
  
  private void logReplicationDetails(Replica replica) throws IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_DETAILS);
      try {
        QueryResponse response = client.query(params);
        log.info("{}: {}", replica.getName(), response.getResponse());
      } catch (SolrServerException e) {
        log.warn("Unable to ger replication details for replica {}", replica.getName(), e);
      }
    }
  }

  private Set<String> getAddFails(List<StoppableIndexingThread> threads) {
    Set<String> addFails = new HashSet<String>();
    for (StoppableIndexingThread thread : threads)   {
      addFails.addAll(thread.getAddFails());
    }
    return addFails;
  }
  
  private Set<String> getDeleteFails(List<StoppableIndexingThread> threads) {
    Set<String> deleteFails = new HashSet<String>();
    for (StoppableIndexingThread thread : threads)   {
      deleteFails.addAll(thread.getDeleteFails());
    }
    return deleteFails;
  }

  class FullThrottleStoppableIndexingThread extends StoppableIndexingThread {
    private CloseableHttpClient httpClient = HttpClientUtil.createClient(null);
    private volatile boolean stop = false;
    int clientIndex = 0;
    private ConcurrentUpdateSolrClient cusc;
    private List<SolrClient> clients;
    private AtomicInteger fails = new AtomicInteger();
    
    public FullThrottleStoppableIndexingThread(List<SolrClient> clients,
                                               String id, boolean doDeletes) {
      super(controlClient, cloudClient, id, doDeletes);
      setName("FullThrottleStopableIndexingThread");
      setDaemon(true);
      this.clients = clients;

      cusc = new ErrorLoggingConcurrentUpdateSolrClient(((HttpSolrClient) clients.get(0)).getBaseURL(), httpClient, 8, 2);
      cusc.setConnectionTimeout(10000);
      cusc.setSoTimeout(clientSoTimeout);
    }
    
    @Override
    public void run() {
      int i = 0;
      int numDeletes = 0;
      int numAdds = 0;

      while (true && !stop) {
        String id = this.id + "-" + i;
        ++i;
        
        if (doDeletes && random().nextBoolean() && deletes.size() > 0) {
          String delete = deletes.remove(0);
          try {
            numDeletes++;
            cusc.deleteById(delete);
          } catch (Exception e) {
            changeUrlOnError(e);
            fails.incrementAndGet();
          }
        }
        
        try {
          numAdds++;
          if (numAdds > (TEST_NIGHTLY ? 4002 : 197))
            continue;
          SolrInputDocument doc = getDoc(
              "id",
              id,
              i1,
              50,
              t1,
              "Saxon heptarchies that used to rip around so in old times and raise Cain.  My, you ought to seen old Henry the Eight when he was in bloom.  He WAS a blossom.  He used to marry a new wife every day, and chop off her head next morning.  And he would do it just as indifferent as if ");
          cusc.add(doc);
        } catch (Exception e) {
          changeUrlOnError(e);
          fails.incrementAndGet();
        }
        
        if (doDeletes && random().nextBoolean()) {
          deletes.add(id);
        }
        
      }

      log.info("FT added docs:" + numAdds + " with " + fails + " fails" + " deletes:" + numDeletes);
    }

    private void changeUrlOnError(Exception e) {
      if (e instanceof ConnectException) {
        clientIndex++;
        if (clientIndex > clients.size() - 1) {
          clientIndex = 0;
        }
        cusc.shutdownNow();
        cusc = new ErrorLoggingConcurrentUpdateSolrClient(((HttpSolrClient) clients.get(clientIndex)).getBaseURL(),
            httpClient, 30, 3);
      }
    }
    
    @Override
    public void safeStop() {
      stop = true;
      cusc.blockUntilFinished();
      cusc.shutdownNow();
      IOUtils.closeQuietly(httpClient);
    }

    @Override
    public int getFailCount() {
      return fails.get();
    }
    
    @Override
    public Set<String> getAddFails() {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public Set<String> getDeleteFails() {
      throw new UnsupportedOperationException();
    }
    
  };
  
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }

  static class ErrorLoggingConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {
    public ErrorLoggingConcurrentUpdateSolrClient(String serverUrl, HttpClient httpClient, int queueSize, int threadCount) {
      super(serverUrl, httpClient, queueSize, threadCount, null, false);
    }
    @Override
    public void handleError(Throwable ex) {
      log.warn("cusc error", ex);
    }
  }
}
