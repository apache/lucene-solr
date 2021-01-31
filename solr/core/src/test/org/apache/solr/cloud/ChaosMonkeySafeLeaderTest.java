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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slow
@LuceneTestCase.Nightly // MRM TODO: finish compare against control, look at setErrorHook
public class ChaosMonkeySafeLeaderTest extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));
  private ClusterChaosMonkey chaosMonkey;

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    //setErrorHook();
  }
  
  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    //clearErrorHook();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    chaosMonkey = new ClusterChaosMonkey(cluster, DEFAULT_COLLECTION);
    //setErrorHook();
  }

  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  

  public ChaosMonkeySafeLeaderTest() throws Exception {
    super();
   // schemaString = "schema15.xml";      // we need a string id
    System.setProperty("solr.autoCommit.maxTime", "15000");
    System.setProperty("solr.httpclient.retries", "1");
    System.setProperty("solr.retries.on.forward", "1");
    System.setProperty("solr.retries.to.followers", "1");
    useFactory(null);
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");

    createControl = true;

    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 5 : 3) + 1;
    }

    replicationFactor = 3;

//    int numShards = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "-1"));
//    if (numShards == -1) {
//      // we make sure that there's at least one shard with more than one replica
//      // so that the ChaosMonkey has something to kill
//      numShards = sliceCount + random().nextInt(TEST_NIGHTLY ? 12 : 2) + 1;
//    }
    this.numJettys = sliceCount * replicationFactor;
  }

  @Test
  public void test() throws Exception {
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 1 seconds 'soft' commit
    //randomlyEnableAutoSoftCommit();

    tryDelete();
    
    List<StoppableIndexingThread> threads = new ArrayList<>();
    int threadCount = 2;
    int batchSize = 1;
    if (random().nextBoolean()) {
      batchSize = random().nextInt(98) + 2;
    }
    
    boolean pauseBetweenUpdates = TEST_NIGHTLY ? random().nextBoolean() : true;
    int maxUpdates = -1;
    if (!pauseBetweenUpdates) {
      maxUpdates = 1000 + random().nextInt(1000);
    } else {
      maxUpdates = 1500;
    }
    
    for (int i = 0; i < threadCount; i++) {
      StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, maxUpdates, batchSize, pauseBetweenUpdates); // random().nextInt(999) + 1
      indexThread.setUseLongId(true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    chaosMonkey.startTheMonkey(false, 500);
    try {
      long runLength;
      if (RUN_LENGTH != -1) {
        runLength = RUN_LENGTH;
      } else {
        int[] runTimes;
        if (TEST_NIGHTLY) {
          runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
              30000, 45000, 90000};
        } else {
          runTimes = new int[] {15000};
        }
        runLength = runTimes[random().nextInt(runTimes.length)];
      }
      
      Thread.sleep(runLength);
    } finally {
      chaosMonkey.stopTheMonkey();
    }
    
    for (StoppableIndexingThread indexThread : threads) {
      indexThread.safeStop();
    }
    
    // wait for stop...
    for (StoppableIndexingThread indexThread : threads) {
      indexThread.join();
    }
    
    for (StoppableIndexingThread indexThread : threads) {
      assertTrue( indexThread.getFailCount() < 10);
    }

    commit();

    checkShardConsistency(batchSize == 1, true);
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    
    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
//    if (TEST_NIGHTLY && random().nextBoolean()) {
//      zkServer.shutdown();
//      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
//      zkServer.run(false);
//    }

//    try (CloudHttp2SolrClient client = createCloudClient("collection1")) {
//        createCollection(null, "testcollection", 1, 1, 1, client, null, "_default");
//
//    }
    List<Integer> numShardsNumReplicas = new ArrayList<>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1);
 //   checkForCollection("testcollection",numShardsNumReplicas, null);
  }

  private void tryDelete() throws Exception {
    long start = System.nanoTime();
    long timeout = start + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      try {
        del("*:*");
        break;
      } catch (SolrServerException e) {
        // cluster may not be up yet
        log.error("", e);
      }
      Thread.sleep(100);
    }
  }
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

  /* Checks both shard replcia consistency and against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency() throws Exception {
    checkShardConsistency(true, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose)
      throws Exception {
    checkShardConsistency(checkVsControl, verbose, null, null);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose, Set<String> addFails, Set<String> deleteFails)
      throws Exception {

    Set<String> theShards = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlicesMap().keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      fail(failMessage);
    }

    if (!checkVsControl) return;

    SolrParams q = params("q","*:*","rows","0", "tests","checkShardConsistency(vsControl)");    // add a tag to aid in debugging via logs

    SolrDocumentList controlDocList = controlClient.query(q).getResults();
    long controlDocs = controlDocList.getNumFound();

    SolrDocumentList cloudDocList = cloudClient.query(q).getResults();
    long cloudClientDocs = cloudDocList.getNumFound();


    // now check that the right # are on each shard
//    theShards = shardToJetty.keySet();
//    int cnt = 0;
//    for (String s : theShards) {
//      int times = shardToJetty.get(s).size();
//      for (int i = 0; i < times; i++) {
//        try {
//          CloudJettyRunner cjetty = shardToJetty.get(s).get(i);
//          ZkNodeProps props = cjetty.info;
//          SolrClient client = cjetty.client.solrClient;
//          boolean active = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP)) == Replica.State.ACTIVE;
//          if (active) {
//            SolrQuery query = new SolrQuery("*:*");
//            query.set("distrib", false);
//            long results = client.query(query).getResults().getNumFound();
//            if (verbose) System.err.println(props + " : " + results);
//            if (verbose) System.err.println("shard:"
//                + props.getStr(ZkStateReader.SHARD_ID_PROP));
//            cnt += results;
//            break;
//          }
//        } catch (Exception e) {
//          ParWork.propagateInterrupt(e);
//          // if we have a problem, try the next one
//          if (i == times - 1) {
//            throw e;
//          }
//        }
//      }
//    }

//controlDocs != cnt ||
    int cnt = -1;
    if (cloudClientDocs != controlDocs) {
      String msg = "document count mismatch.  control=" + controlDocs + " sum(shards)="+ cnt + " cloudClient="+cloudClientDocs;
      log.error(msg);

      boolean shouldFail = CloudInspectUtil.compareResults(controlClient, cloudClient, addFails, deleteFails);
      if (shouldFail) {
        fail(msg);
      }
    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is
   * consistent.
   */
  protected String checkShardConsistency(String shard, boolean expectFailure, boolean verbose)
      throws Exception {

    List<JettySolrRunner> solrJetties = cluster.getJettysForShard(DEFAULT_COLLECTION, shard);
    if (solrJetties == null) {
      throw new RuntimeException("shard not found:" + shard);
    }
    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("check const of " + shard);
    int cnt = 0;
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();

    DocCollection coll = zkStateReader.getClusterState().getCollection(DEFAULT_COLLECTION);
    
    assertEquals(
        "The client count does not match up with the shard count for slice:"
            + shard,
        coll.getSlice(shard)
            .getReplicasMap().size(), solrJetties.size());

    Slice replicas = coll.getSlice(shard);
    
    Replica lastReplica = null;
    for (Replica replica : replicas) {

      if (verbose) System.err.println("client" + cnt++);
      if (verbose) System.err.println("Replica:" + replica);
      try (SolrClient client = getClient(replica.getCoreUrl())) {
      try {
        SolrParams query = params("q","*:*", "rows","0", "distrib","false", "tests","checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
        num = client.query(query).getResults().getNumFound();
      } catch (SolrException | SolrServerException e) {
        if (verbose) System.err.println("error contacting client: "
            + e.getMessage() + "\n");
        continue;
      }

      boolean live = false;
      String nodeName = replica.getNodeName();
      if (zkStateReader.isNodeLive(nodeName)) {
        live = true;
      }
      if (verbose) System.err.println(" live:" + live);
      if (verbose) System.err.println(" num:" + num + "\n");

      boolean active = replica.getState() == Replica.State.ACTIVE;
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastReplica.getCoreUrl() + " (previous client)" + " and got " + num + " from " + replica.getCoreUrl();

          if (!expectFailure || verbose) {
            System.err.println("######" + failMessage);
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            query.set("fl", "id,_version_");
            query.set("rows", "100000");
            query.set("sort", "id asc");
            query.set("tests", "checkShardConsistency/showDiff");

            try (SolrClient lastClient = getClient(lastReplica.getCoreUrl())) {
              SolrDocumentList lst1 = lastClient.query(query).getResults();
              SolrDocumentList lst2 = client.query(query).getResults();

              CloudInspectUtil.showDiff(lst1, lst2, lastReplica.getCoreUrl(), replica.getCoreUrl());
            }
          }

        }
        lastNum = num;
        lastReplica = replica;
      }
      }
    }
    return failMessage;

  }

}
