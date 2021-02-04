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
package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.ClusterProp;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;

/**
 * Used to test the traditional (now deprecated) 'full-snapshot' method of backup/restoration.
 *
 * For a similar test harness for incremental backup/restoration see {@link AbstractIncrementalBackupTest}
 */
public abstract class AbstractCloudBackupRestoreTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SHARDS = 2;//granted we sometimes shard split to get more
  protected static final int NUM_SPLIT_SHARDS = 3; //We always split shard1 so total shards post split will be 3
  protected static final String BACKUPNAME_PREFIX = "mytestbackup";

  int replFactor;
  int numTlogReplicas;
  int numPullReplicas;

  private static long docsSeed; // see indexDocs()
  protected String testSuffix = "test1";

  @BeforeClass
  public static void createCluster() throws Exception {
    docsSeed = random().nextLong();
    System.setProperty("solr.allowPaths", "*");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solr.allowPaths");
  }

  /**
   * @return The name of the collection to use.
   */
  public abstract String getCollectionNamePrefix();

  /**
   * @return The name of the backup repository to use.
   */
  public abstract String getBackupRepoName();

  /**
   * @return The absolute path for the backup location.
   *         Could return null.
   */
  public abstract String getBackupLocation();


  public String getCollectionName(){
    return getCollectionNamePrefix() + "_" + testSuffix;
  }

  public void setTestSuffix(String testSuffix) {
    this.testSuffix = testSuffix;
  }

  @Test
  public void test() throws Exception {
    setTestSuffix("testok");
    boolean isImplicit = random().nextBoolean();
    boolean doSplitShardOperation = !isImplicit && random().nextBoolean();
    replFactor = TestUtil.nextInt(random(), 1, 2);
    numTlogReplicas = TestUtil.nextInt(random(), 0, 1);
    numPullReplicas = TestUtil.nextInt(random(), 0, 1);
    int backupReplFactor = replFactor + numPullReplicas + numTlogReplicas;

    CollectionAdminRequest.Create create = isImplicit ?
        // NOTE: use shard list with same # of shards as NUM_SHARDS; we assume this later
        CollectionAdminRequest.createCollectionWithImplicitRouter(getCollectionName(), "conf1", "shard1,shard2", replFactor, numTlogReplicas, numPullReplicas) :
        CollectionAdminRequest.createCollection(getCollectionName(), "conf1", NUM_SHARDS, replFactor, numTlogReplicas, numPullReplicas);

    if (random().nextBoolean()) {
      create.setMaxShardsPerNode(-1);
    } else if (doSplitShardOperation) {
      create.setMaxShardsPerNode((int) Math.ceil(NUM_SPLIT_SHARDS * backupReplFactor / (double) cluster.getJettySolrRunners().size()));
    } else if (NUM_SHARDS * (backupReplFactor) > cluster.getJettySolrRunners().size() || random().nextBoolean()) {
      create.setMaxShardsPerNode((int) Math.ceil(NUM_SHARDS * backupReplFactor / (double) cluster.getJettySolrRunners().size()));//just to assert it survives the restoration
    }

    if (random().nextBoolean()) {
      create.setAutoAddReplicas(true);//just to assert it survives the restoration
    }
    Properties coreProps = new Properties();
    coreProps.put("customKey", "customValue");//just to assert it survives the restoration
    create.setProperties(coreProps);
    if (isImplicit) { //implicit router
      create.setRouterField("shard_s");
    } else {//composite id router
      if (random().nextBoolean()) {
        create.setRouterField("shard_s");
      }
    }

    CloudSolrClient solrClient = cluster.getSolrClient();
    create.process(solrClient);

    indexDocs(getCollectionName(), false);

    if (doSplitShardOperation) {
      // shard split the first shard
      int prevActiveSliceCount = getActiveSliceCount(getCollectionName());
      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(getCollectionName());
      splitShard.setShardName("shard1");
      splitShard.process(solrClient);
      // wait until we see one more active slice...
      for (int i = 0; getActiveSliceCount(getCollectionName()) != prevActiveSliceCount + 1; i++) {
        assertTrue(i < 30);
        Thread.sleep(500);
      }
      // issue a hard commit.  Split shard does a soft commit which isn't good enough for the backup/snapshooter to see
      solrClient.commit(getCollectionName());
    }

    testBackupAndRestore(getCollectionName(), backupReplFactor);
    testConfigBackupOnly("conf1", getCollectionName());
    testInvalidPath(getCollectionName());
  }

  @Test
  public void testRestoreFailure() throws Exception {
    setTestSuffix("testfailure");
    replFactor = TestUtil.nextInt(random(), 1, 2);
    numTlogReplicas = TestUtil.nextInt(random(), 0, 1);
    numPullReplicas = TestUtil.nextInt(random(), 0, 1);

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(getCollectionName(), "conf1", NUM_SHARDS, replFactor, numTlogReplicas, numPullReplicas);

    if (NUM_SHARDS * (replFactor + numTlogReplicas + numPullReplicas) > cluster.getJettySolrRunners().size()) {
      create.setMaxShardsPerNode((int)Math.ceil(NUM_SHARDS * (replFactor + numTlogReplicas + numPullReplicas) / cluster.getJettySolrRunners().size())); //just to assert it survives the restoration
    }

    CloudSolrClient solrClient = cluster.getSolrClient();
    create.process(solrClient);

    indexDocs(getCollectionName(), false);


    String backupLocation = getBackupLocation();
    String backupName = BACKUPNAME_PREFIX + testSuffix;

    DocCollection backupCollection = solrClient.getZkStateReader().getClusterState().getCollection(getCollectionName());

    log.info("Triggering Backup command");

    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(getCollectionName(), backupName)
          .setLocation(backupLocation)
          .setIncremental(false)
          .setRepositoryName(getBackupRepoName());
      assertEquals(0, backup.process(solrClient).getStatus());
    }

    log.info("Triggering Restore command");

    String restoreCollectionName = getCollectionName() + "_restored";

    {
      CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
          .setLocation(backupLocation)
          .setRepositoryName(getBackupRepoName());
      if (backupCollection.getReplicas().size() > cluster.getJettySolrRunners().size()) {
        // may need to increase maxShardsPerNode (e.g. if it was shard split, then now we need more)
        restore.setMaxShardsPerNode((int)Math.ceil(backupCollection.getReplicas().size()/cluster.getJettySolrRunners().size()));
      }

      restore.setConfigName("confFaulty");
      assertEquals(RequestStatusState.FAILED, restore.processAndWait(solrClient, 30));
      assertThat("Failed collection is still in the clusterstate: " + cluster.getSolrClient().getClusterStateProvider().getClusterState().getCollectionOrNull(restoreCollectionName), 
          CollectionAdminRequest.listCollections(solrClient), not(hasItem(restoreCollectionName)));
    }
  }

  /**
   * This test validates the backup of collection configuration using
   *  {@linkplain CollectionAdminParams#NO_INDEX_BACKUP_STRATEGY}.
   *
   * @param configName The config name for the collection to be backed up.
   * @param collectionName The name of the collection to be backed up.
   * @throws Exception in case of errors.
   */
  protected void testConfigBackupOnly(String configName, String collectionName) throws Exception {
    // This is deliberately no-op since we want to run this test only for one of the backup repository
    // implementation (mainly to avoid redundant test execution). Currently HDFS backup repository test
    // implements this.
  }

  // This test verifies the system behavior when the backup location cluster property is configured with an invalid
  // value for the specified repository (and the default backup location is not configured in solr.xml).
  private void testInvalidPath(String collectionName) throws Exception {
    // Execute this test only if the default backup location is NOT configured in solr.xml
    if (getBackupLocation() == null) {
      return;
    }

    String backupName = "invalidbackuprequest";
    CloudSolrClient solrClient = cluster.getSolrClient();

    ClusterProp req = CollectionAdminRequest.setClusterProperty(CoreAdminParams.BACKUP_LOCATION, "/location/does/not/exist");
    assertEquals(0, req.process(solrClient).getStatus());

    // Do not specify the backup location.
    CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
        .setIncremental(false)
        .setRepositoryName(getBackupRepoName());
    try {
      backup.process(solrClient);
      fail("This request should have failed since the cluster property value for backup location property is invalid.");
    } catch (SolrException ex) {
      assertEquals(ErrorCode.SERVER_ERROR.code, ex.code());
    }

    String restoreCollectionName = collectionName + "_invalidrequest";
    CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
        .setRepositoryName(getBackupRepoName());
    try {
      restore.process(solrClient);
      fail("This request should have failed since the cluster property value for backup location property is invalid.");
    } catch (SolrException ex) {
      assertEquals(ErrorCode.SERVER_ERROR.code, ex.code());
    }
  }

  private int getActiveSliceCount(String collectionName) {
    return cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName).getActiveSlices().size();
  }

  private int indexDocs(String collectionName, boolean useUUID) throws Exception {
    Random random = new Random(docsSeed);// use a constant seed for the whole test run so that we can easily re-index.
    int numDocs = random.nextInt(100);
    if (numDocs == 0) {
      log.info("Indexing ZERO test docs");
      return 0;
    }

    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i=0; i<numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", ((useUUID == true) ? java.util.UUID.randomUUID().toString() : i));
      doc.addField("shard_s", "shard" + (1 + random.nextInt(NUM_SHARDS))); // for implicit router
      docs.add(doc);
    }

    CloudSolrClient client = cluster.getSolrClient();
    client.add(collectionName, docs); //batch
    client.commit(collectionName);

    log.info("Indexed {} docs to collection: {}", numDocs, collectionName);

    return numDocs;
  }

  private void testBackupAndRestore(String collectionName, int backupReplFactor) throws Exception {
    String backupLocation = getBackupLocation();
    String backupName = BACKUPNAME_PREFIX + testSuffix;

    CloudSolrClient client = cluster.getSolrClient();
    DocCollection backupCollection = client.getZkStateReader().getClusterState().getCollection(collectionName);

    Map<String, Integer> origShardToDocCount = getShardToDocCountMap(client, backupCollection);
    assert origShardToDocCount.isEmpty() == false;

    log.info("Triggering Backup command");

    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
          .setIncremental(false)
          .setLocation(backupLocation)
          .setRepositoryName(getBackupRepoName());
      if (random().nextBoolean()) {
        assertEquals(0, backup.process(client).getStatus());
      } else {
        assertEquals(RequestStatusState.COMPLETED, backup.processAndWait(client, 30));//async
      }
    }

    log.info("Triggering Restore command");

    String restoreCollectionName = collectionName + "_restored";
    boolean sameConfig = random().nextBoolean();

    int restoreReplcationFactor = replFactor;
    int restoreTlogReplicas = numTlogReplicas;
    int restorePullReplicas = numPullReplicas;
    boolean setExternalReplicationFactor = false;
    if (random().nextBoolean()) { //Override replicationFactor / tLogReplicas / pullReplicas
      setExternalReplicationFactor = true;
      restoreTlogReplicas = TestUtil.nextInt(random(), 0, 1);
      restoreReplcationFactor = TestUtil.nextInt(random(), 1, 2);
      restorePullReplicas = TestUtil.nextInt(random(), 0, 1);
    }
    int numShards = backupCollection.getActiveSlices().size();

    int restoreReplFactor = restoreReplcationFactor + restoreTlogReplicas + restorePullReplicas;

    boolean isMaxShardsPerNodeExternal = false;
    boolean isMaxShardsUnlimited = false;
    CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
        .setLocation(backupLocation).setRepositoryName(getBackupRepoName());

    //explicitly specify the replicationFactor/pullReplicas/nrtReplicas/tlogReplicas.
    if (setExternalReplicationFactor)  {
      restore.setReplicationFactor(restoreReplcationFactor);
      restore.setTlogReplicas(restoreTlogReplicas);
      restore.setPullReplicas(restorePullReplicas);
    }
    int computeRestoreMaxShardsPerNode = (int) Math.ceil((restoreReplFactor * numShards/(double) cluster.getJettySolrRunners().size()));

    if (restoreReplFactor > backupReplFactor) { //else the backup maxShardsPerNode should be enough
      if (log.isInfoEnabled()) {
        log.info("numShards={} restoreReplFactor={} maxShardsPerNode={} totalNodes={}",
            numShards, restoreReplFactor, computeRestoreMaxShardsPerNode, cluster.getJettySolrRunners().size());
      }

      if (random().nextBoolean()) { //set it to -1
        isMaxShardsUnlimited = true;
        restore.setMaxShardsPerNode(-1);
      } else {
        isMaxShardsPerNodeExternal = true;
        restore.setMaxShardsPerNode(computeRestoreMaxShardsPerNode);
      }
    }

    if (rarely()) { // Try with createNodeSet configuration
      //Always 1 as cluster.getJettySolrRunners().size()=NUM_SHARDS=2
      restore.setCreateNodeSet(cluster.getJettySolrRunners().get(0).getNodeName());
      // we need to double maxShardsPerNode value since we reduced number of available nodes by half.
      isMaxShardsPerNodeExternal = true;
      computeRestoreMaxShardsPerNode = origShardToDocCount.size() * restoreReplFactor;
      restore.setMaxShardsPerNode(computeRestoreMaxShardsPerNode);
    }

    final int restoreMaxShardsPerNode = computeRestoreMaxShardsPerNode;

    Properties props = new Properties();
    props.setProperty("customKey", "customVal");
    restore.setProperties(props);

    if (sameConfig==false) {
      restore.setConfigName("customConfigName");
    }
    if (random().nextBoolean()) {
      assertEquals(0, restore.process(client).getStatus());
    } else {
      assertEquals(RequestStatusState.COMPLETED, restore.processAndWait(client, 60));//async
    }
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);

    //Check the number of results are the same
    DocCollection restoreCollection = client.getZkStateReader().getClusterState().getCollection(restoreCollectionName);
    assertEquals(origShardToDocCount, getShardToDocCountMap(client, restoreCollection));
    //Re-index same docs (should be identical docs given same random seed) and test we have the same result.  Helps
    //  test we reconstituted the hash ranges / doc router.
    if (!(restoreCollection.getRouter() instanceof ImplicitDocRouter) && random().nextBoolean()) {
      indexDocs(restoreCollectionName, false);
      assertEquals(origShardToDocCount, getShardToDocCountMap(client, restoreCollection));
    }

    assertEquals(backupCollection.getAutoAddReplicas(), restoreCollection.getAutoAddReplicas());
    assertEquals(sameConfig ? "conf1" : "customConfigName",
        cluster.getSolrClient().getZkStateReader().readConfigName(restoreCollectionName));

    Map<String, Integer> numReplicasByNodeName = new HashMap<>();
    restoreCollection.getReplicas().forEach(x -> {
      numReplicasByNodeName.put(x.getNodeName(), numReplicasByNodeName.getOrDefault(x.getNodeName(), 0) + 1);
    });
    numReplicasByNodeName.forEach((k, v) -> {
      assertTrue("Node " + k + " has " + v + " replicas. Expected num replicas : " + restoreMaxShardsPerNode
              + ". state: \n" + restoreCollection, v <= restoreMaxShardsPerNode);
    });

    assertEquals(restoreCollection.toString(), restoreReplcationFactor, restoreCollection.getReplicationFactor().intValue());
    assertEquals(restoreCollection.toString(), restoreReplcationFactor, restoreCollection.getNumNrtReplicas().intValue());
    assertEquals(restoreCollection.toString(), restorePullReplicas, restoreCollection.getNumPullReplicas().intValue());
    assertEquals(restoreCollection.toString(), restoreTlogReplicas, restoreCollection.getNumTlogReplicas().intValue());
    if (isMaxShardsPerNodeExternal) {
      assertEquals(restoreCollectionName, restoreMaxShardsPerNode, restoreCollection.getMaxShardsPerNode());
    } else if (isMaxShardsUnlimited){
      assertEquals(restoreCollectionName, -1, restoreCollection.getMaxShardsPerNode());
    } else {
      assertEquals(restoreCollectionName, backupCollection.getMaxShardsPerNode(), restoreCollection.getMaxShardsPerNode());
    }

    assertEquals("Restore collection should use stateFormat=2", 2, restoreCollection.getStateFormat());

    //SOLR-12605: Add more docs after restore is complete to see if they are getting added fine
    //explicitly querying the leaders. If we use CloudSolrClient there is no guarantee that we'll hit a nrtReplica
    {
      Map<String, Integer> restoredCollectionPerShardCount =  getShardToDocCountMap(client, restoreCollection);
      long restoredCollectionDocCount = restoredCollectionPerShardCount.values().stream().mapToInt(Number::intValue).sum();
      int numberNewDocsIndexed = indexDocs(restoreCollectionName, true);
      Map<String, Integer> restoredCollectionPerShardCountAfterIndexing = getShardToDocCountMap(client, restoreCollection);
      int restoredCollectionFinalDocCount = restoredCollectionPerShardCountAfterIndexing.values().stream().mapToInt(Number::intValue).sum();

      log.info("Original doc count in restored collection:{} , number of newly added documents to the restored collection: {}"
          + ", after indexing: {}"
          , restoredCollectionDocCount, numberNewDocsIndexed, restoredCollectionFinalDocCount);
      assertEquals((restoredCollectionDocCount + numberNewDocsIndexed), restoredCollectionFinalDocCount);
    }

    // assert added core properties:
    // DWS: did via manual inspection.
    // TODO Find the applicable core.properties on the file system but how?
  }

  public static Map<String, Integer> getShardToDocCountMap(CloudSolrClient client, DocCollection docCollection) throws SolrServerException, IOException {
    Map<String,Integer> shardToDocCount = new TreeMap<>();
    for (Slice slice : docCollection.getActiveSlices()) {
      String shardName = slice.getName();
      try (HttpSolrClient leaderClient = new HttpSolrClient.Builder(slice.getLeader().getCoreUrl()).withHttpClient(client.getHttpClient()).build()) {
        long docsInShard = leaderClient.query(new SolrQuery("*:*").setParam("distrib", "false"))
            .getResults().getNumFound();
        shardToDocCount.put(shardName, (int) docsInShard);
      }
    }
    return shardToDocCount;
  }
}
