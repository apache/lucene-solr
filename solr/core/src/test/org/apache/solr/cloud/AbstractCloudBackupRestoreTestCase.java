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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;

import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.ClusterProp;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.ShardParams._ROUTE_;

/**
 * This class implements the logic required to test Solr cloud backup/restore capability.
 */
public abstract class AbstractCloudBackupRestoreTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SHARDS = 2;//granted we sometimes shard split to get more

  private static long docsSeed; // see indexDocs()

  @BeforeClass
  public static void createCluster() throws Exception {
    docsSeed = random().nextLong();
  }

  /**
   * @return The name of the collection to use.
   */
  public abstract String getCollectionName();

  /**
   * @return The name of the backup repository to use.
   */
  public abstract String getBackupRepoName();

  /**
   * @return The absolute path for the backup location.
   *         Could return null.
   */
  public abstract String getBackupLocation();

  @Test
  public void test() throws Exception {
    boolean isImplicit = random().nextBoolean();
    int replFactor = TestUtil.nextInt(random(), 1, 2);
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(getCollectionName(), "conf1", NUM_SHARDS, replFactor);
    if (NUM_SHARDS * replFactor > cluster.getJettySolrRunners().size() || random().nextBoolean()) {
      create.setMaxShardsPerNode(NUM_SHARDS);//just to assert it survives the restoration
    }
    if (random().nextBoolean()) {
      create.setAutoAddReplicas(true);//just to assert it survives the restoration
    }
    Properties coreProps = new Properties();
    coreProps.put("customKey", "customValue");//just to assert it survives the restoration
    create.setProperties(coreProps);
    if (isImplicit) { //implicit router
      create.setRouterName(ImplicitDocRouter.NAME);
      create.setNumShards(null);//erase it. TODO suggest a new createCollectionWithImplicitRouter method
      create.setShards("shard1,shard2"); // however still same number as NUM_SHARDS; we assume this later
      create.setRouterField("shard_s");
    } else {//composite id router
      if (random().nextBoolean()) {
        create.setRouterField("shard_s");
      }
    }

    CloudSolrClient solrClient = cluster.getSolrClient();
    create.process(solrClient);

    indexDocs(getCollectionName());

    if (!isImplicit && random().nextBoolean()) {
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

    testBackupAndRestore(getCollectionName());
    testConfigBackupOnly("conf1", getCollectionName());
    testInvalidPath(getCollectionName());
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

  private void indexDocs(String collectionName) throws Exception {
    Random random = new Random(docsSeed);// use a constant seed for the whole test run so that we can easily re-index.
    int numDocs = random.nextInt(100);
    if (numDocs == 0) {
      log.info("Indexing ZERO test docs");
      return;
    }
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i=0; i<numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("shard_s", "shard" + (1 + random.nextInt(NUM_SHARDS))); // for implicit router
      docs.add(doc);
    }
    CloudSolrClient client = cluster.getSolrClient();
    client.add(collectionName, docs);// batch
    client.commit(collectionName);
  }

  private void testBackupAndRestore(String collectionName) throws Exception {
    String backupLocation = getBackupLocation();
    String backupName = "mytestbackup";

    CloudSolrClient client = cluster.getSolrClient();
    DocCollection backupCollection = client.getZkStateReader().getClusterState().getCollection(collectionName);

    Map<String, Integer> origShardToDocCount = getShardToDocCountMap(client, backupCollection);
    assert origShardToDocCount.isEmpty() == false;

    log.info("Triggering Backup command");

    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
          .setLocation(backupLocation).setRepositoryName(getBackupRepoName());
      if (random().nextBoolean()) {
        assertEquals(0, backup.process(client).getStatus());
      } else {
        assertEquals(RequestStatusState.COMPLETED, backup.processAndWait(client, 30));//async
      }
    }

    log.info("Triggering Restore command");

    String restoreCollectionName = collectionName + "_restored";
    boolean sameConfig = random().nextBoolean();

    {
      CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
          .setLocation(backupLocation).setRepositoryName(getBackupRepoName());

      if (origShardToDocCount.size() > cluster.getJettySolrRunners().size()) {
        // may need to increase maxShardsPerNode (e.g. if it was shard split, then now we need more)
        restore.setMaxShardsPerNode(origShardToDocCount.size());
      }
      Properties props = new Properties();
      props.setProperty("customKey", "customVal");
      restore.setProperties(props);
      if (sameConfig==false) {
        restore.setConfigName("customConfigName");
      }
      if (random().nextBoolean()) {
        assertEquals(0, restore.process(client).getStatus());
      } else {
        assertEquals(RequestStatusState.COMPLETED, restore.processAndWait(client, 30));//async
      }
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(
          restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);
    }

    //Check the number of results are the same
    DocCollection restoreCollection = client.getZkStateReader().getClusterState().getCollection(restoreCollectionName);
    assertEquals(origShardToDocCount, getShardToDocCountMap(client, restoreCollection));
    //Re-index same docs (should be identical docs given same random seed) and test we have the same result.  Helps
    //  test we reconstituted the hash ranges / doc router.
    if (!(restoreCollection.getRouter() instanceof ImplicitDocRouter) && random().nextBoolean()) {
      indexDocs(restoreCollectionName);
      assertEquals(origShardToDocCount, getShardToDocCountMap(client, restoreCollection));
    }

    assertEquals(backupCollection.getReplicationFactor(), restoreCollection.getReplicationFactor());
    assertEquals(backupCollection.getAutoAddReplicas(), restoreCollection.getAutoAddReplicas());
    assertEquals(backupCollection.getActiveSlices().iterator().next().getReplicas().size(),
        restoreCollection.getActiveSlices().iterator().next().getReplicas().size());
    assertEquals(sameConfig ? "conf1" : "customConfigName",
        cluster.getSolrClient().getZkStateReader().readConfigName(restoreCollectionName));

    // assert added core properties:
    // DWS: did via manual inspection.
    // TODO Find the applicable core.properties on the file system but how?
  }

  private Map<String, Integer> getShardToDocCountMap(CloudSolrClient client, DocCollection docCollection) throws SolrServerException, IOException {
    Map<String,Integer> shardToDocCount = new TreeMap<>();
    for (Slice slice : docCollection.getActiveSlices()) {
      String shardName = slice.getName();
      long docsInShard = client.query(docCollection.getName(), new SolrQuery("*:*").setParam(_ROUTE_, shardName))
          .getResults().getNumFound();
      shardToDocCount.put(shardName, (int) docsInShard);
    }
    return shardToDocCount;
  }
}
