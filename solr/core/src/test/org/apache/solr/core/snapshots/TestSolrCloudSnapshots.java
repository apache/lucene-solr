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
package org.apache.solr.core.snapshots;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.ListSnapshots;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData.CoreSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.handler.BackupRestoreUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests snapshot functionality in a SolrCloud cluster.
 *
 * This test uses the (now deprecated) traditional backup method/format.  For more thorough tests using the new format,
 * see {@link org.apache.solr.handler.TestIncrementalCoreBackup}
 */
@LuceneTestCase.SuppressCodecs({"SimpleText"}) // Backups do checksum validation against a footer value not present in 'SimpleText'
@SolrTestCaseJ4.SuppressSSL // Currently unknown why SSL does not work with this test
@Slow
public class TestSolrCloudSnapshots extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static long docsSeed; // see indexDocs()
  private static final int NUM_SHARDS = 2;
  private static final int NUM_REPLICAS = 2;
  private static final int NUM_NODES = NUM_REPLICAS * NUM_SHARDS;

  @BeforeClass
  public static void setupClass() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    System.setProperty("solr.allowPaths", "*");
    configureCluster(NUM_NODES)// nodes
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    docsSeed = random().nextLong();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    System.clearProperty("solr.allowPaths");
  }

  @Test
  public void testSnapshots() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "SolrCloudSnapshots";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", NUM_SHARDS, NUM_REPLICAS);
    create.process(solrClient);
    cluster.waitForActiveCollection(collectionName, NUM_SHARDS, NUM_SHARDS * NUM_REPLICAS);

    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);
    BackupRestoreUtils.verifyDocs(nDocs, solrClient, collectionName);

    // Set a collection property
    final boolean collectionPropertySet = usually();
    if (collectionPropertySet) {
      CollectionAdminRequest.CollectionProp setProperty = CollectionAdminRequest.setCollectionProperty(collectionName, "test.property", "test.value");
      setProperty.process(solrClient);
    }

    String commitName = TestUtil.randomSimpleString(random(), 1, 5);

    // Verify if snapshot creation works with replica failures.
    boolean replicaFailures = usually();
    Optional<String> stoppedCoreName = Optional.empty();
    if (replicaFailures) {
      // Here the assumption is that Solr will spread the replicas uniformly across nodes.
      // If this is not true for some reason, then we will need to add some logic to find a
      // node with a single replica.
      cluster.getRandomJetty(random()).stop();

      // Sleep a bit for allowing ZK watch to fire.
      Thread.sleep(5000);

      // Figure out if at-least one replica is "down".
      DocCollection collState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
      for (Slice s : collState.getSlices()) {
        for (Replica replica : s.getReplicas()) {
          if (replica.getState() == State.DOWN) {
            stoppedCoreName = Optional.of(replica.getCoreName());
          }
        }
      }
    }

    int expectedCoresWithSnapshot = stoppedCoreName.isPresent() ? (NUM_SHARDS * NUM_REPLICAS) - 1 : (NUM_SHARDS * NUM_REPLICAS);

    CollectionAdminRequest.CreateSnapshot createSnap = new CollectionAdminRequest.CreateSnapshot(collectionName, commitName);
    createSnap.process(solrClient);

    Collection<CollectionSnapshotMetaData> collectionSnaps = listCollectionSnapshots(solrClient, collectionName);
    assertEquals(1, collectionSnaps.size());
    CollectionSnapshotMetaData meta = collectionSnaps.iterator().next();
    assertEquals(commitName, meta.getName());
    assertEquals(CollectionSnapshotMetaData.SnapshotStatus.Successful, meta.getStatus());
    assertEquals(expectedCoresWithSnapshot, meta.getReplicaSnapshots().size());
    Map<String, CoreSnapshotMetaData> snapshotByCoreName = meta.getReplicaSnapshots().stream()
        .collect(Collectors.toMap(CoreSnapshotMetaData::getCoreName, Function.identity()));

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(2, collectionState.getActiveSlices().size());
    for ( Slice shard : collectionState.getActiveSlices() ) {
      assertEquals(2, shard.getReplicas().size());
      for (Replica replica : shard.getReplicas()) {
        if (stoppedCoreName.isPresent() && stoppedCoreName.get().equals(replica.getCoreName())) {
          continue; // We know that the snapshot is not created for this replica.
        }

        String replicaBaseUrl = replica.getBaseUrl();
        String coreName = replica.getCoreName();

        assertTrue(snapshotByCoreName.containsKey(coreName));
        CoreSnapshotMetaData coreSnapshot = snapshotByCoreName.get(coreName);

        try (SolrClient adminClient = getHttpSolrClient(replicaBaseUrl)) {
          Collection<SnapshotMetaData> snapshots = listCoreSnapshots(adminClient, coreName);
          Optional<SnapshotMetaData> metaData = snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst();
          assertTrue("Snapshot not created for core " + coreName, metaData.isPresent());
          assertEquals(coreSnapshot.getIndexDirPath(), metaData.get().getIndexDirPath());
          assertEquals(coreSnapshot.getGenerationNumber(), metaData.get().getGenerationNumber());
        }
      }
    }

    // Delete all documents.
    {
      solrClient.deleteByQuery(collectionName, "*:*");
      solrClient.commit(collectionName);
      BackupRestoreUtils.verifyDocs(0, solrClient, collectionName);
    }

    String backupLocation = createTempDir().toFile().getAbsolutePath();
    String backupName = "mytestbackup";
    String restoreCollectionName = collectionName + "_restored";

    //Create a backup using the earlier created snapshot.
    {
      CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
          .setLocation(backupLocation).setCommitName(commitName).setIncremental(false);
      if (random().nextBoolean()) {
        assertEquals(0, backup.process(solrClient).getStatus());
      } else {
        assertEquals(RequestStatusState.COMPLETED, backup.processAndWait(solrClient, 30));//async
      }
    }

    // Restore backup.
    {
      CollectionAdminRequest.Restore restore = CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
          .setLocation(backupLocation);
      if (replicaFailures) {
        // In this case one of the Solr servers would be down. Hence we need to increase
        // max_shards_per_node property for restore command to succeed.
        restore.setMaxShardsPerNode(2);
      }
      if (random().nextBoolean()) {
        assertEquals(0, restore.process(solrClient).getStatus());
      } else {
        assertEquals(RequestStatusState.COMPLETED, restore.processAndWait(solrClient, 30));//async
      }
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(
          restoreCollectionName, cluster.getSolrClient().getZkStateReader(), log.isDebugEnabled(), true, 30);
      BackupRestoreUtils.verifyDocs(nDocs, solrClient, restoreCollectionName);
    }

    // Check collection property
    Map<String, String> collectionProperties = solrClient.getZkStateReader().getCollectionProperties(restoreCollectionName);
    if (collectionPropertySet) {
      assertEquals("Snapshot restore hasn't restored collection properties", "test.value", collectionProperties.get("test.property"));
    } else {
      assertNull("Collection property shouldn't be present", collectionProperties.get("test.property"));
    }

    // Verify if the snapshot deletion works correctly when one or more replicas containing the snapshot are
    // deleted
    boolean replicaDeletion = rarely();
    if (replicaDeletion) {
      CoreSnapshotMetaData replicaToDelete = null;
      for (String shardId : meta.getShards()) {
        List<CoreSnapshotMetaData> replicas = meta.getReplicaSnapshotsForShard(shardId);
        if (replicas.size() > 1) {
          int r_index = random().nextInt(replicas.size());
          replicaToDelete = replicas.get(r_index);
        }
      }

      if (replicaToDelete != null) {
        collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
        for (Slice s : collectionState.getSlices()) {
          for (Replica r : s.getReplicas()) {
            if (r.getCoreName().equals(replicaToDelete.getCoreName())) {
              log.info("Deleting replica {}", r);
              CollectionAdminRequest.DeleteReplica delReplica = CollectionAdminRequest.deleteReplica(collectionName,
                  replicaToDelete.getShardId(), r.getName());
              delReplica.process(solrClient);
              // The replica deletion will cleanup the snapshot meta-data.
              snapshotByCoreName.remove(r.getCoreName());
              break;
            }
          }
        }
      }
    }

    // Delete snapshot
    CollectionAdminRequest.DeleteSnapshot deleteSnap = new CollectionAdminRequest.DeleteSnapshot(collectionName, commitName);
    deleteSnap.process(solrClient);

    // Wait for a while so that the clusterstate.json updates are propagated to the client side.
    Thread.sleep(2000);
    collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);

    for ( Slice shard : collectionState.getActiveSlices() ) {
      for (Replica replica : shard.getReplicas()) {
        if (stoppedCoreName.isPresent() && stoppedCoreName.get().equals(replica.getCoreName())) {
          continue; // We know that the snapshot was not created for this replica.
        }

        String replicaBaseUrl = replica.getBaseUrl();
        String coreName = replica.getCoreName();

        try (SolrClient adminClient = getHttpSolrClient(replicaBaseUrl)) {
          Collection<SnapshotMetaData> snapshots = listCoreSnapshots(adminClient, coreName);
          Optional<SnapshotMetaData> metaData = snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst();
          assertFalse("Snapshot not deleted for core " + coreName, metaData.isPresent());
          // Remove the entry for core if the snapshot is deleted successfully.
          snapshotByCoreName.remove(coreName);
        }
      }
    }

    // Verify all core-level snapshots are deleted.
    assertTrue("The cores remaining " + snapshotByCoreName, snapshotByCoreName.isEmpty());
    assertTrue(listCollectionSnapshots(solrClient, collectionName).isEmpty());

    // Verify if the collection deletion result in proper cleanup of snapshot metadata.
    {
      String commitName_2 = commitName + "_2";

      CollectionAdminRequest.CreateSnapshot createSnap_2 = new CollectionAdminRequest.CreateSnapshot(collectionName, commitName_2);
      assertEquals(0, createSnap_2.process(solrClient).getStatus());

      Collection<CollectionSnapshotMetaData> collectionSnaps_2 = listCollectionSnapshots(solrClient, collectionName);
      assertEquals(1, collectionSnaps.size());
      assertEquals(commitName_2, collectionSnaps_2.iterator().next().getName());

      // Delete collection
      CollectionAdminRequest.Delete deleteCol = CollectionAdminRequest.deleteCollection(collectionName);
      assertEquals(0, deleteCol.process(solrClient).getStatus());
      assertTrue(SolrSnapshotManager.listSnapshots(solrClient.getZkStateReader().getZkClient(), collectionName).isEmpty());
    }

  }

  @SuppressWarnings({"unchecked"})
  private Collection<CollectionSnapshotMetaData> listCollectionSnapshots(SolrClient adminClient, String collectionName) throws Exception {
    CollectionAdminRequest.ListSnapshots listSnapshots = new CollectionAdminRequest.ListSnapshots(collectionName);
    CollectionAdminResponse resp = listSnapshots.process(adminClient);

    assertTrue( resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO) instanceof NamedList );
    @SuppressWarnings({"rawtypes"})
    NamedList apiResult = (NamedList) resp.getResponse().get(SolrSnapshotManager.SNAPSHOTS_INFO);

    Collection<CollectionSnapshotMetaData> result = new ArrayList<>();
    for (int i = 0; i < apiResult.size(); i++) {
      result.add(new CollectionSnapshotMetaData((NamedList<Object>)apiResult.getVal(i)));
    }

    return result;
  }

  private Collection<SnapshotMetaData> listCoreSnapshots(SolrClient adminClient, String coreName) throws Exception {
    ListSnapshots req = new ListSnapshots();
    req.setCoreName(coreName);
    @SuppressWarnings({"rawtypes"})
    NamedList resp = adminClient.request(req);
    assertTrue( resp.get(SolrSnapshotManager.SNAPSHOTS_INFO) instanceof NamedList );
    @SuppressWarnings({"rawtypes"})
    NamedList apiResult = (NamedList) resp.get(SolrSnapshotManager.SNAPSHOTS_INFO);

    List<SnapshotMetaData> result = new ArrayList<>(apiResult.size());
    for(int i = 0 ; i < apiResult.size(); i++) {
      String commitName = apiResult.getName(i);
      String indexDirPath = (String)((NamedList)apiResult.get(commitName)).get(SolrSnapshotManager.INDEX_DIR_PATH);
      long genNumber = Long.parseLong((String)((NamedList)apiResult.get(commitName)).get(SolrSnapshotManager.GENERATION_NUM));
      result.add(new SnapshotMetaData(commitName, indexDirPath, genNumber));
    }
    return result;
  }
}
