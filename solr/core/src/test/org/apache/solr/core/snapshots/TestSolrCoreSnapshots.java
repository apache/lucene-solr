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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.CreateSnapshot;
import org.apache.solr.client.solrj.request.CoreAdminRequest.DeleteSnapshot;
import org.apache.solr.client.solrj.request.CoreAdminRequest.ListSnapshots;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager.SnapshotMetaData;
import org.apache.solr.handler.BackupRestoreUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for index backing up and restoring index snapshots.
 *
 * These tests use the deprecated "full-snapshot" based backup method.  For tests that cover similar snapshot
 * functionality incrementally, see {@link org.apache.solr.handler.TestIncrementalCoreBackup}
 */
@LuceneTestCase.SuppressCodecs({"SimpleText"}) // Backups do checksum validation against a footer value not present in 'SimpleText'
@SolrTestCaseJ4.SuppressSSL // Currently unknown why SSL does not work with this test
@Slow
public class TestSolrCoreSnapshots extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static long docsSeed; // see indexDocs()

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.allowPaths", "*");
    useFactory("solr.StandardDirectoryFactory");
    configureCluster(1)// nodes
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
  public void testBackupRestore() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "SolrCoreSnapshots";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.process(solrClient);

    String location = createTempDir().toFile().getAbsolutePath();
    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(1, collectionState.getActiveSlices().size());
    Slice shard = collectionState.getActiveSlices().iterator().next();
    assertEquals(1, shard.getReplicas().size());
    Replica replica = shard.getReplicas().iterator().next();

    String replicaBaseUrl = replica.getBaseUrl();
    String coreName = replica.getCoreName();
    String backupName = TestUtil.randomSimpleString(random(), 1, 5);
    String commitName = TestUtil.randomSimpleString(random(), 1, 5);
    String duplicateName = commitName.concat("_duplicate");

    try (
        SolrClient adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
        SolrClient leaderClient = getHttpSolrClient(replica.getCoreUrl())) {

      SnapshotMetaData metaData = createSnapshot(adminClient, coreName, commitName);
      // Create another snapshot referring to the same index commit to verify the
      // reference counting implementation during snapshot deletion.
      SnapshotMetaData duplicateCommit = createSnapshot(adminClient, coreName, duplicateName);

      assertEquals (metaData.getIndexDirPath(), duplicateCommit.getIndexDirPath());
      assertEquals (metaData.getGenerationNumber(), duplicateCommit.getGenerationNumber());

      // Delete all documents
      leaderClient.deleteByQuery("*:*");
      leaderClient.commit();
      BackupRestoreUtils.verifyDocs(0, cluster.getSolrClient(), collectionName);

      // Verify that the index directory contains at least 2 index commits - one referred by the snapshots
      // and the other containing document deletions.
      {
        List<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
        assertTrue(commits.size() >= 2);
      }

      // Backup the earlier created snapshot.
      {
        Map<String,String> params = new HashMap<>();
        params.put("name", backupName);
        params.put("commitName", commitName);
        params.put("location", location);
        params.put("incremental", "false");
        BackupRestoreUtils.runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.BACKUPCORE.toString(), params);
      }

      // Restore the backup
      {
        Map<String,String> params = new HashMap<>();
        params.put("name", "snapshot." + backupName);
        params.put("location", location);
        BackupRestoreUtils.runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.RESTORECORE.toString(), params);
        BackupRestoreUtils.verifyDocs(nDocs, cluster.getSolrClient(), collectionName);
      }

      // Verify that the old index directory (before restore) contains only those index commits referred by snapshots.
      // The IndexWriter (used to cleanup index files) creates an additional commit during closing. Hence we expect 2 commits (instead
      // of 1).
      {
        List<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
        assertEquals(2, commits.size());
        assertEquals(metaData.getGenerationNumber(), commits.get(0).getGeneration());
      }

      // Delete first snapshot
      deleteSnapshot(adminClient, coreName, commitName);

      // Verify that corresponding index files have NOT been deleted (due to reference counting).
      assertFalse(listCommits(metaData.getIndexDirPath()).isEmpty());

      // Delete second snapshot
      deleteSnapshot(adminClient, coreName, duplicateCommit.getName());

      // Verify that corresponding index files have been deleted. Ideally this directory should
      // be removed immediately. But the current DirectoryFactory impl waits until the
      // closing the core (or the directoryFactory) for actual removal. Since the IndexWriter
      // (used to cleanup index files) creates an additional commit during closing, we expect a single
      // commit (instead of 0).
      assertEquals(1, listCommits(duplicateCommit.getIndexDirPath()).size());
    }
  }

  @Test
  public void testIndexOptimization() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "SolrCoreSnapshots_IndexOptimization";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.process(solrClient);

    int nDocs = BackupRestoreUtils.indexDocs(cluster.getSolrClient(), collectionName, docsSeed);

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(1, collectionState.getActiveSlices().size());
    Slice shard = collectionState.getActiveSlices().iterator().next();
    assertEquals(1, shard.getReplicas().size());
    Replica replica = shard.getReplicas().iterator().next();

    String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String commitName = TestUtil.randomSimpleString(random(), 1, 5);

    try (
        SolrClient adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
        SolrClient leaderClient = getHttpSolrClient(replica.getCoreUrl())) {

      SnapshotMetaData metaData = createSnapshot(adminClient, coreName, commitName);

      int numTests = nDocs > 0 ? TestUtil.nextInt(random(), 1, 5) : 1;
      for (int attempt=0; attempt<numTests; attempt++) {
        //Modify existing index before we call optimize.
        if (nDocs > 0) {
          //Delete a few docs
          int numDeletes = TestUtil.nextInt(random(), 1, nDocs);
          for(int i=0; i<numDeletes; i++) {
            leaderClient.deleteByQuery("id:" + i);
          }
          //Add a few more
          int moreAdds = TestUtil.nextInt(random(), 1, 100);
          for (int i=0; i<moreAdds; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i + nDocs);
            doc.addField("name", "name = " + (i + nDocs));
            leaderClient.add(doc);
          }
          leaderClient.commit();
        }
      }

      // Before invoking optimize command, verify that the index directory contains multiple commits (including the one we snapshotted earlier).
      {
        Collection<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
        // Verify that multiple index commits are stored in this directory.
        assertTrue(commits.size() > 0);
        // Verify that the snapshot commit is present in this directory.
        assertTrue(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
      }

      // Optimize the index.
      leaderClient.optimize(true, true, 1);

      // After invoking optimize command, verify that the index directory contains multiple commits (including the one we snapshotted earlier).
      {
        List<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
        // Verify that multiple index commits are stored in this directory.
        assertTrue(commits.size() > 1);
        // Verify that the snapshot commit is present in this directory.
        assertTrue(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
      }

      // Delete the snapshot
      deleteSnapshot(adminClient, coreName, metaData.getName());

      // Add few documents. Without this the optimize command below does not take effect.
      {
        int moreAdds = TestUtil.nextInt(random(), 1, 100);
        for (int i=0; i<moreAdds; i++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", i + nDocs);
          doc.addField("name", "name = " + (i + nDocs));
          leaderClient.add(doc);
        }
        leaderClient.commit();
      }

      // Optimize the index.
      leaderClient.optimize(true, true, 1);

      // Verify that the index directory contains only 1 index commit (which is not the same as the snapshotted commit).
      Collection<IndexCommit> commits = listCommits(metaData.getIndexDirPath());
      assertTrue(commits.size() == 1);
      assertFalse(commits.stream().filter(x -> x.getGeneration() == metaData.getGenerationNumber()).findFirst().isPresent());
    }
  }

  private SnapshotMetaData createSnapshot (SolrClient adminClient, String coreName, String commitName) throws Exception {
    CreateSnapshot req = new CreateSnapshot(commitName);
    req.setCoreName(coreName);
    adminClient.request(req);

    Collection<SnapshotMetaData> snapshots = listSnapshots(adminClient, coreName);
    Optional<SnapshotMetaData> metaData = snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst();
    assertTrue(metaData.isPresent());

    return metaData.get();
  }

  private void deleteSnapshot(SolrClient adminClient, String coreName, String commitName) throws Exception {
    DeleteSnapshot req = new DeleteSnapshot(commitName);
    req.setCoreName(coreName);
    adminClient.request(req);

    Collection<SnapshotMetaData> snapshots = listSnapshots(adminClient, coreName);
    assertFalse(snapshots.stream().filter(x -> commitName.equals(x.getName())).findFirst().isPresent());
  }

  private Collection<SnapshotMetaData> listSnapshots(SolrClient adminClient, String coreName) throws Exception {
    ListSnapshots req = new ListSnapshots();
    req.setCoreName(coreName);
    @SuppressWarnings({"rawtypes"})
    NamedList resp = adminClient.request(req);
    assertTrue( resp.get("snapshots") instanceof NamedList );
    @SuppressWarnings({"rawtypes"})
    NamedList apiResult = (NamedList) resp.get("snapshots");

    List<SnapshotMetaData> result = new ArrayList<>(apiResult.size());
    for(int i = 0 ; i < apiResult.size(); i++) {
      String commitName = apiResult.getName(i);
      String indexDirPath = (String)((NamedList)apiResult.get(commitName)).get("indexDirPath");
      long genNumber = Long.parseLong((String)((NamedList)apiResult.get(commitName)).get("generation"));
      result.add(new SnapshotMetaData(commitName, indexDirPath, genNumber));
    }
    return result;
  }

  private List<IndexCommit> listCommits(String directory) throws Exception {
    SimpleFSDirectory dir = new SimpleFSDirectory(Paths.get(directory));
    try {
      return DirectoryReader.listCommits(dir);
    } catch (IndexNotFoundException ex) {
      // This can happen when the delete snapshot functionality cleans up the index files (when the directory
      // storing these files is not the *current* index directory).
      return Collections.emptyList();
    }
  }
}
