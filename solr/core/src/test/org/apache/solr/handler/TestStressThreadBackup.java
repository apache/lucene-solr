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
package org.apache.solr.handler;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Nightly
@SuppressCodecs({"SimpleText"}) // Backups do checksum validation against a footer value not present in 'SimpleText'
public class TestStressThreadBackup extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern ENDS_WITH_INT_DIGITS = Pattern.compile("\\d+$");
  private File backupDir;
  private SolrClient adminClient;
  private SolrClient coreClient;
  private String coreName;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty("solr.allowPaths", "*");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solr.allowPaths");
  }

  @Before
  public void beforeTest() throws Exception {
    backupDir = createTempDir(getTestClass().getSimpleName() + "_backups").toFile();

    // NOTE: we don't actually care about using SolrCloud, but we want to use SolrClient and I can't
    // bring myself to deal with the nonsense that is SolrJettyTestBase.
    
    // We do however explicitly want a fresh "cluster" every time a test is run
    configureCluster(1)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    assertEquals(0, (CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf1", 1, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient()).getStatus()));
    adminClient = getHttpSolrClient(cluster.getJettySolrRunners().get(0).getBaseUrl().toString());
    initCoreNameAndSolrCoreClient();
  }

  @After
  public void afterTest() throws Exception {
    // we use a clean cluster instance for every test, so we need to clean it up
    shutdownCluster();
    
    if (null != adminClient) {
      adminClient.close();
    }
    if (null != coreClient) {
      coreClient.close();
    }
  }

  @Test
  public void testCoreAdminHandler() throws Exception {
    // Use default BackupAPIImpl which hits CoreAdmin API for everything
    testSnapshotsAndBackupsDuringConcurrentCommitsAndOptimizes(new BackupAPIImpl());
  }

  @Test
  public void testReplicationHandler() throws Exception {
    // Create a custom BackupAPIImpl which uses ReplicatoinHandler for the backups
    // but still defaults to CoreAdmin for making named snapshots (since that's what's documented)
    testSnapshotsAndBackupsDuringConcurrentCommitsAndOptimizes(new BackupAPIImpl() {
      final BackupStatusChecker backupStatus = new BackupStatusChecker(coreClient);
      /** no solrj API for ReplicationHandler */
      private GenericSolrRequest makeReplicationReq(SolrParams p) {
        return new GenericSolrRequest(GenericSolrRequest.METHOD.GET, "/replication", p);
      }
      
      /** 
       * Override default backup impl to hit ReplicationHandler, 
       * and then poll that same handler until success
       */
      public void makeBackup(final String backupName, final String snapName) throws Exception {
        final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        ModifiableSolrParams p = params("command", "backup",
                                        "name", backupName,
                                        CoreAdminParams.BACKUP_LOCATION, backupDir.getAbsolutePath());
        if (null != snapName) {
          p.add(CoreAdminParams.COMMIT_NAME, snapName);
        }
        makeReplicationReq(p).process(coreClient);
        backupStatus.waitForBackupSuccess(backupName, timeout);
      }
    });
    
  }

  public void testSnapshotsAndBackupsDuringConcurrentCommitsAndOptimizes(final BackupAPIImpl impl) throws Exception {
    final int numBackupIters = 20; // don't use 'atLeast', we don't want to blow up on nightly
    
    final AtomicReference<Throwable> heavyCommitFailure = new AtomicReference<>();
    final AtomicBoolean keepGoing = new AtomicBoolean(true);

    // this thread will do nothing but add/commit new 'dummy' docs over and over again as fast as possible
    // to create a lot of index churn w/ segment merging
    final Thread heavyCommitting = new Thread() {
      public void run() {
        try {
          int docIdCounter = 0;
          while (keepGoing.get()) {
            docIdCounter++;

            final UpdateRequest req = new UpdateRequest().add(makeDoc("dummy_" + docIdCounter, "dummy"));
            // always commit to force lots of new segments
            req.setParam(UpdateParams.COMMIT,"true");
            req.setParam(UpdateParams.OPEN_SEARCHER,"false");           // we don't care about searching

            // frequently forceMerge to ensure segments are frequently deleted
            if (0 == (docIdCounter % 13)) {                             // arbitrary
              req.setParam(UpdateParams.OPTIMIZE, "true");
              req.setParam(UpdateParams.MAX_OPTIMIZE_SEGMENTS, "5");    // arbitrary
            }
            
            log.info("Heavy Committing #{}: {}", docIdCounter, req);
            final UpdateResponse rsp = req.process(coreClient);
            assertEquals("Dummy Doc#" + docIdCounter + " add status: " + rsp.toString(), 0, rsp.getStatus());
                   
          }
        } catch (Throwable t) {
          heavyCommitFailure.set(t);
        }
      }
    };
    
    heavyCommitting.start();
    try {
      // now have the "main" test thread try to take a serious of backups/snapshots
      // while adding other "real" docs
      
      final Queue<String> namedSnapshots = new LinkedList<>();

      // NOTE #1: start at i=1 for 'id' & doc counting purposes...
      // NOTE #2: abort quickly if the oher thread reports a heavyCommitFailure...
      for (int i = 1; (i <= numBackupIters && null == heavyCommitFailure.get()); i++) {
        
        // in each iteration '#i', the commit we create should have exactly 'i' documents in
        // it with the term 'type_s:real' (regardless of what the other thread does with dummy docs)
        
        // add & commit a doc #i
        final UpdateRequest req = new UpdateRequest().add(makeDoc("doc_" + i, "real"));
        req.setParam(UpdateParams.COMMIT,"true"); // make immediately available for backup
        req.setParam(UpdateParams.OPEN_SEARCHER,"false"); // we don't care about searching
        
        final UpdateResponse rsp = req.process(coreClient);
        assertEquals("Real Doc#" + i + " add status: " + rsp.toString(), 0, rsp.getStatus());

        // create a backup of the 'current' index
        impl.makeBackup("backup_currentAt_" + i);
        
        // verify backup is valid and has the number of 'real' docs we expect...
        validateBackup("backup_currentAt_" + i);

        // occasionally make a "snapshot_i", add it to 'namedSnapshots'
        // NOTE: we don't want to do this too often, or the SnapShotMetadataManager will protect
        // too many segment files "long term".  It's more important to stress the thread contention
        // between backups calling save/release vs the DelPolicy trying to delete segments
        if ( 0 == random().nextInt(7 + namedSnapshots.size()) ) {
          final String snapshotName = "snapshot_" + i;
          log.info("Creating snapshot: {}", snapshotName);
          impl.makeSnapshot(snapshotName);
          namedSnapshots.add(snapshotName);
        }

        // occasionally make a backup of a snapshot and remove it
        // the odds of doing this increase based on how many snapshots currently exist,
        // and how few iterations we have left
        if (3 < namedSnapshots.size() &&
            random().nextInt(3 + numBackupIters - i) < random().nextInt(namedSnapshots.size())) {

          assert 0 < namedSnapshots.size() : "Someone broke the conditionl";
          final String snapshotName = namedSnapshots.poll();
          final String backupName = "backup_as_of_" + snapshotName;
          log.info("Creating {} from {} in iter={}", backupName, snapshotName, i);
          impl.makeBackup(backupName, snapshotName);
          log.info("Deleting {} in iter={}", snapshotName, i);
          impl.deleteSnapshot(snapshotName);

          validateBackup(backupName);

          // NOTE: we can't directly compare our backups, because the stress thread
          // may have added/committed documents
          // ie: backup_as_of_snapshot_4 and backup_currentAt_4 should have the same 4 "real"
          // documents, but they may have other commits that affect the data files
          // between when the backup was taken and when the snapshot was taken

        }
      }
      
    } finally {
      keepGoing.set(false);
      heavyCommitting.join();
    }
    assertNull(heavyCommitFailure.get());

    { log.info("Done with (concurrent) updates, Deleting all docs...");
      final UpdateRequest delAll = new UpdateRequest().deleteByQuery("*:*");
      delAll.setParam(UpdateParams.COMMIT,"true");
      delAll.setParam(UpdateParams.OPTIMIZE, "true");
      delAll.setParam(UpdateParams.MAX_OPTIMIZE_SEGMENTS, "1"); // purge as many files as possible
      final UpdateResponse delRsp = delAll.process(coreClient);
      assertEquals("dellAll status: " + delRsp.toString(), 0, delRsp.getStatus());
    }

    { // Validate some backups at random...
      final int numBackupsToCheck = atLeast(1);
      log.info("Validating {} random backups to ensure they are un-affected by deleting all docs...",
               numBackupsToCheck);
      final List<File> allBackups = Arrays.asList(backupDir.listFiles());
      // insure consistent (arbitrary) ordering before shuffling
      Collections.sort(allBackups); 
      Collections.shuffle(allBackups, random());
      for (int i = 0; i < numBackupsToCheck; i++) {
        final File backup = allBackups.get(i);
        validateBackup(backup);
      }
    }
  }
  
  /**
   * Given a backup name, extrats the numberic suffix identifying how many "real" docs should be in it
   *
   * @see #ENDS_WITH_INT_DIGITS
   */
  private static int getNumRealDocsFromBackupName(final String backupName) {
    final Matcher m = ENDS_WITH_INT_DIGITS.matcher(backupName);
    assertTrue("Backup name does not end with int digits: " + backupName, m.find());
    return Integer.parseInt(m.group());
  }
       
  /** 
   * Validates a backup exists, passes check index, and contains a number of "real" documents
   * that match it's name
   * 
   * @see #validateBackup(File)
   */
  private void validateBackup(final String backupName) throws IOException {
    final File backup = new File(backupDir, "snapshot." + backupName);
    validateBackup(backup);
  }
  
  /** 
   * Validates a backup dir exists, passes check index, and contains a number of "real" documents
   * that match it's name
   * 
   * @see #getNumRealDocsFromBackupName
   */
  private void validateBackup(final File backup) throws IOException {
    log.info("Checking Validity of {}", backup);
    assertTrue(backup.toString() + ": isDir?", backup.isDirectory());
    final Matcher m = ENDS_WITH_INT_DIGITS.matcher(backup.getName());
    assertTrue("Backup dir name does not end with int digits: " + backup.toString(), m.find());
    final int numRealDocsExpected = Integer.parseInt(m.group());
    
    try (Directory dir = FSDirectory.open(backup.toPath())) {
      TestUtil.checkIndex(dir, true, true, true, null);
      try (DirectoryReader r = DirectoryReader.open(dir)) {
        assertEquals("num real docs in " + backup.toString(),
                     numRealDocsExpected, r.docFreq(new Term("type_s","real")));
      }
    }
  }
  
  
  /** 
   * Creates a "large" document with lots of fields (to stimulate lots of files in each segment) 
   * @param id the uniqueKey
   * @param type the type of the doc for use in the 'type_s' field (for term counting later)
   */
  static SolrInputDocument makeDoc(String id, String type) {
    final SolrInputDocument doc = new SolrInputDocument("id", id, "type_s", type);
    for (int f = 0; f < 100; f++) {
      doc.addField(f + "_s", TestUtil.randomUnicodeString(random(), 20));
    }
    return doc;
  }

  private void initCoreNameAndSolrCoreClient() {
    // Sigh.
    Replica r = cluster.getSolrClient().getZkStateReader().getClusterState()
      .getCollection(DEFAULT_TEST_COLLECTION_NAME).getActiveSlices().iterator().next()
      .getReplicas().iterator().next();
    coreName = r.getCoreName();
    coreClient = getHttpSolrClient(r.getCoreUrl());
  }

  /** 
   * API for taking backups and snapshots that can hide the impl quirks of 
   * using ReplicationHandler vs CoreAdminHandler (the default)
   */
  private class BackupAPIImpl {
    /** TODO: SOLR-9239, no solrj API for CoreAdmin Backups */
    protected GenericSolrRequest makeCoreAdmin(CoreAdminAction action, SolrParams p) {
      return new GenericSolrRequest(GenericSolrRequest.METHOD.POST, "/admin/cores",
                                    SolrParams.wrapDefaults(params(CoreAdminParams.ACTION, action.toString()), p));
    }

    /** Make a backup or the named commit snapshot (or null for latest), and only return if successful */
    public void makeBackup(final String backupName) throws Exception {
      makeBackup(backupName, null);
    }

    /** Make a backup or latest commit, and only return if successful */
    public void makeBackup(final String backupName, final String snapName) throws Exception {
      ModifiableSolrParams p = params(CoreAdminParams.CORE, coreName,
                                      CoreAdminParams.NAME, backupName,
                                      CoreAdminParams.BACKUP_LOCATION, backupDir.getAbsolutePath(),
                                      CoreAdminParams.BACKUP_INCREMENTAL, "false");
      if (null != snapName) {
        p.add(CoreAdminParams.COMMIT_NAME, snapName);
      }
      makeCoreAdmin(CoreAdminAction.BACKUPCORE, p).process(adminClient);
      // CoreAdmin BACKUPCORE is synchronous by default, no need to wait for anything.
    }
    
    /** Make a named snapshot, and only return if successful */
    public void makeSnapshot(final String snapName) throws Exception {
      makeCoreAdmin(CoreAdminAction.CREATESNAPSHOT,
                    params(CoreAdminParams.CORE, coreName,
                           CoreAdminParams.COMMIT_NAME, snapName)).process(adminClient);
      // CoreAdmin CREATESNAPSHOT is synchronous by default, no need to wait for anything.
    }
    
    /** Delete a named snapshot, and only return if successful */
    public void deleteSnapshot(final String snapName) throws Exception {
      makeCoreAdmin(CoreAdminAction.DELETESNAPSHOT,
                    params(CoreAdminParams.CORE, coreName,
                           CoreAdminParams.COMMIT_NAME, snapName)).process(adminClient);
      // CoreAdmin DELETESNAPSHOT is synchronous by default, no need to wait for anything.
    }
  }

}
