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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;


/**
 * Integration test verifying particular errors are reported correctly by the Collection-level backup/restore APIs
 */
public class BackupRestoreApiErrorConditionsTest extends SolrCloudTestCase {
  // TODO could these be unit tests somehow and still validate the response users see with certainty

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 1;
  private static final String COLLECTION_NAME = "initial_collection";
  private static final String BACKUP_NAME = "backup_name";
  private static final String VALID_REPOSITORY_NAME = "local";
  private static final long ASYNC_COMMAND_WAIT_PERIOD_MILLIS = 10 * 1000;

  private static String validBackupLocation;

  /*
   * Creates a single node cluster with an empty collection COLLECTION_NAME a configured BackupRepository named
   * VALID_REPOSITORY_NAME, and an existing incremental backup at location 'validBackupLocation'
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    System.setProperty("solr.allowPaths", "*");
    validBackupLocation = createTempDir().toAbsolutePath().toString();

    String solrXml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML;
    String local =
            "<backup>" +
                    "<repository  name=\"local\" class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\">" +
                    "</repository>" +
                    "</backup>";
    solrXml = solrXml.replace("</solr>", local + "</solr>");

    configureCluster(NUM_SHARDS)// nodes
            .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
            .withSolrXml(solrXml)
            .configure();

    final RequestStatusState createState = CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf1", NUM_SHARDS, NUM_REPLICAS)
            .processAndWait(cluster.getSolrClient(), ASYNC_COMMAND_WAIT_PERIOD_MILLIS);
    assertEquals(RequestStatusState.COMPLETED, createState);

    final RequestStatusState backupState = CollectionAdminRequest.backupCollection(COLLECTION_NAME, BACKUP_NAME)
            .setRepositoryName(VALID_REPOSITORY_NAME)
            .setLocation(validBackupLocation)
            .processAndWait(cluster.getSolrClient(), ASYNC_COMMAND_WAIT_PERIOD_MILLIS);
    assertEquals(RequestStatusState.COMPLETED, backupState);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    System.clearProperty("solr.allowPaths");
  }

  @Test
  public void testBackupOperationsReportErrorWhenUnknownBackupRepositoryRequested() throws Exception {
    // Check message for create-backup
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.backupCollection(COLLECTION_NAME, BACKUP_NAME)
              .setRepositoryName("some-nonexistent-repo-name")
              .setLocation(validBackupLocation)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("Could not find a backup repository with name some-nonexistent-repo-name"));

    // Check message for list-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.listBackup(BACKUP_NAME)
              .setBackupLocation(validBackupLocation)
              .setBackupRepository("some-nonexistent-repo-name")
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("Could not find a backup repository with name some-nonexistent-repo-name"));

    // Check message for delete-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteBackupById(BACKUP_NAME, 1)
              .setLocation(validBackupLocation)
              .setRepositoryName("some-nonexistent-repo-name")
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("Could not find a backup repository with name some-nonexistent-repo-name"));

    // Check message for restore-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.restoreCollection(COLLECTION_NAME + "_restored", BACKUP_NAME)
              .setLocation(validBackupLocation)
              .setRepositoryName("some-nonexistent-repo-name")
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("Could not find a backup repository with name some-nonexistent-repo-name"));
  }

  @Test
  public void testBackupOperationsReportErrorWhenNonexistentLocationProvided() {
    // Check message for create-backup
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.backupCollection(COLLECTION_NAME, BACKUP_NAME)
            .setRepositoryName(VALID_REPOSITORY_NAME)
            .setLocation(validBackupLocation + File.pathSeparator + "someNonexistentLocation")
            .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("specified location"));
    assertTrue(e.getMessage().contains("does not exist"));

    // Check message for list-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.listBackup(BACKUP_NAME)
              .setBackupLocation(validBackupLocation + File.pathSeparator + "someNonexistentLocation")
              .setBackupRepository(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("specified location"));
    assertTrue(e.getMessage().contains("does not exist"));

    // Check message for delete-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteBackupById(BACKUP_NAME, 1)
              .setLocation(validBackupLocation + File.pathSeparator + "someNonexistentLocation")
              .setRepositoryName(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("specified location"));
    assertTrue(e.getMessage().contains("does not exist"));

    // Check message for restore-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.restoreCollection(COLLECTION_NAME + "_restored", BACKUP_NAME)
              .setLocation(validBackupLocation + File.pathSeparator + "someNonexistentLocation")
              .setRepositoryName(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("specified location"));
    assertTrue(e.getMessage().contains("does not exist"));
  }

  @Test
  public void testListAndDeleteFailOnOldBackupLocations() throws Exception {
    final String nonIncrementalBackupLocation = createTempDir().toAbsolutePath().toString();
    final RequestStatusState backupState = CollectionAdminRequest.backupCollection(COLLECTION_NAME, BACKUP_NAME)
            .setRepositoryName(VALID_REPOSITORY_NAME)
            .setLocation(nonIncrementalBackupLocation)
            .setIncremental(false)
            .processAndWait(cluster.getSolrClient(), ASYNC_COMMAND_WAIT_PERIOD_MILLIS);
    assertEquals(RequestStatusState.COMPLETED, backupState);

    // Check message for list-backup
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.listBackup(BACKUP_NAME)
              .setBackupLocation(nonIncrementalBackupLocation)
              .setBackupRepository(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("The backup name [backup_name] at location"));
    assertTrue(e.getMessage().contains("holds a non-incremental (legacy) backup, but backup-listing is only supported on incremental backups"));

    // Check message for delete-backup
    e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteBackupById(BACKUP_NAME, 1)
              .setLocation(nonIncrementalBackupLocation)
              .setRepositoryName(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("The backup name [backup_name] at location"));
    assertTrue(e.getMessage().contains("holds a non-incremental (legacy) backup, but backup-deletion is only supported on incremental backups"));
  }

  @Test
  public void testDeleteFailsOnNonexistentBackupId() {
    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteBackupById(BACKUP_NAME, 123)
              .setLocation(validBackupLocation)
              .setRepositoryName(VALID_REPOSITORY_NAME)
              .process(cluster.getSolrClient());
    });
    assertTrue(e.getMessage().contains("Backup ID [123] not found; cannot be deleted"));
  }
}
