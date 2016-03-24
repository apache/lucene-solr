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
package org.apache.solr.core.backup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;

import org.apache.lucene.util.Version;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class implements functionality to create a backup with extension points provided
 * to integrate with different types of file-systems.
 */
public class BackupManager {
  protected static Logger log = LoggerFactory.getLogger(BackupManager.class);
  public static final String COLLECTION_PROPS_FILE = "collection_state.json";
  public static final String BACKUP_PROPS_FILE = "backup.properties";
  public static final String ZK_STATE_DIR = "zk_backup";
  public static final String CONFIG_STATE_DIR = "configs";

  // Backup properties
  public static final String COLLECTION_NAME_PROP = "collection";
  public static final String BACKUP_ID_PROP = "backupId";
  public static final String COLLECTION_CONFIG_NAME_PROP = "collection.configName";
  public static final String INDEX_VERSION_PROP = "index.version";
  // version of the backup implementation. Defined to enable backwards compatibility
  public static final String VERSION_PROP = "version";

  protected final ZkStateReader zkStateReader;
  protected final String collectionName;
  protected final BackupRepository repository;
  protected final IndexBackupStrategy strategy;

  public BackupManager(BackupRepository repository, IndexBackupStrategy strategy, ZkStateReader zkStateReader,
      String collectionName) {
    this.repository = Preconditions.checkNotNull(repository);
    this.strategy = Preconditions.checkNotNull(strategy);
    this.zkStateReader = Preconditions.checkNotNull(zkStateReader);
    this.collectionName = Preconditions.checkNotNull(collectionName);
  }

  /**
   * @return The version of this backup implementation.
   */
  public final String getVersion() {
    return "1.0";
  }

  /**
   * This method implements the backup functionality.
   *
   * @param backupId The unique name for the backup to be created
   * @throws IOException in case of errors
   */
  public final void createBackup(String backupId) throws IOException {
    Preconditions.checkNotNull(backupId);

    // Fetch the collection state (and validate its existence).
    DocCollection collectionState = zkStateReader.getClusterState().getCollection(collectionName);

    // Backup location
    URI backupPath = repository.createURI(backupId);

    if(repository.exists(backupPath)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "The backup directory already exists " + backupPath);
    }

    // Create a directory to store backup details.
    repository.createDirectory(backupPath);

    try {
      // Backup the index data.
      log.info("Starting backup of collection={} with backupName={} at location={}", collectionName, backupId,
          backupPath);
      strategy.createBackup(backupPath, collectionName, backupId);

      log.info("Starting to backup ZK data for backupName={}", backupId);

      // Save the configset for the collection
      String configName = zkStateReader.readConfigName(collectionName);
      URI configPath = repository.createURI(backupId, ZK_STATE_DIR, CONFIG_STATE_DIR, configName);
      downloadConfigDir(configName, configPath);

      Properties props = new Properties();
      props.setProperty(COLLECTION_NAME_PROP, collectionName);
      props.setProperty(BACKUP_ID_PROP, backupId);
      props.put(COLLECTION_CONFIG_NAME_PROP, configName);
      props.put(INDEX_VERSION_PROP, Version.LATEST.toString());
      props.put(VERSION_PROP, getVersion());

      try ( Writer propsWriter = new OutputStreamWriter(repository.createOutput(repository.createURI(backupId, BACKUP_PROPS_FILE)));
            OutputStream collectionStateOs = repository.createOutput(repository.createURI(backupId, ZK_STATE_DIR, COLLECTION_PROPS_FILE))) {
        collectionStateOs.write(Utils.toJSON(Collections.singletonMap(collectionName, collectionState)));
        props.store(propsWriter, null);
      }

      log.info("Completed backing up ZK data for backupName={}", backupId);
    } catch (IOException ex) {
      log.error("Unable to create a snapshot...", ex);
      try {
        log.info("Cleaning up the snapshot " + backupId + " for collection " + collectionName);
        deleteBackup(backupId);
      } catch (IOException e) {
        log.error("Failed to cleanup a snapshot...", e);
      }
    }
  }

  /**
   * @param backupId The name for the backup to be deleted
   * @throws IOException in case of errors
   */
  public void deleteBackup(String backupId) throws IOException {
    // Backup location
    URI backupPath = repository.createURI(backupId);
    repository.deleteDirectory(backupPath);
  }

  private void downloadConfigDir(String configName, URI dest) throws IOException {
    // TODO - Enable copying ZK data to other file-systems too.
    zkStateReader.getConfigManager().downloadConfigDir(configName, Paths.get(dest));
  }
}
