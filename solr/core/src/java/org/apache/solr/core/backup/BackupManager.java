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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepository.PathType;
import org.apache.solr.util.PropertiesInputStream;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements functionality to create a backup with extension points provided to integrate with different
 * types of file-systems.
 */
public class BackupManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String COLLECTION_PROPS_FILE = "collection_state.json";
  public static final String BACKUP_PROPS_FILE = "backup.properties";
  public static final String ZK_STATE_DIR = "zk_backup";
  public static final String CONFIG_STATE_DIR = "configs";

  // Backup properties
  public static final String COLLECTION_NAME_PROP = "collection";
  public static final String BACKUP_NAME_PROP = "backupName";
  public static final String INDEX_VERSION_PROP = "index.version";
  public static final String START_TIME_PROP = "startTime";

  protected final ZkStateReader zkStateReader;
  protected final BackupRepository repository;

  public BackupManager(BackupRepository repository, ZkStateReader zkStateReader) {
    this.repository = Objects.requireNonNull(repository);
    this.zkStateReader = Objects.requireNonNull(zkStateReader);
  }

  /**
   * @return The version of this backup implementation.
   */
  public final String getVersion() {
    return "1.0";
  }

  /**
   * This method returns the configuration parameters for the specified backup.
   *
   * @param backupLoc The base path used to store the backup data.
   * @param backupId  The unique name for the backup whose configuration params are required.
   * @return the configuration parameters for the specified backup.
   * @throws IOException In case of errors.
   */
  public Properties readBackupProperties(URI backupLoc, String backupId) throws IOException {
    Objects.requireNonNull(backupLoc);
    Objects.requireNonNull(backupId);

    // Backup location
    URI backupPath = repository.resolve(backupLoc, backupId);
    if (!repository.exists(backupPath)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Couldn't restore since doesn't exist: " + backupPath);
    }

    Properties props = new Properties();
    try (Reader is = new InputStreamReader(new PropertiesInputStream(
        repository.openInput(backupPath, BACKUP_PROPS_FILE, IOContext.DEFAULT)), StandardCharsets.UTF_8)) {
      props.load(is);
      return props;
    }
  }

  /**
   * This method stores the backup properties at the specified location in the repository.
   *
   * @param backupLoc  The base path used to store the backup data.
   * @param backupId  The unique name for the backup whose configuration params are required.
   * @param props The backup properties
   * @throws IOException in case of I/O error
   */
  public void writeBackupProperties(URI backupLoc, String backupId, Properties props) throws IOException {
    URI dest = repository.resolve(backupLoc, backupId, BACKUP_PROPS_FILE);
    try (Writer propsWriter = new OutputStreamWriter(repository.createOutput(dest), StandardCharsets.UTF_8)) {
      props.store(propsWriter, "Backup properties file");
    }
  }

  /**
   * This method reads the meta-data information for the backed-up collection.
   *
   * @param backupLoc The base path used to store the backup data.
   * @param backupId The unique name for the backup.
   * @param collectionName The name of the collection whose meta-data is to be returned.
   * @return the meta-data information for the backed-up collection.
   * @throws IOException in case of errors.
   */
  public DocCollection readCollectionState(URI backupLoc, String backupId, String collectionName) throws IOException {
    Objects.requireNonNull(collectionName);

    URI zkStateDir = repository.resolve(backupLoc, backupId, ZK_STATE_DIR);
    try (IndexInput is = repository.openInput(zkStateDir, COLLECTION_PROPS_FILE, IOContext.DEFAULT)) {
      byte[] arr = new byte[(int) is.length()]; // probably ok since the json file should be small.
      is.readBytes(arr, 0, (int) is.length());
      ClusterState c_state = ClusterState.load(-1, arr, Collections.emptySet());
      return c_state.getCollection(collectionName);
    }
  }

  /**
   * This method writes the collection meta-data to the specified location in the repository.
   *
   * @param backupLoc The base path used to store the backup data.
   * @param backupId  The unique name for the backup.
   * @param collectionName The name of the collection whose meta-data is being stored.
   * @param collectionState The collection meta-data to be stored.
   * @throws IOException in case of I/O errors.
   */
  public void writeCollectionState(URI backupLoc, String backupId, String collectionName,
                                   DocCollection collectionState) throws IOException {
    URI dest = repository.resolve(backupLoc, backupId, ZK_STATE_DIR, COLLECTION_PROPS_FILE);
    try (OutputStream collectionStateOs = repository.createOutput(dest)) {
      collectionStateOs.write(Utils.toJSON(Collections.singletonMap(collectionName, collectionState)));
    }
  }

  /**
   * This method uploads the Solr configuration files to the desired location in Zookeeper.
   *
   * @param backupLoc  The base path used to store the backup data.
   * @param backupId  The unique name for the backup.
   * @param sourceConfigName The name of the config to be copied
   * @param targetConfigName  The name of the config to be created.
   * @throws IOException in case of I/O errors.
   */
  public void uploadConfigDir(URI backupLoc, String backupId, String sourceConfigName, String targetConfigName)
      throws IOException {
    URI source = repository.resolve(backupLoc, backupId, ZK_STATE_DIR, CONFIG_STATE_DIR, sourceConfigName);
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + targetConfigName;
    uploadToZk(zkStateReader.getZkClient(), source, zkPath);
  }

  /**
   * This method stores the contents of a specified Solr config at the specified location in repository.
   *
   * @param backupLoc  The base path used to store the backup data.
   * @param backupId  The unique name for the backup.
   * @param configName The name of the config to be saved.
   * @throws IOException in case of I/O errors.
   */
  public void downloadConfigDir(URI backupLoc, String backupId, String configName) throws IOException {
    URI dest = repository.resolve(backupLoc, backupId, ZK_STATE_DIR, CONFIG_STATE_DIR, configName);
    repository.createDirectory(repository.resolve(backupLoc, backupId, ZK_STATE_DIR));
    repository.createDirectory(repository.resolve(backupLoc, backupId, ZK_STATE_DIR, CONFIG_STATE_DIR));
    repository.createDirectory(dest);

    downloadFromZK(zkStateReader.getZkClient(), ZkConfigManager.CONFIGS_ZKNODE + "/" + configName, dest);
  }

  private void downloadFromZK(SolrZkClient zkClient, String zkPath, URI dir) throws IOException {
    try {
      if (!repository.exists(dir)) {
        repository.createDirectory(dir);
      }
      List<String> files = zkClient.getChildren(zkPath, null, true);
      for (String file : files) {
        List<String> children = zkClient.getChildren(zkPath + "/" + file, null, true);
        if (children.size() == 0) {
          log.info("Writing file {}", file);
          byte[] data = zkClient.getData(zkPath + "/" + file, null, null, true);
          try (OutputStream os = repository.createOutput(repository.resolve(dir, file))) {
            os.write(data);
          }
        } else {
          downloadFromZK(zkClient, zkPath + "/" + file, repository.resolve(dir, file));
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error downloading files from zookeeper path " + zkPath + " to " + dir.toString(),
          SolrZkClient.checkInterrupted(e));
    }
  }

  private void uploadToZk(SolrZkClient zkClient, URI sourceDir, String destZkPath) throws IOException {
    Preconditions.checkArgument(repository.exists(sourceDir), "Path {} does not exist", sourceDir);
    Preconditions.checkArgument(repository.getPathType(sourceDir) == PathType.DIRECTORY,
        "Path {} is not a directory", sourceDir);

    for (String file : repository.listAll(sourceDir)) {
      String zkNodePath = destZkPath + "/" + file;
      URI path = repository.resolve(sourceDir, file);
      PathType t = repository.getPathType(path);
      switch (t) {
        case FILE: {
          try (IndexInput is = repository.openInput(sourceDir, file, IOContext.DEFAULT)) {
            byte[] arr = new byte[(int) is.length()]; // probably ok since the config file should be small.
            is.readBytes(arr, 0, (int) is.length());
            zkClient.makePath(zkNodePath, arr, true);
          } catch (KeeperException | InterruptedException e) {
            throw new IOException(e);
          }
          break;
        }

        case DIRECTORY: {
          if (!file.startsWith(".")) {
            uploadToZk(zkClient, path, zkNodePath);
          }
          break;
        }
        default:
          throw new IllegalStateException("Unknown path type " + t);
      }
    }
  }
}
