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
package org.apache.solr.common.cloud;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class that manages named configs in Zookeeper
 */
public class ZkConfigManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** ZkNode where named configs are stored */
  public static final String CONFIGS_ZKNODE = "/configs";

  public static final String UPLOAD_FILENAME_EXCLUDE_REGEX = "^\\..*$";
  public static final Pattern UPLOAD_FILENAME_EXCLUDE_PATTERN = Pattern.compile(UPLOAD_FILENAME_EXCLUDE_REGEX);

  private final SolrZkClient zkClient;
  
  /**
   * Creates a new ZkConfigManager
   * @param zkClient the {@link SolrZkClient} to use
   */
  public ZkConfigManager(SolrZkClient zkClient) { this.zkClient = zkClient; }



  /**
   * Upload files from a given path to a config in Zookeeper
   * @param dir         {@link java.nio.file.Path} to the files
   * @param configName  the name to give the config
   * @throws IOException
   *                    if an I/O error occurs or the path does not exist
   */
  public void uploadConfigDir(Path dir, String configName) throws IOException {
    zkClient.uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName, UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  /**
   * Upload matching files from a given path to a config in Zookeeper
   * @param dir         {@link java.nio.file.Path} to the files
   * @param configName  the name to give the config
   * @param filenameExclusions  files matching this pattern will not be uploaded
   * @throws IOException
   *                    if an I/O error occurs or the path does not exist
   */
  public void uploadConfigDir(Path dir, String configName,
      Pattern filenameExclusions) throws IOException {
    zkClient.uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName, filenameExclusions);
  }

  /**
   * Download a config from Zookeeper and write it to the filesystem
   * @param configName  the config to download
   * @param dir         the {@link Path} to write files under
   * @throws IOException
   *                    if an I/O error occurs or the config does not exist
   */
  public void downloadConfigDir(String configName, Path dir) throws IOException {
    zkClient.downloadFromZK(CONFIGS_ZKNODE + "/" + configName, dir);
  }

  public List<String> listConfigs() throws IOException {
    try {
      return zkClient.getChildren(ZkConfigManager.CONFIGS_ZKNODE, null, true);
    }
    catch (KeeperException.NoNodeException e) {
      return Collections.emptyList();
    }
    catch (KeeperException | InterruptedException e) {
      throw new IOException("Error listing configs", SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Check whether a config exists in Zookeeper
   *
   * @param configName the config to check existance on
   * @return whether the config exists or not
   * @throws IOException if an I/O error occurs
   */
  public Boolean configExists(String configName) throws IOException {
    try {
      return zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName, true);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error checking whether config exists",
          SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Delete a config in ZooKeeper
   *
   * @param configName the config to delete
   * @throws IOException if an I/O error occurs
   */
  public void deleteConfigDir(String configName) throws IOException {
    try {
      zkClient.clean(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error checking whether config exists",
          SolrZkClient.checkInterrupted(e));
    }
  }

  private void copyConfigDirFromZk(String fromZkPath, String toZkPath, Set<String> copiedToZkPaths) throws IOException {
    try {
      List<String> files = zkClient.getChildren(fromZkPath, null, true);
      for (String file : files) {
        List<String> children = zkClient.getChildren(fromZkPath + "/" + file, null, true);
        if (children.size() == 0) {
          copyData(copiedToZkPaths, fromZkPath + "/" + file, toZkPath + "/" + file);
        } else {
          copyConfigDirFromZk(fromZkPath + "/" + file, toZkPath + "/" + file, copiedToZkPaths);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error copying nodes from zookeeper path " + fromZkPath + " to " + toZkPath,
          SolrZkClient.checkInterrupted(e));
    }
  }

  private void copyData(Set<String> copiedToZkPaths, String fromZkFilePath, String toZkFilePath) throws KeeperException, InterruptedException {
    log.info("Copying zk node {} to {}", fromZkFilePath, toZkFilePath);
    byte[] data = zkClient.getData(fromZkFilePath, null, null, true);
    zkClient.makePath(toZkFilePath, data, true);
    if (copiedToZkPaths != null) copiedToZkPaths.add(toZkFilePath);
  }

  /**
   * Copy a config in ZooKeeper
   *
   * @param fromConfig the config to copy from
   * @param toConfig the config to copy to
   * @throws IOException if an I/O error occurs
   */
  public void copyConfigDir(String fromConfig, String toConfig) throws IOException {
    copyConfigDir(fromConfig, toConfig, null);
  }

  /**
   * Copy a config in ZooKeeper
   *
   * @param fromConfig the config to copy from
   * @param toConfig the config to copy to
   * @param copiedToZkPaths should be an empty Set, will be filled in by function
                            with the paths that were actually copied to.
   * @throws IOException if an I/O error occurs
   */
  public void copyConfigDir(String fromConfig, String toConfig, Set<String> copiedToZkPaths) throws IOException {
    String fromConfigPath = CONFIGS_ZKNODE + "/" + fromConfig;
    String toConfigPath = CONFIGS_ZKNODE + "/" + toConfig;
    try {
      copyData(copiedToZkPaths, fromConfigPath, toConfigPath);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error config " + fromConfig + " to " + toConfig,
              SolrZkClient.checkInterrupted(e));
    }
    copyConfigDirFromZk(fromConfigPath, toConfigPath, copiedToZkPaths);
  }

  // This method is used by configSetUploadTool and CreateTool to resolve the configset directory.
  // Check several possibilities:
  // 1> confDir/solrconfig.xml exists
  // 2> confDir/conf/solrconfig.xml exists
  // 3> configSetDir/confDir/conf/solrconfig.xml exists (canned configs)
  
  // Order is important here since "confDir" may be
  // 1> a full path to the parent of a solrconfig.xml or parent of /conf/solrconfig.xml
  // 2> one of the canned config sets only, e.g. _default
  // and trying to assemble a path for configsetDir/confDir is A Bad Idea. if confDir is a full path.
  
  public static Path getConfigsetPath(String confDir, String configSetDir) throws IOException {

    // A local path to the source, probably already includes "conf".
    Path ret = Paths.get(confDir, "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(confDir).normalize();
    }

    // a local path to the parent of a "conf" directory 
    ret = Paths.get(confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(confDir, "conf").normalize();
    }

    // one of the canned configsets.
    ret = Paths.get(configSetDir, confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Paths.get(configSetDir, confDir, "conf").normalize();
    }


    throw new IllegalArgumentException(String.format(Locale.ROOT,
        "Could not find solrconfig.xml at %s, %s or %s",
        Paths.get(configSetDir, "solrconfig.xml").normalize().toAbsolutePath().toString(),
        Paths.get(configSetDir, "conf", "solrconfig.xml").normalize().toAbsolutePath().toString(),
        Paths.get(configSetDir, confDir, "conf", "solrconfig.xml").normalize().toAbsolutePath().toString()
    ));
  }
}
