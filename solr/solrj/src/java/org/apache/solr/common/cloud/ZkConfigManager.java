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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;

/**
 * Class that manages named configs in Zookeeper
 */
public class ZkConfigManager {

  private static final Logger logger = LoggerFactory.getLogger(ZkConfigManager.class);

  /** ZkNode where named configs are stored */
  public static final String CONFIGS_ZKNODE = "/configs";

  private final SolrZkClient zkClient;

  /**
   * Creates a new ZkConfigManager
   * @param zkClient the {@link SolrZkClient} to use
   */
  public ZkConfigManager(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }

  private void uploadToZK(final Path rootPath, final String zkPath) throws IOException {

    if (!Files.exists(rootPath))
      throw new IOException("Path " + rootPath + " does not exist");

    Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>(){
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        String filename = file.getFileName().toString();
        if (filename.startsWith("."))
          return FileVisitResult.CONTINUE;
        String zkNode = createZkNodeName(zkPath, rootPath, file);
        try {
          zkClient.makePath(zkNode, file.toFile(), false, true);
        } catch (KeeperException | InterruptedException e) {
          throw new IOException("Error uploading file " + file.toString() + " to zookeeper path " + zkNode,
              SolrZkClient.checkInterrupted(e));
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return (dir.getFileName().toString().startsWith(".")) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
      }
    });
  }

  private static String createZkNodeName(String zkRoot, Path root, Path file) {
    String relativePath = root.relativize(file).toString();
    // Windows shenanigans
    String separator = root.getFileSystem().getSeparator();
    if ("\\".equals(separator))
      relativePath = relativePath.replaceAll("\\\\", "/");
    return zkRoot + "/" + relativePath;
  }

  private void downloadFromZK(String zkPath, Path dir) throws IOException {
    try {
      List<String> files = zkClient.getChildren(zkPath, null, true);
      Files.createDirectories(dir);
      for (String file : files) {
        List<String> children = zkClient.getChildren(zkPath + "/" + file, null, true);
        if (children.size() == 0) {
          byte[] data = zkClient.getData(zkPath + "/" + file, null, null, true);
          Path filename = dir.resolve(file);
          logger.info("Writing file {}", filename);
          Files.write(filename, data);
        } else {
          downloadFromZK(zkPath + "/" + file, dir.resolve(file));
        }
      }
    }
    catch (KeeperException | InterruptedException e) {
      throw new IOException("Error downloading files from zookeeper path " + zkPath + " to " + dir.toString(),
          SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Upload files from a given path to a config in Zookeeper
   * @param dir         {@link java.nio.file.Path} to the files
   * @param configName  the name to give the config
   * @throws IOException
   *                    if an I/O error occurs or the path does not exist
   */
  public void uploadConfigDir(Path dir, String configName) throws IOException {
    uploadToZK(dir, CONFIGS_ZKNODE + "/" + configName);
  }

  /**
   * Download a config from Zookeeper and write it to the filesystem
   * @param configName  the config to download
   * @param dir         the {@link Path} to write files under
   * @throws IOException
   *                    if an I/O error occurs or the config does not exist
   */
  public void downloadConfigDir(String configName, Path dir) throws IOException {
    downloadFromZK(CONFIGS_ZKNODE + "/" + configName, dir);
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
}
