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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold  ZK upload/download/move common code. With the advent of the upconfig/downconfig/cp/ls/mv commands
 * in bin/solr it made sense to keep the individual transfer methods in a central place, so here it is.
 */
public class ZkMaintenanceUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ZkMaintenanceUtils() {} // don't let it be instantiated, all methods are static.
  /**
   * Lists a ZNode child and (optionally) the znodes of all the children. No data is dumped.
   *
   * @param path    The node to remove on Zookeeper
   * @param recurse Whether to remove children.
   * @throws KeeperException      Could not perform the Zookeeper operation.
   * @throws InterruptedException Thread interrupted
   * @throws SolrServerException  zookeeper node has children and recurse not specified.
   * @return an indented list of the znodes suitable for display
   */
  public static String listZnode(SolrZkClient zkClient, String path, Boolean recurse) throws KeeperException, InterruptedException, SolrServerException {
    String root = path;

    if (path.toLowerCase(Locale.ROOT).startsWith("zk:")) {
      root = path.substring(3);
    }
    if (root.equals("/") == false && root.endsWith("/")) {
      root = root.substring(0, root.length() - 1);
    }

    StringBuilder sb = new StringBuilder();
    
    if (recurse == false) {
      for (String node : zkClient.getChildren(root, null, true)) {
        if (node.equals("zookeeper") == false) {
          sb.append(node).append(System.lineSeparator());
        }
      }
      return sb.toString();
    }
    
    traverseZkTree(zkClient, root, VISIT_ORDER.VISIT_PRE, znode -> {
      if (znode.startsWith("/zookeeper")) return; // can't do anything with this node!
      int iPos = znode.lastIndexOf("/");
      if (iPos > 0) {
        for (int idx = 0; idx < iPos; ++idx) sb.append(" ");
        sb.append(znode.substring(iPos + 1)).append(System.lineSeparator());
      } else {
        sb.append(znode).append(System.lineSeparator());
      }
    });

    return sb.toString();
  }

  /**
   * Copy between local file system and Zookeeper, or from one Zookeeper node to another,
   * optionally copying recursively.
   *
   * @param src     Source to copy from. Both src and dst may be Znodes. However, both may NOT be local
   * @param dst     The place to copy the files too. Both src and dst may be Znodes. However both may NOT be local
   * @param recurse if the source is a directory, reccursively copy the contents iff this is true.
   * @throws SolrServerException  Explanatory exception due to bad params, failed operation, etc.
   * @throws KeeperException      Could not perform the Zookeeper operation.
   * @throws InterruptedException Thread interrupted
   */
  public static void zkTransfer(SolrZkClient zkClient, String src, Boolean srcIsZk,
                         String dst, Boolean dstIsZk, 
                         Boolean recurse) throws SolrServerException, KeeperException, InterruptedException, IOException {

    if (srcIsZk == false && dstIsZk == false) {
      throw new SolrServerException("One or both of source or destination must specify ZK nodes.");
    }

    // Make sure -recurse is specified if the source has children.
    if (recurse == false) {
      if (srcIsZk) {
        if (zkClient.getChildren(src, null, true).size() != 0) {
          throw new SolrServerException("Zookeeper node " + src + " has children and recurse is false");
        }
      } else if (Files.isDirectory(Paths.get(src))) {
        throw new SolrServerException("Local path " + Paths.get(src).toAbsolutePath() + " is a directory and recurse is false");
      }
    }
    if (srcIsZk == false && dstIsZk == false) {
      throw new SolrServerException("At least one of the source and dest parameters must be prefixed with 'zk:' ");
    }
    dst = normalizeDest(src, dst);

    if (srcIsZk && dstIsZk) {
      traverseZkTree(zkClient, src, VISIT_ORDER.VISIT_PRE, new ZkCopier(zkClient, src, dst));
      return;
    }
    if (dstIsZk) {
      uploadToZK(zkClient, Paths.get(src), dst, null);
      return;
    }

    // Copying individual files from ZK requires special handling since downloadFromZK assumes it's a directory.
    // This is kind of a weak test for the notion of "directory" on Zookeeper.
    if (zkClient.getChildren(src, null, true).size() > 0) {
      downloadFromZK(zkClient, src, Paths.get(dst));
      return;
    }

    if (Files.isDirectory(Paths.get(dst))) {
      if (dst.endsWith("/") == false) dst += "/";
      dst = normalizeDest(src, dst);
    }
    byte[] data = zkClient.getData(src, null, null, true);
    Path filename = Paths.get(dst);
    Files.createDirectories(filename.getParent());
    log.info("Writing file {}", filename);
    Files.write(filename, data);
  }

  private static String normalizeDest(String srcName, String dstName) {
    // Pull the last element of the src path and add it to the dst.
    if (dstName.endsWith("/")) {
      int pos = srcName.lastIndexOf("/");
      if (pos < 0) {
        dstName += srcName;
      } else {
        dstName += srcName.substring(pos + 1);
      }
    } else if (dstName.equals(".")) {
      dstName = Paths.get(".").normalize().toAbsolutePath().toString();
    }
    return dstName;
  }

  public static void moveZnode(SolrZkClient zkClient, String src, String dst) throws SolrServerException, KeeperException, InterruptedException {
    String destName = normalizeDest(src, dst);

    // Special handling if the source has no children, i.e. copying just a single file.
    if (zkClient.getChildren(src, null, true).size() == 0) {
      zkClient.makePath(destName, false, true);
      zkClient.setData(destName, zkClient.getData(src, null, null, true), true);
    } else {
      traverseZkTree(zkClient, src, VISIT_ORDER.VISIT_PRE, new ZkCopier(zkClient, src, destName));
    }

    // Insure all source znodes are present in dest before deleting the source.
    // throws error if not all there so the source is left intact. Throws error if source and dest don't match.
    checkAllZnodesThere(zkClient, src, destName);

    clean(zkClient, src);
  }


  // Insure that all the nodes in one path match the nodes in the other as a safety check before removing
  // the source in a 'mv' command.
  private static void checkAllZnodesThere(SolrZkClient zkClient, String src, String dst) throws KeeperException, InterruptedException, SolrServerException {

    for (String node : zkClient.getChildren(src, null, true)) {
      if (zkClient.exists(dst + "/" + node, true) == false) {
        throw new SolrServerException("mv command did not move node " + dst + "/" + node + " source left intact");
      }
      checkAllZnodesThere(zkClient, src + "/" + node, dst + "/" + node);
    }
  }

  // This not just a copy operation since the config manager takes care of construction the znode path to configsets
  public static void downConfig(SolrZkClient zkClient, String confName, Path confPath) throws IOException {
    ZkConfigManager manager = new ZkConfigManager(zkClient);

    // Try to download the configset
    manager.downloadConfigDir(confName, confPath);
  }

  // This not just a copy operation since the config manager takes care of construction the znode path to configsets
  public static void upConfig(SolrZkClient zkClient, Path confPath, String confName) throws IOException {
    ZkConfigManager manager = new ZkConfigManager(zkClient);

    // Try to download the configset
    manager.uploadConfigDir(confPath, confName);
  }

  // yeah, it's recursive :(
  public static void clean(SolrZkClient zkClient, String path) throws InterruptedException, KeeperException {
    traverseZkTree(zkClient, path, VISIT_ORDER.VISIT_POST, znode -> {
      try {
        if (!znode.equals("/")) {
          try {
            zkClient.delete(znode, -1, true);
          } catch (KeeperException.NotEmptyException e) {
            clean(zkClient, znode);
          }
        }
      } catch (KeeperException.NoNodeException r) {
        return;
      }
    });
  }

  public static void uploadToZK(SolrZkClient zkClient, final Path rootPath, final String zkPath,
                         final Pattern filenameExclusions) throws IOException {

    if (!Files.exists(rootPath))
      throw new IOException("Path " + rootPath + " does not exist");

    Files.walkFileTree(rootPath, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        String filename = file.getFileName().toString();
        if (filenameExclusions != null && filenameExclusions.matcher(filename).matches()) {
          log.info("uploadToZK skipping '{}' due to filenameExclusions '{}'", filename, filenameExclusions);
          return FileVisitResult.CONTINUE;
        }
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

  public static void downloadFromZK(SolrZkClient zkClient, String zkPath, Path dir) throws IOException {
    try {
      List<String> files = zkClient.getChildren(zkPath, null, true);
      Files.createDirectories(dir);
      for (String file : files) {
        List<String> children = zkClient.getChildren(zkPath + "/" + file, null, true);
        if (children.size() == 0) {
          byte[] data = zkClient.getData(zkPath + "/" + file, null, null, true);
          Path filename = dir.resolve(file);
          log.info("Writing file {}", filename);
          Files.write(filename, data);
        } else {
          downloadFromZK(zkClient, zkPath + "/" + file, dir.resolve(file));
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error downloading files from zookeeper path " + zkPath + " to " + dir.toString(),
          SolrZkClient.checkInterrupted(e));
    }
  }

  @FunctionalInterface
  public interface ZkVisitor {
    /**
     * Visit the target path
     *
     * @param path the path to visit
     */
    void visit(String path) throws InterruptedException, KeeperException;
  }

  public enum VISIT_ORDER {
    VISIT_PRE,
    VISIT_POST
  }

  /**
   * Recursively visit a zk tree rooted at path and apply the given visitor to each path. Exists as a separate method
   * because some of the logic can get nuanced.
   *
   * @param path       the path to start from
   * @param visitOrder whether to call the visitor at the at the ending or beginning of the run.
   * @param visitor    the operation to perform on each path
   */
  public static void traverseZkTree(SolrZkClient zkClient, final String path, final VISIT_ORDER visitOrder, final ZkVisitor visitor)
      throws InterruptedException, KeeperException {
    if (visitOrder == VISIT_ORDER.VISIT_PRE) {
      visitor.visit(path);
    }
    List<String> children;
    try {
      children = zkClient.getChildren(path, null, true);
    } catch (KeeperException.NoNodeException r) {
      return;
    }
    for (String string : children) {
      // we can't do anything to the built-in zookeeper node
      if (path.equals("/") && string.equals("zookeeper")) continue;
      if (path.startsWith("/zookeeper")) continue;
      if (path.equals("/")) {
        traverseZkTree(zkClient, path + string, visitOrder, visitor);
      } else {
        traverseZkTree(zkClient, path + "/" + string, visitOrder, visitor);
      }
    }
    if (visitOrder == VISIT_ORDER.VISIT_POST) {
      visitor.visit(path);
    }
  }

  // Take into account Windows file separaters when making a Znode's name.
  public static String createZkNodeName(String zkRoot, Path root, Path file) {
    String relativePath = root.relativize(file).toString();
    // Windows shenanigans
    String separator = root.getFileSystem().getSeparator();
    if ("\\".equals(separator))
      relativePath = relativePath.replaceAll("\\\\", "/");
    // It's possible that the relative path and file are the same, in which case
    // adding the bare slash is A Bad Idea
    if (relativePath.length() == 0) return zkRoot;
    
    return zkRoot + "/" + relativePath;
  }
}

class ZkCopier implements ZkMaintenanceUtils.ZkVisitor {

  String source;
  String dest;
  SolrZkClient zkClient;

  ZkCopier(SolrZkClient zkClient, String source, String dest) {
    this.source = source;
    this.dest = dest;
    if (dest.endsWith("/")) {
      this.dest = dest.substring(0, dest.length() - 1);
    }
    this.zkClient = zkClient;
  }

  @Override
  public void visit(String path) throws InterruptedException, KeeperException {
    String finalDestination = dest;
    if (path.equals(source) == false) finalDestination +=  "/" + path.substring(source.length() + 1);
    zkClient.makePath(finalDestination, false, true);
    zkClient.setData(finalDestination, zkClient.getData(path, null, null, true), true);
  }
}
