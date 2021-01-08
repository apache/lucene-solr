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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to hold  ZK upload/download/move common code. With the advent of the upconfig/downconfig/cp/ls/mv commands
 * in bin/solr it made sense to keep the individual transfer methods in a central place, so here it is.
 */
public class ZkMaintenanceUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String ZKNODE_DATA_FILE = "zknode.data";

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

    if (dstIsZk && dst.length() == 0) {
      dst = "/"; // for consistency, one can copy from zk: and send to zk:/
    }
    dst = normalizeDest(src, dst, srcIsZk, dstIsZk);

    // ZK -> ZK copy.
    if (srcIsZk && dstIsZk) {
      traverseZkTree(zkClient, src, VISIT_ORDER.VISIT_PRE, new ZkCopier(zkClient, src, dst));
      return;
    }

    //local -> ZK copy
    if (dstIsZk) {
      uploadToZK(zkClient, Paths.get(src), dst, null);
      return;
    }

    // Copying individual files from ZK requires special handling since downloadFromZK assumes the node has children.
    // This is kind of a weak test for the notion of "directory" on Zookeeper.
    // ZK -> local copy where ZK is a parent node
    if (zkClient.getChildren(src, null, true).size() > 0) {
      downloadFromZK(zkClient, src, Paths.get(dst));
      return;
    }

    // Single file ZK -> local copy where ZK is a leaf node
    if (Files.isDirectory(Paths.get(dst))) {
      if (dst.endsWith(File.separator) == false) dst += File.separator;
      dst = normalizeDest(src, dst, srcIsZk, dstIsZk);
    }
    byte[] data = zkClient.getData(src, null, null, true);
    Path filename = Paths.get(dst);
    Files.createDirectories(filename.getParent());
    log.info("Writing file {}", filename);
    Files.write(filename, data);
  }

  // If the dest ends with a separator, it's a directory or non-leaf znode, so return the
  // last element of the src to appended to the dstName.
  private static String normalizeDest(String srcName, String dstName, boolean srcIsZk, boolean dstIsZk) {
    // Special handling for "."
    if (dstName.equals(".")) {
      return Paths.get(".").normalize().toAbsolutePath().toString();
    }

    String dstSeparator = (dstIsZk) ? "/" : File.separator;
    String srcSeparator = (srcIsZk) ? "/" : File.separator;

    if (dstName.endsWith(dstSeparator)) { // Dest is a directory or non-leaf znode, append last element of the src path.
      int pos = srcName.lastIndexOf(srcSeparator);
      if (pos < 0) {
        dstName += srcName;
      } else {
        dstName += srcName.substring(pos + 1);
      }
    }

    log.info("copying from '{}' to '{}'", srcName, dstName);
    return dstName;
  }

  public static void moveZnode(SolrZkClient zkClient, String src, String dst) throws SolrServerException, KeeperException, InterruptedException {
    String destName = normalizeDest(src, dst, true, true);

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

  /**
   * Delete a path and all of its sub nodes
   * @param filter for node to be deleted
   */
  public static void clean(SolrZkClient zkClient, String path, Predicate<String> filter) throws InterruptedException, KeeperException {
    if (filter == null) {
      clean(zkClient, path);
      return;
    }

    ArrayList<String> paths = new ArrayList<>();

    traverseZkTree(zkClient, path, VISIT_ORDER.VISIT_POST, znode -> {
      if (!znode.equals("/") && filter.test(znode)) paths.add(znode);
    });

    // sort the list in descending order to ensure that child entries are deleted first
    paths.sort(Comparator.comparingInt(String::length).reversed());

    for (String subpath : paths) {
      if (!subpath.equals("/")) {
        try {
          zkClient.delete(subpath, -1, true);
        } catch (KeeperException.NotEmptyException | KeeperException.NoNodeException e) {
          // expected
        }
      }
    }
  }

  public static void uploadToZK(SolrZkClient zkClient, final Path fromPath, final String zkPath,
                                final Pattern filenameExclusions) throws IOException {

    String path = fromPath.toString();
    if (path.endsWith("*")) {
      path = path.substring(0, path.length() - 1);
    }

    final Path rootPath = Paths.get(path);

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
          // if the path exists (and presumably we're uploading data to it) just set its data
          if (file.toFile().getName().equals(ZKNODE_DATA_FILE) && zkClient.exists(zkNode, true)) {
            zkClient.setData(zkNode, file.toFile(), true);
          } else {
            zkClient.makePath(zkNode, file.toFile(), false, true);
          }
        } catch (KeeperException | InterruptedException e) {
          throw new IOException("Error uploading file " + file.toString() + " to zookeeper path " + zkNode,
              SolrZkClient.checkInterrupted(e));
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (dir.getFileName().toString().startsWith(".")) return FileVisitResult.SKIP_SUBTREE;

        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static boolean isEphemeral(SolrZkClient zkClient, String zkPath) throws KeeperException, InterruptedException {
    Stat znodeStat = zkClient.exists(zkPath, null, true);
    return znodeStat.getEphemeralOwner() != 0;
  }

  private static int copyDataDown(SolrZkClient zkClient, String zkPath, File file) throws IOException, KeeperException, InterruptedException {
    byte[] data = zkClient.getData(zkPath, null, null, true);
    if (data != null && data.length > 0) { // There are apparently basically empty ZNodes.
      log.info("Writing file {}", file);
      Files.write(file.toPath(), data);
      return data.length;
    }
    return 0;
  }

  public static void downloadFromZK(SolrZkClient zkClient, String zkPath, Path file) throws IOException {
    try {
      List<String> children = zkClient.getChildren(zkPath, null, true);
      // If it has no children, it's a leaf node, write the associated data from the ZNode.
      // Otherwise, continue recursing, but write the associated data to a special file if any
      if (children.size() == 0) {
        // If we didn't copy data down, then we also didn't create the file. But we still need a marker on the local
        // disk so create an empty file.
        if (copyDataDown(zkClient, zkPath, file.toFile()) == 0) {
          Files.createFile(file);
        }
      } else {
        Files.createDirectories(file); // Make parent dir.
        // ZK nodes, whether leaf or not can have data. If it's a non-leaf node and
        // has associated data write it into the special file.
        copyDataDown(zkClient, zkPath, new File(file.toFile(), ZKNODE_DATA_FILE));

        for (String child : children) {
          String zkChild = zkPath;
          if (zkChild.endsWith("/") == false) zkChild += "/";
          zkChild += child;
          if (isEphemeral(zkClient, zkChild)) { // Don't copy ephemeral nodes
            continue;
          }
          // Go deeper into the tree now
          downloadFromZK(zkClient, zkChild, file.resolve(child));
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error downloading files from zookeeper path " + zkPath + " to " + file.toString(),
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

  // Get the parent path. This is really just the string before the last slash (/)
  // Will return empty string if there are no slashes.
  // Will return empty string if the path is just "/"
  // Will return empty string if the path is just ""
  public static String getZkParent(String path) {
    // Remove trailing slash if present.
    if (StringUtils.endsWith(path, "/")) {
      path = StringUtils.substringBeforeLast(path, "/");
    }
    if (StringUtils.contains(path, "/") == false) {
      return "";
    }
    return (StringUtils.substringBeforeLast(path, "/"));
  }

  // Take into account Windows file separators when making a Znode's name.
  // Used particularly when uploading configsets since the path we're copying
  // up may be a file path.
  public static String createZkNodeName(String zkRoot, Path root, Path file) {
    String relativePath = root.relativize(file).toString();
    // Windows shenanigans
    if ("\\".equals(File.separator))
      relativePath = relativePath.replaceAll("\\\\", "/");
    // It's possible that the relative path and file are the same, in which case
    // adding the bare slash is A Bad Idea unless it's a non-leaf data node
    boolean isNonLeafData = file.toFile().getName().equals(ZKNODE_DATA_FILE);
    if (relativePath.length() == 0 && isNonLeafData == false) return zkRoot;

    // Important to have this check if the source is file:whatever/ and the destination is just zk:/
    if (zkRoot.endsWith("/") == false) zkRoot += "/";

    String ret = zkRoot + relativePath;

    // Special handling for data associated with non-leaf node.
    if (isNonLeafData) {
      // special handling since what we need to do is add the data to the parent.
      ret = ret.substring(0, ret.indexOf(ZKNODE_DATA_FILE));
      if (ret.endsWith("/")) {
        ret = ret.substring(0, ret.length() - 1);
      }
    }
    return ret;
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
