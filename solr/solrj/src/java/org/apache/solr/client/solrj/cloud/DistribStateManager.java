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
package org.apache.solr.client.solrj.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;

/**
 * This interface represents a distributed state repository.
 */
public interface DistribStateManager extends SolrCloseable {

  // state accessors

  boolean hasData(String path) throws IOException, KeeperException, InterruptedException;

  List<String> listData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException;

  List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException;

  VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException;

  default VersionedData getData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return getData(path, null);
  }

  // state mutators

  void makePath(String path) throws AlreadyExistsException, IOException, KeeperException, InterruptedException;

  void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException;

  /**
   * Create data (leaf) node at specified path.
   * @param path base path name of the node.
   * @param data data to be stored.
   * @param mode creation mode.
   * @return actual path of the node - in case of sequential nodes this will differ from the base path because
   * of the appended sequence number.
   */
  String createData(String path, byte[] data, CreateMode mode) throws AlreadyExistsException, IOException, KeeperException, InterruptedException;

  void removeData(String path, int version) throws NoSuchElementException, IOException, NotEmptyException, KeeperException, InterruptedException, BadVersionException;

  void setData(String path, byte[] data, int version) throws BadVersionException, NoSuchElementException, IOException, KeeperException, InterruptedException;

  List<OpResult> multi(final Iterable<Op> ops) throws BadVersionException, NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException;

  AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException;

  default AutoScalingConfig getAutoScalingConfig() throws InterruptedException, IOException {
    return getAutoScalingConfig(null);
  }

  /**
   * List a subtree including the root path, using breadth-first traversal.
   * @param root root path
   * @return list of full paths, with the root path being the first element
   */
  default List<String> listTree(String root) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    Deque<String> queue = new LinkedList<String>();
    List<String> tree = new ArrayList<String>();
    if (!root.startsWith("/")) {
      root = "/" + root;
    }
    queue.add(root);
    tree.add(root);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children = listData(node);
      for (final String child : children) {
        final String childPath = node + (node.equals("/") ? "" : "/") + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

  default PerReplicaStates getReplicaStates(String path) throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException("Not implemented");


  }

  /**
   * Remove data recursively.
   * @param root root path
   * @param ignoreMissing ignore errors if root or any children path is missing
   * @param includeRoot when true delete also the root path
   */
  default void removeRecursively(String root, boolean ignoreMissing, boolean includeRoot) throws NoSuchElementException, IOException, NotEmptyException, KeeperException, InterruptedException, BadVersionException {
    List<String> tree;
    try {
      tree = listTree(root);
    } catch (NoSuchElementException e) {
      if (ignoreMissing) {
        return;
      } else {
        throw e;
      }
    }
    Collections.reverse(tree);
    for (String p : tree) {
      if (p.equals("/")) {
        continue;
      }
      if (p.equals(root) && !includeRoot) {
        continue;
      }
      try {
        removeData(p, -1);
      } catch (NoSuchElementException e) {
        if (!ignoreMissing) {
          throw e;
        }
      }
    }
  }
}
