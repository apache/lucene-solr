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
package org.apache.solr.cloud;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * <p>A ZK-based distributed queue. Optimized for single-consumer,
 * multiple-producer: if there are multiple consumers on the same ZK queue,
 * the results should be correct but inefficient.</p>
 *
 * <p>This implementation (with help from subclass {@link OverseerTaskQueue}) is used for the
 * <code>/overseer/collection-queue-work</code> queue used for Collection and Config Set API calls to the Overseer.</p>
 *
 * <p><i>Implementation note:</i> In order to enqueue a message into this queue, a {@link CreateMode#EPHEMERAL_SEQUENTIAL} response node is created
 * and watched at <code>/overseer/collection-queue-work/qnr-<i>monotonically_increasng_id</i></code>, then a corresponding
 * {@link CreateMode#PERSISTENT} request node reusing the same id is created at <code>/overseer/collection-queue-work/qn-<i>response_id</i></code>.</p>
 */
public class ZkDistributedQueue implements DistributedQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String PREFIX = "qn-";

  /**
   * Theory of operation:
   * <p>
   * Under ordinary circumstances we neither watch nor poll for children in ZK.
   * Instead we keep an in-memory list of known child names.  When the in-memory
   * list is exhausted, we then fetch from ZK.
   * <p>
   * We only bother setting a child watcher when the queue has no children in ZK.
   */
  private static final Object _IMPLEMENTATION_NOTES = null;
  public static final byte[] DATA = new byte[0];

  final String dir;

  final SolrZkClient zookeeper;

  final Stats stats;

  /**
   * A lock that guards all of the mutable state that follows.
   */
  private final ReentrantLock updateLock = new ReentrantLock();

  /**
   * Contains the last set of children fetched from ZK. Elements are removed from the head of
   * this in-memory set as they are consumed from the queue.  Due to the distributed nature
   * of the queue, elements may appear in this set whose underlying nodes have been consumed in ZK.
   * Therefore, methods like {@link #peek()} have to double-check actual node existence, and methods
   * like {@link #poll()} must resolve any races by attempting to delete the underlying node.
   */
  private TreeSet<String> knownChildren = new TreeSet<>();

  /**
   * Used to wait on ZK changes to the child list; you must hold {@link #updateLock} before waiting on this condition.
   */
  private final Condition changed = updateLock.newCondition();

  private boolean isDirty = true;

  private int watcherCount = 0;

  private final int maxQueueSize;

  /**
   * If {@link #maxQueueSize} is set, the number of items we can queue without rechecking the server.
   */
  private final AtomicInteger offerPermits = new AtomicInteger(0);

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Stats());
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats) {
    this(zookeeper, dir, stats, 0);
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats, int maxQueueSize) {
    this(zookeeper, dir, stats, maxQueueSize, null);
  }

  public ZkDistributedQueue(SolrZkClient zookeeper, String dir, Stats stats, int maxQueueSize, IsClosed higherLevelIsClosed) {
    this.dir = dir;

    this.zookeeper = zookeeper;
    this.stats = stats;
    this.maxQueueSize = maxQueueSize;
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   *
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek() throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty and block is false.
   *
   * @param block if true, blocks until an element enters the queue
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek(boolean block) throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty after wait ms.
   *
   * @param wait max wait time in ms.
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek(long wait) throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Attempts to remove the head of the queue and return it. Returns null if the
   * queue is empty.
   *
   * @return Head of the queue or null.
   */
  @Override
  public byte[] poll() throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Attempts to remove the head of the queue and return it.
   *
   * @return The former head of the queue
   */
  @Override
  public byte[] remove() throws NoSuchElementException, KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_remove");
    try {
      byte[] result = removeFirst();
      if (result == null) {
        throw new NoSuchElementException();
      }
      return result;
    } finally {
      time.stop();
    }
  }

//  public void remove(Collection<String> paths) throws KeeperException, InterruptedException {
//
//    if (log.isDebugEnabled()) log.debug("Remove paths from queue dir={} paths={}", dir, paths);
//
//    if (paths.isEmpty()) {
//      if (log.isDebugEnabled()) log.debug("paths is empty, return");
//      return;
//    }
//
//    List<String> fullPaths = new ArrayList<>(paths.size());
//    for (String node : paths) {
//      fullPaths.add(dir + "/" + node);
//    }
//
//
//    if (log.isDebugEnabled()) log.debug("delete nodes {}", fullPaths);
//    zookeeper.delete(fullPaths, false);
//    if (log.isDebugEnabled()) log.debug("after delete nodes");
//
//    int cacheSizeBefore = knownChildren.size();
//    knownChildren.removeAll(paths);
//    if (cacheSizeBefore - paths.size() == knownChildren.size() && knownChildren.size() != 0) {
//      stats.setQueueLength(knownChildren.size());
//    } else {
//      // There are elements get deleted but not present in the cache,
//      // the cache seems not valid anymore
//      knownChildren.clear();
//      isDirty = true;
//    }
//  }

  public void remove(Collection<String> paths) throws KeeperException, InterruptedException {
    if (paths.isEmpty()) return;
    List<Op> ops = new ArrayList<>();
    for (String path : paths) {
      ops.add(Op.delete(dir + "/" + path, -1));
    }
    for (int from = 0; from < ops.size(); from += 1000) {
      int to = Math.min(from + 1000, ops.size());
      if (from < to) {
        try {
          zookeeper.multi(ops.subList(from, to), true);
        } catch (KeeperException.NoNodeException e) {
          // don't know which nodes are not exist, so try to delete one by one node
          for (int j = from; j < to; j++) {
            try {
              zookeeper.delete(ops.get(j).getPath(), -1, true);
            } catch (KeeperException.NoNodeException e2) {
              if (log.isDebugEnabled()) {
                log.debug("Can not remove node which is not exist : {}", ops.get(j).getPath());
              }
            }
          }
        }
      }
    }

    int cacheSizeBefore = knownChildren.size();
    knownChildren.removeAll(paths);
    if (cacheSizeBefore - paths.size() == knownChildren.size() && knownChildren.size() != 0) {
      stats.setQueueLength(knownChildren.size());
    } else {
      // There are elements get deleted but not present in the cache,
      // the cache seems not valid anymore
      knownChildren.clear();
      isDirty = true;
    }
  }

  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   *
   * @return The former head of the queue
   */
  @Override
  public byte[] take() throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  private static Set<String> OPERATIONS = new HashSet<>();
  static {
    OPERATIONS.add("state");
    OPERATIONS.add("leader");
    OPERATIONS.add("downnode");
    OPERATIONS.add("updateshardstate");
  }

  /**
   * Inserts data into queue.  If there are no other queue consumers, the offered element
   * will be immediately visible when this method returns.
   */
  @Override
  public void offer(byte[] data) throws KeeperException, InterruptedException {
    // TODO - if too many items on the queue, just block
    zookeeper.create(dir + "/" + PREFIX, data, CreateMode.PERSISTENT_SEQUENTIAL, true);
    return;
  }

  public Stats getZkStats() {
    return stats;
  }

  @Override
  public Map<String, Object> getStats() {
    if (stats == null) {
      return Collections.emptyMap();
    }
    Map<String, Object> res = new HashMap<>();
    res.put("queueLength", stats.getQueueLength());
    final Map<String, Object> statsMap = new HashMap<>();
    res.put("stats", statsMap);
    stats.getStats().forEach((op, stat) -> {
      final Map<String, Object> statMap = new HashMap<>();
      statMap.put("success", stat.success.get());
      statMap.put("errors", stat.errors.get());
      final List<Map<String, Object>> failed = new ArrayList<>(stat.failureDetails.size());
      statMap.put("failureDetails", failed);
      stat.failureDetails.forEach(failedOp -> {
        Map<String, Object> fo = new HashMap<>();
        fo.put("req", failedOp.req);
        fo.put("resp", failedOp.resp);
      });
      statsMap.put(op, statMap);
    });
    return res;
  }

  /**
   * Returns the name if the first known child node, or {@code null} if the queue is empty.
   * This is the only place {@link #knownChildren} is ever updated!
   * The caller must double check that the actual node still exists, since the in-memory
   * list is inherently stale.
   */
  private String firstChild(boolean remove, boolean refetchIfDirty) throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the current set of children from ZK; does not change internal state.
   */
  TreeSet<String> fetchZkChildren(Watcher watcher) throws InterruptedException, KeeperException {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the currently-known set of elements, using child names from memory. If no children are found, or no
   * children pass {@code acceptFilter}, waits up to {@code waitMillis} for at least one child to become available.
   * <p>
   * Package-private to support {@link OverseerTaskQueue} specifically.</p>
   */
  @Override
  public Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  private byte[] removeFirst() throws KeeperException, InterruptedException {
    while (true) {
      String firstChild = firstChild(true, false);
      if (firstChild == null) {
        return null;
      }
      try {
        String path = dir + "/" + firstChild;
        byte[] result = zookeeper.getData(path, null, null, true);
        zookeeper.delete(path, -1, true);
        stats.setQueueLength(knownChildren.size());
        return result;
      } catch (KeeperException.NoNodeException e) {
        // Another client deleted the node first, remove the in-memory and retry.
        updateLock.lockInterruptibly();
        try {
          // Efficient only for single-consumer
          knownChildren.clear();
          isDirty = true;
        } finally {
          updateLock.unlock();
        }
      }
    }
  }
}
