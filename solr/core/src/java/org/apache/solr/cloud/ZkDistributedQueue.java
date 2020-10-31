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

import java.io.File;
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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  final String dir;

  final SolrZkClient zookeeper;

  final Stats stats;

  /**
   * A lock that guards all of the mutable state that follows.
   */
  protected final ReentrantLock updateLock = new ReentrantLock();

  /**
   * Contains the last set of children fetched from ZK. Elements are removed from the head of
   * this in-memory set as they are consumed from the queue.  Due to the distributed nature
   * of the queue, elements may appear in this set whose underlying nodes have been consumed in ZK.
   * Therefore, methods like {@link #peek(Predicate<String>)} have to double-check actual node existence, and methods
   * like {@link #poll(Predicate<String>)} must resolve any races by attempting to delete the underlying node.
   */
  protected volatile TreeMap<String,byte[]> knownChildren;

  /**
   * Used to wait on ZK changes to the child list; you must hold {@link #updateLock} before waiting on this condition.
   */
  private final Condition changed = updateLock.newCondition();

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

    try {
      try {
        updateLock.lockInterruptibly();
        fetchZkChildren(null, null);
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    } finally {
      if (updateLock.isHeldByCurrentThread()) {
        updateLock.unlock();
      }
    }
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   *
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek(Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_peek");
    try {
      return firstElement(acceptFilter);
    } finally {
      time.stop();
    }
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty and block is false.
   *
   * @param block if true, blocks until an element enters the queue
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek(Predicate<String> acceptFilter, boolean block) throws KeeperException, InterruptedException {
    return block ? peek(acceptFilter, Long.MAX_VALUE) : peek(acceptFilter);
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty after wait ms.
   *
   * @param wait max wait time in ms.
   * @return data at the first element of the queue, or null.
   */
  @Override
  public byte[] peek(Predicate<String> acceptFilter, long wait) throws KeeperException, InterruptedException {
    byte[] result = null;

    Timer.Context time;
    if (wait == Long.MAX_VALUE) {
      time = stats.time(dir + "_peek_wait_forever");
    } else {
      time = stats.time(dir + "_peek_wait" + wait);
    }

    long waitNanos = TimeUnit.MILLISECONDS.toNanos(wait);

    result = firstElement(acceptFilter);
    if (result != null) {
      return result;
    }

    ChildWatcher watcher = new ChildWatcher();
    TreeMap<String,byte[]> foundChildren = fetchZkChildren(watcher, null);

    if (foundChildren.size() > 0) {
      result = firstElement(acceptFilter);
      return result;
    }

    TimeOut timeout = new TimeOut(waitNanos, TimeUnit.NANOSECONDS, TimeSource.NANO_TIME);

    waitForChildren(null, foundChildren, timeout, watcher);
    if (foundChildren.size() == 0) {
      return null;
    }
    result = firstElement(acceptFilter);
    return result;

  }

  /**
   * Attempts to remove the head of the queue and return it. Returns null if the
   * queue is empty.
   *
   * @return Head of the queue or null.
   */
  @Override
  public byte[] poll(Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_poll");
    try {
      return removeFirst(acceptFilter);
    } finally {
      time.stop();
    }
  }

  /**
   * Attempts to remove the head of the queue and return it.
   *
   * @return The former head of the queue
   */
  @Override
  public byte[] remove(Predicate<String> acceptFilter) throws NoSuchElementException, KeeperException{
    Timer.Context time = stats.time(dir + "_remove");
    try {
      byte[] result = removeFirst(acceptFilter);
      if (result == null) {
        throw new NoSuchElementException();
      }
      return result;
    } finally {
      time.stop();
    }
  }

  // TODO: use async
  public void remove(Collection<String> paths) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("Remove paths from queue {} {}", dir, paths);
    if (paths.isEmpty()) return;

    updateLock.lockInterruptibly();
    try {
      for (String path : paths) {
        knownChildren.remove(path);
      }
    } finally {
      if (updateLock.isHeldByCurrentThread()) {
        updateLock.unlock();
      }
    }

    List<Op> ops = new ArrayList<>();
    for (String path : paths) {
      ops.add(Op.delete(dir + "/" + path, -1));
    }
    for (int from = 0; from < ops.size(); from += 1000) {
      int to = Math.min(from + 1000, ops.size());
      List<Op> opList = ops.subList(from, to);
      if (opList.size() > 0) {
        try {
          zookeeper.multi(ops.subList(from, to));
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
  }

  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   *
   * @return The former head of the queue
   */
  @Override
  public byte[] take(Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    // Same as for element. Should refactor this.
    Timer.Context timer = stats.time(dir + "_take");
    updateLock.lockInterruptibly();
    try {
      long waitNanos = TimeUnit.MILLISECONDS.toNanos(60000);

      byte[] result = removeFirst(acceptFilter);
      if (result != null) {
        return result;
      }

      ChildWatcher watcher = new ChildWatcher();
      TreeMap<String,byte[]> foundChildren = fetchZkChildren(watcher, acceptFilter);

      TimeOut timeout = new TimeOut(waitNanos, TimeUnit.NANOSECONDS, TimeSource.NANO_TIME);

      waitForChildren( s -> s.startsWith(PREFIX) || acceptFilter.test(s), foundChildren, timeout, watcher);
      if (foundChildren.size() == 0) {
        return null;
      }

      result = removeFirst(acceptFilter);
      if (result != null) {
        return result;
      }
    } finally {
      if (updateLock.isHeldByCurrentThread()) {
        updateLock.unlock();
      }
      timer.stop();
    }
    return null;
  }

  /**
   * Inserts data into queue.  If there are no other queue consumers, the offered element
   * will be immediately visible when this method returns.
   */
  @Override
  public void offer(byte[] data) throws KeeperException {
    Timer.Context time = stats.time(dir + "_offer");
    if (log.isDebugEnabled()) log.debug("Over item to queue {}", dir);
    try {
      try {
        if (maxQueueSize > 0) {
          if (offerPermits.get() <= 0 || offerPermits.getAndDecrement() <= 0) {
            // If a max queue size is set, check it before creating a new queue item.
            Stat stat = zookeeper.exists(dir, null, true);
            if (stat == null) {
              // jump to the code below, which tries to create dir if it doesn't exist
              throw new KeeperException.NoNodeException();
            }
            int remainingCapacity = maxQueueSize - stat.getNumChildren();
            if (remainingCapacity <= 0) {
              throw new IllegalStateException("queue is full");
            }

            // Allow this client to push up to 1% of the remaining queue capacity without rechecking.
            offerPermits.set(remainingCapacity / 100);
          }
        }

        // Explicitly set isDirty here so that synchronous same-thread calls behave as expected.
        // This will get set again when the watcher actually fires, but that's ok.
        zookeeper.create(dir + "/" + PREFIX, data, CreateMode.PERSISTENT_SEQUENTIAL, true);
        return;
      } catch (KeeperException.NoNodeException e) {
        // someone created it
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new AlreadyClosedException(e);
      }

    } finally {
      time.stop();
    }
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
   * @return
   */
  private Map.Entry<String,byte[]> firstChild(boolean remove, Predicate<String> acceptFilter) {
    try {
      updateLock.lockInterruptibly();
      try {
        // We always return from cache first, the cache will be cleared if the node is not exist
        if (!knownChildren.isEmpty()) {
          for (Map.Entry<String,byte[]> entry : knownChildren.entrySet()) {
            if (acceptFilter != null && acceptFilter.test(entry.getKey())) {
              continue;
            }
            if (remove) {
              knownChildren.remove(entry.getKey());
              try {
                zookeeper.delete(dir + "/" + entry.getKey(), -1);
              } catch (KeeperException.NoNodeException e) {
                if (log.isDebugEnabled()) log.debug("No node found for {}", entry.getKey());
              }
              return entry;
            }
          }
          return null;
        }
      } finally {
        if (updateLock.isHeldByCurrentThread()) {
          updateLock.unlock();
        }
      }

      fetchZkChildren(null, acceptFilter);

      return null;
    } catch (InterruptedException | KeeperException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }
  }

  /**
   * Return the current set of children from ZK; does not change internal state.
   */
  TreeMap<String,byte[]> fetchZkChildren(Watcher watcher, Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("fetchZkChildren");
    TreeMap<String,byte[]> orderedChildren = new TreeMap<>();
    updateLock.lockInterruptibly();
    try {
      if (knownChildren != null && !knownChildren.isEmpty()) {
        Set<Map.Entry<String,byte[]>> entrySet = knownChildren.entrySet();
        for (Map.Entry<String,byte[]> entry : entrySet) {
          String childName = entry.getKey();
          // Check format
          if (!childName.startsWith(PREFIX)) {

            // responses can be written to same queue with different naming scheme
            if (log.isDebugEnabled()) log.debug("Filtering child out by prefix name=: {}, prefix={}", childName, PREFIX);
            continue;
          }
          if (acceptFilter != null && acceptFilter.test(childName)) {
            if (log.isDebugEnabled()) log.debug("Found child that matched exclude filter: {}", dir + "/" + childName);
            continue;
          }
          if (log.isDebugEnabled()) log.debug("found: {}", childName);
          orderedChildren.put(childName, entry.getValue());
        }
      }
    } finally {
      if (updateLock.isHeldByCurrentThread()) {
        updateLock.unlock();
      }
    }

    if (!orderedChildren.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("found children from fetch {}", orderedChildren.size());
      return orderedChildren;
    }
    if (log.isDebugEnabled()) log.debug("found no children to fetch");
    TreeMap<String,byte[]> remoteKnownChildren = new TreeMap<>();
    try {
      List<String> childNames = zookeeper.getChildren(dir, watcher, true);
      stats.setQueueLength(childNames.size());
      for (String childName : childNames) {
        if (log.isDebugEnabled()) log.debug("Examine child: {} out of children={} acceptFilter={}", childName, childNames.size(), acceptFilter);
        // Check format
        if (!childName.startsWith(PREFIX)) {
          // responses can be written to same queue with different naming scheme
          if (log.isDebugEnabled()) log.debug("Excluding child by prefix: {}", childName);
          continue;
        }
        remoteKnownChildren.put(childName, null);
        if (acceptFilter != null && acceptFilter.test(childName)) {
          if (log.isDebugEnabled()) log.debug("Found child that matched exclude filter: {}", childName);
          continue;
        }
        if (log.isDebugEnabled()) log.debug("Add child to fetched children: {}", childName);
        orderedChildren.put(childName, null);
      }
      if (log.isDebugEnabled()) log.debug("found {} remote children", remoteKnownChildren.size());
      updateKnownChildren(remoteKnownChildren);
      if (log.isDebugEnabled()) log.debug("returning {} matched children", orderedChildren.size());
      return orderedChildren;
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }
  }

  private void updateKnownChildren(TreeMap<String,byte[]> children) {
    if (log.isDebugEnabled()) log.debug("Update known children size={}", children.size());
    TreeMap<String,byte[]> newKnownChildren = new TreeMap<>();
    updateLock.lock();
    try {
      Set<Map.Entry<String,byte[]>> entrySet = children.entrySet();
      for (Map.Entry<String,byte[]> entry : entrySet) {
        String childName = entry.getKey();
        byte[] data = entry.getValue();
        if (data == null) {
          data = knownChildren == null ? null : knownChildren.get(childName);
        }
        newKnownChildren.put(childName, data);
      }
      knownChildren = newKnownChildren;
    } catch (Exception e) {
      log.error("", e);
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Return the currently-known set of elements, using child names from memory. If no children are found, or no
   * children pass {@code acceptFilter}, waits up to {@code waitMillis} for at least one child to become available.
   * <p>
   * Package-private to support {@link OverseerTaskQueue} specifically.</p>
   */
  @Override
  public Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("peekElements {} {}", max, acceptFilter);
    List<Pair<String,byte[]>> result = null;
    ChildWatcher watcher = new ChildWatcher();
    TreeMap<String,byte[]> foundChildren = fetchZkChildren(watcher, acceptFilter);
    long waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
    TimeOut timeout = new TimeOut(waitNanos, TimeUnit.NANOSECONDS, TimeSource.NANO_TIME);
    try {
      if (foundChildren.size() == 0) {
        if (log.isDebugEnabled()) log.debug("found no children, watch for them  excludeFilter={}", acceptFilter);
        waitForChildren(acceptFilter, foundChildren, timeout, watcher);
      }

      // Technically we could restart the method if we fasil to actually obtain any valid children
      // from ZK, but this is a super rare case, and the latency of the ZK fetches would require
      // much more sophisticated waitNanos tracking.
      if (log.isDebugEnabled()) log.debug("found children to process {}", foundChildren.size());
      result = Collections.synchronizedList(new ArrayList<>(foundChildren.size()));
      Set<String> dataPaths = new HashSet<>();
      for (Map.Entry<String,byte[]> child : foundChildren.entrySet()) {
        if (result.size() >= max) {
          break;
        }

        byte[] data = child.getValue();
        if (data == null) {
          updateLock.lockInterruptibly();
          try {
            data = knownChildren.get(child.getKey());
          } finally {
            if (updateLock.isHeldByCurrentThread()) {
              updateLock.unlock();
            }
          }
        }

        if (data == null) {
          // nocommit - lets not reget what we have in knownChildren, also, use asyncp
          dataPaths.add(dir + "/" + child.getKey());
          if (log.isDebugEnabled()) log.debug("get data for child={}", child.getKey());
        } else {
          if (log.isDebugEnabled()) log.debug("found data locally already {}", child.getKey());
          result.add(new Pair<>(child.getKey(), data));
        }
      }

      if (log.isDebugEnabled()) log.debug("fetch data for paths {}", dataPaths);
      Map<String,byte[]> dataMap = zookeeper.getData(dataPaths);
      updateLock.lockInterruptibly();
      List<Pair<String,byte[]>> finalResult = result;
      try {
        dataMap.forEach((k, bytes) -> {
          finalResult.add(new Pair<>(k, bytes));
          if (bytes != null) {
            knownChildren.put(new File(k).getName(), bytes);
          }
        });
      } finally {
        if (updateLock.isHeldByCurrentThread()) {
          updateLock.unlock();
        }
      }
      if (log.isDebugEnabled()) log.debug("peek elements returning {} nodes", finalResult.size());
      return new ArrayList<>(finalResult);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }
  }

  private void waitForChildren(Predicate<String> acceptFilter, TreeMap<String,byte[]> foundChildren, TimeOut timeout, ChildWatcher watcher) throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) log.debug("wait for children ... {}ms", timeout.getInterval(TimeUnit.MILLISECONDS));

    updateLock.lockInterruptibly();
    try {
      for (Map.Entry<String,byte[]> child : knownChildren.entrySet()) {
        if (!child.getKey().startsWith(PREFIX) && (acceptFilter == null || !acceptFilter.test(child.getKey()))) {
          foundChildren.put(child.getKey(), child.getValue());
        }
      }
    } finally {
      if (updateLock.isHeldByCurrentThread()) {
        updateLock.unlock();
      }
    }
    if (!foundChildren.isEmpty()) {
      if (log.isDebugEnabled()) log.debug("Found new children ... {}", foundChildren.size());
      return;
    }
    if (timeout.hasTimedOut()) {
      if (log.isDebugEnabled()) log.debug("0 wait time and no children found, return");
      return;
    }
    TreeMap<String,byte[]> fc = null;
    while (fc == null || fc.isEmpty()) {
      fc = fetchZkChildren(watcher, acceptFilter);
      if (!fc.isEmpty()) {
        foundChildren.putAll(fc);
        return;
      }
      updateLock.lockInterruptibly();
      try {
        try {
          changed.await(Math.min(timeout.getInterval(TimeUnit.MILLISECONDS), 5000), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
        }
        if (zookeeper.isClosed() || !zookeeper.isConnected()) {
          throw new AlreadyClosedException();
        }
        if (timeout.hasTimedOut()) {
          return;
        }
        for (Map.Entry<String,byte[]> child : knownChildren.entrySet()) {
          if (acceptFilter == null || !acceptFilter.test(child.getKey())) {
            foundChildren.put(child.getKey(), child.getValue());
          }
        }
        if (!foundChildren.isEmpty()) {
          try {
            if (log.isDebugEnabled()) log.debug("Remove watches for {}");
            zookeeper.getSolrZooKeeper().removeAllWatches(dir, Watcher.WatcherType.Children, false);
          } catch (Exception e) {
            log.info(e.getMessage());
          }
          return;
        }

      } finally {
        if (updateLock.isHeldByCurrentThread()) {
          updateLock.unlock();
        }
      }
    }
  }

  /**
   * Return the head of the queue without modifying the queue.
   *
   * @return the data at the head of the queue.
   */
  private byte[] firstElement(Predicate<String> acceptFilter) throws KeeperException {
    try {

      Map.Entry<String,byte[]> firstChild = firstChild(false, acceptFilter);
      if (firstChild == null) {
        return null;
      }
      byte[] data;
      updateLock.lockInterruptibly();
      try {
        data = knownChildren.get(firstChild.getKey());
        if (data != null) {
          return data;
        }
      } finally {
        if (updateLock.isHeldByCurrentThread()) {
          updateLock.unlock();
        }
      }

      try {
        data = zookeeper.getData(dir + "/" + firstChild.getKey(), null, null, true);
        if (data != null) {
          updateLock.lockInterruptibly();
          try {
            knownChildren.put(firstChild.getKey(), data);
          } finally {
            if (updateLock.isHeldByCurrentThread()) {
              updateLock.unlock();
            }
          }
        }

        return data;
      } catch (KeeperException.NoNodeException e) {
        updateLock.lockInterruptibly();
        try {
          knownChildren.remove(firstChild.getKey());
        } finally {
          if (updateLock.isHeldByCurrentThread()) {
            updateLock.unlock();
          }
        }
        return null;
      }

    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }
  }

  private byte[] removeFirst(Predicate<String> acceptFilter) throws KeeperException {
    try {
      Map.Entry<String,byte[]> firstChild = firstChild(true, acceptFilter);
      if (firstChild == null) {
        return null;
      }

      byte[] data;
      updateLock.lockInterruptibly();
      try {
        data = knownChildren.remove(firstChild.getKey());
      } finally {
        if (updateLock.isHeldByCurrentThread()) {
          updateLock.unlock();
        }
      }

      try {
        String path = dir + "/" + firstChild.getKey();
        if (data == null) {
          data = zookeeper.getData(path, null, null, true);
        }
        zookeeper.delete(path, -1, true);
        updateLock.lockInterruptibly();
        try {
          knownChildren.remove(firstChild.getKey());

        } finally {
          if (updateLock.isHeldByCurrentThread()) {
            updateLock.unlock();
          }
        }
        return data;
      } catch (KeeperException.NoNodeException e) {
        return null;
      }
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException(e);
    }
  }

  @VisibleForTesting long watcherCount() throws InterruptedException {
    return 0;
  }

  @VisibleForTesting class ChildWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher; except for Expired
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (log.isDebugEnabled()) log.debug("DistributedQueue changed {} {}", event.getPath(), event.getType());

      // nocommit - all the nodes are watching this currently instead of just the Overseer
      if (event.getType() == Event.EventType.NodeChildrenChanged) {
        updateLock.lock();
        try {
          fetchZkChildren(null, null);
          changed.signalAll();
        } catch (KeeperException | InterruptedException e) {
          log.error("", e);
        } finally {
          updateLock.unlock();
        }
      } else {

      }
    }
  }
}
