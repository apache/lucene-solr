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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.util.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed queue.
 */
public class DistributedQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

  final Overseer.Stats stats;

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

  /**
   * If non-null, the last watcher to listen for child changes.  If null, the in-memory contents are dirty.
   */
  private ChildWatcher lastWatcher = null;

  public DistributedQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Overseer.Stats());
  }

  public DistributedQueue(SolrZkClient zookeeper, String dir, Overseer.Stats stats) {
    this.dir = dir;

    ZkCmdExecutor cmdExecutor = new ZkCmdExecutor(zookeeper.getZkClientTimeout());
    try {
      cmdExecutor.ensureExists(dir, zookeeper);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    this.zookeeper = zookeeper;
    this.stats = stats;
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   *
   * @return data at the first element of the queue, or null.
   */
  public byte[] peek() throws KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_peek");
    try {
      return firstElement();
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
  public byte[] peek(boolean block) throws KeeperException, InterruptedException {
    return block ? peek(Long.MAX_VALUE) : peek();
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty after wait ms.
   *
   * @param wait max wait time in ms.
   * @return data at the first element of the queue, or null.
   */
  public byte[] peek(long wait) throws KeeperException, InterruptedException {
    Preconditions.checkArgument(wait > 0);
    Timer.Context time;
    if (wait == Long.MAX_VALUE) {
      time = stats.time(dir + "_peek_wait_forever");
    } else {
      time = stats.time(dir + "_peek_wait" + wait);
    }
    updateLock.lockInterruptibly();
    try {
      long waitNanos = TimeUnit.MILLISECONDS.toNanos(wait);
      while (waitNanos > 0) {
        byte[] result = firstElement();
        if (result != null) {
          return result;
        }
        waitNanos = changed.awaitNanos(waitNanos);
      }
      return null;
    } finally {
      updateLock.unlock();
      time.stop();
    }
  }

  /**
   * Attempts to remove the head of the queue and return it. Returns null if the
   * queue is empty.
   *
   * @return Head of the queue or null.
   */
  public byte[] poll() throws KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_poll");
    try {
      return removeFirst();
    } finally {
      time.stop();
    }
  }

  /**
   * Attempts to remove the head of the queue and return it.
   *
   * @return The former head of the queue
   */
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

  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   *
   * @return The former head of the queue
   */
  public byte[] take() throws KeeperException, InterruptedException {
    // Same as for element. Should refactor this.
    Timer.Context timer = stats.time(dir + "_take");
    updateLock.lockInterruptibly();
    try {
      while (true) {
        byte[] result = removeFirst();
        if (result != null) {
          return result;
        }
        changed.await();
      }
    } finally {
      updateLock.unlock();
      timer.stop();
    }
  }

  /**
   * Inserts data into queue.  Successfully calling this method does NOT guarantee
   * that the element will be immediately available in the in-memory queue. In particular,
   * calling this method on an empty queue will not necessarily cause {@link #poll()} to
   * return the offered element.  Use a blocking method if you must wait for the offered
   * element to become visible.
   */
  public void offer(byte[] data) throws KeeperException, InterruptedException {
    Timer.Context time = stats.time(dir + "_offer");
    try {
      while (true) {
        try {
          // We don't need to explicitly set isDirty here; if there is a watcher, it will
          // see the update and set the bit itself; if there is no watcher we can defer
          // the update anyway.
          zookeeper.create(dir + "/" + PREFIX, data, CreateMode.PERSISTENT_SEQUENTIAL, true);
          return;
        } catch (KeeperException.NoNodeException e) {
          try {
            zookeeper.create(dir, new byte[0], CreateMode.PERSISTENT, true);
          } catch (KeeperException.NodeExistsException ne) {
            // someone created it
          }
        }
      }
    } finally {
      time.stop();
    }
  }

  public Overseer.Stats getStats() {
    return stats;
  }

  /**
   * Returns the name if the first known child node, or {@code null} if the queue is empty.
   * This is the only place {@link #knownChildren} is ever updated!
   * The caller must double check that the actual node still exists, since the in-memory
   * list is inherently stale.
   */
  private String firstChild(boolean remove) throws KeeperException, InterruptedException {
    updateLock.lockInterruptibly();
    try {
      // If we're not in a dirty state, and we have in-memory children, return from in-memory.
      if (lastWatcher != null && !knownChildren.isEmpty()) {
        return remove ? knownChildren.pollFirst() : knownChildren.first();
      }

      // Try to fetch an updated list of children from ZK.
      ChildWatcher newWatcher = new ChildWatcher();
      knownChildren = fetchZkChildren(newWatcher);
      lastWatcher = newWatcher; // only set after fetchZkChildren returns successfully
      if (knownChildren.isEmpty()) {
        return null;
      }
      changed.signalAll();
      return remove ? knownChildren.pollFirst() : knownChildren.first();
    } finally {
      updateLock.unlock();
    }
  }

  /**
   * Return the current set of children from ZK; does not change internal state.
   */
  TreeSet<String> fetchZkChildren(Watcher watcher) throws InterruptedException, KeeperException {
    while (true) {
      try {
        TreeSet<String> orderedChildren = new TreeSet<>();

        List<String> childNames = zookeeper.getChildren(dir, watcher, true);
        stats.setQueueLength(childNames.size());
        for (String childName : childNames) {
          // Check format
          if (!childName.regionMatches(0, PREFIX, 0, PREFIX.length())) {
            LOG.debug("Found child node with improper name: " + childName);
            continue;
          }
          orderedChildren.add(childName);
        }
        return orderedChildren;
      } catch (KeeperException.NoNodeException e) {
        zookeeper.makePath(dir, false, true);
        // go back to the loop and try again
      }
    }
  }

  /**
   * Return the currently-known set of elements, using child names from memory. If no children are found, or no
   * children pass {@code acceptFilter}, waits up to {@code waitMillis} for at least one child to become available.
   * <p/>
   * Package-private to support {@link OverseerTaskQueue} specifically.
   */
  Collection<Pair<String, byte[]>> peekElements(int max, long waitMillis, Predicate<String> acceptFilter) throws KeeperException, InterruptedException {
    List<String> foundChildren = new ArrayList<>();
    long waitNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
    while (true) {
      // Trigger a fetch if needed.
      firstChild(false);

      updateLock.lockInterruptibly();
      try {
        for (String child : knownChildren) {
          if (acceptFilter.test(child)) {
            foundChildren.add(child);
          }
        }
        if (!foundChildren.isEmpty()) {
          break;
        }
        if (waitNanos <= 0) {
          break;
        }
        waitNanos = changed.awaitNanos(waitNanos);
      } finally {
        updateLock.unlock();
      }

      if (!foundChildren.isEmpty()) {
        break;
      }
    }

    // Technically we could restart the method if we fail to actually obtain any valid children
    // from ZK, but this is a super rare case, and the latency of the ZK fetches would require
    // much more sophisticated waitNanos tracking.
    List<Pair<String, byte[]>> result = new ArrayList<>();
    for (String child : foundChildren) {
      if (result.size() >= max) {
        break;
      }
      try {
        byte[] data = zookeeper.getData(dir + "/" + child, null, null, true);
        result.add(new Pair<>(child, data));
      } catch (KeeperException.NoNodeException e) {
        // Another client deleted the node first, remove the in-memory and continue.
        updateLock.lockInterruptibly();
        try {
          knownChildren.remove(child);
        } finally {
          updateLock.unlock();
        }
      }
    }
    return result;
  }

  /**
   * Return the head of the queue without modifying the queue.
   *
   * @return the data at the head of the queue.
   */
  private byte[] firstElement() throws KeeperException, InterruptedException {
    while (true) {
      String firstChild = firstChild(false);
      if (firstChild == null) {
        return null;
      }
      try {
        return zookeeper.getData(dir + "/" + firstChild, null, null, true);
      } catch (KeeperException.NoNodeException e) {
        // Another client deleted the node first, remove the in-memory and retry.
        updateLock.lockInterruptibly();
        try {
          knownChildren.remove(firstChild);
        } finally {
          updateLock.unlock();
        }
      }
    }
  }

  private byte[] removeFirst() throws KeeperException, InterruptedException {
    while (true) {
      String firstChild = firstChild(true);
      if (firstChild == null) {
        return null;
      }
      try {
        String path = dir + "/" + firstChild;
        byte[] result = zookeeper.getData(path, null, null, true);
        zookeeper.delete(path, -1, true);
        return result;
      } catch (KeeperException.NoNodeException e) {
        // Another client deleted the node first, remove the in-memory and retry.
        updateLock.lockInterruptibly();
        try {
          knownChildren.remove(firstChild);
        } finally {
          updateLock.unlock();
        }
      }
    }
  }

  @VisibleForTesting boolean hasWatcher() throws InterruptedException {
    updateLock.lockInterruptibly();
    try {
      return lastWatcher != null;
    } finally {
      updateLock.unlock();
    }
  }

  private class ChildWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher; except for Expired
      if (Event.EventType.None.equals(event.getType()) && !Event.KeeperState.Expired.equals(event.getState())) {
        return;
      }
      updateLock.lock();
      try {
        // this watcher is automatically cleared when fired
        if (lastWatcher == this) {
          lastWatcher = null;
        }
        // optimistically signal any waiters that the queue may not be empty now, so they can wake up and retry
        changed.signalAll();
      } finally {
        updateLock.unlock();
      }
    }
  }
}
