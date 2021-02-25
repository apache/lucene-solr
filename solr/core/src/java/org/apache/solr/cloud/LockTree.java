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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.OverseerMessageHandler.Lock;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.LockLevel;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a utility class that offers fine grained locking for various Collection Operations
 * This class is designed for single threaded operation. It's safe for multiple threads to use it
 * but internally it is synchronized so that only one thread can perform any operation.
 */
public class LockTree {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Node root = new Node(null, LockLevel.CLUSTER, null);

  private class LockImpl implements Lock {
    final Node node;

    LockImpl( Node node) {
      this.node = node;
    }

    @Override
    public void unlock() {
      synchronized (LockTree.this) {
        node.unlock(this);
      }
    }

    @Override
    public String toString() {
      return StrUtils.join(node.constructPath(new LinkedList<>()), '/');
    }
  }


  /**
   * This class is used to mark nodes for which acquiring a lock was attempted but didn't succeed. Lock acquisition failure
   * needs to be "remembered" to trigger failures to acquire a competing lock until the Session is replaced, to prevent
   * tasks enqueued later (and dequeued later once the busy lock got released) from being executed before earlier tasks
   * that failed to execute because the lock wasn't available earlier when they attempted to acquire it.<p>
   *
   * A new Session is created each time the iteration over the queue tasks is restarted starting at the oldest non
   * running or completed tasks.
   */
  public class Session {
    private SessionNode root = new SessionNode(LockLevel.CLUSTER);

    public Lock lock(CollectionParams.CollectionAction action, List<String> path) {
      if (action.lockLevel == LockLevel.NONE) return FREELOCK;
      synchronized (LockTree.this) {
        if (root.isBusy(action.lockLevel, path)) return null;
        Lock lockObject = LockTree.this.root.lock(action.lockLevel, path);
        if (lockObject == null) root.markBusy(action.lockLevel, path);
        return lockObject;
      }
    }
  }

  private static class SessionNode {
    final LockLevel level;
    Map<String, SessionNode> kids;
    boolean busy = false;

    SessionNode(LockLevel level) {
      this.level = level;
    }

    /**
     * Marks busy the SessionNode corresponding to <code>lockLevel</code> (node names coming from <code>path</code>).
     * @param path contains at least <code>lockLevel.getHeight()</code> strings, capturing the names of the
     *             <code>SessionNode</code> being walked from the {@link Session#root} to the <code>SessionNode</code>
     *             that is to be marked busy.
     * @param lockLevel the level of the node that should be marked busy.
     */
    void markBusy(LockLevel lockLevel, List<String> path) {
      if (level == lockLevel) {
        // Lock is to be set on current node
        busy = true;
      } else {
        // Recursively create the required SessionNode subtree to capture lock being set on child node.
        String s = path.get(level.getHeight());
        if (kids == null) kids = new HashMap<>();
        SessionNode child = kids.get(s);
        if (child == null) kids.put(s, child = new SessionNode(level.getChild()));
        child.markBusy(lockLevel, path);
      }
    }

    boolean isBusy(LockLevel lockLevel, List<String> path) {
      if (lockLevel.isHigherOrEqual(level)) {
        if (busy) return true;
        String s = path.get(level.getHeight());
        if (kids == null || kids.get(s) == null) return false;
        return kids.get(s).isBusy(lockLevel, path);
      } else {
        return false;
      }
    }
  }

  public Session getSession() {
    return new Session();
  }

  private class Node {
    final String name;
    final Node mom;
    final LockLevel level;
    HashMap<String, Node> children = new HashMap<>();
    LockImpl myLock;

    Node(String name, LockLevel level, Node mom) {
      this.name = name;
      this.level = level;
      this.mom = mom;
    }

    //if this or any of its children are locked
    boolean isLocked() {
      if (myLock != null) return true;
      for (Node node : children.values()) if (node.isLocked()) return true;
      return false;
    }


    void unlock(LockImpl lockObject) {
      if (myLock == lockObject) myLock = null;
      else {
        log.info("Unlocked multiple times : {}", lockObject);
      }
    }


    Lock lock(LockLevel lockLevel, List<String> path) {
      if (myLock != null) return null;//I'm already locked. no need to go any further
      if (lockLevel == level) {
        //lock is supposed to be acquired at this level
        //If I am locked or any of my children or grandchildren are locked
        // it is not possible to acquire a lock
        if (isLocked()) return null;
        return myLock = new LockImpl(this);
      } else {
        String childName = path.get(level.getHeight());
        Node child = children.get(childName);
        if (child == null)
          children.put(childName, child = new Node(childName, level.getChild(), this));
        return child.lock(lockLevel, path);
      }
    }

    LinkedList<String> constructPath(LinkedList<String> collect) {
      if (name != null) collect.addFirst(name);
      if (mom != null) mom.constructPath(collect);
      return collect;
    }
  }
  static final Lock FREELOCK = () -> {};

}
