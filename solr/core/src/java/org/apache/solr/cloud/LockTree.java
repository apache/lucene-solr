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

  public void clear() {
    synchronized (this) {
      root.clear();
    }
  }

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


  public class Session {
    private SessionNode root = new SessionNode(LockLevel.CLUSTER);

    public Lock lock(CollectionParams.CollectionAction action, List<String> path) {
      synchronized (LockTree.this) {
        if (action.lockLevel == LockLevel.NONE) return FREELOCK;
        if (root.isBusy(action.lockLevel, path)) return null;
        Lock lockObject = LockTree.this.root.lock(action.lockLevel, path);
        if (lockObject == null) root.markBusy(path, 0);
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

    void markBusy(List<String> path, int depth) {
      if (path.size() == depth) {
        busy = true;
      } else {
        String s = path.get(depth);
        if (kids == null) kids = new HashMap<>();
        SessionNode node = kids.get(s);
        if (node == null) kids.put(s, node = new SessionNode(level.getChild()));
        node.markBusy(path, depth + 1);
      }
    }

    boolean isBusy(LockLevel lockLevel, List<String> path) {
      if (lockLevel.isHigherOrEqual(level)) {
        if (busy) return true;
        String s = path.get(level.level);
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
        String childName = path.get(level.level);
        Node child = children.get(childName);
        if (child == null)
          children.put(childName, child = new Node(childName, LockLevel.getLevel(level.level + 1), this));
        return child.lock(lockLevel, path);
      }
    }

    LinkedList<String> constructPath(LinkedList<String> collect) {
      if (name != null) collect.addFirst(name);
      if (mom != null) mom.constructPath(collect);
      return collect;
    }

    void clear() {
      if (myLock != null) {
        log.warn("lock_is_leaked at {}", constructPath(new LinkedList<>()));
        myLock = null;
      }
      for (Node node : children.values()) node.clear();
    }
  }
  static final Lock FREELOCK = () -> {};

}
