/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed queue from zk recipes.
 */
public class DistributedQueue {
  private static final Logger LOG = LoggerFactory
      .getLogger(DistributedQueue.class);
  
  private static long DEFAULT_TIMEOUT = 5*60*1000;
  
  private final String dir;
  
  private SolrZkClient zookeeper;
  private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  
  private final String prefix = "qn-";
  
  private final String response_prefix = "qnr-" ;
  
  public DistributedQueue(SolrZkClient zookeeper, String dir, List<ACL> acl) {
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
    
    if (acl != null) {
      this.acl = acl;
    }
    this.zookeeper = zookeeper;
    
  }
  
  /**
   * Returns a Map of the children, ordered by id.
   * 
   * @param watcher
   *          optional watcher on getChildren() operation.
   * @return map from id to child name for all children
   */
  private TreeMap<Long,String> orderedChildren(Watcher watcher)
      throws KeeperException, InterruptedException {
    TreeMap<Long,String> orderedChildren = new TreeMap<Long,String>();
    
    List<String> childNames = null;
    try {
      childNames = zookeeper.getChildren(dir, watcher, true);
    } catch (KeeperException.NoNodeException e) {
      throw e;
    }
    
    for (String childName : childNames) {
      try {
        // Check format
        if (!childName.regionMatches(0, prefix, 0, prefix.length())) {
          LOG.debug("Found child node with improper name: " + childName);
          continue;
        }
        String suffix = childName.substring(prefix.length());
        Long childId = new Long(suffix);
        orderedChildren.put(childId, childName);
      } catch (NumberFormatException e) {
        LOG.warn("Found child node with improper format : " + childName + " "
            + e, e);
      }
    }
    
    return orderedChildren;
  }
  
  /**
   * Return the head of the queue without modifying the queue.
   * 
   * @return the data at the head of the queue.
   */
  private QueueEvent element() throws NoSuchElementException, KeeperException,
      InterruptedException {
    TreeMap<Long,String> orderedChildren;
    
    // element, take, and remove follow the same pattern.
    // We want to return the child node with the smallest sequence number.
    // Since other clients are remove()ing and take()ing nodes concurrently,
    // the child with the smallest sequence number in orderedChildren might be
    // gone by the time we check.
    // We don't call getChildren again until we have tried the rest of the nodes
    // in sequence order.
    while (true) {
      try {
        orderedChildren = orderedChildren(null);
      } catch (KeeperException.NoNodeException e) {
        throw new NoSuchElementException();
      }
      if (orderedChildren.size() == 0) throw new NoSuchElementException();
      
      for (String headNode : orderedChildren.values()) {
        if (headNode != null) {
          try {
            return new QueueEvent(dir + "/" + headNode, zookeeper.getData(dir + "/" + headNode, null, null, true), null);
          } catch (KeeperException.NoNodeException e) {
            // Another client removed the node first, try next
          }
        }
      }
    }
  }
  
  /**
   * Attempts to remove the head of the queue and return it.
   * 
   * @return The former head of the queue
   */
  public byte[] remove() throws NoSuchElementException, KeeperException,
      InterruptedException {
    TreeMap<Long,String> orderedChildren;
    // Same as for element. Should refactor this.
    while (true) {
      try {
        orderedChildren = orderedChildren(null);
      } catch (KeeperException.NoNodeException e) {
        throw new NoSuchElementException();
      }
      if (orderedChildren.size() == 0) throw new NoSuchElementException();
      
      for (String headNode : orderedChildren.values()) {
        String path = dir + "/" + headNode;
        try {
          byte[] data = zookeeper.getData(path, null, null, true);
          zookeeper.delete(path, -1, true);
          return data;
        } catch (KeeperException.NoNodeException e) {
          // Another client deleted the node first.
        }
      }
      
    }
  }
  
  /**
   * Remove the event and save the response into the other path.
   * 
   */
  public byte[] remove(QueueEvent event) throws KeeperException,
      InterruptedException {
    String path = event.getId();
    String responsePath = dir + "/" + response_prefix
        + path.substring(path.lastIndexOf("-") + 1);
    if (zookeeper.exists(responsePath, true)) {
      zookeeper.setData(responsePath, event.getBytes(), true);
    }
    byte[] data = zookeeper.getData(path, null, null, true);
    zookeeper.delete(path, -1, true);
    return data;
  }
  
  
  private class LatchChildWatcher implements Watcher {
    
    Object lock = new Object();
    private WatchedEvent event = null;
    
    public LatchChildWatcher() {}
    
    public LatchChildWatcher(Object lock) {
      this.lock = lock;
    }
    
    @Override
    public void process(WatchedEvent event) {
      LOG.info("Watcher fired on path: " + event.getPath() + " state: "
          + event.getState() + " type " + event.getType());
      synchronized (lock) {
        this.event = event;
        lock.notifyAll();
      }
    }
    
    public void await(long timeout) throws InterruptedException {
      synchronized (lock) {
        lock.wait(timeout);
      }
    }
    
    public WatchedEvent getWatchedEvent() {
      return event;
    }
  }
  
  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   * 
   * @return The former head of the queue
   */
  public byte[] take() throws KeeperException, InterruptedException {
    TreeMap<Long,String> orderedChildren;
    // Same as for element. Should refactor this.
    while (true) {
      LatchChildWatcher childWatcher = new LatchChildWatcher();
      try {
        orderedChildren = orderedChildren(childWatcher);
      } catch (KeeperException.NoNodeException e) {
        zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT, true);
        continue;
      }
      if (orderedChildren.size() == 0) {
        childWatcher.await(DEFAULT_TIMEOUT);
        continue;
      }
      
      for (String headNode : orderedChildren.values()) {
        String path = dir + "/" + headNode;
        try {
          byte[] data = zookeeper.getData(path, null, null, true);
          zookeeper.delete(path, -1, true);
          return data;
        } catch (KeeperException.NoNodeException e) {
          // Another client deleted the node first.
        }
      }
    }
  }
  
  /**
   * Inserts data into queue.
   * 
   * @return true if data was successfully added
   */
  public boolean offer(byte[] data) throws KeeperException,
      InterruptedException {
    return createData(dir + "/" + prefix, data,
        CreateMode.PERSISTENT_SEQUENTIAL) != null;
  }
  
  /**
   * Inserts data into zookeeper.
   * 
   * @return true if data was successfully added
   */
  private String createData(String path, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {
    for (;;) {
      try {
        return zookeeper.create(path, data, acl, mode, true);
      } catch (KeeperException.NoNodeException e) {
        try {
          zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException ne) {
          // someone created it
        }
      }
    }
  }
  
  /**
   * Offer the data and wait for the response
   * 
   */
  public QueueEvent offer(byte[] data, long timeout) throws KeeperException,
      InterruptedException {
    String path = createData(dir + "/" + prefix, data,
        CreateMode.PERSISTENT_SEQUENTIAL);
    String watchID = createData(
        dir + "/" + response_prefix + path.substring(path.lastIndexOf("-") + 1),
        null, CreateMode.EPHEMERAL);
    Object lock = new Object();
    LatchChildWatcher watcher = new LatchChildWatcher(lock);
    synchronized (lock) {
      if (zookeeper.exists(watchID, watcher, true) != null) {
        watcher.await(timeout);
      }
    }
    byte[] bytes = zookeeper.getData(watchID, null, null, true);
    zookeeper.delete(watchID, -1, true);
    return new QueueEvent(watchID, bytes, watcher.getWatchedEvent());
  }
  
  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   * 
   * @return data at the first element of the queue, or null.
   */
  public byte[] peek() throws KeeperException, InterruptedException {
    try {
      return element().getBytes();
    } catch (NoSuchElementException e) {
      return null;
    }
  }
  
  public static class QueueEvent {
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      QueueEvent other = (QueueEvent) obj;
      if (id == null) {
        if (other.id != null) return false;
      } else if (!id.equals(other.id)) return false;
      return true;
    }
    
    private WatchedEvent event = null;
    private String id;
    private byte[] bytes;
    
    QueueEvent(String id, byte[] bytes, WatchedEvent event) {
      this.id = id;
      this.bytes = bytes;
      this.event = event;
    }
    
    public void setId(String id) {
      this.id = id;
    }
    
    public String getId() {
      return id;
    }
    
    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }
    
    public byte[] getBytes() {
      return bytes;
    }
    
    public WatchedEvent getWatchedEvent() {
      return event;
    }
    
  }
  
  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   * 
   * @return data at the first element of the queue, or null.
   */
  public QueueEvent peek(boolean block) throws KeeperException, InterruptedException {
    if (!block) {
      return element();
    }
    
    TreeMap<Long,String> orderedChildren;
    while (true) {
      LatchChildWatcher childWatcher = new LatchChildWatcher();
      try {
        orderedChildren = orderedChildren(childWatcher);
      } catch (KeeperException.NoNodeException e) {
        zookeeper.create(dir, new byte[0], acl, CreateMode.PERSISTENT, true);
        continue;
      }
      if (orderedChildren.size() == 0) {
        childWatcher.await(DEFAULT_TIMEOUT);
        continue;
      }
      
      for (String headNode : orderedChildren.values()) {
        String path = dir + "/" + headNode;
        try {
          byte[] data = zookeeper.getData(path, null, null, true);
          return new QueueEvent(path, data, childWatcher.getWatchedEvent());
        } catch (KeeperException.NoNodeException e) {
          // Another client deleted the node first.
        }
      }
    }
  }
  
  /**
   * Attempts to remove the head of the queue and return it. Returns null if the
   * queue is empty.
   * 
   * @return Head of the queue or null.
   */
  public byte[] poll() throws KeeperException, InterruptedException {
    try {
      return remove();
    } catch (NoSuchElementException e) {
      return null;
    }
  }
  
}
