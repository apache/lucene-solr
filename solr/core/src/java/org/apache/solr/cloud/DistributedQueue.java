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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A distributed queue from zk recipes.
 */
public class DistributedQueue {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedQueue.class);
  
  private static long DEFAULT_TIMEOUT = 5*60*1000;
  
  private final String dir;
  
  private SolrZkClient zookeeper;
  
  private final String prefix = "qn-";
  
  private final String response_prefix = "qnr-" ;

  private final Overseer.Stats stats;
  
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
   * Returns a Map of the children, ordered by id.
   * 
   * @param watcher
   *          optional watcher on getChildren() operation.
   * @return map from id to child name for all children
   */
  private TreeMap<Long,String> orderedChildren(Watcher watcher)
      throws KeeperException, InterruptedException {
    TreeMap<Long,String> orderedChildren = new TreeMap<>();

    List<String> childNames = zookeeper.getChildren(dir, watcher, true);
    stats.setQueueLength(childNames.size());
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
   * Returns true if the queue contains a task with the specified async id.
   */
  public boolean containsTaskWithRequestId(String requestId)
      throws KeeperException, InterruptedException {

    List<String> childNames = zookeeper.getChildren(dir, null, true);
    stats.setQueueLength(childNames.size());
    for (String childName : childNames) {
      if (childName != null) {
        try {
          byte[] data = zookeeper.getData(dir + "/" + childName, null, null, true);
          if (data != null) {
            ZkNodeProps message = ZkNodeProps.load(data);
            if (message.containsKey(OverseerCollectionProcessor.ASYNC)) {
              LOG.debug(">>>> {}", message.get(OverseerCollectionProcessor.ASYNC));
              if(message.get(OverseerCollectionProcessor.ASYNC).equals(requestId)) return true;
            }
          }
        } catch (KeeperException.NoNodeException e) {
          // Another client removed the node first, try next
        }
      }
    }

    return false;
  }


  /**
   * Return the head of the queue without modifying the queue.
   * 
   * @return the data at the head of the queue.
   */
  private QueueEvent element() throws KeeperException,
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
        return null;
      }
      if (orderedChildren.size() == 0) return null;
      
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
    TimerContext time = stats.time(dir + "_remove");
    try {
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
    } finally {
      time.stop();
    }
  }
  
  /**
   * Remove the event and save the response into the other path.
   * 
   */
  public byte[] remove(QueueEvent event) throws KeeperException,
      InterruptedException {
    TimerContext time = stats.time(dir + "_remove_event");
    try {
      String path = event.getId();
      String responsePath = dir + "/" + response_prefix
          + path.substring(path.lastIndexOf("-") + 1);
      if (zookeeper.exists(responsePath, true)) {
        zookeeper.setData(responsePath, event.getBytes(), true);
      }
      byte[] data = zookeeper.getData(path, null, null, true);
      zookeeper.delete(path, -1, true);
      return data;
    } finally {
      time.stop();
    }
  }

  /**
   * Watcher that blocks until a WatchedEvent occurs for a znode.
   */
  private final class LatchWatcher implements Watcher {

    private final Object lock;
    private WatchedEvent event;
    private Event.EventType latchEventType;

    LatchWatcher(Object lock) {
      this(lock, null);
    }

    LatchWatcher(Event.EventType eventType) {
      this(new Object(), eventType);
    }

    LatchWatcher(Object lock, Event.EventType eventType) {
      this.lock = lock;
      this.latchEventType = eventType;
    }

    @Override
    public void process(WatchedEvent event) {
      Event.EventType eventType = event.getType();
      // None events are ignored
      // If latchEventType is not null, only fire if the type matches
      if (eventType != Event.EventType.None && (latchEventType == null || eventType == latchEventType)) {
        LOG.info("{} fired on path {} state {}", eventType, event.getPath(), event.getState());
        synchronized (lock) {
          this.event = event;
          lock.notifyAll();
        }
      }
    }

    public void await(long timeout) throws InterruptedException {
      synchronized (lock) {
        if (this.event != null) return;
        lock.wait(timeout);
      }
    }

    public WatchedEvent getWatchedEvent() {
      return event;
    }
  }

  // we avoid creating *many* watches in some cases
  // by saving the childrenWatcher and the children associated - see SOLR-6336
  private LatchWatcher childrenWatcher;
  private TreeMap<Long,String> fetchedChildren;
  private final Object childrenWatcherLock = new Object();

  private Map<Long, String> getChildren(long wait) throws InterruptedException, KeeperException
  {
    LatchWatcher watcher;
    TreeMap<Long,String> children;
    synchronized (childrenWatcherLock) {
      watcher = childrenWatcher;
      children = fetchedChildren;
    }

    if (watcher == null ||  watcher.getWatchedEvent() != null) {
      // this watcher is only interested in child change events
      watcher = new LatchWatcher(Watcher.Event.EventType.NodeChildrenChanged);
      while (true) {
        try {
          children = orderedChildren(watcher);
          break;
        } catch (KeeperException.NoNodeException e) {
          zookeeper.create(dir, new byte[0], CreateMode.PERSISTENT, true);
          // go back to the loop and try again
        }
      }
      synchronized (childrenWatcherLock) {
        childrenWatcher = watcher;
        fetchedChildren = children;
      }
    }

    while (true) {
      if (!children.isEmpty()) break;
      watcher.await(wait == Long.MAX_VALUE ? DEFAULT_TIMEOUT : wait);
      if (watcher.getWatchedEvent() != null) {
        children = orderedChildren(null);
      }
      if (wait != Long.MAX_VALUE) break;
    }
    return Collections.unmodifiableMap(children);
  }

  /**
   * Removes the head of the queue and returns it, blocks until it succeeds.
   * 
   * @return The former head of the queue
   */
  public byte[] take() throws KeeperException, InterruptedException {
    // Same as for element. Should refactor this.
    TimerContext timer = stats.time(dir + "_take");
    try {
      Map<Long, String> orderedChildren = getChildren(Long.MAX_VALUE);
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
      return null; // shouldn't really reach here..
    } finally {
      timer.stop();
    }
  }
  
  /**
   * Inserts data into queue.
   * 
   * @return true if data was successfully added
   */
  public boolean offer(byte[] data) throws KeeperException,
      InterruptedException {
    TimerContext time = stats.time(dir + "_offer");
    try {
      return createData(dir + "/" + prefix, data,
          CreateMode.PERSISTENT_SEQUENTIAL) != null;
    } finally {
      time.stop();
    }
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
        return zookeeper.create(path, data, mode, true);
      } catch (KeeperException.NoNodeException e) {
        try {
          zookeeper.create(dir, new byte[0], CreateMode.PERSISTENT, true);
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
    TimerContext time = stats.time(dir + "_offer");
    try {
      String path = createData(dir + "/" + prefix, data,
          CreateMode.PERSISTENT_SEQUENTIAL);
      String watchID = createData(
          dir + "/" + response_prefix + path.substring(path.lastIndexOf("-") + 1),
          null, CreateMode.EPHEMERAL);

      Object lock = new Object();
      LatchWatcher watcher = new LatchWatcher(lock);
      synchronized (lock) {
        if (zookeeper.exists(watchID, watcher, true) != null) {
          watcher.await(timeout);
        }
      }
      byte[] bytes = zookeeper.getData(watchID, null, null, true);
      zookeeper.delete(watchID, -1, true);
      return new QueueEvent(watchID, bytes, watcher.getWatchedEvent());
    } finally {
      time.stop();
    }
  }
  
  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty.
   * 
   * @return data at the first element of the queue, or null.
   */
  public byte[] peek() throws KeeperException, InterruptedException {
    TimerContext time = stats.time(dir + "_peek");
    try {
      QueueEvent element = element();
      if (element == null) return null;
      return element.getBytes();
    } finally {
      time.stop();
    }
  }
  
  public List<QueueEvent> peekTopN(int n, Set<String> excludeSet, Long wait)
      throws KeeperException, InterruptedException {
    ArrayList<QueueEvent> topN = new ArrayList<>();

    LOG.debug("Peeking for top {} elements. ExcludeSet: " + excludeSet.toString());
    TimerContext time = null;
    if (wait == Long.MAX_VALUE) time = stats.time(dir + "_peekTopN_wait_forever");
    else time = stats.time(dir + "_peekTopN_wait" + wait);

    try {
      Map<Long, String> orderedChildren = getChildren(wait);
      for (String headNode : orderedChildren.values()) {
        if (headNode != null && topN.size() < n) {
          try {
            String id = dir + "/" + headNode;
            if (excludeSet != null && excludeSet.contains(id)) continue;
            QueueEvent queueEvent = new QueueEvent(id,
                zookeeper.getData(dir + "/" + headNode, null, null, true), null);
            topN.add(queueEvent);
          } catch (KeeperException.NoNodeException e) {
            // Another client removed the node first, try next
          }
        } else {
          if (topN.size() >= 1) {
            printQueueEventsListElementIds(topN);
            return topN;
          }
        }
      }

      if (topN.size() > 0 ) {
        printQueueEventsListElementIds(topN);
        return topN;
      }
      return null;
    } finally {
      time.stop();
    }
  }

  private void printQueueEventsListElementIds(ArrayList<QueueEvent> topN) {
    if(LOG.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer("[");
      for(QueueEvent queueEvent: topN) {
        sb.append(queueEvent.getId()).append(", ");
      }
      sb.append("]");
      LOG.debug("Returning topN elements: {}", sb.toString());
    }
  }


  /**
   *
   * Gets last element of the Queue without removing it.
   */
  public String getTailId() throws KeeperException, InterruptedException {
    TreeMap<Long, String> orderedChildren = null;
    orderedChildren = orderedChildren(null);
    if(orderedChildren == null || orderedChildren.isEmpty()) return null;

    for(String headNode : orderedChildren.descendingMap().values())
      if (headNode != null) {
        try {
          QueueEvent queueEvent = new QueueEvent(dir + "/" + headNode, zookeeper.getData(dir + "/" + headNode,
              null, null, true), null);
          return queueEvent.getId();
        } catch (KeeperException.NoNodeException e) {
          // Another client removed the node first, try next
        }
      }
    return null;
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
   * empty and block is false.
   * 
   * @param block if true, blocks until an element enters the queue
   * @return data at the first element of the queue, or null.
   */
  public QueueEvent peek(boolean block) throws KeeperException, InterruptedException {
    return peek(block ? Long.MAX_VALUE : 0);
  }

  /**
   * Returns the data at the first element of the queue, or null if the queue is
   * empty after wait ms.
   * 
   * @param wait max wait time in ms.
   * @return data at the first element of the queue, or null.
   */
  public QueueEvent peek(long wait) throws KeeperException, InterruptedException {
    TimerContext time = null;
    if (wait == Long.MAX_VALUE) {
      time = stats.time(dir + "_peek_wait_forever");
    } else {
      time = stats.time(dir + "_peek_wait" + wait);
    }
    try {
      if (wait == 0) {
        return element();
      }

      Map<Long, String> orderedChildren = getChildren(wait);
      for (String headNode : orderedChildren.values()) {
        String path = dir + "/" + headNode;
        try {
          byte[] data = zookeeper.getData(path, null, null, true);
          return new QueueEvent(path, data, null);
        } catch (KeeperException.NoNodeException e) {
          // Another client deleted the node first.
        }
      }
      return null;
    } finally {
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
    TimerContext time = stats.time(dir + "_poll");
    try {
      return remove();
    } catch (NoSuchElementException e) {
      return null;
    } finally {
      time.stop();
    }
  }

  public Overseer.Stats getStats() {
    return stats;
  }
}
