package org.apache.solr.cloud;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.util.stats.TimerContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DistributedQueue} augmented with helper methods specific to the collection queue.
 * Methods specific to this subclass ignore superclass internal state and hit ZK directly.
 * This is inefficient!  But the API on this class is kind of muddy..
 */
public class OverseerCollectionQueue extends DistributedQueue {
  private static final Logger LOG = LoggerFactory.getLogger(OverseerCollectionQueue.class);
  
  private final String response_prefix = "qnr-" ;

  public OverseerCollectionQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Overseer.Stats());
  }

  public OverseerCollectionQueue(SolrZkClient zookeeper, String dir, Overseer.Stats stats) {
    super(zookeeper, dir, stats);
  }
  
  /**
   * Returns true if the queue contains a task with the specified async id.
   */
  public boolean containsTaskWithRequestId(String requestIdKey, String requestId)
      throws KeeperException, InterruptedException {

    List<String> childNames = zookeeper.getChildren(dir, null, true);
    stats.setQueueLength(childNames.size());
    for (String childName : childNames) {
      if (childName != null) {
        try {
          byte[] data = zookeeper.getData(dir + "/" + childName, null, null, true);
          if (data != null) {
            ZkNodeProps message = ZkNodeProps.load(data);
            if (message.containsKey(requestIdKey)) {
              LOG.debug(">>>> {}", message.get(requestIdKey));
              if(message.get(requestIdKey).equals(requestId)) return true;
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
      String path = createData(dir + "/" + PREFIX, data,
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

  public List<QueueEvent> peekTopN(int n, Set<String> excludeSet, long waitMillis)
      throws KeeperException, InterruptedException {
    ArrayList<QueueEvent> topN = new ArrayList<>();

    LOG.debug("Peeking for top {} elements. ExcludeSet: {}", n, excludeSet);
    TimerContext time = null;
    if (waitMillis == Long.MAX_VALUE) time = stats.time(dir + "_peekTopN_wait_forever");
    else time = stats.time(dir + "_peekTopN_wait" + waitMillis);

    try {
      for (String headNode : getChildren(waitMillis)) {
        if (topN.size() < n) {
          try {
            String id = dir + "/" + headNode;
            if (excludeSet.contains(id)) continue;
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

  private static void printQueueEventsListElementIds(ArrayList<QueueEvent> topN) {
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
    // TODO: could we use getChildren here?  Unsure what freshness guarantee the caller needs.
    TreeSet<String> orderedChildren = fetchZkChildren(null);

    for (String headNode : orderedChildren.descendingSet())
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
}
