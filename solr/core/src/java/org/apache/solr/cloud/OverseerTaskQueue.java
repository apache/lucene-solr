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
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;

import com.codahale.metrics.Timer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DistributedQueue} augmented with helper methods specific to the overseer task queues.
 * Methods specific to this subclass ignore superclass internal state and hit ZK directly.
 * This is inefficient!  But the API on this class is kind of muddy..
 */
public class OverseerTaskQueue extends DistributedQueue {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final String response_prefix = "qnr-" ;

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Overseer.Stats());
  }

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir, Overseer.Stats stats) {
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
      if (childName != null && childName.startsWith(PREFIX)) {
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
   */
  public void remove(QueueEvent event) throws KeeperException,
      InterruptedException {
    Timer.Context time = stats.time(dir + "_remove_event");
    try {
      String path = event.getId();
      String responsePath = dir + "/" + response_prefix
          + path.substring(path.lastIndexOf("-") + 1);
      if (zookeeper.exists(responsePath, true)) {
        zookeeper.setData(responsePath, event.getBytes(), true);
      } else {
        LOG.info("Response ZK path: " + responsePath + " doesn't exist."
            + "  Requestor may have disconnected from ZooKeeper");
      }
      try {
        zookeeper.delete(path, -1, true);
      } catch (KeeperException.NoNodeException ignored) {
      }
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
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      // If latchEventType is not null, only fire if the type matches
      LOG.debug("{} fired on path {} state {} latchEventType {}", event.getType(), event.getPath(), event.getState(), latchEventType);
      if (latchEventType == null || event.getType() == latchEventType) {
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
    Timer.Context time = stats.time(dir + "_offer");
    try {
      // Create and watch the response node before creating the request node;
      // otherwise we may miss the response.
      String watchID = createResponseNode();

      Object lock = new Object();
      LatchWatcher watcher = new LatchWatcher(lock);
      Stat stat = zookeeper.exists(watchID, watcher, true);

      // create the request node
      createRequestNode(data, watchID);

      synchronized (lock) {
        if (stat != null && watcher.getWatchedEvent() == null) {
          watcher.await(timeout);
        }
      }
      byte[] bytes = zookeeper.getData(watchID, null, null, true);
      // create the event before deleting the node, otherwise we can get the deleted
      // event from the watcher.
      QueueEvent event =  new QueueEvent(watchID, bytes, watcher.getWatchedEvent());
      zookeeper.delete(watchID, -1, true);
      return event;
    } finally {
      time.stop();
    }
  }

  void createRequestNode(byte[] data, String watchID) throws KeeperException, InterruptedException {
    createData(dir + "/" + PREFIX + watchID.substring(watchID.lastIndexOf("-") + 1),
        data, CreateMode.PERSISTENT);
  }

  String createResponseNode() throws KeeperException, InterruptedException {
    return createData(
            dir + "/" + response_prefix,
            null, CreateMode.EPHEMERAL_SEQUENTIAL);
  }


  public List<QueueEvent> peekTopN(int n, Predicate<String> excludeSet, long waitMillis)
      throws KeeperException, InterruptedException {
    ArrayList<QueueEvent> topN = new ArrayList<>();

    LOG.debug("Peeking for top {} elements. ExcludeSet: {}", n, excludeSet);
    Timer.Context time;
    if (waitMillis == Long.MAX_VALUE) time = stats.time(dir + "_peekTopN_wait_forever");
    else time = stats.time(dir + "_peekTopN_wait" + waitMillis);

    try {
      for (Pair<String, byte[]> element : peekElements(n, waitMillis, child -> !excludeSet.test(dir + "/" + child))) {
        topN.add(new QueueEvent(dir + "/" + element.first(),
            element.second(), null));
      }
      printQueueEventsListElementIds(topN);
      return topN;
    } finally {
      time.stop();
    }
  }

  private static void printQueueEventsListElementIds(ArrayList<QueueEvent> topN) {
    if (LOG.isDebugEnabled() && !topN.isEmpty()) {
      StringBuilder sb = new StringBuilder("[");
      for (QueueEvent queueEvent : topN) {
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
