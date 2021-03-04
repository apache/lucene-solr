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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link ZkDistributedQueue} augmented with helper methods specific to the overseer task queues.
 * Methods specific to this subclass ignore superclass internal state and hit ZK directly.
 * This is inefficient!  But the API on this class is kind of muddy..
 */
public class OverseerTaskQueue extends ZkDistributedQueue {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String RESPONSE_PREFIX = "qnr-" ;
  public static final byte[] BYTES = new byte[0];

  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private final AtomicInteger pendingResponses = new AtomicInteger(0);

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir) {
    this(zookeeper, dir, new Stats());
  }

  public OverseerTaskQueue(SolrZkClient zookeeper, String dir, Stats stats) {
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
              if (log.isDebugEnabled()) {
                log.debug("Looking for {}, found {}", message.get(requestIdKey), requestId);
              }
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
      String responsePath = dir + "/" + RESPONSE_PREFIX
          + path.substring(path.lastIndexOf("-") + 1);

      try {
        zookeeper.setData(responsePath, event.getBytes(), true);
      } catch (KeeperException.NoNodeException ignored) {
        // we must handle the race case where the node no longer exists
        log.info("Response ZK path: {} doesn't exist. Requestor may have disconnected from ZooKeeper", responsePath);
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
  static final class LatchWatcher implements Watcher, Closeable {

    private final Lock lock;
    private final Condition eventReceived;
    private final String path;
    private final SolrZkClient zkClient;
    private volatile WatchedEvent event;

    LatchWatcher(String path, SolrZkClient zkClient) {
      this.lock = new ReentrantLock(true);
      this.eventReceived = lock.newCondition();
      this.path = path;
      this.zkClient = zkClient;
    }


    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      // If latchEventType is not null, only fire if the type matches

      if (log.isDebugEnabled()) log.debug("{} fired on path {} state {} latchEventType {}", event.getType(), event.getPath(), event.getState());

      this.event = event;

      lock.lock();
      try {
        eventReceived.signalAll();
      } finally {
        lock.unlock();
      }
    }

    public void await(long timeoutMs) {

      if (event != null) {
        return;
      }

      createWatch();

      TimeOut timeout = new TimeOut(timeoutMs, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
      lock.lock();
      try {
        while (!timeout.hasTimedOut() && event == null) {
          try {
            eventReceived.await(500, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {

          }
        }

        if (timeout.hasTimedOut()) {
          log.warn("Timeout waiting for response after {}ms", timeout.timeElapsed(TimeUnit.MILLISECONDS));
        }
      } finally {
        lock.unlock();
      }
    }

    private void createWatch() {
      try {
        zkClient.addWatch(path, this, AddWatchMode.PERSISTENT);
      } catch (Exception e) {
       throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }

    public WatchedEvent getWatchedEvent() {
      return event;
    }

    @Override
    public void close() {
      try {
        zkClient.removeWatches(path, this, WatcherType.Data, true);
      }  catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
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
      return zookeeper.create(path, data, mode, true);
    }
  }

  /**
   * Offer the data and wait for the response
   *
   */
  public QueueEvent offer(byte[] data, long timeout) throws KeeperException,
      InterruptedException {
    if (log.isDebugEnabled()) log.debug("offer operation to the Overseeer queue {}", Utils.fromJSON(data));

    if (shuttingDown.get()) {
      throw new SolrException(SolrException.ErrorCode.CONFLICT,"Solr is shutting down, no more overseer tasks may be offered");
    }
   // Timer.Context time = stats.time(dir + "_offer");
    LatchWatcher watcher = null;
    try {
      // Create and watch the response node before creating the request node;
      // otherwise we may miss the response.
      String watchID = createResponseNode();

      if (log.isDebugEnabled()) log.debug("watchId for response node {}, setting a watch ... ", watchID);

      watcher = new LatchWatcher(watchID, zookeeper);

      // create the request node
      String path = createRequestNode(data, watchID);
      if (log.isDebugEnabled()) log.debug("created request node at {}", path);

      pendingResponses.incrementAndGet();
      if (log.isDebugEnabled()) log.debug("wait on latch {}", timeout);
      watcher.await(timeout);

      byte[] bytes = zookeeper.getData(watchID, null, null, true);
      if (log.isDebugEnabled()) log.debug("get data from response node {} {} {}", watchID, bytes == null ? null : bytes.length, watcher.getWatchedEvent());

      if (bytes == null || bytes.length == 0) {
        log.error("Found no data at response node, Overseer likely changed {}", watchID);
      }

      QueueEvent event =  new QueueEvent(watchID, bytes, watcher.getWatchedEvent());

      zookeeper.delete(watchID,-1);

      return event;
    } finally {
     // time.stop();
      pendingResponses.decrementAndGet();
      IOUtils.closeQuietly(watcher);
    }
  }


  String createRequestNode(byte[] data, String watchID) throws KeeperException, InterruptedException {
    return createData(dir + "/" + PREFIX + watchID.substring(watchID.lastIndexOf("-") + 1),
        data, CreateMode.EPHEMERAL);
  }

  String createResponseNode() throws KeeperException, InterruptedException {
    return createData(
        Overseer.OVERSEER_COLLECTION_MAP_COMPLETED + "/" + RESPONSE_PREFIX,
        null, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  private static void printQueueEventsListElementIds(ArrayList<QueueEvent> topN) {
    if (log.isDebugEnabled() && !topN.isEmpty()) {
      StringBuilder sb = new StringBuilder("[");
      for (QueueEvent queueEvent : topN) {
        sb.append(queueEvent.getId()).append(", ");
      }
      sb.append("]");
      log.debug("Returning topN elements: {}", sb);
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
}
