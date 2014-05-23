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
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A distributed map.
 * This supports basic map functions e.g. get, put, contains for interaction with zk which
 * don't have to be ordered i.e. DistributedQueue.
 */
public class DistributedMap {
  private static final Logger LOG = LoggerFactory
      .getLogger(DistributedMap.class);

  private static long DEFAULT_TIMEOUT = 5*60*1000;

  private final String dir;

  private SolrZkClient zookeeper;
  private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

  private final String prefix = "mn-";

  private final String response_prefix = "mnr-" ;

  public DistributedMap(SolrZkClient zookeeper, String dir, List<ACL> acl) {
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

  private class LatchChildWatcher implements Watcher {

    Object lock = new Object();
    private WatchedEvent event = null;

    public LatchChildWatcher() {}

    public LatchChildWatcher(Object lock) {
      this.lock = lock;
    }

    @Override
    public void process(WatchedEvent event) {
      LOG.info("LatchChildWatcher fired on path: " + event.getPath() + " state: "
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


  public boolean put(String trackingId, byte[] data) throws KeeperException, InterruptedException {
    return createData(dir + "/" + prefix + trackingId, data,
        CreateMode.PERSISTENT) != null;
  }

  /**
   * Offer the data and wait for the response
   *
   */
  public MapEvent put(String trackingId, byte[] data, long timeout) throws KeeperException,
      InterruptedException {
    String path = createData(dir + "/" + prefix + trackingId, data,
        CreateMode.PERSISTENT);
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
    return new MapEvent(watchID, bytes, watcher.getWatchedEvent());
  }

  public MapEvent get(String trackingId) throws KeeperException, InterruptedException {
    return new MapEvent(trackingId, zookeeper.getData(dir + "/" + prefix + trackingId, null, null, true), null);
  }

  public boolean contains(String trackingId) throws KeeperException, InterruptedException {
    return zookeeper.exists(dir + "/" + prefix + trackingId, true);
  }

  public int size() throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zookeeper.getData(dir, null, stat, true);
    return stat.getNumChildren();
  }

  public void remove(String trackingId) throws KeeperException, InterruptedException {
    zookeeper.delete(dir + "/" + prefix + trackingId, -1, true);
  }

  /**
   * Helper method to clear all child nodes for a parent node.
   */
  public void clear() throws KeeperException, InterruptedException {
    List<String> childNames = zookeeper.getChildren(dir, null, true);
    for(String childName: childNames) {
      zookeeper.delete(dir + "/" + childName, -1, true);
    }

  }

  public static class MapEvent {
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
      MapEvent other = (MapEvent) obj;
      if (id == null) {
        if (other.id != null) return false;
      } else if (!id.equals(other.id)) return false;
      return true;
    }

    private WatchedEvent event = null;
    private String id;
    private byte[] bytes;

    MapEvent(String id, byte[] bytes, WatchedEvent event) {
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
