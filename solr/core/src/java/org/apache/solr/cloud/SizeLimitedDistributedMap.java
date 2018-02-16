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

import java.util.List;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * A size limited distributed map maintained in zk.
 * Oldest znodes (as per modification time) are evicted as newer ones come in.
 *
 * When the map hits the specified maximum size, the oldest <code>maxSize / 10</code> items
 * are evicted on the next {@link #put(String, byte[])} invocation.
 */
public class SizeLimitedDistributedMap extends DistributedMap {

  private final int maxSize;

  /**
   * This observer will be called when this map overflows, and deletes the excess of elements
   */
  private final OnOverflowObserver onOverflowObserver;

  public SizeLimitedDistributedMap(SolrZkClient zookeeper, String dir, int maxSize) {
    this(zookeeper, dir, maxSize, null);
  }
  
  public SizeLimitedDistributedMap(SolrZkClient zookeeper, String dir, int maxSize, OnOverflowObserver onOverflowObserver) {
    super(zookeeper, dir);
    this.maxSize = maxSize;
    this.onOverflowObserver = onOverflowObserver;
  }

  @Override
  public void put(String trackingId, byte[] data) throws KeeperException, InterruptedException {
    if (this.size() >= maxSize) {
      // Bring down the size
      List<String> children = zookeeper.getChildren(dir, null, true);

      int cleanupSize = maxSize / 10;

      final PriorityQueue<Long> priorityQueue = new PriorityQueue<Long>(cleanupSize) {
        @Override
        protected boolean lessThan(Long a, Long b) {
          return (a > b);
        }
      };

      for (String child : children) {
        Stat stat = zookeeper.exists(dir + "/" + child, null, true);
        priorityQueue.insertWithOverflow(stat.getMzxid());
      }

      long topElementMzxId = priorityQueue.top();

      for (String child : children) {
        Stat stat = zookeeper.exists(dir + "/" + child, null, true);
        if (stat.getMzxid() <= topElementMzxId) {
          zookeeper.delete(dir + "/" + child, -1, true);
          if (onOverflowObserver != null) onOverflowObserver.onChildDelete(child.substring(PREFIX.length()));
        }
      }
    }

    super.put(trackingId, data);
  }
  
  interface OnOverflowObserver {
    void onChildDelete(String child) throws KeeperException, InterruptedException;
  }
}
