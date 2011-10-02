package org.apache.solr.cloud;

import org.apache.solr.cloud.lock.LockListener;
import org.apache.solr.cloud.lock.WriteLock;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class ZkCollectionLock {
  private WriteLock lock;
  private volatile boolean gotLock = false;
  
  public ZkCollectionLock(SolrZkClient zkClient) {
    lock = new WriteLock(zkClient.getSolrZooKeeper(),
        "/collections/collection1/shards_lock", null, new LockListener() {
          
          @Override
          public void lockReleased() {
            gotLock = false;
          }
          
          @Override
          public void lockAcquired() {
            gotLock = true;
          }
        });
  
  }
  
  public void lock() throws KeeperException, InterruptedException {
    if (lock.lock()) {
      // we got the lock, just return
      gotLock = true;
      return;
    }
    
    // we need to wait for the lock
    int cnt = 0;
    while (!gotLock) {
      if (cnt++ == 15) {
        // get out of line
        lock.unlock();
        throw new RuntimeException("Coulnd't aquire the shard lock");
      }
      Thread.sleep(1000);
    }
  }
  
  public void unlock() {
    lock.unlock();
  }
}
