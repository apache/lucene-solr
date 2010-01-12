package org.apache.solr.cloud;

/**
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

import java.io.IOException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// nocommit - explore handling shard changes
// watches the shards zkNode
class ShardsWatcher implements Watcher {

  private static Logger log = LoggerFactory.getLogger(ZkController.class);

  // thread safe
  private ZkController controller;

  public ShardsWatcher(ZkController controller) {
    this.controller = controller;
  }

  public void process(WatchedEvent event) {
    // nocommit : this will be called too often as shards register themselves?
    System.out.println("shard node changed");

    try {
      // nocommit : refresh watcher
      // controller.getKeeperConnection().exists(event.getPath(), this);

      // TODO: need to load whole state?
      controller.readCloudInfo();

    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "IOException", e);
    }

  }

}
