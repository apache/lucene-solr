package org.apache.solr.common.cloud;

/*
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

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: improve backoff retry impl
 */
public class DefaultConnectionStrategy extends ZkClientConnectionStrategy {

  private static Logger log = LoggerFactory.getLogger(DefaultConnectionStrategy.class);
  
  @Override
  public void connect(String serverAddress, int timeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
    SolrZooKeeper zk = new SolrZooKeeper(serverAddress, timeout, watcher);
    boolean success = false;
    try {
      updater.update(zk);
      success = true;
    } finally {
      if (!success) {
        zk.close();
      }
    }
  }

  @Override
  public void reconnect(final String serverAddress, final int zkClientTimeout,
      final Watcher watcher, final ZkUpdate updater) throws IOException {
    log.info("Connection expired - starting a new one...");
    SolrZooKeeper zk = new SolrZooKeeper(serverAddress, zkClientTimeout, watcher);
    boolean success = false;
    try {
      updater
          .update(zk);
      success = true;
      log.info("Reconnected to ZooKeeper");
    } catch (Exception e) {
      SolrException.log(log, "Reconnect to ZooKeeper failed", e);
      log.info("Reconnect to ZooKeeper failed");
    } finally {
      if (!success) {
        try {
          zk.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
  }

}
