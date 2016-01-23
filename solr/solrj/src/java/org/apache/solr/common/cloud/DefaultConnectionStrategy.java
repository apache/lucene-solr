package org.apache.solr.common.cloud;

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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: improve backoff retry impl
 */
public class DefaultConnectionStrategy extends ZkClientConnectionStrategy {

  private static Logger log = LoggerFactory.getLogger(DefaultConnectionStrategy.class);
  private ScheduledExecutorService executor;
  
  @Override
  public void connect(String serverAddress, int timeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
    updater.update(new SolrZooKeeper(serverAddress, timeout, watcher));
  }

  @Override
  public void reconnect(final String serverAddress, final int zkClientTimeout,
      final Watcher watcher, final ZkUpdate updater) throws IOException {
    log.info("Starting reconnect to ZooKeeper attempts ...");
    executor = Executors.newScheduledThreadPool(1);
    executor.schedule(new Runnable() {
      private int delay = 1000;
      public void run() {
        log.info("Attempting the connect...");
        boolean connected = false;
        try {
          updater.update(new SolrZooKeeper(serverAddress, zkClientTimeout, watcher));
          log.info("Reconnected to ZooKeeper");
          connected = true;
        } catch (Exception e) {
          log.error("", e);
          log.info("Reconnect to ZooKeeper failed");
        }
        if(connected) {
          executor.shutdownNow();
        } else {
          if(delay < 240000) {
            delay = delay * 2;
          }
          executor.schedule(this, delay, TimeUnit.MILLISECONDS);
        }
        
      }
    }, 1000, TimeUnit.MILLISECONDS);
  }

}
