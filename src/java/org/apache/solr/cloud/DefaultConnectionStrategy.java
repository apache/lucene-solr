package org.apache.solr.cloud;

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
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * nocommit : default needs backoff retry reconnection attempts
 */
public class DefaultConnectionStrategy extends ZkClientConnectionStrategy {

  private static Logger log = LoggerFactory.getLogger(DefaultConnectionStrategy.class);
  private ScheduledExecutorService executor;
  
  @Override
  public void connect(String serverAddress, int timeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
    updater.update(new ZooKeeper(serverAddress, timeout, watcher));
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
          updater.update(new ZooKeeper(serverAddress, zkClientTimeout, watcher));
          // nocommit
          log.info("Reconnected to ZooKeeper");
          connected = true;
        } catch (Exception e) {
          // nocommit
          e.printStackTrace();
          log.info("Reconnect to ZooKeeper failed");
        }
        if(connected) {
          executor.shutdownNow();
        } else {
          delay = delay * 2; // nocommit : back off retry that levels off
          executor.schedule(this, delay, TimeUnit.MILLISECONDS);
        }
        
      }
    }, 1000, TimeUnit.MILLISECONDS);
  }

}
