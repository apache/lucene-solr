package org.apache.zookeeper;

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

import java.io.IOException;
import java.nio.channels.SocketChannel;

// we use this class to expose nasty stuff for tests
public class SolrZooKeeper extends ZooKeeper {

  public SolrZooKeeper(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
  }
  
  public ClientCnxn getConnection() {
    return cnxn;
  }
  
  /**
   * Cause this ZooKeeper object to stop receiving from the ZooKeeperServer
   * for the given number of milliseconds.
   * @param ms the number of milliseconds to pause.
   */
  public void pauseCnxn(final long ms) {
    new Thread() {
      public void run() {
        synchronized (cnxn) {
          try {
            try {
              ((SocketChannel) cnxn.sendThread.sockKey.channel()).socket()
                  .close();
            } catch (Exception e) {

            }
            Thread.sleep(ms);
          } catch (InterruptedException e) {}
        }
      }
    }.start();
  }

}
