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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

// we use this class to expose nasty stuff for tests
public class SolrZooKeeper extends ZooKeeper {
  List<Thread> spawnedThreads = new CopyOnWriteArrayList<Thread>();
  
  // for test debug
  //static Map<SolrZooKeeper,Exception> clients = new ConcurrentHashMap<SolrZooKeeper,Exception>();

  public SolrZooKeeper(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    //clients.put(this, new RuntimeException());
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
    Thread t = new Thread() {
      public void run() {
        try {
          synchronized (cnxn) {
            try {
              ((SocketChannel) cnxn.sendThread.sockKey.channel()).socket()
                  .close();
            } catch (Exception e) {
            }
            Thread.sleep(ms);
          }

          // Wait a long while to make sure we properly clean up these threads.
          Thread.sleep(500000);
        } catch (InterruptedException e) {}
      }
    };
    t.start();
    spawnedThreads.add(t);
  }

  @Override
  public synchronized void close() throws InterruptedException {
    //clients.remove(this);
    for (Thread t : spawnedThreads) {
      t.interrupt();
    }
    super.close();
  }
  
//  public static void assertCloses() {
//    if (clients.size() > 0) {
//      Iterator<Exception> stacktraces = clients.values().iterator();
//      Exception cause = null;
//      cause = stacktraces.next();
//      throw new RuntimeException("Found a bad one!", cause);
//    }
//  }
  
}
