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
package org.apache.solr.common.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeperExposed;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.proto.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// we use this class to expose nasty stuff for tests
@SuppressWarnings({"try"})
public class SolrZooKeeper extends ZooKeeperAdmin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final Set<Thread> spawnedThreads = ConcurrentHashMap.newKeySet();
  private CloseTracker closeTracker;

  public SolrZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    assert (closeTracker = new CloseTracker()) != null;
  }

  public ClientCnxn getConnection() {
    return cnxn;
  }

  public SocketAddress getSocketAddress() {
    return testableLocalSocketAddress();
  }

  public void closeCnxn() {
    final Thread t = new Thread("ZkThread") {
      @Override
      public void run() {
        try {
          AccessController.doPrivileged((PrivilegedAction<Void>) this::closeZookeeperChannel);
        } finally {
          spawnedThreads.remove(this);
        }
      }

      @SuppressForbidden(reason = "Hack for Zookeper needs access to private methods.")
      private Void closeZookeeperChannel() {
        final ClientCnxn cnxn = getConnection();

        synchronized (cnxn) {
          try {
            final Field sendThreadFld = cnxn.getClass().getDeclaredField("sendThread");
            sendThreadFld.setAccessible(true);
            Object sendThread = sendThreadFld.get(cnxn);
            if (sendThread != null) {
              Method method = sendThread.getClass().getDeclaredMethod("testableCloseSocket");
              method.setAccessible(true);
              try {
                method.invoke(sendThread);
              } catch (InvocationTargetException e) {
                // is fine
              }
            }
          } catch (Exception e) {
            ParWork.propagateInterrupt(e);
            throw new RuntimeException("Closing Zookeeper send channel failed.", e);
          }
        }
        return null; // Void
      }
    };
    spawnedThreads.add(t);
    t.start();
  }

  @Override
  public void close() {
    if (closeTracker != null) closeTracker.close();
    try {
      try {
        RequestHeader h = new RequestHeader();
        h.setType(ZooDefs.OpCode.closeSession);

        cnxn.submitRequest(h, null, null, null);
      } catch (InterruptedException e) {
        // ignore, close the send/event threads
      } finally {
        ZooKeeperExposed zk = new ZooKeeperExposed(this, cnxn);
        zk.closeCnxn();
      }
    } catch (Exception e) {
      log.warn("Exception closing zookeeper client", e);
    }
//    try {
//      super.close();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
  }

}

