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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperExposed;
import org.apache.zookeeper.proto.RequestHeader;

// we use this class to expose nasty stuff for tests
@SuppressWarnings({"try"})
public class SolrZooKeeper extends ZooKeeper {
  final Set<Thread> spawnedThreads = ConcurrentHashMap.newKeySet();
  private final CloseTracker closeTracker;

  // for test debug
  //static Map<SolrZooKeeper,Exception> clients = new ConcurrentHashMap<SolrZooKeeper,Exception>();

  public SolrZooKeeper(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    closeTracker = new CloseTracker();
    //clients.put(this, new RuntimeException());
  }
  
  public ClientCnxn getConnection() {
    return cnxn;
  }
  
  public SocketAddress getSocketAddress() {
    return testableLocalSocketAddress();
  }
  
  public void closeCnxn() {
    final Thread t = new Thread() {
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
            ParWork.propegateInterrupt(e);
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
    if (closeTracker.isClosed()) {
      return;
    }
    closeTracker.close();
    try {
      SolrZooKeeper.super.close();
    } catch (InterruptedException e) {
      ParWork.propegateInterrupt(e);
    }
             ZooKeeperExposed exposed = new ZooKeeperExposed(this, cnxn);
     //exposed.intteruptSendThread();
  //  exposed.interruptEventThread();
   // exposed.interruptSendThread();
//    try (ParWork worker = new ParWork(this, true)) {
//      worker.collect(() -> {
//        try {
//          ZooKeeperExposed exposed = new ZooKeeperExposed(this, cnxn);
//          //exposed.intteruptSendThread();
//
//          SolrZooKeeper.super.close(5000);
//          exposed.interruptEventThread();
//        } catch (InterruptedException e) {
//          ParWork.propegateInterrupt(e);
//        }
//      });
//        RequestHeader h = new RequestHeader();
//        h.setType(ZooDefs.OpCode.closeSession);
//
//        try {
//          cnxn.submitRequest(h, null, null, null);
//        } catch (InterruptedException e) {
//          ParWork.propegateInterrupt(e);
//        }
//
//        ZooKeeperExposed exposed = new ZooKeeperExposed(this, cnxn);
//        exposed.setSendThreadState( ZooKeeper.States.CLOSED);
////     /   zkcall(cnxn, "sendThread", "close", null);
//        // zkcall(cnxn, "sendThread", "close", null);
//      }); // we don't wait for close because we wait below
//      worker.collect( () -> {
//        ZooKeeperExposed exposed = new ZooKeeperExposed(this, cnxn);
//        exposed.intteruptSendThread();
//        exposed.intteruptSendThread();
//      });
//      worker.collect( () -> {
//        ZooKeeperExposed exposed = new ZooKeeperExposed(this, cnxn);
//        exposed.intteruptSendThread();
//        exposed.intteruptSendThread();
//      });// we don't wait for close because we wait below
     // worker.addCollect("zkClientClose");

//      worker.collect(() -> {
//        for (Thread t : spawnedThreads) {
//          t.interrupt();
//        }
//      });

//      worker.collect(() -> {
//        zkcall(cnxn, "sendThread", "interrupt", null);
//        zkcall(cnxn, "eventThread", "interrupt", null);
////
////      //  zkcall(cnxn, "sendThread", "join", 10l);
////      //  zkcall(cnxn, "eventThread", "join", 10l);
////
////        zkcall(cnxn, "sendThread", "interrupt", null);
////        zkcall(cnxn, "eventThread", "interrupt", null);
////
////        zkcall(cnxn, "sendThread", "join", 10l);
////        zkcall(cnxn, "eventThread", "join", 10l);
//      });
//      worker.addCollect("zkClientClose");
  }

  private void zkcall(final ClientCnxn cnxn, String field, String meth, Object arg) {
    try {
      final Field sendThreadFld = cnxn.getClass().getDeclaredField(field);
      sendThreadFld.setAccessible(true);
      Object sendThread = sendThreadFld.get(cnxn);
      if (sendThread != null) {
        Method method;
        if (arg != null) {
          method = sendThread.getClass().getMethod(meth, long.class);
        } else {
          method = sendThread.getClass().getMethod(meth);
        }
        method.setAccessible(true);
        try {
          if (arg != null) {
            method.invoke(sendThread, arg);
          } else {
            method.invoke(sendThread);
          }
        } catch (InvocationTargetException e) {
          // is fine
        }
      }
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
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
