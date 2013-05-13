package org.apache.lucene.replicator;

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

import java.net.SocketException;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;

@SuppressCodecs("Lucene3x")
public abstract class ReplicatorTestCase extends LuceneTestCase {
  
  private static final int BASE_PORT = 7000;
  
  // if a test calls newServer() multiple times, or some ports already failed,
  // don't start from BASE_PORT again
  private static int lastPortUsed = -1;
  
  private static ClientConnectionManager clientConnectionManager;
  
  @AfterClass
  public static void afterClassReplicatorTestCase() throws Exception {
    if (clientConnectionManager != null) {
      clientConnectionManager.shutdown();
      clientConnectionManager = null;
    }
  }
  
  /**
   * Returns a new {@link Server HTTP Server} instance. To obtain its port, use
   * {@link #serverPort(Server)}.
   */
  public static synchronized Server newHttpServer(Handler handler) throws Exception {
    int port = lastPortUsed == -1 ? BASE_PORT : lastPortUsed + 1;
    Server server = null;
    while (true) {
      try {
        server = new Server(port);
        
        server.setHandler(handler);
        
        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setDaemon(true);
        threadPool.setMaxIdleTimeMs(0);
        server.setThreadPool(threadPool);
        
        // this will test the port
        server.start();
        
        // if here, port is available
        lastPortUsed = port;
        return server;
      } catch (SocketException e) {
        stopHttpServer(server);
        // this is ok, we'll try the next port until successful.
        ++port;
      }
    }
  }
  
  /**
   * Returns a {@link Server}'s port. This method assumes that no
   * {@link Connector}s were added to the Server besides the default one.
   */
  public static int serverPort(Server httpServer) {
    return httpServer.getConnectors()[0].getPort();
  }
  
  /**
   * Stops the given HTTP Server instance. This method does its best to guarantee
   * that no threads will be left running following this method.
   */
  public static void stopHttpServer(Server httpServer) throws Exception {
    httpServer.stop();
    httpServer.join();
  }
  
  /**
   * Returns a {@link ClientConnectionManager}.
   * <p>
   * <b>NOTE:</b> do not {@link ClientConnectionManager#shutdown()} this
   * connection manager, it will be shutdown automatically after all tests have
   * finished.
   */
  public static synchronized ClientConnectionManager getClientConnectionManager() {
    if (clientConnectionManager == null) {
      PoolingClientConnectionManager ccm = new PoolingClientConnectionManager();
      ccm.setDefaultMaxPerRoute(128);
      ccm.setMaxTotal(128);
      clientConnectionManager = ccm;
    }
    
    return clientConnectionManager;
  }
  
}
