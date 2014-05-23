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

import java.util.Random;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;

@SuppressCodecs("Lucene3x")
public abstract class ReplicatorTestCase extends LuceneTestCase {
  
  private static HttpClientConnectionManager clientConnectionManager;
  
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
    Server server = new Server(0);
    
    server.setHandler(handler);
    
    final String connectorName = System.getProperty("tests.jettyConnector", "SelectChannel");
    
    // if this property is true, then jetty will be configured to use SSL
    // leveraging the same system properties as java to specify
    // the keystore/truststore if they are set
    //
    // This means we will use the same truststore, keystore (and keys) for
    // the server as well as any client actions taken by this JVM in
    // talking to that server, but for the purposes of testing that should 
    // be good enough
    final boolean useSsl = Boolean.getBoolean("tests.jettySsl");
    final SslContextFactory sslcontext = new SslContextFactory(false);
    
    if (useSsl) {
      if (null != System.getProperty("javax.net.ssl.keyStore")) {
        sslcontext.setKeyStorePath
        (System.getProperty("javax.net.ssl.keyStore"));
      }
      if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
        sslcontext.setKeyStorePassword
        (System.getProperty("javax.net.ssl.keyStorePassword"));
      }
      if (null != System.getProperty("javax.net.ssl.trustStore")) {
        sslcontext.setTrustStore
        (System.getProperty("javax.net.ssl.trustStore"));
      }
      if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
        sslcontext.setTrustStorePassword
        (System.getProperty("javax.net.ssl.trustStorePassword"));
      }
      sslcontext.setNeedClientAuth(Boolean.getBoolean("tests.jettySsl.clientAuth"));
    }
    
    final Connector connector;
    final QueuedThreadPool threadPool;
    if ("SelectChannel".equals(connectorName)) {
      final SelectChannelConnector c = useSsl ? new SslSelectChannelConnector(sslcontext) : new SelectChannelConnector();
      c.setReuseAddress(true);
      c.setLowResourcesMaxIdleTime(1500);
      connector = c;
      threadPool = (QueuedThreadPool) c.getThreadPool();
    } else if ("Socket".equals(connectorName)) {
      final SocketConnector c = useSsl ? new SslSocketConnector(sslcontext) : new SocketConnector();
      c.setReuseAddress(true);
      connector = c;
      threadPool = (QueuedThreadPool) c.getThreadPool();
    } else {
      throw new IllegalArgumentException("Illegal value for system property 'tests.jettyConnector': " + connectorName);
    }
    
    connector.setPort(0);
    connector.setHost("127.0.0.1");
    if (threadPool != null) {
      threadPool.setDaemon(true);
      threadPool.setMaxThreads(10000);
      threadPool.setMaxIdleTimeMs(5000);
      threadPool.setMaxStopTimeMs(30000);
    }
    
    server.setConnectors(new Connector[] {connector});
    server.setSessionIdManager(new HashSessionIdManager(new Random(random().nextLong())));
    
    server.start();
    
    return server;
  }
  
  /** Returns a {@link Server}'s port. */
  public static int serverPort(Server server) {
    return server.getConnectors()[0].getLocalPort();
  }
  
  /** Returns a {@link Server}'s host. */
  public static String serverHost(Server server) {
    return server.getConnectors()[0].getHost();
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
   * Returns a {@link HttpClientConnectionManager}.
   * <p>
   * <b>NOTE:</b> do not {@link HttpClientConnectionManager#shutdown()} this
   * connection manager, it will be shutdown automatically after all tests have
   * finished.
   */
  public static synchronized HttpClientConnectionManager getClientConnectionManager() {
    if (clientConnectionManager == null) {
      PoolingHttpClientConnectionManager ccm = new PoolingHttpClientConnectionManager();
      ccm.setDefaultMaxPerRoute(128);
      ccm.setMaxTotal(128);
      clientConnectionManager = ccm;
    }
    
    return clientConnectionManager;
  }
  
}
