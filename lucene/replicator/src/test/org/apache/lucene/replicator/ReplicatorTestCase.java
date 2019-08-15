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
package org.apache.lucene.replicator;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.util.Random;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.lucene.util.LuceneTestCase;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.session.DefaultSessionIdManager;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.AfterClass;

@ThreadLeakLingering(linger = 80000) // Jetty might ignore interrupt for a minute 
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
  @SuppressWarnings("deprecation")
  public static synchronized Server newHttpServer(Handler handler) throws Exception {
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
        sslcontext.setKeyStorePath
        (System.getProperty("javax.net.ssl.trustStore"));
      }
      if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
        sslcontext.setTrustStorePassword
        (System.getProperty("javax.net.ssl.trustStorePassword"));
      }
      sslcontext.setNeedClientAuth(Boolean.getBoolean("tests.jettySsl.clientAuth"));
    }
    
    final QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setDaemon(true);
    threadPool.setMaxThreads(10000);
    threadPool.setIdleTimeout(5000);
    threadPool.setStopTimeout(30000);

    Server server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.manage(threadPool);


    final ServerConnector connector;
    if (useSsl) {
      HttpConfiguration configuration = new HttpConfiguration();
      configuration.setSecureScheme("https");
      configuration.addCustomizer(new SecureRequestCustomizer());
      ServerConnector c = new ServerConnector(server, new SslConnectionFactory(sslcontext, "http/1.1"),
          new HttpConnectionFactory(configuration));
      connector = c;
    } else {
      ServerConnector c = new ServerConnector(server, new HttpConnectionFactory());
      connector = c;
    }
    
    connector.setPort(0);
    connector.setHost("127.0.0.1");

    server.setConnectors(new Connector[] {connector});
    server.setSessionIdManager(new DefaultSessionIdManager(server, new Random(random().nextLong())));
    server.setHandler(handler);
    
    server.start();
    
    return server;
  }
  
  /** Returns a {@link Server}'s port. */
  public static int serverPort(Server server) {
    return ((ServerConnector)server.getConnectors()[0]).getLocalPort();
  }
  
  /** Returns a {@link Server}'s host. */
  public static String serverHost(Server server) {
    return ((ServerConnector)server.getConnectors()[0]).getHost();
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
   * connection manager, it will be close automatically after all tests have
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
