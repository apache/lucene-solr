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
package org.apache.solr.client.solrj.util;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrInternalHttpClient extends HttpClient implements Closeable {
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final int DEFAULT_IDLE_TIMEOUT = 90000;

  public static final int MAX_OUTSTANDING_REQUESTS = 1000;

  private static AtomicInteger count = new AtomicInteger();

  private SolrQueuedThreadPool qtp;

  private boolean internalQtp;

  public SolrInternalHttpClient(String name) {
    this(name, null, false);
  }

  public SolrInternalHttpClient(String name, boolean start) {
    this(name, null, start);
  }

  public SolrInternalHttpClient(String name, SolrQueuedThreadPool qtp) {
    this(name, qtp, false);
  }

  public SolrInternalHttpClient(String name, SolrQueuedThreadPool qtp, boolean start) {
    super(createTransport(), createSslContextFactory());

    int cnt = count.incrementAndGet();
    String fullName = "JettyHttpClient-" + name + ":" + cnt;
    SolrQueuedThreadPool useQtp;
    if (qtp == null) {
      useQtp = new SolrQueuedThreadPool(10000, 0, 3000, null,
          new ThreadGroup(fullName));
      useQtp.setName(fullName);
      try {
        useQtp.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      // qtp.setReservedThreads(0);
      this.internalQtp = true;
    } else {
      useQtp = qtp;
    }
    this.qtp = useQtp;

    // Scheduler scheduler = new SolrHttpClientScheduler("HttpClientScheduler", true, null, new
    // ThreadGroup("HttpClientScheduler"), 1);

    // httpClient.setScheduler(scheduler);

    setRequestBufferSize(16384);
    setResponseBufferSize(16384);

    setExecutor(this.qtp);
    // httpClient.setRemoveIdleDestinations(true); // nocommit
    setStrictEventOrdering(true);
    setConnectBlocking(true);
    setFollowRedirects(false);
    setMaxConnectionsPerDestination(1);
    setMaxRequestsQueuedPerDestination(MAX_OUTSTANDING_REQUESTS * 4); // comfortably above max outstanding
                                                                      // requests
    setIdleTimeout(DEFAULT_IDLE_TIMEOUT);
    setConnectTimeout(30000); // TODO set lower for tests? nocommit use config

    if (start) {
      try {
        start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    assert ObjectReleaseTracker.track(this);
  }

  private static SslContextFactory createSslContextFactory() {
    // TODO: create connection factory?

    // Instantiate and configure the SslContextFactory
    SslContextFactory sslContextFactory = new SslContextFactory(true);
    sslContextFactory.setProvider("Conscrypt");

    String keyStorePath = System.getProperty("javax.net.ssl.keyStore");
    if (keyStorePath != null) {
      sslContextFactory.setKeyStorePath(keyStorePath);
    }
    String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    if (keyStorePassword != null) {
      sslContextFactory.setKeyStorePassword(keyStorePassword);
    }

    if (Boolean.getBoolean("org.apache.solr.ssl.enabled")) {
      sslContextFactory.setNeedClientAuth(true);
      String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
      if (trustStorePath != null) {
        sslContextFactory.setKeyStorePassword(trustStorePath);
      }
      String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
      if (trustStorePassword != null) {
        sslContextFactory.setKeyStorePassword(trustStorePassword);
      }
    } else {
      sslContextFactory.setNeedClientAuth(false);
    }

    return sslContextFactory;
  }

  public static HttpClientTransport createTransport() {
    HttpClientTransport transport;
    boolean useHttp1_1 = false;
    if (!useHttp1_1) {
      HTTP2Client http2client = new HTTP2Client();
      // TODO
      http2client.setSelectors(2);

      transport = new HttpClientTransportOverHTTP2(http2client);

      // httpClient = new HttpClient(transport, sslContextFactory);
    } else {
      assert false : "We should not use http1.1";
      transport = new HttpClientTransportOverHTTP(2);
      // transport.setConnectionPoolFactory(factory);

      // httpClient = new HttpClient(transport, sslContextFactory);
    }
    return transport;
  }

  public void close() {

    try {
      // this allows 30 seconds until we start interrupting
      setStopTimeout(60000); // nocommit
      stop();

      // now we wait up 30 seconds gracefully, then interrupt again before waiting for the rest of the timeout
      // setStopTimeout(60000);
      // doStop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (internalQtp) {
      IOUtils.closeQuietly(qtp);
    }

    assert ObjectReleaseTracker.release(this);
  }
}
