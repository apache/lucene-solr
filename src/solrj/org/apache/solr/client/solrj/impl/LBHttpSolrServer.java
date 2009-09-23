/**
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
package org.apache.solr.client.solrj.impl;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LBHttpSolrServer or "LoadBalanced HttpSolrServer" is just a wrapper to CommonsHttpSolrServer. This is useful when you
 * have multiple SolrServers and the requests need to be Load Balanced among them. This should <b>NOT</b> be used for
 * indexing. Also see the <a href="http://wiki.apache.org/solr/LBHttpSolrServer">wiki</a> page.
 * <p/>
 * It offers automatic failover when a server goes down and it detects when the server comes back up.
 * <p/>
 * Load balancing is done using a simple roundrobin on the list of servers.
 * <p/>
 * If a request to a server fails by an IOException due to a connection timeout or read timeout then the host is taken
 * off the list of live servers and moved to a 'dead server list' and the request is resent to the next live server.
 * This process is continued till it tries all the live servers. If atleast one server is alive, the request succeeds,
 * andif not it fails.
 * <blockquote><pre>
 * SolrServer lbHttpSolrServer = new LBHttpSolrServer("http://host1:8080/solr/","http://host2:8080/solr","http://host2:8080/solr");
 * //or if you wish to pass the HttpClient do as follows
 * httpClient httpClient =  new HttpClient();
 * SolrServer lbHttpSolrServer = new LBHttpSolrServer(httpClient,"http://host1:8080/solr/","http://host2:8080/solr","http://host2:8080/solr");
 * </pre></blockquote>
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in a dedicated thread.
 * This interval can be set using {@see #setAliveCheckInterval} , the default is set to one minute.
 * <p/>
 * <b>When to use this?</b><br/> This can be used as a software load balancer when you do not wish to setup an external
 * load balancer. The code is relatively new and the API is currently experimental. Alternatives to this code are to use
 * a dedicated hardware load balancer or using Apache httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @version $Id$
 * @since solr 1.4
 */
public class LBHttpSolrServer extends SolrServer {
  private final CopyOnWriteArrayList<ServerWrapper> aliveServers = new CopyOnWriteArrayList<ServerWrapper>();
  private final CopyOnWriteArrayList<ServerWrapper> zombieServers = new CopyOnWriteArrayList<ServerWrapper>();
  private ScheduledExecutorService aliveCheckExecutor;

  private HttpClient httpClient;
  private final AtomicInteger counter = new AtomicInteger(-1);

  private ReentrantLock checkLock = new ReentrantLock();
  private static final SolrQuery solrQuery = new SolrQuery("*:*");

  static {
    solrQuery.setRows(0);
  }

  private static class ServerWrapper {
    final CommonsHttpSolrServer solrServer;

    // Used only by the thread in aliveCheckExecutor
    long lastUsed, lastChecked;

    int failedPings = 0;

    public ServerWrapper(CommonsHttpSolrServer solrServer) {
      this.solrServer = solrServer;
    }

    public String toString() {
      return solrServer.getBaseURL();
    }
  }

  public LBHttpSolrServer(String... solrServerUrls) throws MalformedURLException {
    this(new HttpClient(new MultiThreadedHttpConnectionManager()), solrServerUrls);
  }

  public LBHttpSolrServer(HttpClient httpClient, String... solrServerUrl)
          throws MalformedURLException {
    this(httpClient, new BinaryResponseParser(), solrServerUrl);
  }

  public LBHttpSolrServer(HttpClient httpClient, ResponseParser parser, String... solrServerUrl)
          throws MalformedURLException {
    this.httpClient = httpClient;
    for (String s : solrServerUrl) {
      aliveServers.add(new ServerWrapper(new CommonsHttpSolrServer(s, httpClient, parser)));
    }
  }

  public void addSolrServer(String server) throws MalformedURLException {
    CommonsHttpSolrServer solrServer = new CommonsHttpSolrServer(server, httpClient);
    checkLock.lock();
    try {
      aliveServers.add(new ServerWrapper(solrServer));
    } finally {
      checkLock.unlock();
    }
  }

  public String removeSolrServer(String server) {
    try {
      server = new URL(server).toExternalForm();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    if (server.endsWith("/")) {
      server = server.substring(0, server.length() - 1);
    }
    this.checkLock.lock();
    try {
      for (ServerWrapper serverWrapper : aliveServers) {
        if (serverWrapper.solrServer.getBaseURL().equals(server)) {
          aliveServers.remove(serverWrapper);
          return serverWrapper.solrServer.getBaseURL();
        }
      }
      if (zombieServers.isEmpty()) return null;

      for (ServerWrapper serverWrapper : zombieServers) {
        if (serverWrapper.solrServer.getBaseURL().equals(server)) {
          zombieServers.remove(serverWrapper);
          return serverWrapper.solrServer.getBaseURL();
        }
      }
    } finally {
      checkLock.unlock();
    }
    return null;
  }

  public void setConnectionTimeout(int timeout) {
    httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(timeout);
  }

  /**
   * set connectionManagerTimeout on the HttpClient.*
   */
  public void setConnectionManagerTimeout(int timeout) {
    httpClient.getParams().setConnectionManagerTimeout(timeout);
  }

  /**
   * set soTimeout (read timeout) on the underlying HttpConnectionManager. This is desirable for queries, but probably
   * not for indexing.
   */
  public void setSoTimeout(int timeout) {
    httpClient.getParams().setSoTimeout(timeout);
  }

  /**
   * Tries to query a live server. If no live servers are found it throws a SolrServerException. If the request failed
   * due to IOException then the live server is moved to dead pool and the request is retried on another live server if
   * available. If all live servers are exhausted then a SolrServerException is thrown.
   *
   * @param request the SolrRequest.
   *
   * @return response
   *
   * @throws SolrServerException
   * @throws IOException
   */
  public NamedList<Object> request(final SolrRequest request)
          throws SolrServerException, IOException {
    int count = counter.incrementAndGet();
    int attempts = 0;
    Exception ex;
    int startSize = aliveServers.size();
    while (true) {
      int size = aliveServers.size();
      if (size < 1) throw new SolrServerException("No live SolrServers available to handle this request");
      ServerWrapper solrServer;
      try {
        solrServer = aliveServers.get(count % size);
      } catch (IndexOutOfBoundsException e) {
        //this list changes dynamically. so it is expected to get IndexOutOfBoundsException
        continue;
      }
      try {
        return solrServer.solrServer.request(request);
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          moveAliveToDead(solrServer);
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
      attempts++;
      if (attempts >= startSize)
        throw new SolrServerException("No live SolrServers available to handle this request", ex);
    }
  }

  /**
   * Takes up one dead server and check for aliveness. The check is done in a roundrobin. Each server is checked for
   * aliveness once in 'x' millis where x is decided by the setAliveCheckinterval() or it is defaulted to 1 minute
   *
   * @param zombieServer a server in the dead pool
   */
  private void checkAZombieServer(ServerWrapper zombieServer) {
    long currTime = System.currentTimeMillis();
    checkLock.lock();
    try {
      zombieServer.lastChecked = currTime;
      QueryResponse resp = zombieServer.solrServer.query(solrQuery);
      if (resp.getStatus() == 0) {
        //server has come back up
        zombieServer.lastUsed = currTime;
        zombieServers.remove(zombieServer);
        aliveServers.add(zombieServer);
        zombieServer.failedPings = 0;
      }
    } catch (Exception e) {
      zombieServer.failedPings++;
      //Expected . The server is still down
    } finally {
      checkLock.unlock();
    }
  }

  private void moveAliveToDead(ServerWrapper solrServer) {
    checkLock.lock();
    try {
      boolean result = aliveServers.remove(solrServer);
      if (result) {
        if (zombieServers.addIfAbsent(solrServer)) {
          startAliveCheckExecutor();
        }
      }
    } finally {
      checkLock.unlock();
    }
  }

  private int interval = CHECK_INTERVAL;

  /**
   * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
   * interval
   *
   * @param interval time in milliseconds
   */
  public void setAliveCheckInterval(int interval) {
    if (interval <= 0) {
      throw new IllegalArgumentException("Alive check interval must be " +
              "positive, specified value = " + interval);
    }
    this.interval = interval;
  }

  private void startAliveCheckExecutor() {
    if (aliveCheckExecutor == null) {
      synchronized (this) {
        if (aliveCheckExecutor == null) {
          aliveCheckExecutor = Executors.newSingleThreadScheduledExecutor();
          aliveCheckExecutor.scheduleAtFixedRate(
                  getAliveCheckRunner(new WeakReference<LBHttpSolrServer>(this)),
                  this.interval, this.interval, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  private static Runnable getAliveCheckRunner(final WeakReference<LBHttpSolrServer> lbHttpSolrServer) {
    return new Runnable() {
      public void run() {
        LBHttpSolrServer solrServer = lbHttpSolrServer.get();
        if (solrServer != null && solrServer.zombieServers != null) {
          for (ServerWrapper zombieServer : solrServer.zombieServers) {
            solrServer.checkAZombieServer(zombieServer);
          }
        }
      }
    };
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  protected void finalize() throws Throwable {
    try {
      if(this.aliveCheckExecutor!=null)
        this.aliveCheckExecutor.shutdownNow();
    } finally {
      super.finalize();
    }
  }

  private static final int CHECK_INTERVAL = 60 * 1000; //1 minute between checks
}
