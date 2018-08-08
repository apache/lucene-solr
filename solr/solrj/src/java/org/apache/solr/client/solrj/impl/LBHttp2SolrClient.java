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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient.Req;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient.Rsp;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

/**
 * LBHttp2SolrClient or "LoadBalanced LBHttp2SolrClient" is a load balancing wrapper around
 * {@link Http2SolrClient}. This is useful when you
 * have multiple Solr servers and the requests need to be Load Balanced among them.
 *
 * Do <b>NOT</b> use this class for indexing in master/slave scenarios since documents must be sent to the
 * correct master; no inter-node routing is done.
 *
 * In SolrCloud (leader/replica) scenarios, it is usually better to use
 * {@link CloudSolrClient}, but this class may be used
 * for updates because the server will forward them to the appropriate leader.
 *
 * <p>
 * It offers automatic failover when a server goes down and it detects when the server comes back up.
 * <p>
 * Load balancing is done using a simple round-robin on the list of servers.
 * <p>
 * If a request to a server fails by an IOException due to a connection timeout or read timeout then the host is taken
 * off the list of live servers and moved to a 'dead server list' and the request is resent to the next live server.
 * This process is continued till it tries all the live servers. If at least one server is alive, the request succeeds,
 * and if not it fails.
 * <blockquote><pre>
 * SolrClient lbHttp2SolrClient = new LBHttp2SolrClient(http2SolrClient, "http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * </pre></blockquote>
 * This detects if a dead server comes alive automatically. The check is done in fixed intervals in a dedicated thread.
 * This interval can be set using {@link #setAliveCheckInterval} , the default is set to one minute.
 * <p>
 * <b>When to use this?</b><br> This can be used as a software load balancer when you do not wish to setup an external
 * load balancer. Alternatives to this code are to use
 * a dedicated hardware load balancer or using Apache httpd with mod_proxy_balancer as a load balancer. See <a
 * href="http://en.wikipedia.org/wiki/Load_balancing_(computing)">Load balancing on Wikipedia</a>
 *
 * @since solr 1.4
 */
public class LBHttp2SolrClient extends SolrClient {
  private static Set<Integer> RETRY_CODES = new HashSet<>(4);

  static {
    RETRY_CODES.add(404);
    RETRY_CODES.add(403);
    RETRY_CODES.add(503);
    RETRY_CODES.add(500);
  }

  // keys to the maps are currently of the form "http://localhost:8983/solr"
  private final Map<String, ServerWrapper> aliveServers = new LinkedHashMap<>();
  // access to aliveServers should be synchronized on itself

  protected final Map<String, ServerWrapper> zombieServers = new ConcurrentHashMap<>();

  // changes to aliveServers are reflected in this array, no need to synchronize
  private volatile ServerWrapper[] aliveServerList = new ServerWrapper[0];


  private ScheduledExecutorService aliveCheckExecutor;

  private final Http2SolrClient httpClient;
  private final AtomicInteger counter = new AtomicInteger(-1);

  private static final SolrQuery solrQuery = new SolrQuery("*:*");
  private volatile ResponseParser parser;
  private volatile RequestWriter requestWriter;

  private Set<String> queryParams = new HashSet<>();
  private Integer connectionTimeout;

  private Integer soTimeout;

  static {
    solrQuery.setRows(0);
    /**
     * Default sort (if we don't supply a sort) is by score and since
     * we request 0 rows any sorting and scoring is not necessary.
     * SolrQuery.DOCID schema-independently specifies a non-scoring sort.
     * <code>_docid_ asc</code> sort is efficient,
     * <code>_docid_ desc</code> sort is not, so choose ascending DOCID sort.
     */
    solrQuery.setSort(SolrQuery.DOCID, SolrQuery.ORDER.asc);
    // not a top-level request, we are interested only in the server being sent to i.e. it need not distribute our request to further servers
    solrQuery.setDistrib(false);
  }

  protected static class ServerWrapper {

    final String baseUrl;

    // "standard" servers are used by default.  They normally live in the alive list
    // and move to the zombie list when unavailable.  When they become available again,
    // they move back to the alive list.
    boolean standard = true;

    int failedPings = 0;

    public ServerWrapper(String baseUrl) {
      this.baseUrl = baseUrl;
    }

    @Override
    public String toString() {
      return baseUrl;
    }

    @Override
    public int hashCode() {
      return baseUrl.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof ServerWrapper)) return false;
      return baseUrl.equals(((ServerWrapper)obj).baseUrl);
    }
  }

  public LBHttp2SolrClient(Http2SolrClient httpClient, String... baseSolrUrls) {
    this.httpClient = httpClient;
    if (baseSolrUrls != null) {
      for (String s : baseSolrUrls) {
        ServerWrapper wrapper = new ServerWrapper(s);
        aliveServers.put(s, wrapper);
      }
    }
    updateAliveList();
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method.
   * @param queryParams set of param keys to only send via the query string
   */
  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }
  public void addQueryParams(String queryOnlyParam) {
    this.queryParams.add(queryOnlyParam) ;
  }

  public static String normalize(String server) {
    if (server.endsWith("/"))
      server = server.substring(0, server.length() - 1);
    return server;
  }

  /**
   * Tries to query a live server from the list provided in Req. Servers in the dead pool are skipped.
   * If a request fails due to an IOException, the server is moved to the dead pool for a certain period of
   * time, or until a test request on that server succeeds.
   *
   * Servers are queried in the exact order given (except servers currently in the dead pool are skipped).
   * If no live servers from the provided list remain to be tried, a number of previously skipped dead servers will be tried.
   * Req.getNumDeadServersToTry() controls how many dead servers will be tried.
   *
   * If no live servers are found a SolrServerException is thrown.
   *
   * @param req contains both the request as well as the list of servers to query
   *
   * @return the result of the request
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public Rsp request(Req req) throws SolrServerException, IOException {
    Rsp rsp = new Rsp();
    Exception ex = null;
    boolean isNonRetryable = req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    List<ServerWrapper> skipped = null;

    final Integer numServersToTry = req.getNumServersToTry();
    int numServersTried = 0;

    boolean timeAllowedExceeded = false;
    long timeAllowedNano = getTimeAllowedInNanos(req.getRequest());
    long timeOutTime = System.nanoTime() + timeAllowedNano;
    for (String serverStr : req.getServers()) {
      if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      serverStr = normalize(serverStr);
      // if the server is currently a zombie, just skip to the next one
      ServerWrapper wrapper = zombieServers.get(serverStr);
      if (wrapper != null) {
        // System.out.println("ZOMBIE SERVER QUERIED: " + serverStr);
        final int numDeadServersToTry = req.getNumDeadServersToTry();
        if (numDeadServersToTry > 0) {
          if (skipped == null) {
            skipped = new ArrayList<>(numDeadServersToTry);
            skipped.add(wrapper);
          }
          else if (skipped.size() < numDeadServersToTry) {
            skipped.add(wrapper);
          }
        }
        continue;
      }
      try {
        MDC.put("LBHttp2SolrClient.url", serverStr);

        if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
          break;
        }

        ++numServersTried;
        ex = doRequest(serverStr, req, rsp, isNonRetryable, false);
        if (ex == null) {
          return rsp; // SUCCESS
        }
      } finally {
        MDC.remove("LBHttp2SolrClient.url");
      }
    }

    // try the servers we previously skipped
    if (skipped != null) {
      for (ServerWrapper wrapper : skipped) {
        if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
          break;
        }

        if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
          break;
        }

        try {
          MDC.put("LBHttp2SolrClient.url", wrapper.baseUrl);
          ++numServersTried;
          ex = doRequest(wrapper.baseUrl, req, rsp, isNonRetryable, true);
          if (ex == null) {
            return rsp; // SUCCESS
          }
        } finally {
          MDC.remove("LBHttp2SolrClient.url");
        }
      }
    }


    final String solrServerExceptionMessage;
    if (timeAllowedExceeded) {
      solrServerExceptionMessage = "Time allowed to handle this request exceeded";
    } else {
      if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
        solrServerExceptionMessage = "No live SolrServers available to handle this request:"
            + " numServersTried="+numServersTried
            + " numServersToTry="+numServersToTry.intValue();
      } else {
        solrServerExceptionMessage = "No live SolrServers available to handle this request";
      }
    }
    if (ex == null) {
      throw new SolrServerException(solrServerExceptionMessage);
    } else {
      throw new SolrServerException(solrServerExceptionMessage+":" + zombieServers.keySet(), ex);
    }

  }

  protected Exception addZombie(String serverStr, Exception e) {

    ServerWrapper wrapper;

    wrapper = new ServerWrapper(serverStr);
    wrapper.standard = false;
    zombieServers.put(serverStr, wrapper);
    startAliveCheckExecutor();
    return e;
  }

  protected Exception doRequest(String serverStr, Req req, Rsp rsp, boolean isNonRetryable,
      boolean isZombie) throws SolrServerException, IOException {
    Exception ex = null;
    try {
      rsp.server = serverStr;
      req.getRequest().setBasePath(serverStr);
      rsp.rsp = httpClient.request(req.getRequest(), null);
      if (isZombie) {
        zombieServers.remove(serverStr);
      }
    } catch (RemoteExecutionException e){
      throw e;
    } catch(SolrException e) {
      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        ex = (!isZombie) ? addZombie(serverStr, e) : e;
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          zombieServers.remove(serverStr);
        }
        throw e;
      }
    } catch (SocketException e) {
      if (!isNonRetryable || e instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(serverStr, e) : e;
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      if (!isNonRetryable) {
        ex = (!isZombie) ? addZombie(serverStr, e) : e;
      } else {
        throw e;
      }
    } catch (SolrServerException e) {
      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        ex = (!isZombie) ? addZombie(serverStr, e) : e;
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(serverStr, e) : e;
      } else {
        throw e;
      }
    } catch (Exception e) {
      throw new SolrServerException(e);
    }

    return ex;
  }

  private void updateAliveList() {
    synchronized (aliveServers) {
      aliveServerList = aliveServers.values().toArray(new ServerWrapper[aliveServers.size()]);
    }
  }

  private ServerWrapper removeFromAlive(String key) {
    synchronized (aliveServers) {
      ServerWrapper wrapper = aliveServers.remove(key);
      if (wrapper != null)
        updateAliveList();
      return wrapper;
    }
  }

  private void addToAlive(ServerWrapper wrapper) {
    synchronized (aliveServers) {
      ServerWrapper prev = aliveServers.put(wrapper.baseUrl, wrapper);
      // TODO: warn if there was a previous entry?
      updateAliveList();
    }
  }

  public void addSolrServer(String server) throws MalformedURLException {
    addToAlive(new ServerWrapper(server));
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

    // there is a small race condition here - if the server is in the process of being moved between
    // lists, we could fail to remove it.
    removeFromAlive(server);
    zombieServers.remove(server);
    return null;
  }


  @Override
  public void close() {
    if (aliveCheckExecutor != null) {
      aliveCheckExecutor.shutdownNow();
    }
  }

  /**
   * Tries to query a live server. A SolrServerException is thrown if all servers are dead.
   * If the request failed due to IOException then the live server is moved to dead pool and the request is
   * retried on another live server.  After live servers are exhausted, any servers previously marked as dead
   * will be tried before failing the request.
   *
   * @param request the SolrRequest.
   *
   * @return response
   *
   * @throws IOException If there is a low-level I/O error.
   */
  @Override
  public NamedList<Object> request(final SolrRequest request, String collection)
          throws SolrServerException, IOException {
    return request(request, collection, null);
  }

  public NamedList<Object> request(final SolrRequest request, String collection,
      final Integer numServersToTry) throws SolrServerException, IOException {
    Exception ex = null;
    ServerWrapper[] serverList = aliveServerList;

    final int maxTries = (numServersToTry == null ? serverList.length : numServersToTry.intValue());
    int numServersTried = 0;
    Map<String,ServerWrapper> justFailed = null;

    boolean timeAllowedExceeded = false;
    long timeAllowedNano = getTimeAllowedInNanos(request);
    long timeOutTime = System.nanoTime() + timeAllowedNano;
    for (int attempts=0; attempts<maxTries; attempts++) {
      if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      int count = counter.incrementAndGet() & Integer.MAX_VALUE;
      ServerWrapper wrapper = serverList[count % serverList.length];

      try {
        ++numServersTried;
        request.setBasePath(wrapper.baseUrl);
        return httpClient.request(request, collection);
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          moveAliveToDead(wrapper);
          if (justFailed == null) justFailed = new HashMap<>();
          justFailed.put(wrapper.baseUrl, wrapper);
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
    }

    // try other standard servers that we didn't try just now
    for (ServerWrapper wrapper : zombieServers.values()) {
      if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      if (wrapper.standard==false || justFailed!=null && justFailed.containsKey(wrapper.baseUrl)) continue;
      try {
        ++numServersTried;
        request.setBasePath(wrapper.baseUrl);
        NamedList<Object> rsp = httpClient.request(request, collection);
        // remove from zombie list *before* adding to alive to avoid a race that could lose a server
        zombieServers.remove(wrapper.baseUrl);
        addToAlive(wrapper);
        return rsp;
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          // still dead
        } else {
          throw e;
        }
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
    }


    final String solrServerExceptionMessage;
    if (timeAllowedExceeded) {
      solrServerExceptionMessage = "Time allowed to handle this request exceeded";
    } else {
      if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
        solrServerExceptionMessage = "No live SolrServers available to handle this request:"
            + " numServersTried="+numServersTried
            + " numServersToTry="+numServersToTry.intValue();
      } else {
        solrServerExceptionMessage = "No live SolrServers available to handle this request";
      }
    }
    if (ex == null) {
      throw new SolrServerException(solrServerExceptionMessage);
    } else {
      throw new SolrServerException(solrServerExceptionMessage, ex);
    }
  }

  /**
   * @return time allowed in nanos, returns -1 if no time_allowed is specified.
   */
  private long getTimeAllowedInNanos(final SolrRequest req) {
    SolrParams reqParams = req.getParams();
    return reqParams == null ? -1 :
      TimeUnit.NANOSECONDS.convert(reqParams.getInt(CommonParams.TIME_ALLOWED, -1), TimeUnit.MILLISECONDS);
  }

  private boolean isTimeExceeded(long timeAllowedNano, long timeOutTime) {
    return timeAllowedNano > 0 && System.nanoTime() > timeOutTime;
  }

  /**
   * Takes up one dead server and check for aliveness. The check is done in a roundrobin. Each server is checked for
   * aliveness once in 'x' millis where x is decided by the setAliveCheckinterval() or it is defaulted to 1 minute
   *
   * @param zombieServer a server in the dead pool
   */
  private void checkAZombieServer(ServerWrapper zombieServer) {
    try {
      QueryRequest queryRequest = new QueryRequest(solrQuery);
      queryRequest.setBasePath(zombieServer.baseUrl);
      QueryResponse resp = queryRequest.process(httpClient);
      if (resp.getStatus() == 0) {
        // server has come back up.
        // make sure to remove from zombies before adding to alive to avoid a race condition
        // where another thread could mark it down, move it back to zombie, and then we delete
        // from zombie and lose it forever.
        ServerWrapper wrapper = zombieServers.remove(zombieServer.baseUrl);
        if (wrapper != null) {
          wrapper.failedPings = 0;
          if (wrapper.standard) {
            addToAlive(wrapper);
          }
        } else {
          // something else already moved the server from zombie to alive
        }
      }
    } catch (Exception e) {
      //Expected. The server is still down.
      zombieServer.failedPings++;

      // If the server doesn't belong in the standard set belonging to this load balancer
      // then simply drop it after a certain number of failed pings.
      if (!zombieServer.standard && zombieServer.failedPings >= NONSTANDARD_PING_LIMIT) {
        zombieServers.remove(zombieServer.baseUrl);
      }
    }
  }

  private void moveAliveToDead(ServerWrapper wrapper) {
    wrapper = removeFromAlive(wrapper.baseUrl);
    if (wrapper == null)
      return;  // another thread already detected the failure and removed it
    zombieServers.put(wrapper.baseUrl, wrapper);
    startAliveCheckExecutor();
  }

  private int interval = CHECK_INTERVAL;

  /**
   * LBHttp2SolrClient keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
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
    // double-checked locking, but it's OK because we don't *do* anything with aliveCheckExecutor
    // if it's not null.
    if (aliveCheckExecutor == null) {
      synchronized (this) {
        if (aliveCheckExecutor == null) {
          aliveCheckExecutor = Executors.newSingleThreadScheduledExecutor(
              new SolrjNamedThreadFactory("aliveCheckExecutor"));
          aliveCheckExecutor.scheduleAtFixedRate(
                  getAliveCheckRunner(new WeakReference<>(this)),
                  this.interval, this.interval, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  private static Runnable getAliveCheckRunner(final WeakReference<LBHttp2SolrClient> lbRef) {
    return () -> {
      LBHttp2SolrClient lb = lbRef.get();
      if (lb != null && lb.zombieServers != null) {
        for (ServerWrapper zombieServer : lb.zombieServers.values()) {
          lb.checkAZombieServer(zombieServer);
        }
      }
    };
  }

  /**
   * Return the HttpClient this instance uses.
   */
  private Http2SolrClient getHttpClient() {
    return httpClient;
  }

  public ResponseParser getParser() {
    return parser;
  }

  /**
   * Changes the {@link ResponseParser} that will be used for the internal
   * SolrServer objects.
   *
   * @param parser Default Response Parser chosen to parse the response if the parser
   *               were not specified as part of the request.
   * @see SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser parser) {
    this.parser = parser;
  }

  /**
   * Changes the {@link RequestWriter} that will be used for the internal
   * SolrServer objects.
   *
   * @param requestWriter Default RequestWriter, used to encode requests sent to the server.
   */
  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }
  
  public RequestWriter getRequestWriter() {
    return requestWriter;
  }
  
  @Override
  protected void finalize() throws Throwable {
    try {
      if(this.aliveCheckExecutor!=null)
        this.aliveCheckExecutor.shutdownNow();
    } finally {
      super.finalize();
    }
  }

  // defaults
  private static final int CHECK_INTERVAL = 60 * 1000; //1 minute between checks
  private static final int NONSTANDARD_PING_LIMIT = 5;  // number of times we'll ping dead servers not in the server list
}
