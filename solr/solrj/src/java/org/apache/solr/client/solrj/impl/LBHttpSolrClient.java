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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteExecutionException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

/**
 * LBHttpSolrClient or "LoadBalanced HttpSolrClient" is a load balancing wrapper around
 * {@link HttpSolrClient}. This is useful when you
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
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient("http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
 * //or if you wish to pass the HttpClient do as follows
 * httpClient httpClient = new HttpClient();
 * SolrClient lbHttpSolrClient = new LBHttpSolrClient(httpClient, "http://host1:8080/solr/", "http://host2:8080/solr", "http://host2:8080/solr");
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
public class LBHttpSolrClient extends SolrClient {
  private static Set<Integer> RETRY_CODES = new HashSet<>(4);

  static {
    RETRY_CODES.add(404);
    RETRY_CODES.add(403);
    RETRY_CODES.add(503);
    RETRY_CODES.add(500);
  }

  // keys to the maps are currently of the form "http://localhost:8983/solr"
  // which should be equivalent to HttpSolrServer.getBaseURL()
  private final Map<String, ServerWrapper> aliveServers = new LinkedHashMap<>();
  // access to aliveServers should be synchronized on itself
  
  protected final Map<String, ServerWrapper> zombieServers = new ConcurrentHashMap<>();

  // changes to aliveServers are reflected in this array, no need to synchronize
  private volatile ServerWrapper[] aliveServerList = new ServerWrapper[0];


  private volatile ScheduledExecutorService aliveCheckExecutor;

  private final HttpClient httpClient;
  private final boolean clientIsInternal;
  private final HttpSolrClient.Builder httpSolrClientBuilder;
  private final AtomicInteger counter = new AtomicInteger(-1);

  private static final SolrQuery solrQuery = new SolrQuery("*:*");
  private volatile ResponseParser parser;
  private volatile RequestWriter requestWriter;

  private Set<String> queryParams = new HashSet<>();
  private Integer connectionTimeout;

  private volatile Integer soTimeout;

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

    final HttpSolrClient client;

    // "standard" servers are used by default.  They normally live in the alive list
    // and move to the zombie list when unavailable.  When they become available again,
    // they move back to the alive list.
    boolean standard = true;

    int failedPings = 0;

    public ServerWrapper(HttpSolrClient client) {
      this.client = client;
    }

    @Override
    public String toString() {
      return client.getBaseURL();
    }

    public String getKey() {
      return client.getBaseURL();
    }

    @Override
    public int hashCode() {
      return this.getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof ServerWrapper)) return false;
      return this.getKey().equals(((ServerWrapper)obj).getKey());
    }
  }

  public static class Req {
    protected SolrRequest request;
    protected List<String> servers;
    protected int numDeadServersToTry;
    private final Integer numServersToTry;

    public Req(SolrRequest request, List<String> servers) {
      this(request, servers, null);
    }

    public Req(SolrRequest request, List<String> servers, Integer numServersToTry) {
      this.request = request;
      this.servers = servers;
      this.numDeadServersToTry = servers.size();
      this.numServersToTry = numServersToTry;
    }

    public SolrRequest getRequest() {
      return request;
    }
    public List<String> getServers() {
      return servers;
    }

    /** @return the number of dead servers to try if there are no live servers left */
    public int getNumDeadServersToTry() {
      return numDeadServersToTry;
    }

    /** @param numDeadServersToTry The number of dead servers to try if there are no live servers left.
     * Defaults to the number of servers in this request. */
    public void setNumDeadServersToTry(int numDeadServersToTry) {
      this.numDeadServersToTry = numDeadServersToTry;
    }

    public Integer getNumServersToTry() {
      return numServersToTry;
    }
  }

  public static class Rsp {
    protected String server;
    protected NamedList<Object> rsp;

    /** The response from the server */
    public NamedList<Object> getResponse() {
      return rsp;
    }

    /** The server that returned the response */
    public String getServer() {
      return server;
    }
  }

  /**
   * The provided httpClient should use a multi-threaded connection manager
   *
   * @deprecated use {@link LBHttpSolrClient#LBHttpSolrClient(Builder)} instead, as it is a more extension/subclassing-friendly alternative
   */
  @Deprecated
  protected LBHttpSolrClient(HttpSolrClient.Builder httpSolrClientBuilder,
                          HttpClient httpClient, String... solrServerUrl) {
    this(new Builder()
        .withHttpSolrClientBuilder(httpSolrClientBuilder)
        .withHttpClient(httpClient)
        .withBaseSolrUrls(solrServerUrl));
  }

  /**
   * The provided httpClient should use a multi-threaded connection manager
   *
   * @deprecated use {@link LBHttpSolrClient#LBHttpSolrClient(Builder)} instead, as it is a more extension/subclassing-friendly alternative
   */
  @Deprecated
  protected LBHttpSolrClient(HttpClient httpClient, ResponseParser parser, String... solrServerUrl) {
    this(new Builder()
        .withBaseSolrUrls(solrServerUrl)
        .withResponseParser(parser)
        .withHttpClient(httpClient));
  }

  protected LBHttpSolrClient(Builder builder) {
    this.clientIsInternal = builder.httpClient == null;
    this.httpSolrClientBuilder = builder.httpSolrClientBuilder;
    this.httpClient = builder.httpClient == null ? constructClient(builder.baseSolrUrls.toArray(new String[builder.baseSolrUrls.size()])) : builder.httpClient;
    this.connectionTimeout = builder.connectionTimeoutMillis;
    this.soTimeout = builder.socketTimeoutMillis;    
    this.parser = builder.responseParser;

    if (! builder.baseSolrUrls.isEmpty()) {
      for (String s : builder.baseSolrUrls) {
        ServerWrapper wrapper = new ServerWrapper(makeSolrClient(s));
        aliveServers.put(wrapper.getKey(), wrapper);
      }
    }
    updateAliveList();
  }

  private HttpClient constructClient(String[] solrServerUrl) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (solrServerUrl != null && solrServerUrl.length > 1) {
      // we prefer retrying another server
      params.set(HttpClientUtil.PROP_USE_RETRY, false);
    } else {
      params.set(HttpClientUtil.PROP_USE_RETRY, true);
    }
    return HttpClientUtil.createClient(params);
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

  protected HttpSolrClient makeSolrClient(String server) {
    HttpSolrClient client;
    if (httpSolrClientBuilder != null) {
      synchronized (this) {
        httpSolrClientBuilder
            .withBaseSolrUrl(server)
            .withHttpClient(httpClient);
        if (connectionTimeout != null) {
          httpSolrClientBuilder.withConnectionTimeout(connectionTimeout);
        }
        if (soTimeout != null) {
          httpSolrClientBuilder.withSocketTimeout(soTimeout);
        }
        client = httpSolrClientBuilder.build();
      }
    } else {
      final HttpSolrClient.Builder clientBuilder = new HttpSolrClient.Builder(server)
          .withHttpClient(httpClient)
          .withResponseParser(parser);
      if (connectionTimeout != null) {
        clientBuilder.withConnectionTimeout(connectionTimeout);
      }
      if (soTimeout != null) {
        clientBuilder.withSocketTimeout(soTimeout);
      }
      client = clientBuilder.build();
    }
    if (requestWriter != null) {
      client.setRequestWriter(requestWriter);
    }
    if (queryParams != null) {
      client.setQueryParams(queryParams);
    }
    return client;
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
        MDC.put("LBHttpSolrClient.url", serverStr);

        if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
          break;
        }

        HttpSolrClient client = makeSolrClient(serverStr);

        ++numServersTried;
        ex = doRequest(client, req, rsp, isNonRetryable, false, null);
        if (ex == null) {
          return rsp; // SUCCESS
        }
      } finally {
        MDC.remove("LBHttpSolrClient.url");
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
          MDC.put("LBHttpSolrClient.url", wrapper.client.getBaseURL());
          ++numServersTried;
          ex = doRequest(wrapper.client, req, rsp, isNonRetryable, true, wrapper.getKey());
          if (ex == null) {
            return rsp; // SUCCESS
          }
        } finally {
          MDC.remove("LBHttpSolrClient.url");
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

  protected Exception addZombie(HttpSolrClient server, Exception e) {

    ServerWrapper wrapper;

    wrapper = new ServerWrapper(server);
    wrapper.standard = false;
    zombieServers.put(wrapper.getKey(), wrapper);
    startAliveCheckExecutor();
    return e;
  }  

  protected Exception doRequest(HttpSolrClient client, Req req, Rsp rsp, boolean isNonRetryable,
      boolean isZombie, String zombieKey) throws SolrServerException, IOException {
    Exception ex = null;
    try {
      rsp.server = client.getBaseURL();
      rsp.rsp = client.request(req.getRequest(), (String) null);
      if (isZombie) {
        zombieServers.remove(zombieKey);
      }
    } catch (RemoteExecutionException e){
      throw e;
    } catch(SolrException e) {
      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        ex = (!isZombie) ? addZombie(client, e) : e;
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          zombieServers.remove(zombieKey);
        }
        throw e;
      }
    } catch (SocketException e) {
      if (!isNonRetryable || e instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(client, e) : e;
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      if (!isNonRetryable) {
        ex = (!isZombie) ? addZombie(client, e) : e;
      } else {
        throw e;
      }
    } catch (SolrServerException e) {
      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        ex = (!isZombie) ? addZombie(client, e) : e;
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(client, e) : e;
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
      ServerWrapper prev = aliveServers.put(wrapper.getKey(), wrapper);
      // TODO: warn if there was a previous entry?
      updateAliveList();
    }
  }

  public void addSolrServer(String server) throws MalformedURLException {
    HttpSolrClient client = makeSolrClient(server);
    addToAlive(new ServerWrapper(client));
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

  /**
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setConnectionTimeout(int timeout) {
    this.connectionTimeout = timeout;
    synchronized (aliveServers) {
      Iterator<ServerWrapper> wrappersIt = aliveServers.values().iterator();
      while (wrappersIt.hasNext()) {
        wrappersIt.next().client.setConnectionTimeout(timeout);
      }
    }
    Iterator<ServerWrapper> wrappersIt = zombieServers.values().iterator();
    while (wrappersIt.hasNext()) {
      wrappersIt.next().client.setConnectionTimeout(timeout);
    }
  }

  /**
   * set soTimeout (read timeout) on the underlying HttpConnectionManager. This is desirable for queries, but probably
   * not for indexing.
   *
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setSoTimeout(int timeout) {
    this.soTimeout = timeout;
    synchronized (aliveServers) {
      Iterator<ServerWrapper> wrappersIt = aliveServers.values().iterator();
      while (wrappersIt.hasNext()) {
        wrappersIt.next().client.setSoTimeout(timeout);
      }
    }
    Iterator<ServerWrapper> wrappersIt = zombieServers.values().iterator();
    while (wrappersIt.hasNext()) {
      wrappersIt.next().client.setSoTimeout(timeout);
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (aliveCheckExecutor != null) {
        aliveCheckExecutor.shutdownNow();
        ExecutorUtil.shutdownAndAwaitTermination(aliveCheckExecutor);
      }
    }

    if(clientIsInternal) {
      HttpClientUtil.close(httpClient);
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
        return wrapper.client.request(request, collection);
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        if (e.getRootCause() instanceof IOException) {
          ex = e;
          moveAliveToDead(wrapper);
          if (justFailed == null) justFailed = new HashMap<>();
          justFailed.put(wrapper.getKey(), wrapper);
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
      
      if (wrapper.standard==false || justFailed!=null && justFailed.containsKey(wrapper.getKey())) continue;
      try {
        ++numServersTried;
        NamedList<Object> rsp = wrapper.client.request(request, collection);
        // remove from zombie list *before* adding to alive to avoid a race that could lose a server
        zombieServers.remove(wrapper.getKey());
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
      QueryResponse resp = zombieServer.client.query(solrQuery);
      if (resp.getStatus() == 0) {
        // server has come back up.
        // make sure to remove from zombies before adding to alive to avoid a race condition
        // where another thread could mark it down, move it back to zombie, and then we delete
        // from zombie and lose it forever.
        ServerWrapper wrapper = zombieServers.remove(zombieServer.getKey());
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
        zombieServers.remove(zombieServer.getKey());
      }
    }
  }

  private void moveAliveToDead(ServerWrapper wrapper) {
    wrapper = removeFromAlive(wrapper.getKey());
    if (wrapper == null)
      return;  // another thread already detected the failure and removed it
    zombieServers.put(wrapper.getKey(), wrapper);
    startAliveCheckExecutor();
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

  private static Runnable getAliveCheckRunner(final WeakReference<LBHttpSolrClient> lbRef) {
    return () -> {
      LBHttpSolrClient lb = lbRef.get();
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
  public HttpClient getHttpClient() {
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
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
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

  // defaults
  private static final int CHECK_INTERVAL = 60 * 1000; //1 minute between checks
  private static final int NONSTANDARD_PING_LIMIT = 5;  // number of times we'll ping dead servers not in the server list

  /**
   * Constructs {@link LBHttpSolrClient} instances from provided configuration.
   */
  public static class Builder extends SolrClientBuilder<Builder> {
    protected final List<String> baseSolrUrls;
    protected HttpSolrClient.Builder httpSolrClientBuilder;

    public Builder() {
      this.baseSolrUrls = new ArrayList<>();
      this.responseParser = new BinaryResponseParser();
    }

    public HttpSolrClient.Builder getHttpSolrClientBuilder() {
      return httpSolrClientBuilder;
    }
    
    /**
     * Provide a Solr endpoint to be used when configuring {@link LBHttpSolrClient} instances.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * Two different paths can be specified as a part of the URL:
     * 
     * 1) A path pointing directly at a particular core
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrl("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     * Note that when a core is provided in the base URL, queries and other requests can be made without mentioning the
     * core explicitly.  However, the client can only send requests to that core.
     * 
     * 2) The path of the root Solr path ("/solr")
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrl("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     * In this case the client is more flexible and can be used to send requests to any cores.  This flexibility though
     * requires that the core is specified on all requests.
     */
    public Builder withBaseSolrUrl(String baseSolrUrl) {
      this.baseSolrUrls.add(baseSolrUrl);
      return this;
    }
 
    /**
     * Provide Solr endpoints to be used when configuring {@link LBHttpSolrClient} instances.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * Two different paths can be specified as a part of each URL:
     * 
     * 1) A path pointing directly at a particular core
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrls("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     * Note that when a core is provided in the base URL, queries and other requests can be made without mentioning the
     * core explicitly.  However, the client can only send requests to that core.
     * 
     * 2) The path of the root Solr path ("/solr")
     * <pre>
     *   SolrClient client = builder.withBaseSolrUrls("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     * In this case the client is more flexible and can be used to send requests to any cores.  This flexibility though
     * requires that the core is specified on all requests.
     */
    public Builder withBaseSolrUrls(String... solrUrls) {
      for (String baseSolrUrl : solrUrls) {
        this.baseSolrUrls.add(baseSolrUrl);
      }
      return this;
    }

    /**
     * Provides a {@link HttpSolrClient.Builder} to be used for building the internally used clients.
     */
    public Builder withHttpSolrClientBuilder(HttpSolrClient.Builder builder) {
      this.httpSolrClientBuilder = builder;
      return this;
    }

    /**
     * Create a {@link HttpSolrClient} based on provided configuration.
     */
    public LBHttpSolrClient build() {
      return new LBHttpSolrClient(this);
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
