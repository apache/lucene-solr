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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.ToleratedUpdateError;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

/**
 * SolrJ client class to communicate with SolrCloud.
 * Instances of this class communicate with Zookeeper to discover
 * Solr endpoints for SolrCloud collections, and then use the 
 * {@link LBHttpSolrClient} to issue requests.
 * 
 * This class assumes the id field for your documents is called
 * 'id' - if this is not the case, you must set the right name
 * with {@link #setIdField(String)}.
 */
@SuppressWarnings("serial")
public class CloudSolrClient extends SolrClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ClusterStateProvider stateProvider;
  private volatile String defaultCollection;
  private final LBHttpSolrClient lbClient;
  private final boolean shutdownLBHttpSolrServer;
  private HttpClient myClient;
  private final boolean clientIsInternal;
  //no of times collection state to be reloaded if stale state error is received
  private static final int MAX_STALE_RETRIES = 5;
  Random rand = new Random();
  
  private final boolean updatesToLeaders;
  private final boolean directUpdatesToLeadersOnly;
  private boolean parallelUpdates = true;
  private ExecutorService threadPool = ExecutorUtil
      .newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory(
          "CloudSolrClient ThreadPool"));
  private String idField = "id";
  public static final String STATE_VERSION = "_stateVer_";
  private long retryExpiryTime = TimeUnit.NANOSECONDS.convert(3, TimeUnit.SECONDS);//3 seconds or 3 million nanos
  private final Set<String> NON_ROUTABLE_PARAMS;
  {
    NON_ROUTABLE_PARAMS = new HashSet<>();
    NON_ROUTABLE_PARAMS.add(UpdateParams.EXPUNGE_DELETES);
    NON_ROUTABLE_PARAMS.add(UpdateParams.MAX_OPTIMIZE_SEGMENTS);
    NON_ROUTABLE_PARAMS.add(UpdateParams.COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.WAIT_SEARCHER);
    NON_ROUTABLE_PARAMS.add(UpdateParams.OPEN_SEARCHER);
    
    NON_ROUTABLE_PARAMS.add(UpdateParams.SOFT_COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.PREPARE_COMMIT);
    NON_ROUTABLE_PARAMS.add(UpdateParams.OPTIMIZE);
    
    // Not supported via SolrCloud
    // NON_ROUTABLE_PARAMS.add(UpdateParams.ROLLBACK);

  }
  private volatile List<Object> locks = objectList(3);


  static class StateCache extends ConcurrentHashMap<String, ExpiringCachedDocCollection> {
    final AtomicLong puts = new AtomicLong();
    final AtomicLong hits = new AtomicLong();
    final Lock evictLock = new ReentrantLock(true);
    private volatile long timeToLive = 60 * 1000L;

    @Override
    public ExpiringCachedDocCollection get(Object key) {
      ExpiringCachedDocCollection val = super.get(key);
      if(val == null) {
        // a new collection is likely to be added now.
        //check if there are stale items and remove them
        evictStale();
        return null;
      }
      if(val.isExpired(timeToLive)) {
        super.remove(key);
        return null;
      }
      hits.incrementAndGet();
      return val;
    }

    @Override
    public ExpiringCachedDocCollection put(String key, ExpiringCachedDocCollection value) {
      puts.incrementAndGet();
      return super.put(key, value);
    }

    void evictStale() {
      if(!evictLock.tryLock()) return;
      try {
        for (Entry<String, ExpiringCachedDocCollection> e : entrySet()) {
          if(e.getValue().isExpired(timeToLive)){
            super.remove(e.getKey());
          }
        }
      } finally {
        evictLock.unlock();
      }
    }

  }

  /**
   * This is the time to wait to refetch the state
   * after getting the same state version from ZK
   * <p>
   * secs
   */
  public void setRetryExpiryTime(int secs) {
    this.retryExpiryTime = TimeUnit.NANOSECONDS.convert(secs, TimeUnit.SECONDS);
  }

  public void setSoTimeout(int timeout) {
    lbClient.setSoTimeout(timeout);
  }

  protected final StateCache collectionStateCache = new StateCache();
  class ExpiringCachedDocCollection {
    final DocCollection cached;
    final long cachedAt;
    //This is the time at which the collection is retried and got the same old version
    long retriedAt = -1;
    //flag that suggests that this is potentially to be rechecked
    boolean maybeStale = false;

    ExpiringCachedDocCollection(DocCollection cached) {
      this.cached = cached;
      this.cachedAt = System.nanoTime();
    }

    boolean isExpired(long timeToLiveMs) {
      return (System.nanoTime() - cachedAt)
          > TimeUnit.NANOSECONDS.convert(timeToLiveMs, TimeUnit.MILLISECONDS);
    }

    boolean shoulRetry() {
      if (maybeStale) {// we are not sure if it is stale so check with retry time
        if ((retriedAt == -1 ||
            (System.nanoTime() - retriedAt) > retryExpiryTime)) {
          return true;// we retried a while back. and we could not get anything new.
          //it's likely that it is not going to be available now also.
        }
      }
      return false;
    }

    void setRetriedAt() {
      retriedAt = System.nanoTime();
    }
  }

  /**
   * Create a new client object that connects to Zookeeper and is always aware
   * of the SolrCloud state. If there is a fully redundant Zookeeper quorum and
   * SolrCloud has enough replicas for every shard in a collection, there is no
   * single point of failure. Updates will be sent to shard leaders by default.
   * 
   * @param zkHost
   *          The client endpoint of the zookeeper quorum containing the cloud
   *          state. The full specification for this string is one or more comma
   *          separated HOST:PORT values, followed by an optional chroot value
   *          that starts with a forward slash. Using a chroot allows multiple
   *          applications to coexist in one ensemble. For full details, see the
   *          Zookeeper documentation. Some examples:
   *          <p>
   *          "host1:2181"
   *          <p>
   *          "host1:2181,host2:2181,host3:2181/mysolrchroot"
   *          <p>
   *          "zoo1.example.com:2181,zoo2.example.com:2181,zoo3.example.com:2181"
   *          
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost) {
    this.stateProvider = new ZkClientClusterStateProvider(zkHost);
      this.clientIsInternal = true;
      this.myClient = HttpClientUtil.createClient(null);
      this.lbClient = new LBHttpSolrClient.Builder()
          .withHttpClient(myClient)
          .build();
      this.lbClient.setRequestWriter(new BinaryRequestWriter());
      this.lbClient.setParser(new BinaryResponseParser());
      this.updatesToLeaders = true;
      this.directUpdatesToLeadersOnly = false;
      shutdownLBHttpSolrServer = true;
      lbClient.addQueryParams(STATE_VERSION);
  }

  /**
   * Create a new client object that connects to Zookeeper and is always aware
   * of the SolrCloud state. If there is a fully redundant Zookeeper quorum and
   * SolrCloud has enough replicas for every shard in a collection, there is no
   * single point of failure. Updates will be sent to shard leaders by default.
   *
   * @param zkHost
   *          The client endpoint of the zookeeper quorum containing the cloud
   *          state. The full specification for this string is one or more comma
   *          separated HOST:PORT values, followed by an optional chroot value
   *          that starts with a forward slash. Using a chroot allows multiple
   *          applications to coexist in one ensemble. For full details, see the
   *          Zookeeper documentation. Some examples:
   *          <p>
   *          "host1:2181"
   *          <p>
   *          "host1:2181,host2:2181,host3:2181/mysolrchroot"
   *          <p>
   *          "zoo1.example.com:2181,zoo2.example.com:2181,zoo3.example.com:2181"
   * @param httpClient
   *          the {@link HttpClient} instance to be used for all requests. The
   *          provided httpClient should use a multi-threaded connection manager.
   *          
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost, HttpClient httpClient) {
    this.stateProvider = new ZkClientClusterStateProvider(zkHost);
    this.clientIsInternal = httpClient == null;
    this.myClient = httpClient == null ? HttpClientUtil.createClient(null) : httpClient;
    this.lbClient = createLBHttpSolrClient(myClient);
    this.updatesToLeaders = true;
    this.directUpdatesToLeadersOnly = false;
    shutdownLBHttpSolrServer = true;
    lbClient.addQueryParams(STATE_VERSION);
  }
  
  /**
   * Create a new client object using multiple string values in a Collection
   * instead of a standard zkHost connection string. Note that this method will
   * not be used if there is only one String argument - that will use
   * {@link #CloudSolrClient(String)} instead.
   * 
   * @param zkHosts
   *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
   *          each host in the zookeeper ensemble. Note that with certain
   *          Collection types like HashSet, the order of hosts in the final
   *          connect string may not be in the same order you added them.
   * @param chroot
   *          A chroot value for zookeeper, starting with a forward slash. If no
   *          chroot is required, use null.
   * @throws IllegalArgumentException
   *           if the chroot value does not start with a forward slash.
   * @see #CloudSolrClient(String)
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(Collection<String> zkHosts, String chroot) {
    this(zkHosts, chroot, null);
  }

  /**
   * Create a new client object using multiple string values in a Collection
   * instead of a standard zkHost connection string. Note that this method will
   * not be used if there is only one String argument - that will use
   * {@link #CloudSolrClient(String)} instead.
   *
   * @param zkHosts
   *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
   *          each host in the zookeeper ensemble. Note that with certain
   *          Collection types like HashSet, the order of hosts in the final
   *          connect string may not be in the same order you added them.
   * @param chroot
   *          A chroot value for zookeeper, starting with a forward slash. If no
   *          chroot is required, use null.
   * @param httpClient
   *          the {@link HttpClient} instance to be used for all requests. The provided httpClient should use a
   *          multi-threaded connection manager.
   * @throws IllegalArgumentException
   *           if the chroot value does not start with a forward slash.
   * @see #CloudSolrClient(String)
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(Collection<String> zkHosts, String chroot, HttpClient httpClient) {
    this.stateProvider = new ZkClientClusterStateProvider(zkHosts, chroot);
    this.clientIsInternal = httpClient == null;
    this.myClient = httpClient == null ? HttpClientUtil.createClient(null) : httpClient;
    this.lbClient = createLBHttpSolrClient(myClient);
    this.updatesToLeaders = true;
    this.directUpdatesToLeadersOnly = false;
    shutdownLBHttpSolrServer = true;
  }
  
  /**
   * Create a new client object that connects to Zookeeper and is always aware
   * of the SolrCloud state. If there is a fully redundant Zookeeper quorum and
   * SolrCloud has enough replicas for every shard in a collection, there is no
   * single point of failure. Updates will be sent to shard leaders by default.
   * 
   * @param zkHosts
   *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
   *          each host in the zookeeper ensemble. Note that with certain
   *          Collection types like HashSet, the order of hosts in the final
   *          connect string may not be in the same order you added them.
   * @param chroot
   *          A chroot value for zookeeper, starting with a forward slash. If no
   *          chroot is required, use null.
   * @param httpClient
   *          the {@link HttpClient} instance to be used for all requests. The provided httpClient should use a
   *          multi-threaded connection manager.  If null, a default HttpClient will be used.
   * @param lbSolrClient
   *          LBHttpSolrClient instance for requests.  If null, a default LBHttpSolrClient will be used.
   * @param updatesToLeaders
   *          If true, sends updates to shard leaders.
   *
   * @deprecated use {@link Builder} instead.  This will soon be a protected method, and will only
   * be available for use in implementing subclasses.
   */
  @Deprecated
  public CloudSolrClient(Collection<String> zkHosts, String chroot, HttpClient httpClient, LBHttpSolrClient lbSolrClient, boolean updatesToLeaders) {
    this(zkHosts, chroot, httpClient, lbSolrClient, null, updatesToLeaders, false, null);
  }

  /**
   * Create a new client object that connects to Zookeeper and is always aware
   * of the SolrCloud state. If there is a fully redundant Zookeeper quorum and
   * SolrCloud has enough replicas for every shard in a collection, there is no
   * single point of failure. Updates will be sent to shard leaders by default.
   *
   * @param zkHosts
   *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
   *          each host in the zookeeper ensemble. Note that with certain
   *          Collection types like HashSet, the order of hosts in the final
   *          connect string may not be in the same order you added them.
   * @param chroot
   *          A chroot value for zookeeper, starting with a forward slash. If no
   *          chroot is required, use null.
   * @param httpClient
   *          the {@link HttpClient} instance to be used for all requests. The provided httpClient should use a
   *          multi-threaded connection manager.  If null, a default HttpClient will be used.
   * @param lbSolrClient
   *          LBHttpSolrClient instance for requests.  If null, a default LBHttpSolrClient will be used.
   * @param lbHttpSolrClientBuilder
   *          LBHttpSolrClient builder to construct the LBHttpSolrClient. If null, a default builder will be used.
   * @param updatesToLeaders
   *          If true, sends updates to shard leaders.
   * @param directUpdatesToLeadersOnly
   *          If true, sends direct updates to shard leaders only.
   */
  private CloudSolrClient(Collection<String> zkHosts,
                          String chroot,
                          HttpClient httpClient,
                          LBHttpSolrClient lbSolrClient,
                          LBHttpSolrClient.Builder lbHttpSolrClientBuilder,
                          boolean updatesToLeaders,
                          boolean directUpdatesToLeadersOnly,
                          ClusterStateProvider stateProvider

  ) {
    if (stateProvider == null) {
      this.stateProvider = new ZkClientClusterStateProvider(zkHosts, chroot);
    } else {
      this.stateProvider = stateProvider;
    }
    this.clientIsInternal = httpClient == null;
    this.shutdownLBHttpSolrServer = lbSolrClient == null;
    if(lbHttpSolrClientBuilder != null) lbSolrClient = lbHttpSolrClientBuilder.build();
    if(lbSolrClient != null) httpClient = lbSolrClient.getHttpClient();
    this.myClient = httpClient == null ? HttpClientUtil.createClient(null) : httpClient;
    if (lbSolrClient == null) lbSolrClient = createLBHttpSolrClient(myClient);
    this.lbClient = lbSolrClient;
    this.updatesToLeaders = updatesToLeaders;
    this.directUpdatesToLeadersOnly = directUpdatesToLeadersOnly;
  }
  
  /**
   * @param zkHost
   *          A zookeeper client endpoint.
   * @param updatesToLeaders
   *          If true, sends updates only to shard leaders.
   * @see #CloudSolrClient(String) for full description and details on zkHost
   * @deprecated use {@link CloudSolrClient.Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost, boolean updatesToLeaders) {
    this(zkHost, updatesToLeaders, null);
  }

  /**
   * @param zkHost
   *          A zookeeper client endpoint.
   * @param updatesToLeaders
   *          If true, sends updates only to shard leaders.
   * @param httpClient
   *          the {@link HttpClient} instance to be used for all requests. The provided httpClient should use a
   *          multi-threaded connection manager.
   * @see #CloudSolrClient(String) for full description and details on zkHost
   * @deprecated use {@link CloudSolrClient.Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost, boolean updatesToLeaders, HttpClient httpClient) {
    this.stateProvider = new ZkClientClusterStateProvider(zkHost);
    this.clientIsInternal = httpClient == null;
    this.myClient = httpClient == null ? HttpClientUtil.createClient(null) : httpClient;
    this.lbClient = new LBHttpSolrClient.Builder()
        .withHttpClient(myClient)
        .build();
    this.lbClient.setRequestWriter(new BinaryRequestWriter());
    this.lbClient.setParser(new BinaryResponseParser());
    this.updatesToLeaders = updatesToLeaders;
    this.directUpdatesToLeadersOnly = false;
    shutdownLBHttpSolrServer = true;
    lbClient.addQueryParams(STATE_VERSION);
  }

  /**Sets the cache ttl for DocCollection Objects cached  . This is only applicable for collections which are persisted outside of clusterstate.json
   * @param seconds ttl value in seconds
   */
  public void setCollectionCacheTTl(int seconds){
    assert seconds > 0;
    this.collectionStateCache.timeToLive = seconds * 1000L;
  }

  /**
   * @param zkHost
   *          A zookeeper client endpoint.
   * @param lbClient
   *          LBHttpSolrServer instance for requests.
   * @see #CloudSolrClient(String) for full description and details on zkHost
   * @deprecated use {@link CloudSolrClient.Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost, LBHttpSolrClient lbClient) {
    this(zkHost, lbClient, true);
  }
  
  /**
   * @param zkHost
   *          A zookeeper client endpoint.
   * @param lbClient
   *          LBHttpSolrServer instance for requests.
   * @param updatesToLeaders
   *          If true, sends updates only to shard leaders.
   * @see #CloudSolrClient(String) for full description and details on zkHost
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public CloudSolrClient(String zkHost, LBHttpSolrClient lbClient, boolean updatesToLeaders) {
    this.lbClient = lbClient;
    this.stateProvider = new ZkClientClusterStateProvider(zkHost);
    this.updatesToLeaders = updatesToLeaders;
    this.directUpdatesToLeadersOnly = false;
    shutdownLBHttpSolrServer = false;
    this.clientIsInternal = false;
    lbClient.addQueryParams(STATE_VERSION);
  }
  
  public ResponseParser getParser() {
    return lbClient.getParser();
  }

  /**
   * Note: This setter method is <b>not thread-safe</b>.
   * 
   * @param processor
   *          Default Response Parser chosen to parse the response if the parser
   *          were not specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser processor) {
    lbClient.setParser(processor);
  }
  
  public RequestWriter getRequestWriter() {
    return lbClient.getRequestWriter();
  }
  
  public void setRequestWriter(RequestWriter requestWriter) {
    lbClient.setRequestWriter(requestWriter);
  }

  /**
   * @return the zkHost value used to connect to zookeeper.
   */
  public String getZkHost() {
    return assertZKStateProvider().zkHost;
  }

  public ZkStateReader getZkStateReader() {
    if (stateProvider instanceof ZkClientClusterStateProvider) {
      ZkClientClusterStateProvider provider = (ZkClientClusterStateProvider) stateProvider;
      return provider.zkStateReader;
    }
    throw new IllegalStateException("This has no Zk stateReader");
  }

  /**
   * @param idField the field to route documents on.
   */
  public void setIdField(String idField) {
    this.idField = idField;
  }

  /**
   * @return the field that updates are routed on.
   */
  public String getIdField() {
    return idField;
  }
  
  /** Sets the default collection for request */
  public void setDefaultCollection(String collection) {
    this.defaultCollection = collection;
  }

  /** Gets the default collection for request */
  public String getDefaultCollection() {
    return defaultCollection;
  }

  /** Set the connect timeout to the zookeeper ensemble in ms */
  public void setZkConnectTimeout(int zkConnectTimeout) {
    assertZKStateProvider().zkConnectTimeout = zkConnectTimeout;
  }

  /** Set the timeout to the zookeeper ensemble in ms */
  public void setZkClientTimeout(int zkClientTimeout) {
    assertZKStateProvider().zkClientTimeout = zkClientTimeout;
  }

  /**
   * Connect to the zookeeper ensemble.
   * This is an optional method that may be used to force a connect before any other requests are sent.
   *
   */
  public void connect() {
    stateProvider.connect();
  }

  /**
   * Connect to a cluster.  If the cluster is not ready, retry connection up to a given timeout.
   * @param duration the timeout
   * @param timeUnit the units of the timeout
   * @throws TimeoutException if the cluster is not ready after the timeout
   * @throws InterruptedException if the wait is interrupted
   */
  public void connect(long duration, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
    log.info("Waiting for {} {} for cluster at {} to be ready", duration, timeUnit, stateProvider);
    long timeout = System.nanoTime() + timeUnit.toNanos(duration);
    while (System.nanoTime() < timeout) {
      try {
        connect();
        log.info("Cluster at {} ready", stateProvider);
        return;
      }
      catch (RuntimeException e) {
        // not ready yet, then...
      }
      TimeUnit.MILLISECONDS.sleep(250);
    }
    throw new TimeoutException("Timed out waiting for cluster");
  }

  public void setParallelUpdates(boolean parallelUpdates) {
    this.parallelUpdates = parallelUpdates;
  }

  /**
   * Upload a set of config files to Zookeeper and give it a name
   *
   * NOTE: You should only allow trusted users to upload configs.  If you
   * are allowing client access to zookeeper, you should protect the
   * /configs node against unauthorised write access.
   *
   *  @deprecated Please use {@link ZkClientClusterStateProvider#uploadConfig(Path, String)} instead
   *
   * @param configPath {@link java.nio.file.Path} to the config files
   * @param configName the name of the config
   * @throws IOException if an IO error occurs
   */
  @Deprecated
  public void uploadConfig(Path configPath, String configName) throws IOException {
    stateProvider.connect();
    assertZKStateProvider().uploadConfig(configPath, configName);
  }

  private ZkClientClusterStateProvider assertZKStateProvider() {
    if (stateProvider instanceof ZkClientClusterStateProvider) {
      return (ZkClientClusterStateProvider) stateProvider;
    }
    throw new IllegalArgumentException("This client does not use ZK");

  }

  /**
   * Download a named config from Zookeeper to a location on the filesystem
   *
   * @deprecated Please use {@link ZkClientClusterStateProvider#downloadConfig(String, Path)} instead
   * @param configName    the name of the config
   * @param downloadPath  the path to write config files to
   * @throws IOException  if an I/O exception occurs
   */
  @Deprecated
  public void downloadConfig(String configName, Path downloadPath) throws IOException {
    assertZKStateProvider().downloadConfig(configName, downloadPath);
  }

  /**
   * Block until a collection state matches a predicate, or a timeout
   *
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  a {@link CollectionStatePredicate} to check the collection state
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   */
  public void waitForState(String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {
    stateProvider.connect();
    assertZKStateProvider().zkStateReader.waitForState(collection, wait, unit, predicate);
  }

  /**
   * Register a CollectionStateWatcher to be called when the cluster state for a collection changes
   *
   * Note that the watcher is unregistered after it has been called once.  To make a watcher persistent,
   * it should re-register itself in its {@link CollectionStateWatcher#onStateChanged(Set, DocCollection)}
   * call
   *
   * @param collection the collection to watch
   * @param watcher    a watcher that will be called when the state changes
   */
  public void registerCollectionStateWatcher(String collection, CollectionStateWatcher watcher) {
    stateProvider.connect();
    assertZKStateProvider().zkStateReader.registerCollectionStateWatcher(collection, watcher);
  }

  private NamedList<Object> directUpdate(AbstractUpdateRequest request, String collection) throws SolrServerException {
    UpdateRequest updateRequest = (UpdateRequest) request;
    ModifiableSolrParams params = (ModifiableSolrParams) request.getParams();
    ModifiableSolrParams routableParams = new ModifiableSolrParams();
    ModifiableSolrParams nonRoutableParams = new ModifiableSolrParams();

    if(params != null) {
      nonRoutableParams.add(params);
      routableParams.add(params);
      for(String param : NON_ROUTABLE_PARAMS) {
        routableParams.remove(param);
      }
    }

    if (collection == null) {
      throw new SolrServerException("No collection param specified on request and no default collection has been set.");
    }


    //Check to see if the collection is an alias.
    collection = stateProvider.getCollectionName(collection);

    DocCollection col = getDocCollection(collection, null);

    DocRouter router = col.getRouter();
    
    if (router instanceof ImplicitDocRouter) {
      // short circuit as optimization
      return null;
    }

    //Create the URL map, which is keyed on slice name.
    //The value is a list of URLs for each replica in the slice.
    //The first value in the list is the leader for the slice.
    final Map<String,List<String>> urlMap = buildUrlMap(col);
    final Map<String, LBHttpSolrClient.Req> routes = (urlMap == null ? null : updateRequest.getRoutes(router, col, urlMap, routableParams, this.idField));
    if (routes == null) {
      if (directUpdatesToLeadersOnly && hasInfoToFindLeaders(updateRequest, idField)) {
          // we have info (documents with ids and/or ids to delete) with
          // which to find the leaders but we could not find (all of) them
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "directUpdatesToLeadersOnly==true but could not find leader(s)");
      } else {
        // we could not find a leader or routes yet - use unoptimized general path
        return null;
      }
    }

    final NamedList<Throwable> exceptions = new NamedList<>();
    final NamedList<NamedList> shardResponses = new NamedList<>(routes.size()+1); // +1 for deleteQuery

    long start = System.nanoTime();

    if (parallelUpdates) {
      final Map<String, Future<NamedList<?>>> responseFutures = new HashMap<>(routes.size());
      for (final Map.Entry<String, LBHttpSolrClient.Req> entry : routes.entrySet()) {
        final String url = entry.getKey();
        final LBHttpSolrClient.Req lbRequest = entry.getValue();
        try {
          MDC.put("CloudSolrClient.url", url);
          responseFutures.put(url, threadPool.submit(() -> lbClient.request(lbRequest).getResponse()));
        } finally {
          MDC.remove("CloudSolrClient.url");
        }
      }

      for (final Map.Entry<String, Future<NamedList<?>>> entry: responseFutures.entrySet()) {
        final String url = entry.getKey();
        final Future<NamedList<?>> responseFuture = entry.getValue();
        try {
          shardResponses.add(url, responseFuture.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          exceptions.add(url, e.getCause());
        }
      }

      if (exceptions.size() > 0) {
        Throwable firstException = exceptions.getVal(0);
        if(firstException instanceof SolrException) {
          SolrException e = (SolrException) firstException;
          throw new RouteException(ErrorCode.getErrorCode(e.code()), exceptions, routes);
        } else {
          throw new RouteException(ErrorCode.SERVER_ERROR, exceptions, routes);
        }
      }
    } else {
      for (Map.Entry<String, LBHttpSolrClient.Req> entry : routes.entrySet()) {
        String url = entry.getKey();
        LBHttpSolrClient.Req lbRequest = entry.getValue();
        try {
          NamedList<Object> rsp = lbClient.request(lbRequest).getResponse();
          shardResponses.add(url, rsp);
        } catch (Exception e) {
          if(e instanceof SolrException) {
            throw (SolrException) e;
          } else {
            throw new SolrServerException(e);
          }
        }
      }
    }

    UpdateRequest nonRoutableRequest = null;
    List<String> deleteQuery = updateRequest.getDeleteQuery();
    if (deleteQuery != null && deleteQuery.size() > 0) {
      UpdateRequest deleteQueryRequest = new UpdateRequest();
      deleteQueryRequest.setDeleteQuery(deleteQuery);
      nonRoutableRequest = deleteQueryRequest;
    }
    
    Set<String> paramNames = nonRoutableParams.getParameterNames();
    
    Set<String> intersection = new HashSet<>(paramNames);
    intersection.retainAll(NON_ROUTABLE_PARAMS);
    
    if (nonRoutableRequest != null || intersection.size() > 0) {
      if (nonRoutableRequest == null) {
        nonRoutableRequest = new UpdateRequest();
      }
      nonRoutableRequest.setParams(nonRoutableParams);
      List<String> urlList = new ArrayList<>();
      urlList.addAll(routes.keySet());
      Collections.shuffle(urlList, rand);
      LBHttpSolrClient.Req req = new LBHttpSolrClient.Req(nonRoutableRequest, urlList);
      try {
        LBHttpSolrClient.Rsp rsp = lbClient.request(req);
        shardResponses.add(urlList.get(0), rsp.getResponse());
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, urlList.get(0), e);
      }
    }

    long end = System.nanoTime();

    RouteResponse rr = condenseResponse(shardResponses, (int) TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS));
    rr.setRouteResponses(shardResponses);
    rr.setRoutes(routes);
    return rr;
  }

  private Map<String,List<String>> buildUrlMap(DocCollection col) {
    Map<String, List<String>> urlMap = new HashMap<>();
    Collection<Slice> slices = col.getActiveSlices();
    Iterator<Slice> sliceIterator = slices.iterator();
    while (sliceIterator.hasNext()) {
      Slice slice = sliceIterator.next();
      String name = slice.getName();
      List<String> urls = new ArrayList<>();
      Replica leader = slice.getLeader();
      if (leader == null) {
        if (directUpdatesToLeadersOnly) {
          continue;
        }
        // take unoptimized general path - we cannot find a leader yet
        return null;
      }
      ZkCoreNodeProps zkProps = new ZkCoreNodeProps(leader);
      String url = zkProps.getCoreUrl();
      urls.add(url);
      if (!directUpdatesToLeadersOnly) {
        for (Replica replica : slice.getReplicas()) {
          if (!replica.getNodeName().equals(leader.getNodeName()) &&
              !replica.getName().equals(leader.getName())) {
            ZkCoreNodeProps zkProps1 = new ZkCoreNodeProps(replica);
            String url1 = zkProps1.getCoreUrl();
            urls.add(url1);
          }
        }
      }
      urlMap.put(name, urls);
    }
    return urlMap;
  }

  public RouteResponse condenseResponse(NamedList response, int timeMillis) {
    RouteResponse condensed = new RouteResponse();
    int status = 0;
    Integer rf = null;
    Integer minRf = null;
    
    // TolerantUpdateProcessor
    List<SimpleOrderedMap<String>> toleratedErrors = null; 
    int maxToleratedErrors = Integer.MAX_VALUE;
      
    for(int i=0; i<response.size(); i++) {
      NamedList shardResponse = (NamedList)response.getVal(i);
      NamedList header = (NamedList)shardResponse.get("responseHeader");      
      Integer shardStatus = (Integer)header.get("status");
      int s = shardStatus.intValue();
      if(s > 0) {
          status = s;
      }
      Object rfObj = header.get(UpdateRequest.REPFACT);
      if (rfObj != null && rfObj instanceof Integer) {
        Integer routeRf = (Integer)rfObj;
        if (rf == null || routeRf < rf)
          rf = routeRf;
      }
      minRf = (Integer)header.get(UpdateRequest.MIN_REPFACT);

      List<SimpleOrderedMap<String>> shardTolerantErrors = 
        (List<SimpleOrderedMap<String>>) header.get("errors");
      if (null != shardTolerantErrors) {
        Integer shardMaxToleratedErrors = (Integer) header.get("maxErrors");
        assert null != shardMaxToleratedErrors : "TolerantUpdateProcessor reported errors but not maxErrors";
        // if we get into some weird state where the nodes disagree about the effective maxErrors,
        // assume the min value seen to decide if we should fail.
        maxToleratedErrors = Math.min(maxToleratedErrors,
                                      ToleratedUpdateError.getEffectiveMaxErrors(shardMaxToleratedErrors.intValue()));
        
        if (null == toleratedErrors) {
          toleratedErrors = new ArrayList<SimpleOrderedMap<String>>(shardTolerantErrors.size());
        }
        for (SimpleOrderedMap<String> err : shardTolerantErrors) {
          toleratedErrors.add(err);
        }
      }
    }

    NamedList cheader = new NamedList();
    cheader.add("status", status);
    cheader.add("QTime", timeMillis);
    if (rf != null)
      cheader.add(UpdateRequest.REPFACT, rf);
    if (minRf != null)
      cheader.add(UpdateRequest.MIN_REPFACT, minRf);
    if (null != toleratedErrors) {
      cheader.add("maxErrors", ToleratedUpdateError.getUserFriendlyMaxErrors(maxToleratedErrors));
      cheader.add("errors", toleratedErrors);
      if (maxToleratedErrors < toleratedErrors.size()) {
        // cumulative errors are too high, we need to throw a client exception w/correct metadata

        // NOTE: it shouldn't be possible for 1 == toleratedErrors.size(), because if that were the case
        // then at least one shard should have thrown a real error before this, so we don't worry
        // about having a more "singular" exception msg for that situation
        StringBuilder msgBuf =  new StringBuilder()
          .append(toleratedErrors.size()).append(" Async failures during distributed update: ");
          
        NamedList metadata = new NamedList<String>();
        for (SimpleOrderedMap<String> err : toleratedErrors) {
          ToleratedUpdateError te = ToleratedUpdateError.parseMap(err);
          metadata.add(te.getMetadataKey(), te.getMetadataValue());
          
          msgBuf.append("\n").append(te.getMessage());
        }
        
        SolrException toThrow = new SolrException(ErrorCode.BAD_REQUEST, msgBuf.toString());
        toThrow.setMetadata(metadata);
        throw toThrow;
      }
    }
    condensed.add("responseHeader", cheader);
    return condensed;
  }

  public static class RouteResponse extends NamedList {
    private NamedList routeResponses;
    private Map<String, LBHttpSolrClient.Req> routes;

    public void setRouteResponses(NamedList routeResponses) {
      this.routeResponses = routeResponses;
    }

    public NamedList getRouteResponses() {
      return routeResponses;
    }

    public void setRoutes(Map<String, LBHttpSolrClient.Req> routes) {
      this.routes = routes;
    }

    public Map<String, LBHttpSolrClient.Req> getRoutes() {
      return routes;
    }

  }

  public static class RouteException extends SolrException {

    private NamedList<Throwable> throwables;
    private Map<String, LBHttpSolrClient.Req> routes;

    public RouteException(ErrorCode errorCode, NamedList<Throwable> throwables, Map<String, LBHttpSolrClient.Req> routes){
      super(errorCode, throwables.getVal(0).getMessage(), throwables.getVal(0));
      this.throwables = throwables;
      this.routes = routes;

      // create a merged copy of the metadata from all wrapped exceptions
      NamedList<String> metadata = new NamedList<String>();
      for (int i = 0; i < throwables.size(); i++) {
        Throwable t = throwables.getVal(i);
        if (t instanceof SolrException) {
          SolrException e = (SolrException) t;
          NamedList<String> eMeta = e.getMetadata();
          if (null != eMeta) {
            metadata.addAll(eMeta);
          }
        }
      }
      if (0 < metadata.size()) {
        this.setMetadata(metadata);
      }
    }

    public NamedList<Throwable> getThrowables() {
      return throwables;
    }

    public Map<String, LBHttpSolrClient.Req> getRoutes() {
      return this.routes;
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
    SolrParams reqParams = request.getParams();

    if (collection == null)
      collection = (reqParams != null) ? reqParams.get("collection", getDefaultCollection()) : getDefaultCollection();
    return requestWithRetryOnStaleState(request, 0, collection);
  }

  /**
   * As this class doesn't watch external collections on the client side,
   * there's a chance that the request will fail due to cached stale state,
   * which means the state must be refreshed from ZK and retried.
   */
  protected NamedList<Object> requestWithRetryOnStaleState(SolrRequest request, int retryCount, String collection)
      throws SolrServerException, IOException {

    connect(); // important to call this before you start working with the ZkStateReader

    // build up a _stateVer_ param to pass to the server containing all of the
    // external collection state versions involved in this request, which allows
    // the server to notify us that our cached state for one or more of the external
    // collections is stale and needs to be refreshed ... this code has no impact on internal collections
    String stateVerParam = null;
    List<DocCollection> requestedCollections = null;
    boolean isAdmin = ADMIN_PATHS.contains(request.getPath());
    if (collection != null &&  !isAdmin) { // don't do _stateVer_ checking for admin requests
      Set<String> requestedCollectionNames = getCollectionNames(collection);

      StringBuilder stateVerParamBuilder = null;
      for (String requestedCollection : requestedCollectionNames) {
        // track the version of state we're using on the client side using the _stateVer_ param
        DocCollection coll = getDocCollection(requestedCollection, null);
        int collVer = coll.getZNodeVersion();
        if (coll.getStateFormat()>1) {
          if(requestedCollections == null) requestedCollections = new ArrayList<>(requestedCollectionNames.size());
          requestedCollections.add(coll);

          if (stateVerParamBuilder == null) {
            stateVerParamBuilder = new StringBuilder();
          } else {
            stateVerParamBuilder.append("|"); // hopefully pipe is not an allowed char in a collection name
          }

          stateVerParamBuilder.append(coll.getName()).append(":").append(collVer);
        }
      }

      if (stateVerParamBuilder != null) {
        stateVerParam = stateVerParamBuilder.toString();
      }
    }

    if (request.getParams() instanceof ModifiableSolrParams) {
      ModifiableSolrParams params = (ModifiableSolrParams) request.getParams();
      if (stateVerParam != null) {
        params.set(STATE_VERSION, stateVerParam);
      } else {
        params.remove(STATE_VERSION);
      }
    } // else: ??? how to set this ???

    NamedList<Object> resp = null;
    try {
      resp = sendRequest(request, collection);
      //to avoid an O(n) operation we always add STATE_VERSION to the last and try to read it from there
      Object o = resp == null || resp.size() == 0 ? null : resp.get(STATE_VERSION, resp.size() - 1);
      if(o != null && o instanceof Map) {
        //remove this because no one else needs this and tests would fail if they are comparing responses
        resp.remove(resp.size()-1);
        Map invalidStates = (Map) o;
        for (Object invalidEntries : invalidStates.entrySet()) {
          Map.Entry e = (Map.Entry) invalidEntries;
          getDocCollection((String) e.getKey(), (Integer) e.getValue());
        }

      }
    } catch (Exception exc) {

      Throwable rootCause = SolrException.getRootCause(exc);
      // don't do retry support for admin requests or if the request doesn't have a collection specified
      if (collection == null || isAdmin) {
        if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else if (exc instanceof IOException) {
          throw (IOException)exc;
        }else if (exc instanceof RuntimeException) {
          throw (RuntimeException) exc;
        }
        else {
          throw new SolrServerException(rootCause);
        }
      }

      int errorCode = (rootCause instanceof SolrException) ?
          ((SolrException)rootCause).code() : SolrException.ErrorCode.UNKNOWN.code;

      log.error("Request to collection {} failed due to ("+errorCode+
          ") {}, retry? "+retryCount, collection, rootCause.toString());

      boolean wasCommError =
          (rootCause instanceof ConnectException ||
              rootCause instanceof ConnectTimeoutException ||
              rootCause instanceof NoHttpResponseException ||
              rootCause instanceof SocketException);

      if (wasCommError) {
        // it was a communication error. it is likely that
        // the node to which the request to be sent is down . So , expire the state
        // so that the next attempt would fetch the fresh state
        // just re-read state for all of them, if it has not been retired
        // in retryExpiryTime time
        for (DocCollection ext : requestedCollections) {
          ExpiringCachedDocCollection cacheEntry = collectionStateCache.get(ext.getName());
          if (cacheEntry == null) continue;
          cacheEntry.maybeStale = true;
        }
        if (retryCount < MAX_STALE_RETRIES) {//if it is a communication error , we must try again
          //may be, we have a stale version of the collection state
          // and we could not get any information from the server
          //it is probably not worth trying again and again because
          // the state would not have been updated
          return requestWithRetryOnStaleState(request, retryCount + 1, collection);
        }
      }

      boolean stateWasStale = false;
      if (retryCount < MAX_STALE_RETRIES  &&
          requestedCollections != null    &&
          !requestedCollections.isEmpty() &&
          SolrException.ErrorCode.getErrorCode(errorCode) == SolrException.ErrorCode.INVALID_STATE)
      {
        // cached state for one or more external collections was stale
        // re-issue request using updated state
        stateWasStale = true;

        // just re-read state for all of them, which is a little heavy handed but hopefully a rare occurrence
        for (DocCollection ext : requestedCollections) {
          collectionStateCache.remove(ext.getName());
        }
      }

      // if we experienced a communication error, it's worth checking the state
      // with ZK just to make sure the node we're trying to hit is still part of the collection
      if (retryCount < MAX_STALE_RETRIES &&
          !stateWasStale &&
          requestedCollections != null &&
          !requestedCollections.isEmpty() &&
          wasCommError) {
        for (DocCollection ext : requestedCollections) {
          DocCollection latestStateFromZk = getDocCollection(ext.getName(), null);
          if (latestStateFromZk.getZNodeVersion() != ext.getZNodeVersion()) {
            // looks like we couldn't reach the server because the state was stale == retry
            stateWasStale = true;
            // we just pulled state from ZK, so update the cache so that the retry uses it
            collectionStateCache.put(ext.getName(), new ExpiringCachedDocCollection(latestStateFromZk));
          }
        }
      }

      if (requestedCollections != null) {
        requestedCollections.clear(); // done with this
      }

      // if the state was stale, then we retry the request once with new state pulled from Zk
      if (stateWasStale) {
        log.warn("Re-trying request to  collection(s) "+collection+" after stale state error from server.");
        resp = requestWithRetryOnStaleState(request, retryCount+1, collection);
      } else {
        if(exc instanceof SolrException) {
          throw exc;
        } if (exc instanceof SolrServerException) {
          throw (SolrServerException)exc;
        } else if (exc instanceof IOException) {
          throw (IOException)exc;
        } else {
          throw new SolrServerException(rootCause);
        }
      }
    }

    return resp;
  }

  protected NamedList<Object> sendRequest(SolrRequest request, String collection)
      throws SolrServerException, IOException {
    connect();

    boolean sendToLeaders = false;
    List<String> replicas = null;
    
    if (request instanceof IsUpdateRequest) {
      if (request instanceof UpdateRequest) {
        NamedList<Object> response = directUpdate((AbstractUpdateRequest) request, collection);
        if (response != null) {
          return response;
        }
      }
      sendToLeaders = true;
      replicas = new ArrayList<>();
    }
    
    SolrParams reqParams = request.getParams();
    if (reqParams == null) {
      reqParams = new ModifiableSolrParams();
    }
    List<String> theUrlList = new ArrayList<>();
    if (ADMIN_PATHS.contains(request.getPath())) {
      Set<String> liveNodes = stateProvider.liveNodes();
      for (String liveNode : liveNodes) {
        theUrlList.add(ZkStateReader.getBaseUrlForNodeName(liveNode,
            (String) stateProvider.getClusterProperties().getOrDefault(ZkStateReader.URL_SCHEME,"http")));
      }
    } else {
      
      if (collection == null) {
        throw new SolrServerException(
            "No collection param specified on request and no default collection has been set.");
      }

      Set<String> collectionNames = getCollectionNames(collection);
      if (collectionNames.size() == 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Could not find collection: " + collection);
      }

      String shardKeys =  reqParams.get(ShardParams._ROUTE_);

      // TODO: not a big deal because of the caching, but we could avoid looking
      // at every shard
      // when getting leaders if we tweaked some things
      
      // Retrieve slices from the cloud state and, for each collection
      // specified,
      // add it to the Map of slices.
      Map<String,Slice> slices = new HashMap<>();
      for (String collectionName : collectionNames) {
        DocCollection col = getDocCollection(collectionName, null);
        Collection<Slice> routeSlices = col.getRouter().getSearchSlices(shardKeys, reqParams , col);
        ClientUtils.addSlices(slices, collectionName, routeSlices, true);
      }
      Set<String> liveNodes = stateProvider.liveNodes();

      List<String> leaderUrlList = null;
      List<String> urlList = null;
      List<String> replicasList = null;
      
      // build a map of unique nodes
      // TODO: allow filtering by group, role, etc
      Map<String,ZkNodeProps> nodes = new HashMap<>();
      List<String> urlList2 = new ArrayList<>();
      for (Slice slice : slices.values()) {
        for (ZkNodeProps nodeProps : slice.getReplicasMap().values()) {
          ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
          String node = coreNodeProps.getNodeName();
          if (!liveNodes.contains(coreNodeProps.getNodeName())
              || Replica.State.getState(coreNodeProps.getState()) != Replica.State.ACTIVE) continue;
          if (nodes.put(node, nodeProps) == null) {
            if (!sendToLeaders || coreNodeProps.isLeader()) {
              String url;
              if (reqParams.get(UpdateParams.COLLECTION) == null) {
                url = ZkCoreNodeProps.getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP), collection);
              } else {
                url = coreNodeProps.getCoreUrl();
              }
              urlList2.add(url);
            } else {
              String url;
              if (reqParams.get(UpdateParams.COLLECTION) == null) {
                url = ZkCoreNodeProps.getCoreUrl(nodeProps.getStr(ZkStateReader.BASE_URL_PROP), collection);
              } else {
                url = coreNodeProps.getCoreUrl();
              }
              replicas.add(url);
            }
          }
        }
      }
      
      if (sendToLeaders) {
        leaderUrlList = urlList2;
        replicasList = replicas;
      } else {
        urlList = urlList2;
      }
      
      if (sendToLeaders) {
        theUrlList = new ArrayList<>(leaderUrlList.size());
        theUrlList.addAll(leaderUrlList);
      } else {
        theUrlList = new ArrayList<>(urlList.size());
        theUrlList.addAll(urlList);
      }

      Collections.shuffle(theUrlList, rand);
      if (sendToLeaders) {
        ArrayList<String> theReplicas = new ArrayList<>(
            replicasList.size());
        theReplicas.addAll(replicasList);
        Collections.shuffle(theReplicas, rand);
        theUrlList.addAll(theReplicas);
      }
      
      if (theUrlList.isEmpty()) {
        for (String s : collectionNames) {
          if (s != null) collectionStateCache.remove(s);
        }
        throw new SolrException(SolrException.ErrorCode.INVALID_STATE,
            "Could not find a healthy node to handle the request.");
      }
    }

    LBHttpSolrClient.Req req = new LBHttpSolrClient.Req(request, theUrlList);
    LBHttpSolrClient.Rsp rsp = lbClient.request(req);
    return rsp.getResponse();
  }

  Set<String> getCollectionNames(String collection) {
    // Extract each comma separated collection name and store in a List.
    List<String> rawCollectionsList = StrUtils.splitSmart(collection, ",", true);
    Set<String> collectionNames = new HashSet<>();
    // validate collections
    for (String collectionName : rawCollectionsList) {
      if (stateProvider.getState(collectionName) == null) {
        String alias = stateProvider.getAlias(collection);
        if (alias != null) {
          List<String> aliasList = StrUtils.splitSmart(alias, ",", true);
          collectionNames.addAll(aliasList);
          continue;
        }

          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection not found: " + collectionName);
        }

      collectionNames.add(collectionName);
    }
    return collectionNames;
  }

  @Override
  public void close() throws IOException {
    stateProvider.close();
    
    if (shutdownLBHttpSolrServer) {
      lbClient.close();
    }
    
    if (clientIsInternal && myClient!=null) {
      HttpClientUtil.close(myClient);
    }

    if(this.threadPool != null && !this.threadPool.isShutdown()) {
      this.threadPool.shutdown();
    }
  }

  public LBHttpSolrClient getLbClient() {
    return lbClient;
  }

  public HttpClient getHttpClient() {
    return myClient;
  }
  
  public boolean isUpdatesToLeaders() {
    return updatesToLeaders;
  }

  /**
   * @return true if direct updates are sent to shard leaders only
   */
  public boolean isDirectUpdatesToLeadersOnly() {
    return directUpdatesToLeadersOnly;
  }

  /**If caches are expired they are refreshed after acquiring a lock.
   * use this to set the number of locks
   */
  public void setParallelCacheRefreshes(int n){ locks = objectList(n); }

  private static ArrayList<Object> objectList(int n) {
    ArrayList<Object> l =  new ArrayList<>(n);
    for(int i=0;i<n;i++) l.add(new Object());
    return l;
  }


  protected DocCollection getDocCollection(String collection, Integer expectedVersion) throws SolrException {
    if (expectedVersion == null) expectedVersion = -1;
    if (collection == null) return null;
    ExpiringCachedDocCollection cacheEntry = collectionStateCache.get(collection);
    DocCollection col = cacheEntry == null ? null : cacheEntry.cached;
    if (col != null) {
      if (expectedVersion <= col.getZNodeVersion()
          && !cacheEntry.shoulRetry()) return col;
    }

    ClusterState.CollectionRef ref = getCollectionRef(collection);
    if (ref == null) {
      //no such collection exists
      return null;
    }
    if (!ref.isLazilyLoaded()) {
      //it is readily available just return it
      return ref.get();
    }
    List locks = this.locks;
    final Object lock = locks.get(Math.abs(Hash.murmurhash3_x86_32(collection, 0, collection.length(), 0) % locks.size()));
    DocCollection fetchedCol = null;
    synchronized (lock) {
      /*we have waited for sometime just check once again*/
      cacheEntry = collectionStateCache.get(collection);
      col = cacheEntry == null ? null : cacheEntry.cached;
      if (col != null) {
        if (expectedVersion <= col.getZNodeVersion()
            && !cacheEntry.shoulRetry()) return col;
      }
      // We are going to fetch a new version
      // we MUST try to get a new version
      fetchedCol = ref.get();//this is a call to ZK
      if (fetchedCol == null) return null;// this collection no more exists
      if (col != null && fetchedCol.getZNodeVersion() == col.getZNodeVersion()) {
        cacheEntry.setRetriedAt();//we retried and found that it is the same version
        cacheEntry.maybeStale = false;
      } else {
        if (fetchedCol.getStateFormat() > 1)
          collectionStateCache.put(collection, new ExpiringCachedDocCollection(fetchedCol));
      }
      return fetchedCol;
    }
  }

  ClusterState.CollectionRef getCollectionRef(String collection) {
    return stateProvider.getState(collection);
  }

  /**
   * Useful for determining the minimum achieved replication factor across
   * all shards involved in processing an update request, typically useful
   * for gauging the replication factor of a batch. 
   */
  @SuppressWarnings("rawtypes")
  public int getMinAchievedReplicationFactor(String collection, NamedList resp) {
    // it's probably already on the top-level header set by condense
    NamedList header = (NamedList)resp.get("responseHeader");
    Integer achRf = (Integer)header.get(UpdateRequest.REPFACT);
    if (achRf != null)
      return achRf.intValue();

    // not on the top-level header, walk the shard route tree
    Map<String,Integer> shardRf = getShardReplicationFactor(collection, resp);
    for (Integer rf : shardRf.values()) {
      if (achRf == null || rf < achRf) {
        achRf = rf;
      }
    }    
    return (achRf != null) ? achRf.intValue() : -1;
  }
  
  /**
   * Walks the NamedList response after performing an update request looking for
   * the replication factor that was achieved in each shard involved in the request.
   * For single doc updates, there will be only one shard in the return value. 
   */
  @SuppressWarnings("rawtypes")
  public Map<String,Integer> getShardReplicationFactor(String collection, NamedList resp) {
    connect();
    
    Map<String,Integer> results = new HashMap<String,Integer>();
    if (resp instanceof CloudSolrClient.RouteResponse) {
      NamedList routes = ((CloudSolrClient.RouteResponse)resp).getRouteResponses();
      DocCollection coll = getDocCollection(collection, null);
      Map<String,String> leaders = new HashMap<String,String>();
      for (Slice slice : coll.getActiveSlices()) {
        Replica leader = slice.getLeader();
        if (leader != null) {
          ZkCoreNodeProps zkProps = new ZkCoreNodeProps(leader);
          String leaderUrl = zkProps.getBaseUrl() + "/" + zkProps.getCoreName();
          leaders.put(leaderUrl, slice.getName());
          String altLeaderUrl = zkProps.getBaseUrl() + "/" + collection;
          leaders.put(altLeaderUrl, slice.getName());
        }
      }
      
      Iterator<Map.Entry<String,Object>> routeIter = routes.iterator();
      while (routeIter.hasNext()) {
        Map.Entry<String,Object> next = routeIter.next();
        String host = next.getKey();
        NamedList hostResp = (NamedList)next.getValue();
        Integer rf = (Integer)((NamedList)hostResp.get("responseHeader")).get(UpdateRequest.REPFACT);
        if (rf != null) {
          String shard = leaders.get(host);
          if (shard == null) {
            if (host.endsWith("/"))
              shard = leaders.get(host.substring(0,host.length()-1));
            if (shard == null) {
              shard = host;
            }
          }
          results.put(shard, rf);
        }
      }
    }    
    return results;
  }
  
  public void setConnectionTimeout(int timeout) {
    this.lbClient.setConnectionTimeout(timeout); 
  }

  public ClusterStateProvider getClusterStateProvider(){
    return stateProvider;
  }

  private static boolean hasInfoToFindLeaders(UpdateRequest updateRequest, String idField) {
    final Map<SolrInputDocument,Map<String,Object>> documents = updateRequest.getDocumentsMap();
    final Map<String,Map<String,Object>> deleteById = updateRequest.getDeleteByIdMap();

    final boolean hasNoDocuments = (documents == null || documents.isEmpty());
    final boolean hasNoDeleteById = (deleteById == null || deleteById.isEmpty());
    if (hasNoDocuments && hasNoDeleteById) {
      // no documents and no delete-by-id, so no info to find leader(s)
      return false;
    }

    if (documents != null) {
      for (final Map.Entry<SolrInputDocument,Map<String,Object>> entry : documents.entrySet()) {
        final SolrInputDocument doc = entry.getKey();
        final Object fieldValue = doc.getFieldValue(idField);
        if (fieldValue == null) {
          // a document with no id field value, so can't find leader for it
          return false;
        }
      }
    }

    return true;
  }

  private static LBHttpSolrClient createLBHttpSolrClient(HttpClient httpClient) {
    final LBHttpSolrClient lbClient = new LBHttpSolrClient.Builder()
        .withHttpClient(httpClient)
        .build();
    lbClient.setRequestWriter(new BinaryRequestWriter());
    lbClient.setParser(new BinaryResponseParser());
    
    return lbClient;
  }

  /**
   * Constructs {@link CloudSolrClient} instances from provided configuration.
   */
  public static class Builder {
    private Collection<String> zkHosts;
    private HttpClient httpClient;
    private String zkChroot;
    private LBHttpSolrClient loadBalancedSolrClient;
    private LBHttpSolrClient.Builder lbClientBuilder;
    private boolean shardLeadersOnly;
    private boolean directUpdatesToLeadersOnly;
    private ClusterStateProvider stateProvider;


    public Builder() {
      this.zkHosts = new ArrayList();
      this.shardLeadersOnly = true;
    }
    
    /**
     * Provide a ZooKeeper client endpoint to be used when configuring {@link CloudSolrClient} instances.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * @param zkHost
     *          The client endpoint of the ZooKeeper quorum containing the cloud
     *          state.
     */
    public Builder withZkHost(String zkHost) {
      this.zkHosts.add(zkHost);
      return this;
    }
    
    /**
     * Provides a {@link HttpClient} for the builder to use when creating clients.
     */
    public Builder withLBHttpSolrClientBuilder(LBHttpSolrClient.Builder lbHttpSolrClientBuilder) {
      this.lbClientBuilder = lbHttpSolrClientBuilder;
      return this;
    }

    /**
     * Provides a {@link HttpClient} for the builder to use when creating clients.
     */
    public Builder withHttpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }


    /**
     * Provide a series of ZooKeeper client endpoints for the builder to use when creating clients.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * @param zkHosts
     *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
     *          each host in the ZooKeeper ensemble. Note that with certain
     *          Collection types like HashSet, the order of hosts in the final
     *          connect string may not be in the same order you added them.
     */
    public Builder withZkHost(Collection<String> zkHosts) {
      this.zkHosts.addAll(zkHosts);
      return this;
    }

    /**
     * Provides a ZooKeeper chroot for the builder to use when creating clients.
     */
    public Builder withZkChroot(String zkChroot) {
      this.zkChroot = zkChroot;
      return this;
    }
    
    /**
     * Provides a {@link LBHttpSolrClient} for the builder to use when creating clients.
     */
    public Builder withLBHttpSolrClient(LBHttpSolrClient loadBalancedSolrClient) {
      this.loadBalancedSolrClient = loadBalancedSolrClient;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients should send updats only to shard leaders.
     */
    public Builder sendUpdatesOnlyToShardLeaders() {
      shardLeadersOnly = true;
      return this;
    }
    
    /**
     * Tells {@link Builder} that created clients should send updates to all replicas for a shard.
     */
    public Builder sendUpdatesToAllReplicasInShard() {
      shardLeadersOnly = false;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients should send direct updates to shard leaders only.
     */
    public Builder sendDirectUpdatesToShardLeadersOnly() {
      directUpdatesToLeadersOnly = true;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients can send updates
     * to any shard replica (shard leaders and non-leaders).
     */
    public Builder sendDirectUpdatesToAnyShardReplica() {
      directUpdatesToLeadersOnly = false;
      return this;
    }

    public Builder withClusterStateProvider(ClusterStateProvider stateProvider) {
      this.stateProvider = stateProvider;
      return this;
    }

    /**
     * Create a {@link CloudSolrClient} based on the provided configuration.
     */
    public CloudSolrClient build() {
      if (stateProvider == null) {
        stateProvider = new ZkClientClusterStateProvider(zkHosts, zkChroot);
      }
      return new CloudSolrClient(zkHosts, zkChroot, httpClient, loadBalancedSolrClient, lbClientBuilder,
          shardLeadersOnly, directUpdatesToLeadersOnly, stateProvider);
    }
  }

  interface ClusterStateProvider extends Closeable {

    ClusterState.CollectionRef getState(String collection);

    Set<String> liveNodes();

    String getAlias(String collection);

    String getCollectionName(String name);

    Map<String, Object> getClusterProperties();

    void connect();
  }
}
