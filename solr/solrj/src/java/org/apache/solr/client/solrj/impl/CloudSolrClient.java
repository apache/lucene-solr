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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

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
public class CloudSolrClient extends BaseCloudSolrClient {

  private final ClusterStateProvider stateProvider;
  private final LBHttpSolrClient lbClient;
  private final boolean shutdownLBHttpSolrServer;
  private HttpClient myClient;
  private final boolean clientIsInternal;

  public static final String STATE_VERSION = BaseCloudSolrClient.STATE_VERSION;

  /**
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setSoTimeout(int timeout) {
    lbClient.setSoTimeout(timeout);
  }

  /**
   * Create a new client object that connects to Zookeeper and is always aware
   * of the SolrCloud state. If there is a fully redundant Zookeeper quorum and
   * SolrCloud has enough replicas for every shard in a collection, there is no
   * single point of failure. Updates will be sent to shard leaders by default.
   *
   * @param builder a {@link CloudSolrClient.Builder} with the options used to create the client.
   */
  protected CloudSolrClient(Builder builder) {
    super(builder.shardLeadersOnly, builder.parallelUpdates, builder.directUpdatesToLeadersOnly);
    if (builder.stateProvider == null) {
      if (builder.zkHosts != null && builder.solrUrls != null) {
        throw new IllegalArgumentException("Both zkHost(s) & solrUrl(s) have been specified. Only specify one.");
      }
      if (builder.zkHosts != null) {
        this.stateProvider = new ZkClientClusterStateProvider(builder.zkHosts, builder.zkChroot);
      } else if (builder.solrUrls != null && !builder.solrUrls.isEmpty()) {
        try {
          this.stateProvider = new HttpClusterStateProvider(builder.solrUrls, builder.httpClient);
        } catch (Exception e) {
          throw new RuntimeException("Couldn't initialize a HttpClusterStateProvider (is/are the "
              + "Solr server(s), "  + builder.solrUrls + ", down?)", e);
        }
      } else {
        throw new IllegalArgumentException("Both zkHosts and solrUrl cannot be null.");
      }
    } else {
      this.stateProvider = builder.stateProvider;
    }
    this.clientIsInternal = builder.httpClient == null;
    this.shutdownLBHttpSolrServer = builder.loadBalancedSolrClient == null;
    if(builder.lbClientBuilder != null) {
      propagateLBClientConfigOptions(builder);
      builder.loadBalancedSolrClient = builder.lbClientBuilder.build();
    }
    if(builder.loadBalancedSolrClient != null) builder.httpClient = builder.loadBalancedSolrClient.getHttpClient();
    this.myClient = (builder.httpClient == null) ? HttpClientUtil.createClient(null) : builder.httpClient;
    if (builder.loadBalancedSolrClient == null) builder.loadBalancedSolrClient = createLBHttpSolrClient(builder, myClient);
    this.lbClient = builder.loadBalancedSolrClient;
  }
  
  private void propagateLBClientConfigOptions(Builder builder) {
    final LBHttpSolrClient.Builder lbBuilder = builder.lbClientBuilder;
    
    if (builder.connectionTimeoutMillis != null) {
      lbBuilder.withConnectionTimeout(builder.connectionTimeoutMillis);
    }
    
    if (builder.socketTimeoutMillis != null) {
      lbBuilder.withSocketTimeout(builder.socketTimeoutMillis);
    }
  }

  protected Map<String, LBHttpSolrClient.Req> createRoutes(UpdateRequest updateRequest, ModifiableSolrParams routableParams,
                                                       DocCollection col, DocRouter router, Map<String, List<String>> urlMap,
                                                       String idField) {
    return urlMap == null ? null : updateRequest.getRoutes(router, col, urlMap, routableParams, idField);
  }

  protected RouteException getRouteException(SolrException.ErrorCode serverError, NamedList<Throwable> exceptions, Map<String, ? extends LBSolrClient.Req> routes) {
    return new RouteException(serverError, exceptions, routes);
  }

  /** @deprecated since 7.2  Use {@link Builder} methods instead. */
  @Deprecated
  public void setParallelUpdates(boolean parallelUpdates) {
    this.parallelUpdates = parallelUpdates;
  }

  /**
   * @deprecated since Solr 8.0
   */
  @Deprecated
  public RouteResponse condenseResponse(@SuppressWarnings({"rawtypes"})NamedList response, int timeMillis) {
    return condenseResponse(response, timeMillis, RouteResponse::new);
  }

  /**
   * @deprecated since Solr 8.0
   */
  @Deprecated
  public static class RouteResponse extends BaseCloudSolrClient.RouteResponse<LBHttpSolrClient.Req> {

  }

  /**
   * @deprecated since Solr 8.0
   */
  @Deprecated
  public static class RouteException extends BaseCloudSolrClient.RouteException {

    public RouteException(ErrorCode errorCode, NamedList<Throwable> throwables, Map<String, ? extends LBSolrClient.Req> routes) {
      super(errorCode, throwables, routes);
    }
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

    super.close();
  }

  public LBHttpSolrClient getLbClient() {
    return lbClient;
  }

  public HttpClient getHttpClient() {
    return myClient;
  }

  /**
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setConnectionTimeout(int timeout) {
    this.lbClient.setConnectionTimeout(timeout); 
  }

  public ClusterStateProvider getClusterStateProvider(){
    return stateProvider;
  }

  @Override
  protected boolean wasCommError(Throwable rootCause) {
    return rootCause instanceof ConnectTimeoutException ||
        rootCause instanceof NoHttpResponseException;
  }

  private static LBHttpSolrClient createLBHttpSolrClient(Builder cloudSolrClientBuilder, HttpClient httpClient) {
    final LBHttpSolrClient.Builder lbBuilder = new LBHttpSolrClient.Builder();
    lbBuilder.withHttpClient(httpClient);
    if (cloudSolrClientBuilder.connectionTimeoutMillis != null) {
      lbBuilder.withConnectionTimeout(cloudSolrClientBuilder.connectionTimeoutMillis);
    }
    if (cloudSolrClientBuilder.socketTimeoutMillis != null) {
      lbBuilder.withSocketTimeout(cloudSolrClientBuilder.socketTimeoutMillis);
    }
    final LBHttpSolrClient lbClient = lbBuilder.build();
    lbClient.setRequestWriter(new BinaryRequestWriter());
    lbClient.setParser(new BinaryResponseParser());
    
    return lbClient;
  }

  /**
   * Constructs {@link CloudSolrClient} instances from provided configuration.
   */
  public static class Builder extends SolrClientBuilder<Builder> {
    protected Collection<String> zkHosts = new ArrayList<>();
    protected List<String> solrUrls = new ArrayList<>();
    protected String zkChroot;
    protected LBHttpSolrClient loadBalancedSolrClient;
    protected LBHttpSolrClient.Builder lbClientBuilder;
    protected boolean shardLeadersOnly = true;
    protected boolean directUpdatesToLeadersOnly = false;
    protected boolean parallelUpdates = true;
    protected ClusterStateProvider stateProvider;
    
    /**
     * @deprecated use other constructors instead.  This constructor will be changing visibility in an upcoming release.
     */
    @Deprecated
    public Builder() {}
    
    /**
     * Provide a series of Solr URLs to be used when configuring {@link CloudSolrClient} instances.
     * The solr client will use these urls to understand the cluster topology, which solr nodes are active etc.
     * 
     * Provided Solr URLs are expected to point to the root Solr path ("http://hostname:8983/solr"); they should not
     * include any collections, cores, or other path components.
     *
     * Usage example:
     *
     * <pre>
     *   final List&lt;String&gt; solrBaseUrls = new ArrayList&lt;String&gt;();
     *   solrBaseUrls.add("http://solr1:8983/solr"); solrBaseUrls.add("http://solr2:8983/solr"); solrBaseUrls.add("http://solr3:8983/solr");
     *   final SolrClient client = new CloudSolrClient.Builder(solrBaseUrls).build();
     * </pre>
     */
    public Builder(List<String> solrUrls) {
      this.solrUrls = solrUrls;
    }

    /**
     * Provide an already created {@link ClusterStateProvider} instance
     */
    public Builder(ClusterStateProvider stateProvider) {
      this.stateProvider = stateProvider;
    }

    /**
     * Provide a series of ZK hosts which will be used when configuring {@link CloudSolrClient} instances.
     *
     * Usage example when Solr stores data at the ZooKeeper root ('/'):
     *
     * <pre>
     *   final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *   zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *   final SolrClient client = new CloudSolrClient.Builder(zkServers, Optional.empty()).build();
     * </pre>
     *
     * Usage example when Solr data is stored in a ZooKeeper chroot:
     *
     *  <pre>
     *    final List&lt;String&gt; zkServers = new ArrayList&lt;String&gt;();
     *    zkServers.add("zookeeper1:2181"); zkServers.add("zookeeper2:2181"); zkServers.add("zookeeper3:2181");
     *    final SolrClient client = new CloudSolrClient.Builder(zkServers, Optional.of("/solr")).build();
     *  </pre>
     *
     * @param zkHosts a List of at least one ZooKeeper host and port (e.g. "zookeeper1:2181")
     * @param zkChroot the path to the root ZooKeeper node containing Solr data.  Provide {@code java.util.Optional.empty()} if no ZK chroot is used.
     */
    public Builder(List<String> zkHosts, Optional<String> zkChroot) {
      this.zkHosts = zkHosts;
      if (zkChroot.isPresent()) this.zkChroot = zkChroot.get();
    }

    /**
     * Provide a ZooKeeper client endpoint to be used when configuring {@link CloudSolrClient} instances.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * @param zkHost
     *          The client endpoint of the ZooKeeper quorum containing the cloud
     *          state.
     *          
     * @deprecated use Zk-host constructor instead
     */
    @Deprecated
    public Builder withZkHost(String zkHost) {
      this.zkHosts.add(zkHost);
      return this;
    }

    /**
     * Provide a Solr URL to be used when configuring {@link CloudSolrClient} instances.
     *
     * Method may be called multiple times. One of the provided values will be used to fetch
     * the list of live Solr nodes that the underlying {@link HttpClusterStateProvider} would be maintaining.
     * 
     * Provided Solr URL is expected to point to the root Solr path ("http://hostname:8983/solr"); it should not
     * include any collections, cores, or other path components.
     * 
     * @deprecated use Solr-URL constructor instead
     */
    @Deprecated
    public Builder withSolrUrl(String solrUrl) {
      this.solrUrls.add(solrUrl);
      return this;
    }

    /**
     * Provide a list of Solr URL to be used when configuring {@link CloudSolrClient} instances.
     * One of the provided values will be used to fetch the list of live Solr
     * nodes that the underlying {@link HttpClusterStateProvider} would be maintaining.
     * 
     * Provided Solr URLs are expected to point to the root Solr path ("http://hostname:8983/solr"); they should not
     * include any collections, cores, or other path components.
     * 
     * @deprecated use Solr URL constructors instead
     */
    @Deprecated
    public Builder withSolrUrl(Collection<String> solrUrls) {
      this.solrUrls.addAll(solrUrls);
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
     * Provide a series of ZooKeeper client endpoints for the builder to use when creating clients.
     * 
     * Method may be called multiple times.  All provided values will be used.
     * 
     * @param zkHosts
     *          A Java Collection (List, Set, etc) of HOST:PORT strings, one for
     *          each host in the ZooKeeper ensemble. Note that with certain
     *          Collection types like HashSet, the order of hosts in the final
     *          connect string may not be in the same order you added them.
     *          
     * @deprecated use Zk-host constructor instead
     */
    @Deprecated
    public Builder withZkHost(Collection<String> zkHosts) {
      this.zkHosts.addAll(zkHosts);
      return this;
    }

    /**
     * Provides a ZooKeeper chroot for the builder to use when creating clients.
     * 
     * @deprecated use Zk-host constructor instead
     */
    @Deprecated
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
     * Tells {@link Builder} that created clients should send updates only to shard leaders.
     *
     * WARNING: This method currently has no effect.  See SOLR-6312 for more information.
     */
    public Builder sendUpdatesOnlyToShardLeaders() {
      shardLeadersOnly = true;
      return this;
    }
    
    /**
     * Tells {@link Builder} that created clients should send updates to all replicas for a shard.
     *
     * WARNING: This method currently has no effect.  See SOLR-6312 for more information.
     */
    public Builder sendUpdatesToAllReplicasInShard() {
      shardLeadersOnly = false;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients should send direct updates to shard leaders only.
     *
     * UpdateRequests whose leaders cannot be found will "fail fast" on the client side with a {@link SolrException}
     */
    public Builder sendDirectUpdatesToShardLeadersOnly() {
      directUpdatesToLeadersOnly = true;
      return this;
    }

    /**
     * Tells {@link Builder} that created clients can send updates to any shard replica (shard leaders and non-leaders).
     *
     * Shard leaders are still preferred, but the created clients will fallback to using other replicas if a leader
     * cannot be found.
     */
    public Builder sendDirectUpdatesToAnyShardReplica() {
      directUpdatesToLeadersOnly = false;
      return this;
    }

    /**
     * Tells {@link Builder} whether created clients should send shard updates serially or in parallel
     *
     * When an {@link UpdateRequest} affects multiple shards, {@link CloudSolrClient} splits it up and sends a request
     * to each affected shard.  This setting chooses whether those sub-requests are sent serially or in parallel.
     * <p>
     * If not set, this defaults to 'true' and sends sub-requests in parallel.
     */
    public Builder withParallelUpdates(boolean parallelUpdates) {
      this.parallelUpdates = parallelUpdates;
      return this;
    }

    /**
     * Expert feature where you want to implement a custom cluster discovery mechanism of the solr nodes as part of the
     * cluster.
     *
     * @deprecated since this is an expert feature we don't want to expose this to regular users. To use this feature
     * extend CloudSolrClient.Builder and pass your custom ClusterStateProvider
     */
    @Deprecated
    public Builder withClusterStateProvider(ClusterStateProvider stateProvider) {
      this.stateProvider = stateProvider;
      return this;
    }
    
    /**
     * Create a {@link CloudSolrClient} based on the provided configuration.
     */
    public CloudSolrClient build() {
      if (stateProvider == null) {
        if (!zkHosts.isEmpty()) {
          stateProvider = new ZkClientClusterStateProvider(zkHosts, zkChroot);
        }
        else if (!this.solrUrls.isEmpty()) {
          try {
            stateProvider = new HttpClusterStateProvider(solrUrls, httpClient);
          } catch (Exception e) {
            throw new RuntimeException("Couldn't initialize a HttpClusterStateProvider (is/are the "
                + "Solr server(s), "  + solrUrls + ", down?)", e);
          }
        } else {
          throw new IllegalArgumentException("Both zkHosts and solrUrl cannot be null.");
        }
      }
      return new CloudSolrClient(this);
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
