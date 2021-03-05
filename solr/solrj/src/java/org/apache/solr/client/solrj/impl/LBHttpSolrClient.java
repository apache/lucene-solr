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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * LBHttpSolrClient or "LoadBalanced HttpSolrClient" is a load balancing wrapper around
 * {@link HttpSolrClient}. This is useful when you
 * have multiple Solr servers and the requests need to be Load Balanced among them.
 *
 * Do <b>NOT</b> use this class for indexing in leader/follower scenarios since documents must be sent to the
 * correct leader; no inter-node routing is done.
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
public class LBHttpSolrClient extends LBSolrClient {

  private final HttpClient httpClient;
  private final boolean clientIsInternal;
  private final ConcurrentHashMap<String, HttpSolrClient> urlToClient = new ConcurrentHashMap<>();
  private final HttpSolrClient.Builder httpSolrClientBuilder;

  private Integer connectionTimeout;
  private volatile Integer soTimeout;

  /**
   * @deprecated use {@link LBSolrClient.Req} instead
   */
  @Deprecated
  public static class Req extends LBSolrClient.Req {
    public Req(@SuppressWarnings({"rawtypes"})SolrRequest request, List<String> servers) {
      super(request, servers);
    }

    public Req(@SuppressWarnings({"rawtypes"})SolrRequest request, List<String> servers, Integer numServersToTry) {
      super(request, servers, numServersToTry);
    }
  }

  /**
   * @deprecated use {@link LBSolrClient.Rsp} instead
   */
  @Deprecated
  public static class Rsp extends LBSolrClient.Rsp {

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
    super(builder.baseSolrUrls);
    this.clientIsInternal = builder.httpClient == null;
    this.httpSolrClientBuilder = builder.httpSolrClientBuilder;
    this.httpClient = builder.httpClient == null ? constructClient(builder.baseSolrUrls.toArray(new String[builder.baseSolrUrls.size()])) : builder.httpClient;
    this.connectionTimeout = builder.connectionTimeoutMillis;
    this.soTimeout = builder.socketTimeoutMillis;    
    this.parser = builder.responseParser;
    for (String baseUrl: builder.baseSolrUrls) {
      urlToClient.put(baseUrl, makeSolrClient(baseUrl));
    }
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
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setConnectionTimeout(int timeout) {
    this.connectionTimeout = timeout;
    this.urlToClient.values().forEach(client -> client.setConnectionTimeout(timeout));
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
    this.urlToClient.values().forEach(client -> client.setSoTimeout(timeout));
  }

  /**
   * @deprecated use {@link LBSolrClient#request(LBSolrClient.Req)} instead
   */
  @Deprecated
  public Rsp request(Req req) throws SolrServerException, IOException {
    LBSolrClient.Rsp rsp = super.request(req);
    // for backward-compatibility support
    Rsp result = new Rsp();
    result.rsp = rsp.rsp;
    result.server = rsp.server;
    return result;
  }

  @Override
  protected SolrClient getClient(String baseUrl) {
    HttpSolrClient client = urlToClient.get(baseUrl);
    if (client == null) {
      return makeSolrClient(baseUrl);
    } else {
      return client;
    }
  }

  @Override
  public String removeSolrServer(String server) {
    urlToClient.remove(server);
    return super.removeSolrServer(server);
  }

  @Override
  public void close() {
    super.close();
    if(clientIsInternal) {
      HttpClientUtil.close(httpClient);
    }
  }

  /**
   * Return the HttpClient this instance uses.
   */
  public HttpClient getHttpClient() {
    return httpClient;
  }

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
