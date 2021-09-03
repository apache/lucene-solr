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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.ssl.SSLContexts;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating/configuring httpclient instances. 
 * 
 * This class can touch internal HttpClient details and is subject to change.
 * 
 * @lucene.experimental
 */
public class HttpClientUtil {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final int DEFAULT_CONNECT_TIMEOUT = 60000;
  public static final int DEFAULT_SO_TIMEOUT = 600000;
  public static final int DEFAULT_MAXCONNECTIONSPERHOST = 100000;
  public static final int DEFAULT_MAXCONNECTIONS = 100000;
  
  private static final int VALIDATE_AFTER_INACTIVITY_DEFAULT = 3000;
  private static final int EVICT_IDLE_CONNECTIONS_DEFAULT = 50000;
  private static final String VALIDATE_AFTER_INACTIVITY = "validateAfterInactivity";
  private static final String EVICT_IDLE_CONNECTIONS = "evictIdleConnections";

  // Maximum connections allowed per host
  public static final String PROP_MAX_CONNECTIONS_PER_HOST = "maxConnectionsPerHost";
  // Maximum total connections allowed
  public static final String PROP_MAX_CONNECTIONS = "maxConnections";
  // Retry http requests on error
  public static final String PROP_USE_RETRY = "retry";
  // Allow compression (deflate,gzip) if server supports it
  public static final String PROP_ALLOW_COMPRESSION = "allowCompression";
  // Basic auth username 
  public static final String PROP_BASIC_AUTH_USER = "httpBasicAuthUser";
  // Basic auth password 
  public static final String PROP_BASIC_AUTH_PASS = "httpBasicAuthPassword";

  /**
   * System property consulted to determine if the default {@link SocketFactoryRegistryProvider} 
   * will require hostname validation of SSL Certificates.  The default behavior is to enforce 
   * peer name validation.
   * <p>
   * This property will have no effect if {@link #setSocketFactoryRegistryProvider} is used to override
   * the default {@link SocketFactoryRegistryProvider} 
   * </p>
   */
  public static final String SYS_PROP_CHECK_PEER_NAME = "solr.ssl.checkPeerName";
  
  // * NOTE* The following params configure the default request config and this
  // is overridden by SolrJ clients. Use the setters on the SolrJ clients to
  // to configure these settings if that is the intent.
  
  // Follow redirects
  public static final String PROP_FOLLOW_REDIRECTS = "followRedirects";
  
  // socket timeout measured in ms, closes a socket if read
  // takes longer than x ms to complete. throws
  // java.net.SocketTimeoutException: Read timed out exception
  public static final String PROP_SO_TIMEOUT = "socketTimeout";
  // connection timeout measures in ms, closes a socket if connection
  // cannot be established within x ms. with a
  // java.net.SocketTimeoutException: Connection timed out
  public static final String PROP_CONNECTION_TIMEOUT = "connTimeout";

  /**
   * A Java system property to select the {@linkplain HttpClientBuilderFactory} used for
   * configuring the {@linkplain HttpClientBuilder} instance by default.
   */
  public static final String SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY = "solr.httpclient.builder.factory";

  /**
   * A Java system property to select the {@linkplain SocketFactoryRegistryProvider} used for
   * configuring the Apache HTTP clients.
   */
  public static final String SYS_PROP_SOCKET_FACTORY_REGISTRY_PROVIDER = "solr.httpclient.socketFactory.registry.provider";

  static final DefaultHttpRequestRetryHandler NO_RETRY = new DefaultHttpRequestRetryHandler(
      0, false);

  private static volatile SolrHttpClientBuilder httpClientBuilder;

  private static SolrHttpClientContextBuilder httpClientRequestContextBuilder = new SolrHttpClientContextBuilder();

  private static volatile SocketFactoryRegistryProvider socketFactoryRegistryProvider;
  private static volatile String cookiePolicy;
  private static final List<HttpRequestInterceptor> interceptors = new CopyOnWriteArrayList<>();


  static {
    resetHttpClientBuilder();

    // Configure the SocketFactoryRegistryProvider if user has specified the provider type.
    String socketFactoryRegistryProviderClassName = System.getProperty(SYS_PROP_SOCKET_FACTORY_REGISTRY_PROVIDER);
    if (socketFactoryRegistryProviderClassName != null) {
      log.debug("Using {}", socketFactoryRegistryProviderClassName);
      try {
        socketFactoryRegistryProvider = (SocketFactoryRegistryProvider)Class.forName(socketFactoryRegistryProviderClassName).getConstructor().newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | InvocationTargetException | NoSuchMethodException e) {
        throw new RuntimeException("Unable to instantiate Solr SocketFactoryRegistryProvider", e);
      }
    }

    // Configure the HttpClientBuilder if user has specified the factory type.
    String factoryClassName = System.getProperty(SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY);
    if (factoryClassName != null) {
      log.debug ("Using {}", factoryClassName);
      try {
        HttpClientBuilderFactory factory = (HttpClientBuilderFactory)Class.forName(factoryClassName).newInstance();
        httpClientBuilder = factory.getHttpClientBuilder(Optional.of(SolrHttpClientBuilder.create()));
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException("Unable to instantiate Solr HttpClientBuilderFactory", e);
      }
    }
  }

  public static abstract class SocketFactoryRegistryProvider {
    /** Must be non-null */
    public abstract Registry<ConnectionSocketFactory> getSocketFactoryRegistry();
  }

  private static class DynamicInterceptor implements HttpRequestInterceptor {

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      // don't synchronize traversal - can lead to deadlock - CopyOnWriteArrayList is critical
      // we also do not want to have to acquire the mutex when the list is empty or put a global
      // mutex around the process calls
      interceptors.forEach(new Consumer<HttpRequestInterceptor>() {

        @Override
        public void accept(HttpRequestInterceptor interceptor) {
          try {
            interceptor.process(request, context);
          } catch (Exception e) {
            log.error("", e);
          }
        }
      });

    }
  }

  public static void setHttpClientBuilder(SolrHttpClientBuilder newHttpClientBuilder) {
    httpClientBuilder = newHttpClientBuilder;
  }
  
  public static void setHttpClientProvider(SolrHttpClientBuilder newHttpClientBuilder) {
    httpClientBuilder = newHttpClientBuilder;
  }

  /**
   * @see #SYS_PROP_CHECK_PEER_NAME
   */
  public static void setSocketFactoryRegistryProvider(SocketFactoryRegistryProvider newRegistryProvider) {
    socketFactoryRegistryProvider = newRegistryProvider;
  }
  
  public static SolrHttpClientBuilder getHttpClientBuilder() {
    return httpClientBuilder;
  }

  /**
   * @see #SYS_PROP_CHECK_PEER_NAME
   */
  public static SocketFactoryRegistryProvider getSocketFactoryRegistryProvider() {
    return socketFactoryRegistryProvider;
  }
  
  public static void resetHttpClientBuilder() {
    socketFactoryRegistryProvider = new DefaultSocketFactoryRegistryProvider();
    httpClientBuilder = SolrHttpClientBuilder.create();
  }

  private static final class DefaultSocketFactoryRegistryProvider extends SocketFactoryRegistryProvider {
    @Override
    public Registry<ConnectionSocketFactory> getSocketFactoryRegistry() {
      // this mimics PoolingHttpClientConnectionManager's default behavior,
      // except that we explicitly use SSLConnectionSocketFactory.getSystemSocketFactory()
      // to pick up the system level default SSLContext (where javax.net.ssl.* properties
      // related to keystore & truststore are specified)
      RegistryBuilder<ConnectionSocketFactory> builder = RegistryBuilder.<ConnectionSocketFactory> create();
      builder.register("http", PlainConnectionSocketFactory.getSocketFactory());

      // logic to turn off peer host check
      SSLConnectionSocketFactory sslConnectionSocketFactory = null;
      boolean sslCheckPeerName = toBooleanDefaultIfNull(
          toBooleanObject(System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME)), true);
      if (sslCheckPeerName) {
        sslConnectionSocketFactory = SSLConnectionSocketFactory.getSystemSocketFactory();
      } else {
        sslConnectionSocketFactory = new SSLConnectionSocketFactory(SSLContexts.createSystemDefault(),
                                                                    NoopHostnameVerifier.INSTANCE);
        log.debug("{} is false, hostname checks disabled.", HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
      }
      builder.register("https", sslConnectionSocketFactory);

      return builder.build();
    }
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   * @param params
   *          http client configuration, if null a client with default
   *          configuration (no additional configuration) is created. 
   */
  public static CloseableHttpClient createClient(SolrParams params) {
    return createClient(params, createPoolingConnectionManager());
  }

  /** test usage subject to change @lucene.experimental */ 
  static PoolingHttpClientConnectionManager createPoolingConnectionManager() {
    return new PoolingHttpClientConnectionManager(socketFactoryRegistryProvider.getSocketFactoryRegistry());
  }
  
  public static CloseableHttpClient createClient(SolrParams params, PoolingHttpClientConnectionManager cm) {
    if (params == null) {
      params = new ModifiableSolrParams();
    }
    
    return createClient(params, cm, false);
  }

  public static CloseableHttpClient createClient(final SolrParams params, PoolingHttpClientConnectionManager cm, boolean sharedConnectionManager, HttpRequestExecutor httpRequestExecutor)  {
    final ModifiableSolrParams config = new ModifiableSolrParams(params);
    if (log.isDebugEnabled()) {
      log.debug("Creating new http client, config: {}", config);
    }

    cm.setMaxTotal(params.getInt(HttpClientUtil.PROP_MAX_CONNECTIONS, 10000));
    cm.setDefaultMaxPerRoute(params.getInt(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 10000));
    cm.setValidateAfterInactivity(Integer.getInteger(VALIDATE_AFTER_INACTIVITY, VALIDATE_AFTER_INACTIVITY_DEFAULT));


    HttpClientBuilder newHttpClientBuilder = HttpClientBuilder.create();

    if (sharedConnectionManager) {
      newHttpClientBuilder.setConnectionManagerShared(true);
    } else {
      newHttpClientBuilder.setConnectionManagerShared(false);
    }

    ConnectionKeepAliveStrategy keepAliveStrat = new ConnectionKeepAliveStrategy() {
      @Override
      public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
        // we only close connections based on idle time, not ttl expiration
        return -1;
      }
    };

    if (httpClientBuilder.getAuthSchemeRegistryProvider() != null) {
      newHttpClientBuilder.setDefaultAuthSchemeRegistry(httpClientBuilder.getAuthSchemeRegistryProvider().getAuthSchemeRegistry());
    }
    if (httpClientBuilder.getCookieSpecRegistryProvider() != null) {
      newHttpClientBuilder.setDefaultCookieSpecRegistry(httpClientBuilder.getCookieSpecRegistryProvider().getCookieSpecRegistry());
    }
    if (httpClientBuilder.getCredentialsProviderProvider() != null) {
      newHttpClientBuilder.setDefaultCredentialsProvider(httpClientBuilder.getCredentialsProviderProvider().getCredentialsProvider());
    }

    newHttpClientBuilder.addInterceptorLast(new DynamicInterceptor());

    newHttpClientBuilder = newHttpClientBuilder.setKeepAliveStrategy(keepAliveStrat)
        .evictIdleConnections((long) Integer.getInteger(EVICT_IDLE_CONNECTIONS, EVICT_IDLE_CONNECTIONS_DEFAULT), TimeUnit.MILLISECONDS);

    if (httpRequestExecutor != null)  {
      newHttpClientBuilder.setRequestExecutor(httpRequestExecutor);
    }

    HttpClientBuilder builder = setupBuilder(newHttpClientBuilder, params);

    HttpClient httpClient = builder.setConnectionManager(cm).build();

    assert ObjectReleaseTracker.track(httpClient);
    return (CloseableHttpClient) httpClient;
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   */
  public static CloseableHttpClient createClient(final SolrParams params, PoolingHttpClientConnectionManager cm, boolean sharedConnectionManager) {
    return createClient(params, cm, sharedConnectionManager, null);
  }
  
  private static HttpClientBuilder setupBuilder(HttpClientBuilder builder, SolrParams config) {
   
    Builder requestConfigBuilder = RequestConfig.custom()
        .setRedirectsEnabled(config.getBool(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false)).setDecompressionEnabled(false)
        .setConnectTimeout(config.getInt(HttpClientUtil.PROP_CONNECTION_TIMEOUT, DEFAULT_CONNECT_TIMEOUT))
        .setSocketTimeout(config.getInt(HttpClientUtil.PROP_SO_TIMEOUT, DEFAULT_SO_TIMEOUT));

    String cpolicy = cookiePolicy;
    if (cpolicy != null) {
      requestConfigBuilder.setCookieSpec(cpolicy);
    }
    
    RequestConfig requestConfig = requestConfigBuilder.build();
    
    HttpClientBuilder retBuilder = builder.setDefaultRequestConfig(requestConfig);

    if (config.getBool(HttpClientUtil.PROP_USE_RETRY, true)) {
      retBuilder = retBuilder.setRetryHandler(new SolrHttpRequestRetryHandler(Integer.getInteger("solr.httpclient.retries", 3)));

    } else {
      retBuilder = retBuilder.setRetryHandler(NO_RETRY);
    }

    final String basicAuthUser = config.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    final String basicAuthPass = config.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    
    if (basicAuthUser != null && basicAuthPass != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(basicAuthUser, basicAuthPass));
      retBuilder.setDefaultCredentialsProvider(credsProvider);
    }
    
    if (config.getBool(HttpClientUtil.PROP_ALLOW_COMPRESSION, false)) {
      retBuilder.addInterceptorFirst(new UseCompressionRequestInterceptor());
      retBuilder.addInterceptorFirst(new UseCompressionResponseInterceptor());
    } else {
      retBuilder.disableContentCompression();
    }

    return retBuilder;
  }

  public static void close(HttpClient httpClient) { 

    org.apache.solr.common.util.IOUtils.closeQuietly((CloseableHttpClient) httpClient);

    assert ObjectReleaseTracker.release(httpClient);
  }

  public static void addRequestInterceptor(HttpRequestInterceptor interceptor) {
    interceptors.add(interceptor);
  }

  public static void removeRequestInterceptor(HttpRequestInterceptor interceptor) {
    interceptors.remove(interceptor);
  }
  
  public static void clearRequestInterceptors() {
    interceptors.clear();
  }
  
  private static class UseCompressionRequestInterceptor implements
      HttpRequestInterceptor {
    
    @Override
    public void process(HttpRequest request, HttpContext context)
        throws HttpException, IOException {
      if (!request.containsHeader("Accept-Encoding")) {
        request.addHeader("Accept-Encoding", "gzip, deflate");
      }
    }
  }
  
  private static class UseCompressionResponseInterceptor implements
      HttpResponseInterceptor {
    
    @Override
    public void process(final HttpResponse response, final HttpContext context)
        throws HttpException, IOException {
      
      HttpEntity entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      if (ceheader != null) {
        HeaderElement[] codecs = ceheader.getElements();
        for (int i = 0; i < codecs.length; i++) {
          if (codecs[i].getName().equalsIgnoreCase("gzip")) {
            response
                .setEntity(new GzipDecompressingEntity(response.getEntity()));
            return;
          }
          if (codecs[i].getName().equalsIgnoreCase("deflate")) {
            response.setEntity(new DeflateDecompressingEntity(response
                .getEntity()));
            return;
          }
        }
      }
    }
  }
  
  protected static class GzipDecompressingEntity extends HttpEntityWrapper {
    private boolean gzipInputStreamCreated = false;
    private InputStream gzipInputStream = null;

    public GzipDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }

    /**
     * Return a InputStream of uncompressed data.
     * If there is an issue with the compression of the data, a null InputStream will be returned,
     * and the underlying compressed InputStream will be closed.
     *
     * The same input stream will be returned if the underlying entity is not repeatable.
     * If the underlying entity is repeatable, then a new input stream will be created.
     */
    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      if (!gzipInputStreamCreated || wrappedEntity.isRepeatable()) {
        gzipInputStreamCreated = true;
        InputStream wrappedContent = wrappedEntity.getContent();
        if (wrappedContent != null) {
          try {
            gzipInputStream = new GZIPInputStream(wrappedContent);
          } catch (IOException ioException) {
            try {
              Utils.readFully(wrappedContent);
            } catch (IOException ignored) {
            } finally {
              IOUtils.closeQuietly(wrappedContent);
            }
            throw new IOException("Cannot open GZipInputStream for response", ioException);
          }
        }
      }
      return gzipInputStream;
    }
    
    @Override
    public long getContentLength() {
      return -1;
    }
  }
  
  private static class DeflateDecompressingEntity extends
      GzipDecompressingEntity {
    public DeflateDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      // InflaterInputStream does not throw a ZipException in the constructor,
      // so it does not need the same checks as the GZIPInputStream.
      return new InflaterInputStream(wrappedEntity.getContent());
    }
  }

  public static void setHttpClientRequestContextBuilder(SolrHttpClientContextBuilder httpClientContextBuilder) {
    httpClientRequestContextBuilder = httpClientContextBuilder;
  }

  /**
   * Create a HttpClientContext object and {@link HttpClientContext#setUserToken(Object)}
   * to an internal singleton. It allows to reuse underneath {@link HttpClient} 
   * in connection pools if client authentication is enabled.
   */
  public static HttpClientContext createNewHttpClientRequestContext() {
    HttpClientContext context = httpClientRequestContextBuilder.createContext(HttpSolrClient.cacheKey);

    return context;
  }
  
  public static Builder createDefaultRequestConfigBuilder() {
    String cpolicy = cookiePolicy;
    Builder builder = RequestConfig.custom();

    builder.setSocketTimeout(DEFAULT_SO_TIMEOUT)
        .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
        .setRedirectsEnabled(false)
        .setDecompressionEnabled(false); // we do our own compression / decompression
    if (cpolicy != null) {
      builder.setCookieSpec(cpolicy);
    }
    return builder;
  }

  public static void setCookiePolicy(String policyName) {
    cookiePolicy = policyName;
  }

  /**
   * @lucene.internal
   */
  static boolean toBooleanDefaultIfNull(Boolean bool, boolean valueIfNull) {
    if (bool == null) {
      return valueIfNull;
    }
    return bool.booleanValue() ? true : false;
  }

  /**
   * @lucene.internal
   */
  static Boolean toBooleanObject(String str) {
    if ("true".equalsIgnoreCase(str)) {
      return Boolean.TRUE;
    } else if ("false".equalsIgnoreCase(str)) {
      return Boolean.FALSE;
    }
    // no match
    return null;
  }
}
