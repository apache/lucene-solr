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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientParamBean;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager; // jdoc
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating/configuring httpclient instances. 
 */
public class HttpClientUtil {
  // socket timeout measured in ms, closes a socket if read
  // takes longer than x ms to complete. throws
  // java.net.SocketTimeoutException: Read timed out exception
  public static final String PROP_SO_TIMEOUT = "socketTimeout";
  // connection timeout measures in ms, closes a socket if connection
  // cannot be established within x ms. with a
  // java.net.SocketTimeoutException: Connection timed out
  public static final String PROP_CONNECTION_TIMEOUT = "connTimeout";
  // Maximum connections allowed per host
  public static final String PROP_MAX_CONNECTIONS_PER_HOST = "maxConnectionsPerHost";
  // Maximum total connections allowed
  public static final String PROP_MAX_CONNECTIONS = "maxConnections";
  // Retry http requests on error
  public static final String PROP_USE_RETRY = "retry";
  // Allow compression (deflate,gzip) if server supports it
  public static final String PROP_ALLOW_COMPRESSION = "allowCompression";
  // Follow redirects
  public static final String PROP_FOLLOW_REDIRECTS = "followRedirects";
  // Basic auth username 
  public static final String PROP_BASIC_AUTH_USER = "httpBasicAuthUser";
  // Basic auth password 
  public static final String PROP_BASIC_AUTH_PASS = "httpBasicAuthPassword";
  
  public static final String SYS_PROP_CHECK_PEER_NAME = "solr.ssl.checkPeerName";
  
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  static final DefaultHttpRequestRetryHandler NO_RETRY = new DefaultHttpRequestRetryHandler(
      0, false);

  private static HttpClientConfigurer configurer = new HttpClientConfigurer();

  private static final List<HttpRequestInterceptor> interceptors = Collections.synchronizedList(new ArrayList<HttpRequestInterceptor>());
  
  /**
   * Replace the {@link HttpClientConfigurer} class used in configuring the http
   * clients with a custom implementation.
   */
  public static void setConfigurer(HttpClientConfigurer newConfigurer) {
    configurer = newConfigurer;
  }
  
  public static HttpClientConfigurer getConfigurer() {
    return configurer;
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   * @param params
   *          http client configuration, if null a client with default
   *          configuration (no additional configuration) is created. 
   */
  public static CloseableHttpClient createClient(final SolrParams params) {
    final ModifiableSolrParams config = new ModifiableSolrParams(params);
    if (logger.isDebugEnabled()) {
      logger.debug("Creating new http client, config:" + config);
    }
    final DefaultHttpClient httpClient = HttpClientFactory.createHttpClient();
    configureClient(httpClient, config);
    return httpClient;
  }
  
  /**
   * Creates new http client by using the provided configuration.
   * 
   */
  public static CloseableHttpClient createClient(final SolrParams params, ClientConnectionManager cm) {
    final ModifiableSolrParams config = new ModifiableSolrParams(params);
    if (logger.isDebugEnabled()) {
      logger.debug("Creating new http client, config:" + config);
    }
    final DefaultHttpClient httpClient = HttpClientFactory.createHttpClient(cm);
    configureClient(httpClient, config);
    return httpClient;
  }

  /**
   * Configures {@link DefaultHttpClient}, only sets parameters if they are
   * present in config.
   */
  public static void configureClient(final DefaultHttpClient httpClient,
      SolrParams config) {
    configurer.configure(httpClient,  config);
    synchronized(interceptors) {
      for(HttpRequestInterceptor interceptor: interceptors) {
        httpClient.addRequestInterceptor(interceptor);
      }
    }
  }
  
  public static void close(HttpClient httpClient) { 
    if (httpClient instanceof CloseableHttpClient) {
      org.apache.solr.common.util.IOUtils.closeQuietly((CloseableHttpClient) httpClient);
    } else {
      httpClient.getConnectionManager().shutdown();
    }
  }

  public static void addRequestInterceptor(HttpRequestInterceptor interceptor) {
    interceptors.add(interceptor);
  }

  public static void removeRequestInterceptor(HttpRequestInterceptor interceptor) {
    interceptors.remove(interceptor);
  }

  /**
   * Control HTTP payload compression.
   * 
   * @param allowCompression
   *          true will enable compression (needs support from server), false
   *          will disable compression.
   */
  public static void setAllowCompression(DefaultHttpClient httpClient,
      boolean allowCompression) {
    httpClient
        .removeRequestInterceptorByClass(UseCompressionRequestInterceptor.class);
    httpClient
        .removeResponseInterceptorByClass(UseCompressionResponseInterceptor.class);
    if (allowCompression) {
      httpClient.addRequestInterceptor(new UseCompressionRequestInterceptor());
      httpClient
          .addResponseInterceptor(new UseCompressionResponseInterceptor());
    }
  }

  /**
   * Set http basic auth information. If basicAuthUser or basicAuthPass is null
   * the basic auth configuration is cleared. Currently this is not preemtive
   * authentication. So it is not currently possible to do a post request while
   * using this setting.
   */
  public static void setBasicAuth(DefaultHttpClient httpClient,
      String basicAuthUser, String basicAuthPass) {
    if (basicAuthUser != null && basicAuthPass != null) {
      httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(basicAuthUser, basicAuthPass));
    } else {
      httpClient.getCredentialsProvider().clear();
    }
  }

  /**
   * Set max connections allowed per host. This call will only work when
   * {@link ThreadSafeClientConnManager} or
   * {@link PoolingClientConnectionManager} is used.
   */
  public static void setMaxConnectionsPerHost(HttpClient httpClient,
      int max) {
    // would have been nice if there was a common interface
    if (httpClient.getConnectionManager() instanceof ThreadSafeClientConnManager) {
      ThreadSafeClientConnManager mgr = (ThreadSafeClientConnManager)httpClient.getConnectionManager();
      mgr.setDefaultMaxPerRoute(max);
    } else if (httpClient.getConnectionManager() instanceof PoolingClientConnectionManager) {
      PoolingClientConnectionManager mgr = (PoolingClientConnectionManager)httpClient.getConnectionManager();
      mgr.setDefaultMaxPerRoute(max);
    }
  }

  /**
   * Set max total connections allowed. This call will only work when
   * {@link ThreadSafeClientConnManager} or
   * {@link PoolingClientConnectionManager} is used.
   */
  public static void setMaxConnections(final HttpClient httpClient,
      int max) {
    // would have been nice if there was a common interface
    if (httpClient.getConnectionManager() instanceof ThreadSafeClientConnManager) {
      ThreadSafeClientConnManager mgr = (ThreadSafeClientConnManager)httpClient.getConnectionManager();
      mgr.setMaxTotal(max);
    } else if (httpClient.getConnectionManager() instanceof PoolingClientConnectionManager) {
      PoolingClientConnectionManager mgr = (PoolingClientConnectionManager)httpClient.getConnectionManager();
      mgr.setMaxTotal(max);
    }
  }
  

  /**
   * Defines the socket timeout (SO_TIMEOUT) in milliseconds. A timeout value of
   * zero is interpreted as an infinite timeout.
   * 
   * @param timeout timeout in milliseconds
   */
  public static void setSoTimeout(HttpClient httpClient, int timeout) {
    HttpConnectionParams.setSoTimeout(httpClient.getParams(),
        timeout);
  }

  /**
   * Control retry handler 
   * @param useRetry when false the client will not try to retry failed requests.
   */
  public static void setUseRetry(final DefaultHttpClient httpClient,
      boolean useRetry) {
    if (!useRetry) {
      httpClient.setHttpRequestRetryHandler(NO_RETRY);
    } else {
      // if the request is not fully sent, we retry
      // streaming updates are not a problem, because they are not retryable
      httpClient.setHttpRequestRetryHandler(new SolrHttpRequestRetryHandler(3));
    }
  }

  /**
   * Set connection timeout. A timeout value of zero is interpreted as an
   * infinite timeout.
   * 
   * @param timeout
   *          connection Timeout in milliseconds
   */
  public static void setConnectionTimeout(final HttpClient httpClient,
      int timeout) {
      HttpConnectionParams.setConnectionTimeout(httpClient.getParams(),
          timeout);
  }

  /**
   * Set follow redirects.
   *
   * @param followRedirects  When true the client will follow redirects.
   */
  public static void setFollowRedirects(HttpClient httpClient,
      boolean followRedirects) {
    new ClientParamBean(httpClient.getParams()).setHandleRedirects(followRedirects);
  }

  public static void setHostNameVerifier(DefaultHttpClient httpClient,
      X509HostnameVerifier hostNameVerifier) {
    Scheme httpsScheme = httpClient.getConnectionManager().getSchemeRegistry().get("https");
    if (httpsScheme != null) {
      SSLSocketFactory sslSocketFactory = (SSLSocketFactory) httpsScheme.getSchemeSocketFactory();
      sslSocketFactory.setHostnameVerifier(hostNameVerifier);
    }
  }
  
  public static void setStaleCheckingEnabled(final HttpClient httpClient, boolean enabled) {
    HttpConnectionParams.setStaleCheckingEnabled(httpClient.getParams(), enabled);
  }
  
  public static void setTcpNoDelay(final HttpClient httpClient, boolean tcpNoDelay) {
    HttpConnectionParams.setTcpNoDelay(httpClient.getParams(), tcpNoDelay);
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
  
  private static class GzipDecompressingEntity extends HttpEntityWrapper {
    public GzipDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }
    
    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      return new GZIPInputStream(wrappedEntity.getContent());
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
      return new InflaterInputStream(wrappedEntity.getContent());
    }
  }

  public static class HttpClientFactory {
    private static Class<? extends DefaultHttpClient> defaultHttpClientClass = DefaultHttpClient.class;
    private static Class<? extends SystemDefaultHttpClient> systemDefaultHttpClientClass = SystemDefaultHttpClient.class;


    public static SystemDefaultHttpClient createHttpClient() {
      Constructor<? extends SystemDefaultHttpClient> constructor;
      try {
        constructor = systemDefaultHttpClientClass.getDeclaredConstructor();
        return constructor.newInstance();
      } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create HttpClient instance. ", e);
      }
    }

    public static DefaultHttpClient createHttpClient(ClientConnectionManager cm) {
      Constructor<? extends DefaultHttpClient> constructor;
      try {
        constructor = defaultHttpClientClass.getDeclaredConstructor(new Class[]{ClientConnectionManager.class});
        return constructor.newInstance(new Object[]{cm});
      } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create HttpClient instance, registered class is: " + defaultHttpClientClass, e);
      }
    }

    public static void setHttpClientImpl(Class<? extends DefaultHttpClient> defaultHttpClient, Class<? extends SystemDefaultHttpClient> systemDefaultHttpClient) {
      defaultHttpClientClass = defaultHttpClient;
      systemDefaultHttpClientClass = systemDefaultHttpClient;
    }
  }

}
