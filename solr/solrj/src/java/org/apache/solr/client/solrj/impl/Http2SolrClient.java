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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.client.solrj.util.Constants;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrInternalHttpClient;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.SolrScheduledExecutorScheduler;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.ByteBufferContentProvider;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartContentProvider;
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.SocketAddressResolver;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteExecutionException;
import static org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import static org.apache.solr.common.util.Utils.getObjectByPath;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Difference between this {@link Http2SolrClient} and {@link HttpSolrClient}:
 * <ul>
 *  <li>{@link Http2SolrClient} sends requests in HTTP/2</li>
 *  <li>{@link Http2SolrClient} can point to multiple urls</li>
 *  <li>{@link Http2SolrClient} does not expose its internal httpClient like {@link HttpSolrClient#getHttpClient()},
 * sharing connection pools should be done by {@link Http2SolrClient.Builder#withHttpClient(Http2SolrClient)} </li>
 * </ul>
 * @lucene.experimental
 */
public class Http2SolrClient extends SolrClient {
  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";

  private static volatile SSLConfig defaultSSLConfig;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String GET = "GET";
  private static final String DELETE = "DELETE";
  private static final String HEAD = "HEAD";

  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final String DEFAULT_PATH = "/select";
  private static final List<String> errPath = Arrays.asList("metadata", "error-class");
  private final Map<String, String> headers;

  private CloseTracker closeTracker;

  private volatile HttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private int idleTimeout;
  private boolean strictEventOrdering;
  private volatile ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();
  private final Set<HttpListenerFactory> listenerFactory = ConcurrentHashMap.newKeySet();
  private final AsyncTracker asyncTracker;
  /**
   * The URL of the Solr server.
   */
  private volatile String serverBaseUrl;
  private volatile boolean closeClient;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    assert (closeTracker = new CloseTracker()) != null;
    assert ObjectReleaseTracker.track(this);
    if (serverBaseUrl != null)  {
      if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
      }
      this.serverBaseUrl = serverBaseUrl;
    }
    Integer moar = 1000;
    if (builder.maxOutstandingAsyncRequests != null) moar = builder.maxOutstandingAsyncRequests;
    asyncTracker = new AsyncTracker(moar); // nocommit
    this.headers = builder.headers;
    this.strictEventOrdering = builder.strictEventOrdering;

    if (builder.idleTimeout != null && builder.idleTimeout > 0) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.http2SolrClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.http2SolrClient.httpClient;
    }
  }

  public void addListenerFactory(HttpListenerFactory factory) {
    this.listenerFactory.add(factory);
  }

  // internal usage only
  public HttpClient getHttpClient() {
    return httpClient;
  }

  public void addHeaders(Map<String,String> headers) {
    this.headers.putAll(headers);
  }

  // internal usage only
  ProtocolHandlers getProtocolHandlers() {
    return httpClient.getProtocolHandlers();
  }

  private HttpClient createHttpClient(Builder builder) {
    HttpClient httpClient;

    SslContextFactory.Client sslContextFactory = null;
    boolean ssl = false;
    if (builder.sslConfig == null) {
      if (System.getProperty("javax.net.ssl.trustStore") != null || System.getProperty("javax.net.ssl.keyStore") != null) {
        sslContextFactory = getDefaultSslContextFactory();
        ssl = sslContextFactory.getTrustStore() != null || sslContextFactory.getTrustStorePath() != null;
      }
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
      ssl = true;
    }
    // nocommit - look at config again as well
    int minThreads = Integer.getInteger("solr.minHttp2ClientThreads", 12);
    SolrQueuedThreadPool httpClientExecutor = new SolrQueuedThreadPool("http2Client", builder.maxThreadPoolSize, minThreads,
        this.headers != null && this.headers.containsKey(QoSParams.REQUEST_SOURCE) && this.headers.get(QoSParams.REQUEST_SOURCE).equals(QoSParams.INTERNAL) ? 3000 : 5000,
        null, -1, null);
    httpClientExecutor.setLowThreadsThreshold(-1);

    boolean sslOnJava8OrLower = ssl && !Constants.JRE_IS_MINIMUM_JAVA9;
    if (builder.useHttp1_1 || sslOnJava8OrLower) {
      if (sslOnJava8OrLower && !builder.useHttp1_1) {
        log.warn("Create Http2SolrClient with HTTP/1.1 transport since Java 8 or lower versions does not support SSL + HTTP/2");
      } else {
        log.info("Create Http2SolrClient with HTTP/1.1 transport");
      }
      SolrHttpClientTransportOverHTTP transport = new SolrHttpClientTransportOverHTTP(2);
      httpClient = new SolrInternalHttpClient(transport, sslContextFactory);
    } else {
      log.info("Create Http2SolrClient with HTTP/2 transport");
      HTTP2Client http2client = new HTTP2Client();
      http2client.setSelectors(2);
      http2client.setMaxConcurrentPushedStreams(512);
      http2client.setInputBufferSize(8192);
      HttpClientTransportOverHTTP2 transport = new HttpClientTransportOverHTTP2(http2client);
      httpClient = new SolrInternalHttpClient(transport, sslContextFactory);
    }

    try {
     // httpClientExecutor.start();
      SecurityManager s = System.getSecurityManager();
      ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      SolrScheduledExecutorScheduler scheduler = new SolrScheduledExecutorScheduler("http2client-scheduler", null, group);
      httpClient.setScheduler(scheduler);
      httpClient.manage(scheduler);
      httpClient.setExecutor(httpClientExecutor);
      httpClient.manage(httpClientExecutor);
      httpClient.setStrictEventOrdering(strictEventOrdering);
      // httpClient.setSocketAddressResolver(new SocketAddressResolver.Sync());
      httpClient.setConnectBlocking(false);
      httpClient.setFollowRedirects(false);
      if (builder.maxConnectionsPerHost != null) httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
      httpClient.setMaxRequestsQueuedPerDestination(builder.maxRequestsQueuedPerDestination);
      httpClient.setRequestBufferSize(8192);
      httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));
      httpClient.setIdleTimeout(idleTimeout);
      httpClient.setTCPNoDelay(true);
      httpClient.setStopTimeout(60000);
      if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
      httpClient.start();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      try {
        close();
      } catch (Exception e1) {
        e.addSuppressed(e1);
      }
      throw new RuntimeException(e);
    }
    return httpClient;
  }

  public void close() {
    log.info("Closing {} closeClient={}", this.getClass().getSimpleName(), closeClient);
   // assert closeTracker != null ? closeTracker.close() : true;
    asyncTracker.close();
    
    if (closeClient) {
      try {
        httpClient.stop();
      } catch (Exception e) {
        log.error("Exception closing httpClient", e);
      }
    }
    log.info("Done closing {}", this.getClass().getSimpleName());
    assert ObjectReleaseTracker.release(this);
  }

  public void waitForOutstandingRequests() {
    asyncTracker.waitForComplete();
  }

  public boolean isV2ApiRequest(final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  public long getIdleTimeout() {
    return idleTimeout;
  }

  public static class OutStream implements Closeable{
    private final String origCollection;
    private final ModifiableSolrParams origParams;
    private final OutputStreamContentProvider outProvider;
    private final InputStreamResponseListener responseListener;
    private final boolean isXml;

    public OutStream(String origCollection, ModifiableSolrParams origParams,
                     OutputStreamContentProvider outProvider, InputStreamResponseListener responseListener, boolean isXml) {
      this.origCollection = origCollection;
      this.origParams = origParams;
      this.outProvider = outProvider;
      this.responseListener = responseListener;
      this.isXml = isXml;
    }

    boolean belongToThisStream(@SuppressWarnings({"rawtypes"})SolrRequest solrRequest, String collection) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams(solrRequest.getParams());
      if (!origParams.toNamedList().equals(solrParams.toNamedList()) || !StringUtils.equals(origCollection, collection)) {
        return false;
      }
      return true;
    }

    public void write(byte b[]) throws IOException {
      this.outProvider.getOutputStream().write(b);
    }

    public void flush() throws IOException {
      this.outProvider.getOutputStream().flush();
    }

    @Override
    public void close() throws IOException {
      if (isXml) {
        write("</stream>".getBytes(FALLBACK_CHARSET));
      }
      //this.outProvider.getOutputStream().close();
    }

    //TODO this class should be hidden
    public InputStreamResponseListener getResponseListener() {
      return responseListener;
    }
  }

  public OutStream initOutStream(String baseUrl,
                                 UpdateRequest updateRequest,
                                 String collection) throws IOException {
    String contentType = requestWriter.getUpdateContentType();
    final ModifiableSolrParams origParams = new ModifiableSolrParams(updateRequest.getParams());

    // The parser 'wt=' and 'version=' params are used instead of the
    // original params
    ModifiableSolrParams requestParams = new ModifiableSolrParams(origParams);
    requestParams.set(CommonParams.WT, parser.getWriterType());
    requestParams.set(CommonParams.VERSION, parser.getVersion());

    String basePath = baseUrl;
    if (collection != null)
      basePath += "/" + collection;
    if (!basePath.endsWith("/"))
      basePath += "/";

    OutputStreamContentProvider provider = new OutputStreamContentProvider();
    Request postRequest = httpClient
        .newRequest(basePath + "update"
            + requestParams.toQueryString())
        .method(HttpMethod.POST)
        .header(HttpHeader.CONTENT_TYPE, contentType)
        .content(provider);
    postRequest.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    for (Map.Entry<String,String> entry : headers.entrySet()) {
      postRequest.header(entry.getKey(), entry.getValue());
    }

    decorateRequest(postRequest, updateRequest);
    updateRequest.setBasePath(baseUrl);
    InputStreamResponseListener responseListener = new InputStreamResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, provider, responseListener,
        isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(FALLBACK_CHARSET));
    }
    return outStream;
  }

  public void send(OutStream outStream, SolrRequest req, String collection) throws IOException {
    assert outStream.belongToThisStream(req, collection);
    this.requestWriter.write(req, outStream.outProvider.getOutputStream());
    if (outStream.isXml) {
      // check for commit or optimize
      SolrParams params = req.getParams();
      if (params != null) {
        String fmt = null;
        if (params.getBool(UpdateParams.OPTIMIZE, false)) {
          fmt = "<optimize waitSearcher=\"%s\" />";
        } else if (params.getBool(UpdateParams.COMMIT, false)) {
          fmt = "<commit waitSearcher=\"%s\" />";
        }
        if (fmt != null) {
          byte[] content = String.format(Locale.ROOT,
              fmt, params.getBool(UpdateParams.WAIT_SEARCHER, false)
                  + "")
              .getBytes(FALLBACK_CHARSET);
          outStream.write(content);
        }
      }
    }
    outStream.flush();
  }

  private static final Exception CANCELLED_EXCEPTION = new Exception();
  private static final Cancellable FAILED_MAKING_REQUEST_CANCELLABLE = () -> {};

  public Cancellable asyncRequest(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<NamedList<Object>> asyncListener) {
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
    } catch (SolrServerException | IOException e) {
      asyncListener.onFailure(e);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    final ResponseParser parser = solrRequest.getResponseParser() == null
        ? this.parser: solrRequest.getResponseParser();
    asyncTracker.register();
    try {
      req.send(new InputStreamResponseListener() {
        @Override
        public void onHeaders(Response response) {
          super.onHeaders(response);
          InputStreamResponseListener listener = this;
          ParWork.getRootSharedExecutor().execute(() -> {
            if (log.isDebugEnabled()) log.debug("async response ready");
            InputStream is = listener.getInputStream();
            try {
              NamedList<Object> body = processErrorsAndResponse(solrRequest, parser, response, is);
              asyncListener.onSuccess(body);
            } catch (RemoteSolrException e) {
              if (SolrException.getRootCause(e) != CANCELLED_EXCEPTION) {
                asyncListener.onFailure(e);
              }
            } catch (Exception e) {
              asyncListener.onFailure(e);
            } finally {
              asyncTracker.arrive();
            }
          });
        }

        @Override
        public void onFailure(Response response, Throwable failure) {
          try {
            super.onFailure(response, failure);
            if (failure != CANCELLED_EXCEPTION) { // avoid retrying on load balanced search requests - keep in mind this
              // means cancelled requests won't notify the caller of fail or complete
              asyncListener.onFailure(new SolrServerException(failure.getMessage(), failure));
            }
          } finally {
            asyncTracker.arrive();
          }
        }
      });
    } catch (Exception e) {
      asyncListener.onFailure(e);
      asyncTracker.arrive();
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, e);
    }
    return () -> {
      req.abort(CANCELLED_EXCEPTION);
    };
  }

  public Cancellable asyncRequestRaw(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection, AsyncListener<InputStream> asyncListener) {
    Request req;
    try {
      req = makeRequest(solrRequest, collection);
    } catch (SolrServerException | IOException e) {
      asyncListener.onFailure(e);
      return FAILED_MAKING_REQUEST_CANCELLABLE;
    }
    asyncTracker.register();
    try {
      req.send(new InputStreamResponseListener() {
        @Override
        public void onHeaders(Response response) {
          super.onHeaders(response);
          InputStreamResponseListener listener = this;
          ParWork.getRootSharedExecutor().execute(() -> {
            if (log.isDebugEnabled()) log.debug("stream async response ready");
            InputStream is = listener.getInputStream();
            try {
              asyncListener.onSuccess(is);
            } catch (RemoteSolrException e) {
              if (SolrException.getRootCause(e) != CANCELLED_EXCEPTION) {
                asyncListener.onFailure(e);
              }
            } catch (Exception e) {
              asyncListener.onFailure(e);
            } finally {
              asyncTracker.arrive();
            }
          });
        }

        @Override
        public void onFailure(Response response, Throwable failure) {
          try {
            super.onFailure(response, failure);
            if (failure != CANCELLED_EXCEPTION) { // avoid retrying on load balanced search requests - keep in mind this
              // means cancelled requests won't notify the caller of fail or complete
              asyncListener.onFailure(new SolrServerException(failure.getMessage(), failure));
            }
          } finally {
            asyncTracker.arrive();
          }
        }
      });
    } catch (Exception e) {
      asyncListener.onFailure(e);
      asyncTracker.arrive();
      throw new SolrException(SolrException.ErrorCode.UNKNOWN, e);
    }
    return () -> {
      req.abort(CANCELLED_EXCEPTION);
    };
  }

  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest solrRequest, String collection) throws SolrServerException, IOException {
    Request req = makeRequest(solrRequest, collection);
    final ResponseParser parser = solrRequest.getResponseParser() == null
        ? this.parser: solrRequest.getResponseParser();

    try {
      InputStreamResponseListener listener = new InputStreamResponseListener();
      req.send(listener);
      Response response = listener.get(idleTimeout, TimeUnit.MILLISECONDS);
      InputStream is = listener.getInputStream();

      return processErrorsAndResponse(solrRequest, parser, response, is);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new SolrServerException(
          "Timeout occured while waiting response from server at: " + req.getURI(), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ConnectException) {
        throw new SolrServerException("Server refused connection at: " + req.getURI(), cause);
      }
      if (cause instanceof SolrServerException) {
        throw (SolrServerException) cause;
      } else if (cause instanceof IOException) {
        throw new SolrServerException(
            "IOException occured when talking to server at: " + getBaseURL(), cause);
      }
      throw new SolrServerException(cause.getMessage(), cause);
    }
  }

  private NamedList<Object> processErrorsAndResponse(@SuppressWarnings({"rawtypes"})SolrRequest solrRequest,
                                                     ResponseParser parser, Response response, InputStream is) throws SolrServerException {
    ContentType contentType = getContentType(response);
    String mimeType = null;
    String encoding = null;
    if (contentType != null) {
      mimeType = contentType.getMimeType();
      encoding = contentType.getCharset() != null? contentType.getCharset().name() : null;
    }
    return processErrorsAndResponse(response, parser, is, mimeType, encoding, isV2ApiRequest(solrRequest));
  }

  private ContentType getContentType(Response response) {
    String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);
    return StringUtils.isEmpty(contentType)? null : ContentType.parse(contentType);
  }

  private void setBasicAuthHeader(SolrRequest solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String userPass = solrRequest.getBasicAuthUser() + ":" + solrRequest.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(FALLBACK_CHARSET));
      req.header("Authorization", "Basic " + encoded);
    }
  }

  public void setBaseUrl(String baseUrl) {
    this.serverBaseUrl = baseUrl;
  }

  private Request makeRequest(SolrRequest solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = createRequest(solrRequest, collection);
    decorateRequest(req, solrRequest);
    return req;
  }

  private void decorateRequest(Request req, SolrRequest solrRequest) {
    req.header(HttpHeader.ACCEPT_ENCODING, null).idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    if (solrRequest.getUserPrincipal() != null) {
      req.attribute(REQ_PRINCIPAL_KEY, solrRequest.getUserPrincipal());
    }

    setBasicAuthHeader(solrRequest, req);
    for (HttpListenerFactory factory : listenerFactory) {
      HttpListenerFactory.RequestResponseListener listener = factory.get();
      listener.onQueued(req);
      req.onRequestBegin(listener);
      req.onComplete(listener);
    }

    Map<String, String> headers = solrRequest.getHeaders();
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
    }
  }
  
  private String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
    URL oldURL = new URL(basePath);
    String newPath = oldURL.getPath().replaceFirst("/solr", "/api");
    return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
  }

  private Request createRequest(SolrRequest solrRequest, String collection) throws IOException, SolrServerException {
    if (solrRequest.getBasePath() == null && serverBaseUrl == null)
      throw new IllegalArgumentException("Destination node is not provided!");

    if (solrRequest instanceof V2RequestSupport) {
      solrRequest = ((V2RequestSupport) solrRequest).getV2Request();
    }
    SolrParams params = solrRequest.getParams();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(solrRequest);
    Collection<ContentStream> streams = contentWriter == null ? requestWriter.getContentStreams(solrRequest) : null;
    String path = requestWriter.getPath(solrRequest);
    if (path == null) {
      path = DEFAULT_PATH;
    }

    ResponseParser parser = solrRequest.getResponseParser();
    if (parser == null) {
      parser = this.parser;
    }

    // The parser 'wt=' and 'version=' params are used instead of the original
    // params
    ModifiableSolrParams wparams = new ModifiableSolrParams(params);
    if (parser != null) {
      wparams.set(CommonParams.WT, parser.getWriterType());
      wparams.set(CommonParams.VERSION, parser.getVersion());
    }

    //TODO add invariantParams support

    String basePath = solrRequest.getBasePath() == null ? serverBaseUrl : solrRequest.getBasePath();
    if (collection != null)
      basePath += "/" + collection;

    if (solrRequest instanceof V2Request) {
      if (System.getProperty("solr.v2RealPath") == null) {
        basePath = changeV2RequestEndpoint(basePath);
      } else {
        basePath = solrRequest.getBasePath() == null ? serverBaseUrl  : solrRequest.getBasePath() + "/____v2";
      }
    }

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.GET).idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
      req.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      return req;
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.DELETE).idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
      req.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
      return req;
    }

    if (SolrRequest.METHOD.POST == solrRequest.getMethod() || SolrRequest.METHOD.PUT == solrRequest.getMethod()) {

      String url = basePath + path;
      boolean hasNullStreamName = false;
      if (streams != null) {
        hasNullStreamName = streams.stream().anyMatch(cs -> cs.getName() == null);
      }

      boolean isMultipart = streams != null && streams.size() > 1 && !hasNullStreamName;

      HttpMethod method = SolrRequest.METHOD.POST == solrRequest.getMethod() ? HttpMethod.POST : HttpMethod.PUT;

      if (contentWriter != null) {
        Request req;
        try {
          req = httpClient.newRequest(url + wparams.toQueryString()).idleTimeout(idleTimeout, TimeUnit.MILLISECONDS).method(method);
        } catch (IllegalArgumentException e) {
          throw new SolrServerException("Illegal url for request url=" + url, e);
        }
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req.header(entry.getKey(), entry.getValue());
        }
        req.idleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
        contentWriter.write(baos);

        //TODO reduce memory usage
        return req.content(new BytesContentProvider(contentWriter.getContentType(), baos.toByteArray()));
      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));
        Request req = httpClient
            .newRequest(url + queryParams.toQueryString())
            .idleTimeout(idleTimeout, TimeUnit.MILLISECONDS)
            .method(method);
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req.header(entry.getKey(), entry.getValue());
        }
        return fillContentStream(req, streams, wparams, isMultipart);
      } else {
        // It is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();
        Request req = httpClient
                .newRequest(url + wparams.toQueryString())
                .method(method)
                .idleTimeout(idleTimeout, TimeUnit.MILLISECONDS)
                .content(new InputStreamContentProvider(contentStream.getStream()), contentStream.getContentType());
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req.header(entry.getKey(), entry.getValue());
        }
        return req;
      }
    }

    throw new SolrServerException("Unsupported method: " + solrRequest.getMethod());
  }

  private Request fillContentStream(Request req, Collection<ContentStream> streams,
                                    ModifiableSolrParams wparams,
                                    boolean isMultipart) throws IOException {
    if (isMultipart) {
      // multipart/form-data
      MultiPartContentProvider content = new MultiPartContentProvider();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            content.addFieldPart(key, new StringContentProvider(val), null);
          }
        }
      }
      if (streams != null) {
        for (ContentStream contentStream : streams) {
          String contentType = contentStream.getContentType();
          if (contentType == null) {
            contentType = BinaryResponseParser.BINARY_CONTENT_TYPE; // default
          }
          String name = contentStream.getName();
          if (name == null) {
            name = "";
          }
          HttpFields fields = new HttpFields();
          fields.add(HttpHeader.CONTENT_TYPE, contentType);
          content.addFilePart(name, contentStream.getName(), new InputStreamContentProvider(contentStream.getStream()), fields);
        }
      }
      req.content(content);
    } else {
      // application/x-www-form-urlencoded
      Fields fields = new Fields();
      Iterator<String> iter = wparams.getParameterNamesIterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String[] vals = wparams.getParams(key);
        if (vals != null) {
          for (String val : vals) {
            fields.add(key, val);
          }
        }
      }
      req.content(new FormContentProvider(fields, FALLBACK_CHARSET));
    }

    return req;
  }

  private boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private NamedList<Object> processErrorsAndResponse(Response response,
                                                     final ResponseParser processor,
                                                     InputStream is,
                                                     String mimeType,
                                                     String encoding,
                                                     final boolean isV2Api)
      throws SolrServerException {
    boolean shouldClose = true;
    try {
      // handle some http level checks before trying to parse the response
      int httpStatus = response.getStatus();

      switch (httpStatus) {
        case HttpStatus.SC_OK:
        case HttpStatus.SC_BAD_REQUEST:
        case HttpStatus.SC_CONFLICT: // 409
          break;
        case HttpStatus.SC_MOVED_PERMANENTLY:
        case HttpStatus.SC_MOVED_TEMPORARILY:
          if (!httpClient.isFollowRedirects()) {
            throw new SolrServerException(
                "Server at " + getBaseURL() + " sent back a redirect ("
                    + httpStatus + ").");
          }
          break;
        default:
          if (processor == null || mimeType == null) {
            throw new RemoteSolrException(response.getRequest().getURI().toString(), httpStatus, "non ok status: " + httpStatus
                + ", message:" + response.getReason(),
                null);
          }
      }

      if (wantStream(processor)) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>(1);
        rsp.add("stream", is);
        // Only case where stream should not be closed
        shouldClose = false;
        return rsp;
      }

      String procCt = processor.getContentType();
      if (procCt != null) {
        String procMimeType = ContentType.parse(procCt).getMimeType().trim()
            .toLowerCase(Locale.ROOT);

        if (!procMimeType.equals(mimeType)) {
          // unexpected mime type
          String msg =
              "Expected mime type " + procMimeType + " but got " + mimeType
                  + ".";
          String exceptionEncoding =
              encoding != null ? encoding : FALLBACK_CHARSET.name();
          try {
            msg = msg + " " + IOUtils.toString(is, exceptionEncoding);
          } catch (IOException e) {
            throw new RemoteSolrException(serverBaseUrl, httpStatus,
                "Could not parse response with encoding " + exceptionEncoding,
                e);
          }
          throw new RemoteSolrException(serverBaseUrl, httpStatus, msg, null);
        }
      }

      NamedList<Object> rsp;
      try {
        rsp = processor.processResponse(is, encoding);
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new RemoteSolrException(serverBaseUrl, httpStatus, e.getMessage(), e);
      }

      Object error = rsp == null ? null : rsp.get("error");

      if (error != null && (error instanceof NamedList && ((NamedList<?>) error).get("metadata") == null || isV2Api)) {
        throw RemoteExecutionException.create(serverBaseUrl, rsp);
      }

      if (httpStatus != HttpStatus.SC_OK && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          if (error != null) {
            reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("msg"));
            if(reason == null) {
              reason = (String) Utils.getObjectByPath(error, false, Collections.singletonList("trace"));
            }
            Object metadataObj = Utils.getObjectByPath(error, false, Collections.singletonList("metadata"));
            if  (metadataObj instanceof NamedList) {
              metadata = (NamedList<String>) metadataObj;
            } else if (metadataObj instanceof List) {
              // NamedList parsed as List convert to NamedList again
              List<Object> list = (List<Object>) metadataObj;
              metadata = new NamedList<>(list.size()/2);
              for (int i = 0; i < list.size(); i+=2) {
                metadata.add((String)list.get(i), (String) list.get(i+1));
              }
            } else if (metadataObj instanceof Map) {
              metadata = new NamedList((Map) metadataObj);
            }
            List details = (ArrayList) Utils.getObjectByPath(error, false, Collections.singletonList("details"));
            if (details != null) {
              reason = reason + " " + details;
            }

          }
        } catch (Exception ex) {
          log.warn("Exception parsing error response", ex);
        }
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getReason())
              .append("\n\n")
              .append("request: ")
              .append(response.getRequest().getMethod());
          reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
        }
        RemoteSolrException rss = new RemoteSolrException(serverBaseUrl, httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }

      return rsp;
    } finally {
      if (shouldClose) {
        try {
          while(is.read() != -1) { }
          // is.close();
        } catch (IOException e) {
          // quietly
        }
      }
    }
  }

  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  public static class AsyncTracker {

    private final Semaphore available;

    // wait for async requests
    private final Phaser phaser = new ThePhaser(1);
    // maximum outstanding requests left

    public AsyncTracker(int maxOutstandingAsyncRequests) {
      if (maxOutstandingAsyncRequests > 0) {
        available = new Semaphore(maxOutstandingAsyncRequests, true);
      } else {
        available = null;
      }
    }

    public synchronized void waitForComplete() {
      if (log.isDebugEnabled()) log.debug("Before wait for outstanding requests registered: {} arrived: {}, {} {}", phaser.getRegisteredParties(), phaser.getArrivedParties(), phaser.getUnarrivedParties(), phaser);

      phaser.arriveAndAwaitAdvance();

      if (log.isDebugEnabled()) log.debug("After wait for outstanding requests {}", phaser);
    }

    public void close() {
      phaser.forceTermination();
      if (available != null) available.release(available.getQueueLength() + 5);
    }

    public void register() {
      if (log.isDebugEnabled()) {
        log.debug("Registered new party {}", phaser);
      }
      phaser.register();
      try {
        if (available != null) available.acquire();
      } catch (InterruptedException e) {
        log.warn("interrupted", e);
      }
    }

    public void arrive() {
      try {
        if (available != null) available.release();
      } finally {
        phaser.arriveAndDeregister();
      }
      if (log.isDebugEnabled()) log.debug("Request complete {}", phaser);
    }
  }

  public static class ThePhaser extends Phaser {

    ThePhaser(int start) {
      super(start);
    }

    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  }

  public abstract static class Abortable {
    public abstract void abort();
  }

  public static class Builder {

    public int maxThreadPoolSize = Integer.getInteger("solr.maxHttp2ClientThreads", Math.max(64, ParWork.PROC_COUNT));
    public int maxRequestsQueuedPerDestination = 2048;
    private Http2SolrClient http2SolrClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout = Integer.getInteger("solr.http2solrclient.default.idletimeout", 120000);
    private Integer connectionTimeout;
    private Integer maxConnectionsPerHost = 4;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    protected String baseSolrUrl;
    protected Map<String,String> headers = new ConcurrentHashMap<>();
    protected boolean strictEventOrdering = false;
    private Integer maxOutstandingAsyncRequests;

    public Builder() {

    }

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    /**
     * Reuse {@code httpClient} connections pool
     */
    public Builder withHttpClient(Http2SolrClient httpClient) {
      this.http2SolrClient = httpClient;
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    /**
     * Set maxConnectionsPerHost for http1 connections, maximum number http2 connections is limited by 4
     */
    public Builder maxConnectionsPerHost(int max) {
      this.maxConnectionsPerHost = max;
      return this;
    }

    public Builder maxRequestsQueuedPerDestination(int max) {
      this.maxRequestsQueuedPerDestination = max;
      return this;
    }

    public Builder maxThreadPoolSize(int max) {
      this.maxThreadPoolSize = max;
      return this;
    }

    public Builder idleTimeout(int idleConnectionTimeout) {
      this.idleTimeout = idleConnectionTimeout;
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return this;
    }

    public Builder strictEventOrdering(boolean strictEventOrdering) {
      this.strictEventOrdering = strictEventOrdering;
      return this;
    }

    public Builder connectionTimeout(int connectionTimeOut) {
      this.connectionTimeout = connectionTimeOut;
      return this;
    }

    //do not set this from an external client
    public Builder markInternalRequest() {
      this.headers.put(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
      return this;
    }

    public Builder withBaseUrl(String url) {
      this.baseSolrUrl = url;
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder withHeader(String header, String value) {
      this.headers.put(header, value);
      return this;
    }

    public Builder maxOutstandingAsyncRequests(int maxOutstandingAsyncRequests) {
      this.maxOutstandingAsyncRequests = maxOutstandingAsyncRequests;
      return this;
    }
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method
   *
   * @param queryParams set of param keys to only send via the query string
   *                    Note that the param will be sent as a query string if the key is part
   *                    of this Set or the SolrRequest's query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   */
  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }

  private ModifiableSolrParams calculateQueryParams(Set<String> queryParamNames,
                                                    ModifiableSolrParams wparams) {
    ModifiableSolrParams queryModParams = new ModifiableSolrParams();
    if (queryParamNames != null) {
      for (String param : queryParamNames) {
        String[] value = wparams.getParams(param);
        if (value != null) {
          for (String v : value) {
            queryModParams.add(param, v);
          }
          wparams.remove(param);
        }
      }
    }
    return queryModParams;
  }

  public ResponseParser getParser() {
    return parser;
  }

  public void setParser(ResponseParser processor) {
    parser = processor;
  }

  public static void setDefaultSSLConfig(SSLConfig sslConfig) {
    Http2SolrClient.defaultSSLConfig = sslConfig;
  }

  // public for testing, only used by tests
  public static void resetSslContextFactory() {
    Http2SolrClient.defaultSSLConfig = null;
  }

  /* package-private for testing */
  static SslContextFactory.Client getDefaultSslContextFactory() {
    String checkPeerNameStr = System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
    boolean sslCheckPeerName = true;
    if (checkPeerNameStr == null || "false".equalsIgnoreCase(checkPeerNameStr)) {
      sslCheckPeerName = false;
    }

    SslContextFactory.Client sslContextFactory = new SslContextFactory.Client(!sslCheckPeerName);

    if (null != System.getProperty("javax.net.ssl.keyStore")) {
      sslContextFactory.setKeyStorePath
          (System.getProperty("javax.net.ssl.keyStore"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
      sslContextFactory.setKeyStorePassword
          (System.getProperty("javax.net.ssl.keyStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStore")) {
      sslContextFactory.setTrustStorePath
          (System.getProperty("javax.net.ssl.trustStore"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
      sslContextFactory.setTrustStorePassword
          (System.getProperty("javax.net.ssl.trustStorePassword"));
    }

    sslContextFactory.setEndpointIdentificationAlgorithm(System.getProperty("solr.jetty.ssl.verifyClientHostName"));

    return sslContextFactory;
  }

  public static int HEAD(String url, Http2SolrClient httpClient) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response;
    Request req = httpClient.getHttpClient().newRequest(URI.create(url));
    response = req.method(HEAD).send();
    if (response.getStatus() != 200) {
      throw new RemoteSolrException(url, response.getStatus(), response.getReason(), null);
    }
    return response.getStatus();
  }

  public static class SimpleResponse {
    public String asString;
    public String contentType;
    public int size;
    public int status;
    public byte[] bytes;
  }


  public static SimpleResponse DELETE(String url, Http2SolrClient httpClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doDelete(url, httpClient, Collections.emptyMap());
  }


  public static SimpleResponse GET(String url, Http2SolrClient httpClient)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, Collections.emptyMap());
  }

  public static SimpleResponse GET(String url, Http2SolrClient httpClient, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient, headers);
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public static SimpleResponse POST(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, headers);
  }

  public static SimpleResponse PUT(String url, Http2SolrClient httpClient, byte[] bytes, String contentType, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doPut(url, httpClient, bytes, contentType, headers);
  }

  private static SimpleResponse doGet(String url, Http2SolrClient httpClient, Map<String,String> headers)
          throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    Request req = httpClient.getHttpClient().newRequest(url).method(GET);
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    sResponse.bytes = response.getContent();
    return sResponse;
  }

  private static SimpleResponse doDelete(String url, Http2SolrClient httpClient, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    assert url != null;
    Request req = httpClient.getHttpClient().newRequest(url).method(DELETE);
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }

  public String httpDelete(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(URI.create(url)).method(DELETE).send();
    return response.getContentAsString();
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(POST).content(new BytesContentProvider(contentType, bytes));
    for (Map.Entry<String,String> entry : headers.entrySet()) {
      req.header(entry.getKey(), entry.getValue());
    }
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }

  private static SimpleResponse doPut(String url, Http2SolrClient httpClient, byte[] bytes, String contentType,
      Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(PUT).content(new BytesContentProvider(contentType, bytes));
    for (Map.Entry<String,String> entry : headers.entrySet()) {
      req.header(entry.getKey(), entry.getValue());
    }
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }

  private static SimpleResponse doPost(String url, Http2SolrClient httpClient, ByteBuffer bytes, String contentType,
                                       Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.getHttpClient().newRequest(url).method(POST).content(new ByteBufferContentProvider(contentType, bytes));
    for (Map.Entry<String,String> entry : headers.entrySet()) {
      req.header(entry.getKey(), entry.getValue());
    }
    ContentResponse response = req.send();
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.status = response.getStatus();
    return sResponse;
  }


  public String httpPut(String url, HttpClient httpClient, byte[] bytes, String contentType)
          throws InterruptedException, ExecutionException, TimeoutException, SolrServerException {
    ContentResponse response = httpClient.newRequest(url).method(PUT).content(new BytesContentProvider(bytes), contentType).send();
    return response.getContentAsString();
  }

  private static class SolrHttpClientTransportOverHTTP extends HttpClientTransportOverHTTP {
    public SolrHttpClientTransportOverHTTP(int selectors) {
      super(selectors);
    }

    public HttpClient getHttpClient() {
      return super.getHttpClient();
    }
  }
}
