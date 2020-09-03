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
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.SolrScheduledExecutorScheduler;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.ProtocolHandlers;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.BufferingResponseListener;
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
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

  private final CloseTracker closeTracker;

  private volatile HttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private int idleTimeout;
  private volatile ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();
  private final Set<HttpListenerFactory> listenerFactory = ConcurrentHashMap.newKeySet();
  private final AsyncTracker asyncTracker = new AsyncTracker();
  /**
   * The URL of the Solr server.
   */
  private volatile String serverBaseUrl;
  private volatile boolean closeClient;
  private volatile SolrQueuedThreadPool httpClientExecutor;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    closeTracker = new CloseTracker();
    if (serverBaseUrl != null)  {
      if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
      }
      this.serverBaseUrl = serverBaseUrl;
    }

    this.headers = builder.headers;

    if (builder.idleTimeout != null && builder.idleTimeout > 0) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.http2SolrClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.http2SolrClient.httpClient;
    }
    assert ObjectReleaseTracker.track(this);
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

    SslContextFactory.Client sslContextFactory;
    boolean ssl;
    if (builder.sslConfig == null) {
      sslContextFactory = getDefaultSslContextFactory();
      ssl = sslContextFactory.getTrustStore() != null || sslContextFactory.getTrustStorePath() != null;
    } else {
      sslContextFactory = builder.sslConfig.createClientContextFactory();
      ssl = true;
    }

    boolean sslOnJava8OrLower = ssl && !Constants.JRE_IS_MINIMUM_JAVA9;
    HttpClientTransport transport;
    if (builder.useHttp1_1 || sslOnJava8OrLower) {
      if (sslOnJava8OrLower && !builder.useHttp1_1) {
        log.warn("Create Http2SolrClient with HTTP/1.1 transport since Java 8 or lower versions does not support SSL + HTTP/2");
      } else {
        log.debug("Create Http2SolrClient with HTTP/1.1 transport");
      }
      transport = new HttpClientTransportOverHTTP(2);
      httpClient = new HttpClient(transport, sslContextFactory);
      if (builder.maxConnectionsPerHost != null) httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
    } else {
      log.debug("Create Http2SolrClient with HTTP/2 transport");
      HTTP2Client http2client = new HTTP2Client();
      transport = new HttpClientTransportOverHTTP2(http2client);
      httpClient = new HttpClient(transport, sslContextFactory);
      if (builder.maxConnectionsPerHost != null) httpClient.setMaxConnectionsPerDestination(builder.maxConnectionsPerHost);
    }
    httpClientExecutor = new SolrQueuedThreadPool("httpClient", Math.max(12, ParWork.PROC_COUNT), 6, idleTimeout);
    httpClientExecutor.setLowThreadsThreshold(-1);

    httpClient.setIdleTimeout(idleTimeout);
    try {
      httpClient.setScheduler(new SolrScheduledExecutorScheduler("http2client-scheduler"));
      httpClient.setExecutor(httpClientExecutor);
      httpClient.setStrictEventOrdering(true);
      httpClient.setConnectBlocking(false);
      httpClient.setFollowRedirects(false);
      httpClient.setMaxRequestsQueuedPerDestination(1024);
      httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));
      httpClient.setIdleTimeout(idleTimeout);
      httpClient.setTCPNoDelay(true);
      if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
      httpClient.start();
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      close();
      throw new RuntimeException(e);
    }
    return httpClient;
  }

  public void close() {
   // closeTracker.close();
    if (closeClient) {
      try {
        httpClient.stop();
      } catch (Exception e) {
        log.error("Exception closing httpClient", e);
      }
    }

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

    boolean belongToThisStream(SolrRequest solrRequest, String collection) {
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
      this.outProvider.getOutputStream().close();
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
    for (Map.Entry<String,String> entry : headers.entrySet()) {
      postRequest.header(entry.getKey(), entry.getValue());
    }

    decorateRequest(postRequest, updateRequest);
    InputStreamResponseListener responseListener = new InputStreamResponseListener() {
      @Override
      public void onComplete(Result result) {
        super.onComplete(result);
      }
    };
    asyncTracker.register();
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

  public NamedList<Object> request(SolrRequest solrRequest,
                                      String collection,
                                      OnComplete onComplete) throws IOException, SolrServerException {
    Request req = makeRequest(solrRequest, collection);
    final ResponseParser parser = solrRequest.getResponseParser() == null
        ? this.parser: solrRequest.getResponseParser();
    if (onComplete != null) {
      // This async call only suitable for indexing since the response size is limited by 5MB
      req.onRequestQueued(asyncTracker.queuedListener)
          .send(new BufferingResponseListener(5 * 1024 * 1024) {

        @Override
        public void onComplete(Result result) {
          try {
            if (result.isFailed()) {
              onComplete.onFailure(result.getFailure());
              return;
            }

            NamedList<Object> rsp;
            try {
              InputStream is = getContentAsInputStream();
              assert ObjectReleaseTracker.track(is);
              rsp = processErrorsAndResponse(req, result.getResponse(),
                      parser, is, getMediaType(), getEncoding(), isV2ApiRequest(solrRequest));
              onComplete.onSuccess(rsp);
            } catch (Exception e) {
              ParWork.propegateInterrupt(e);
            }
          } finally {
            asyncTracker.completeListener.onComplete(result);
          }
        }
      });
      return null;
    } else {
      try {
        InputStreamResponseListener listener = new InputStreamResponseListener() {
          @Override
          public void onComplete(Result result) {
            super.onComplete(result);
          }
        };
        req.send(listener);
        Response response = listener.get(idleTimeout, TimeUnit.MILLISECONDS);
        InputStream is = listener.getInputStream();
        // nocommit - track this again when streaming use is fixed
        //assert ObjectReleaseTracker.track(is);

        ContentType contentType = getContentType(response);
        String mimeType = null;
        String encoding = null;
        if (contentType != null) {
          mimeType = contentType.getMimeType();
          encoding = contentType.getCharset() != null? contentType.getCharset().name() : null;
        }
        return processErrorsAndResponse(req, response, parser, is, mimeType, encoding, isV2ApiRequest(solrRequest));
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
    req.header(HttpHeader.ACCEPT_ENCODING, null);
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
    if (path == null || !path.startsWith("/")) {
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
        basePath = serverBaseUrl + "/____v2";
      }
    }


    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.GET);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
      return req;
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      Request req = httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.DELETE);
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
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
        Request req = httpClient
            .newRequest(url + wparams.toQueryString())
            .method(method);
        for (Map.Entry<String,String> entry : headers.entrySet()) {
          req.header(entry.getKey(), entry.getValue());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        contentWriter.write(baos);

        //TODO reduce memory usage
        return req.content(new BytesContentProvider(contentWriter.getContentType(), baos.toByteArray()));
      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(solrRequest.getQueryParams(), wparams));
        Request req = httpClient
            .newRequest(url + queryParams.toQueryString())
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

  private NamedList<Object> processErrorsAndResponse(Request req, Response response,
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
            throw new RemoteSolrException(serverBaseUrl, httpStatus,
                "non ok status: " + httpStatus + ", message:" + response
                    .getReason(), null);
          }
      }

      if (wantStream(processor)) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
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
        ParWork.propegateInterrupt(e);
        throw new RemoteSolrException(serverBaseUrl, httpStatus, e.getMessage(),
            e);
      }

      Object error = rsp == null ? null : rsp.get("error");
      if (error != null && (String
          .valueOf(getObjectByPath(error, true, errPath))
          .endsWith("ExceptionWithErrObject"))) {
        throw RemoteExecutionException.create(serverBaseUrl, rsp);
      }
      if (httpStatus != HttpStatus.SC_OK && !isV2Api) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          Object errorObject = rsp.get("error");
          NamedList err;
          if (errorObject instanceof LinkedHashMap) {
            err = new NamedList((LinkedHashMap) errorObject);
          } else {
            err = (NamedList) rsp.get("error");
          }

          if (err != null) {
            reason = (String) err.get("msg");
            if (reason == null) {
              reason = (String) err.get("trace");
            }
            metadata = (NamedList<String>) err.get("metadata");
          }
        } catch (Exception ex) {
          ParWork.propegateInterrupt(ex);
          log.warn("Unexpected exception", ex);
        }
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getReason()).append("\n\n").append("request: ")
              .append(response.getRequest().getMethod());
          reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET);
        }
        RemoteSolrException rss = new RemoteSolrException(serverBaseUrl,
            httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }

      return rsp;
    } finally {
      if (shouldClose) {
        try {
          is.close();
          assert ObjectReleaseTracker.release(is);
        } catch (IOException e) {
          // quitely
        }
      }
    }
  }


  @Override
  public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
    return request(request, collection, null);
  }

  public void enableCloseLock() {
    closeTracker.enableCloseLock();
  }

  public void disableCloseLock() {
    closeTracker.disableCloseLock();
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  public interface OnComplete {
    void onSuccess(NamedList<Object> result);

    void onFailure(Throwable e);
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  private static class AsyncTracker {

    // nocommit - look at outstanding max again
    private static final int MAX_OUTSTANDING_REQUESTS = 30;

    private final Semaphore available;

    // wait for async requests
    private final Phaser phaser = new ThePhaser(1);
    // maximum outstanding requests left
    private final Request.QueuedListener queuedListener;
    private final Response.CompleteListener completeListener;

    AsyncTracker() {
      available = new Semaphore(MAX_OUTSTANDING_REQUESTS, false);
      queuedListener = request -> {
        phaser.register();
        try {
          available.acquire();
        } catch (InterruptedException ignored) {
          ParWork.propegateInterrupt(ignored);
        }
        if (log.isDebugEnabled()) log.debug("Request queued registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());
      };

      completeListener = result -> {
       if (log.isDebugEnabled()) log.debug("Request complete registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());
        phaser.arriveAndDeregister();
        available.release();
      };
    }

    int getMaxRequestsQueuedPerDestination() {
      // comfortably above max outstanding requests
      return MAX_OUTSTANDING_REQUESTS * 10;
    }

    public void waitForComplete() {
      if (log.isDebugEnabled()) log.debug("Before wait for outstanding requests registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());

      int arrival = phaser.arriveAndAwaitAdvance();

     // phaser.awaitAdvance(phaser.arriveAndDeregister());

      if (log.isDebugEnabled()) log.debug("After wait for outstanding requests registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());
    }

    public void waitForCompleteFinal() {
      if (log.isDebugEnabled()) log.debug("Before wait for complete final registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());
      phaser.awaitAdvance(phaser.arriveAndDeregister());

      if (log.isDebugEnabled()) log.debug("After wait for complete final registered: {} arrived: {}", phaser.getRegisteredParties(), phaser.getArrivedParties());
    }

    public void register() {
      if (log.isDebugEnabled()) {
        log.debug("Registered new party");
      }
   //   phaser.register();
//      try {
//        available.acquire();
//      } catch (InterruptedException ignored) {
//        ParWork.propegateInterrupt(ignored);
//      }
    }

    private static class ThePhaser extends Phaser {

      ThePhaser(int start) {
        super(start);
      }

      @Override
      protected boolean onAdvance(int phase, int parties) {
        return false;
      }
    }
  }

  public static class Builder {

    private Http2SolrClient http2SolrClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout = Integer.getInteger("solr.http2solrclient.default.idletimeout", 30000);
    private Integer connectionTimeout;
    private Integer maxConnectionsPerHost;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    protected String baseSolrUrl;
    protected Map<String,String> headers = new ConcurrentHashMap<>();

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

    public Builder idleTimeout(int idleConnectionTimeout) {
      this.idleTimeout = idleConnectionTimeout;
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
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
}
