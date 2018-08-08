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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import static org.apache.solr.common.util.Utils.getObjectByPath;

// TODO: error handling, small Http2SolrClient features, basic auth, security, ssl, apiV2 ...
/**
 * @lucene.experimental
 */
public class Http2SolrClient extends SolrClient {
  private static volatile SslContextFactory defaultSslContextFactory = getDefaultSslContextFactory();
  private static final int MAX_OUTSTANDING_REQUESTS = 1000;
  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final String UTF_8 = StandardCharsets.UTF_8.name();
  private static final String DEFAULT_PATH = "/select";
  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  private HttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private Phaser phaser = new Phaser(1);
  private final Semaphore available;
  private int idleTimeout;

  private ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();

  private Request.QueuedListener requestQueuedListener = new Request.QueuedListener() {

    @Override
    public void onQueued(Request request) {
      phaser.register();
      try {
        available.acquire();
      } catch (InterruptedException e) {

      }
    }
  };

  private Request.BeginListener beginListener = req -> {

  };

  private Response.CompleteListener requestCompleteListener = new Response.CompleteListener() {

    @Override
    public void onComplete(Result arg0) {
      phaser.arriveAndDeregister();
      available.release();
    }
  };

  /**
   * The URL of the Solr server.
   */
  private String serverBaseUrl;
  private boolean closeClient;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    // TODO: what about shared instances?
    available = new Semaphore(MAX_OUTSTANDING_REQUESTS, false);

    if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
      serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
    }

    if (serverBaseUrl.startsWith("//")) {
      serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
    }

    if (builder.idleTimeout != null) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    this.serverBaseUrl = serverBaseUrl;
    if (builder.httpClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.httpClient;
    }
    if (builder.beginListener != null) this.beginListener = builder.beginListener;
    if (!httpClient.isStarted()) {
      try {
        httpClient.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    assert ObjectReleaseTracker.track(this);
  }

  private HttpClient createHttpClient(Builder builder) {

    HttpClient httpClient;

    QueuedThreadPool httpClientExecutor = new QueuedThreadPool(100, 4);
    httpClientExecutor.setDaemon(true);

    HttpClientTransport transport;
    if (builder.useHttp1_1 || builder.sslContextFactory != null) {
      transport = new HttpClientTransportOverHTTP(2);
      httpClient = new HttpClient(transport, builder.sslContextFactory);
    } else {
      HTTP2Client http2client = new HTTP2Client();
      transport = new HttpClientTransportOverHTTP2(http2client);
      httpClient = new HttpClient(transport, builder.sslContextFactory);
    }
    httpClient.setExecutor(httpClientExecutor);
    httpClient.setStrictEventOrdering(false);
    httpClient.setConnectBlocking(true);
    httpClient.setFollowRedirects(false);
    httpClient.setMaxConnectionsPerDestination(4);
    httpClient.setMaxRequestsQueuedPerDestination(MAX_OUTSTANDING_REQUESTS * 4); // comfortably above max outstanding     // requests
    httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));

    if (builder.idleTimeout != null) httpClient.setIdleTimeout(builder.idleTimeout);
    if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
    return httpClient;
  }


  public HttpClient getHttpClient() {
    return httpClient;
  }

  public void close() {
    // we wait for async requests, so far devs don't want to give sugar for this
    phaser.arriveAndAwaitAdvance();
    phaser.arriveAndDeregister();
    if (closeClient) {
      close(httpClient);
    }

    assert ObjectReleaseTracker.release(this);
  }

  public static void close(HttpClient httpClient) {
    try {
      // TODO: stop time?
      httpClient.setStopTimeout(1000);
      httpClient.stop();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Http2ClientResponse request(SolrRequest solrRequest, String collection, OnComplete onComplete)
      throws SolrServerException, IOException {
    return request(solrRequest, collection, onComplete, false);
  }

  private boolean isV2ApiRequest(final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  private Http2ClientResponse request(SolrRequest solrRequest,
                                      String collection,
                                      OnComplete onComplete,
                                      boolean returnStream) throws IOException, SolrServerException {
    Request req = makeRequest(solrRequest, collection);
    try {
      if (onComplete != null) {
        req.onRequestBegin(beginListener).onRequestQueued(requestQueuedListener)
            .onComplete(requestCompleteListener).send(new BufferingResponseListener() {

          @Override
          public void onComplete(Result result) {
            if (result.isFailed()) {
              onComplete.onFailure(result.getFailure());
              return;
            }

            // TODO: should we stream this?
            try (InputStream ris = getContentAsInputStream()) {
              NamedList<Object> rsp;
              try {
                rsp = processErrorsAndResponse(result.getResponse(),
                    parser, ris, getEncoding(), isV2ApiRequest(solrRequest));
                onComplete.onSuccess(rsp);
              } catch (Exception e) {
                onComplete.onFailure(e);
              }
            } catch (IOException e1) {
              onComplete.onFailure(e1);
            }
          }
        });
        return null;
      } else {
        Http2ClientResponse arsp = new Http2ClientResponse();
        if (returnStream) {
          InputStreamResponseListener listener = new InputStreamResponseListener();
          req.onRequestBegin(beginListener).send(listener);
          // Wait for the response headers to arrive
          listener.get(idleTimeout, TimeUnit.SECONDS);
          // TODO: process response
          arsp.stream = listener.getInputStream();
        } else {
          ContentResponse response = req.onRequestBegin(beginListener).send();
          ByteArrayInputStream is = new ByteArrayInputStream(response.getContent());
          arsp.response = processErrorsAndResponse(response, parser,
              is, response.getEncoding(), isV2ApiRequest(solrRequest));
        }
        return arsp;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new SolrServerException(
          "Timeout occured while waiting response from server at: "
              + getBaseURL(), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof ConnectException) {
        throw new SolrServerException("Server refused connection at: "
            + getBaseURL(), cause);
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

  private Request makeRequest(SolrRequest solrRequest, String collection)
      throws SolrServerException, IOException {
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
        basePath = serverBaseUrl.replace("/solr", "/api");
      } else {
        basePath = serverBaseUrl + "/____v2";
      }
    }

    if (SolrRequest.METHOD.GET == solrRequest.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      return httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.GET);
    }

    if (SolrRequest.METHOD.DELETE == solrRequest.getMethod()) {
      return httpClient.newRequest(basePath + path + wparams.toQueryString()).method(HttpMethod.DELETE);
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
        return fillContentStream(req, streams, wparams, isMultipart);
      } else {
        // It is has one stream, it is the post body, put the params in the URL
        ContentStream contentStream = streams.iterator().next();
        return httpClient
            .newRequest(url + wparams.toQueryString())
            .method(method)
            .content(new InputStreamContentProvider(contentStream.getStream()), contentStream.getContentType());
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
          String name = contentStream.getName();
          if (name == null) {
            name = "";
          }
          content.addFieldPart(name, new InputStreamContentProvider(contentStream.getStream()), null);
        }
      }
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
      req.content(new FormContentProvider(fields, StandardCharsets.UTF_8));
    }

    return req;
  }

  private NamedList<Object> processErrorsAndResponse(Response response,
                                                     final ResponseParser processor,
                                                     InputStream is,
                                                     String encoding,
                                                     final boolean isV2Api)
      throws SolrServerException {
    // handle some http level checks before trying to parse the response
    int httpStatus = response.getStatus();

    String contentType;
    contentType = response.getHeaders().get("content-type");
    if (contentType == null) contentType = "";

    switch (httpStatus) {
      case HttpStatus.SC_OK:
      case HttpStatus.SC_BAD_REQUEST:
      case HttpStatus.SC_CONFLICT: // 409
        break;
      case HttpStatus.SC_MOVED_PERMANENTLY:
      case HttpStatus.SC_MOVED_TEMPORARILY:
        if (!httpClient.isFollowRedirects()) {
          throw new SolrServerException("Server at " + getBaseURL()
              + " sent back a redirect (" + httpStatus + ").");
        }
        break;
      default:
        if (processor == null || "".equals(contentType)) {
          throw new RemoteSolrException(serverBaseUrl, httpStatus, "non ok status: " + httpStatus
              + ", message:" + response.getReason(),
              null);
        }
    }

    String procCt = processor.getContentType();
    if (procCt != null) {
      String procMimeType = ContentType.parse(procCt).getMimeType().trim().toLowerCase(Locale.ROOT);
      String mimeType = ContentType.parse(contentType).getMimeType().trim().toLowerCase(Locale.ROOT);
      if (!procMimeType.equals(mimeType)) {
        // unexpected mime type
        String msg = "Expected mime type " + procMimeType + " but got " + mimeType + ".";
        try {
          msg = msg + " " + IOUtils.toString(is, encoding);
        } catch (IOException e) {
          throw new RemoteSolrException(serverBaseUrl, httpStatus, "Could not parse response with encoding " + encoding, e);
        }
        throw new RemoteSolrException(serverBaseUrl, httpStatus, msg, null);
      }
    }

    NamedList<Object> rsp;
    try {
      rsp = parser.processResponse(is, encoding);
    } catch (Exception e) {
      throw new RemoteSolrException(serverBaseUrl, httpStatus, e.getMessage(), e);
    }

    Object error = rsp == null ? null : rsp.get("error");
    if (error != null && (String.valueOf(getObjectByPath(error, true, errPath)).endsWith("ExceptionWithErrObject"))) {
      throw RemoteExecutionException.create(serverBaseUrl, rsp);
    }
    if (httpStatus != HttpStatus.SC_OK && !isV2Api) {
      NamedList<String> metadata = null;
      String reason = null;
      try {
        NamedList err = (NamedList) rsp.get("error");
        if (err != null) {
          reason = (String) err.get("msg");
          if (reason == null) {
            reason = (String) err.get("trace");
          }
          metadata = (NamedList<String>) err.get("metadata");
        }
      } catch (Exception ex) {
      }
      if (reason == null) {
        StringBuilder msg = new StringBuilder();
        msg.append(response.getReason())
            .append("\n\n")
            .append("request: ")
            .append(response.getRequest().getMethod());
        try {
          reason = java.net.URLDecoder.decode(msg.toString(), UTF_8);
        } catch (UnsupportedEncodingException e) {
        }
      }
      RemoteSolrException rss = new RemoteSolrException(serverBaseUrl, httpStatus, reason, null);
      if (metadata != null) rss.setMetadata(metadata);
      throw rss;
    }
    return rsp;
  }

  @Override
  public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
    return request(request, collection, null).response;
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  // sample new async method
  public void add(String collection, SolrInputDocument doc, int commitWithinMs, OnComplete<UpdateResponse> onComplete)
      throws SolrServerException, IOException {

    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    request(req, collection, onComplete);
  }

  public String get(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.GET(url);
    return response.getContentAsString();
  }

  // sample new async method
  public void query(String collection, SolrParams params, OnComplete<QueryResponse> onComplete)
      throws SolrServerException, IOException {
    QueryRequest queryRequest = new QueryRequest(params);
    request(queryRequest, collection, onComplete);
  }

  public InputStream queryAndStreamResponse(String collection, SolrParams params)
      throws SolrServerException, IOException {
    QueryRequest queryRequest = new QueryRequest(params);
    Http2ClientResponse resp = request(queryRequest, collection, null, true);
    assert resp.stream != null;
    return resp.stream;
  }

  public void commit(String collection, boolean softCommit, boolean waitSearcher, OnComplete<UpdateResponse> onComplete)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(UpdateParams.COMMIT, "true");
    params.set(UpdateParams.SOFT_COMMIT, String.valueOf(softCommit));

    params.set(UpdateParams.WAIT_SEARCHER, String.valueOf(waitSearcher));
    req.setParams(params);

    request(req, collection, onComplete);
  }

  public interface OnComplete<T> {
    void onSuccess(T result);

    void onFailure(Throwable e);
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  public static class Builder {

    private HttpClient httpClient;
    private SslContextFactory sslContextFactory;
    private Integer idleTimeout;
    private Integer connectionTimeout;
    private boolean useHttp1_1 = false;
    protected String baseSolrUrl;
    private Request.BeginListener beginListener = request -> {};

    public Builder() {
      this.sslContextFactory = defaultSslContextFactory;
    }

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
      this.sslContextFactory = defaultSslContextFactory;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    public Builder withHttpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public Builder withSslContextFactory(SslContextFactory factory) {
      this.sslContextFactory = factory;
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

    public Builder withListener(Request.BeginListener beginListener) {
      this.beginListener = beginListener;
      return this;
    }
  }

  /**
   * Subclass of SolrException that allows us to capture an arbitrary HTTP status code that may have been returned by
   * the remote server or a proxy along the way.
   */
  public static class RemoteSolrException extends SolrException {
    /**
     * @param remoteHost the host the error was received from
     * @param code       Arbitrary HTTP status code
     * @param msg        Exception Message
     * @param th         Throwable to wrap with this Exception
     */
    public RemoteSolrException(String remoteHost, int code, String msg, Throwable th) {
      super(code, "Error from server at " + remoteHost + ": " + msg, th);
    }
  }

  /**
   * This should be thrown when a server has an error in executing the request and it sends a proper payload back to the
   * client
   */
  public static class RemoteExecutionException extends RemoteSolrException {
    private NamedList meta;

    public RemoteExecutionException(String remoteHost, int code, String msg, NamedList meta) {
      super(remoteHost, code, msg, null);
      this.meta = meta;
    }

    public static RemoteExecutionException create(String host, NamedList errResponse) {
      Object errObj = errResponse.get("error");
      if (errObj != null) {
        Number code = (Number) getObjectByPath(errObj, true, Collections.singletonList("code"));
        String msg = (String) getObjectByPath(errObj, true, Collections.singletonList("msg"));
        return new RemoteExecutionException(host, code == null ? ErrorCode.UNKNOWN.code : code.intValue(),
            msg == null ? "Unknown Error" : msg, errResponse);

      } else {
        throw new RuntimeException("No error");
      }
    }

    public NamedList getMetaData() {
      return meta;
    }
  }

  protected static class Http2ClientResponse {
    NamedList response;
    InputStream stream;
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

  // public for testing, only used by tests
  public static void setSslContextFactory(SslContextFactory sslContextFactory) {
    Http2SolrClient.defaultSslContextFactory = sslContextFactory;
  }

  // public for testing, only used by tests
  public static void resetSslContextFactory() {
    Http2SolrClient.defaultSslContextFactory = getDefaultSslContextFactory();
  }

  private static SslContextFactory getDefaultSslContextFactory() {
    SslContextFactory sslContextFactory = new SslContextFactory(false);

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

    String checkPeerNameStr = System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
    boolean sslCheckPeerName = true;
    if (checkPeerNameStr == null && "false".equalsIgnoreCase(checkPeerNameStr)) {
      sslCheckPeerName = false;
    }

    if (System.getProperty("tests.jettySsl.clientAuth") != null) {
      sslCheckPeerName = sslCheckPeerName || Boolean.getBoolean("tests.jettySsl.clientAuth");
    }

    sslContextFactory.setNeedClientAuth(sslCheckPeerName);
    return sslContextFactory;
  }

}
