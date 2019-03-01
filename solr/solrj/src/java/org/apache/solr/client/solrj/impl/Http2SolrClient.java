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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
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
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.client.solrj.util.Constants;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.ProtocolHandlers;
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
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.getObjectByPath;

// TODO: error handling, small Http2SolrClient features, security, ssl
/**
 * @lucene.experimental
 */
public class Http2SolrClient extends SolrClient {
  public static final String REQ_PRINCIPAL_KEY = "solr-req-principal";

  private static volatile SSLConfig defaultSSLConfig;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  private static final String UTF_8 = StandardCharsets.UTF_8.name();
  private static final String DEFAULT_PATH = "/select";
  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  private HttpClient httpClient;
  private volatile Set<String> queryParams = Collections.emptySet();
  private int idleTimeout;

  private ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();
  private List<HttpListenerFactory> listenerFactory = new LinkedList<>();
  private AsyncTracker asyncTracker = new AsyncTracker();
  /**
   * The URL of the Solr server.
   */
  private String serverBaseUrl;
  private boolean closeClient;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    if (serverBaseUrl != null)  {
      if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
        serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
      }

      if (serverBaseUrl.startsWith("//")) {
        serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
      }
      this.serverBaseUrl = serverBaseUrl;
    }

    if (builder.idleTimeout != null) idleTimeout = builder.idleTimeout;
    else idleTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;

    if (builder.httpClient == null) {
      httpClient = createHttpClient(builder);
      closeClient = true;
    } else {
      httpClient = builder.httpClient;
    }
    if (!httpClient.isStarted()) {
      try {
        httpClient.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    assert ObjectReleaseTracker.track(this);
  }

  public void addListenerFactory(HttpListenerFactory factory) {
    this.listenerFactory.add(factory);
  }

  HttpClient getHttpClient() {
    return httpClient;
  }

  ProtocolHandlers getProtocolHandlers() {
    return httpClient.getProtocolHandlers();
  }

  private HttpClient createHttpClient(Builder builder) {
    HttpClient httpClient;

    BlockingArrayQueue<Runnable> queue = new BlockingArrayQueue<>(256, 256);
    ThreadPoolExecutor httpClientExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(32,
        256, 60, TimeUnit.SECONDS, queue, new SolrjNamedThreadFactory("h2sc"));

    SslContextFactory sslContextFactory;
    boolean ssl;
    if (builder.sslConfig == null) {
      sslContextFactory = getDefaultSslContextFactory();
      ssl = sslContextFactory.getTrustStore() != null || sslContextFactory.getTrustStorePath() != null;
    } else {
      sslContextFactory = builder.sslConfig.createContextFactory();
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
      httpClient.setMaxConnectionsPerDestination(4);
    }

    httpClient.setExecutor(httpClientExecutor);
    httpClient.setStrictEventOrdering(false);
    httpClient.setConnectBlocking(true);
    httpClient.setFollowRedirects(false);
    httpClient.setMaxRequestsQueuedPerDestination(asyncTracker.getMaxRequestsQueuedPerDestination());
    httpClient.setUserAgentField(new HttpField(HttpHeader.USER_AGENT, AGENT));

    if (builder.idleTimeout != null) httpClient.setIdleTimeout(builder.idleTimeout);
    if (builder.connectionTimeout != null) httpClient.setConnectTimeout(builder.connectionTimeout);
    return httpClient;
  }

  public void close() {
    // we wait for async requests, so far devs don't want to give sugar for this
    asyncTracker.waitForComplete();
    if (closeClient) {
      try {
        ExecutorService executor = (ExecutorService) httpClient.getExecutor();
        httpClient.setStopTimeout(1000);
        httpClient.stop();
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      } catch (Exception e) {
        throw new RuntimeException("Exception on closing client", e);
      }
    }

    assert ObjectReleaseTracker.release(this);
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
        write("</stream>".getBytes(StandardCharsets.UTF_8));
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
        .header("User-Agent", HttpSolrClient.AGENT)
        .header("Content-Type", contentType)
        .content(provider);
    setListeners(updateRequest, postRequest);
    InputStreamResponseListener responseListener = new InputStreamResponseListener();
    postRequest.send(responseListener);

    boolean isXml = ClientUtils.TEXT_XML.equals(requestWriter.getUpdateContentType());
    OutStream outStream = new OutStream(collection, origParams, provider, responseListener,
        isXml);
    if (isXml) {
      outStream.write("<stream>".getBytes(StandardCharsets.UTF_8));
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
              .getBytes(StandardCharsets.UTF_8);
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
          .onComplete(asyncTracker.completeListener).send(new BufferingResponseListener(5 * 1024 * 1024) {

        @Override
        public void onComplete(Result result) {
          if (result.isFailed()) {
            onComplete.onFailure(result.getFailure());
            return;
          }

          NamedList<Object> rsp;
          try {
            InputStream is = getContentAsInputStream();
            assert ObjectReleaseTracker.track(is);
            rsp = processErrorsAndResponse(result.getResponse(),
                parser, is, getEncoding(), isV2ApiRequest(solrRequest));
            onComplete.onSuccess(rsp);
          } catch (Exception e) {
            onComplete.onFailure(e);
          }
        }
      });
      return null;
    } else {
      try {
        InputStreamResponseListener listener = new InputStreamResponseListener();
        req.send(listener);
        Response response = listener.get(idleTimeout, TimeUnit.MILLISECONDS);
        InputStream is = listener.getInputStream();
        assert ObjectReleaseTracker.track(is);
        return processErrorsAndResponse(response, parser, is, getEncoding(response), isV2ApiRequest(solrRequest));
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

  private String getEncoding(Response response) {
    String contentType = response.getHeaders().get(HttpHeader.CONTENT_TYPE);
    if (contentType != null) {
      String charset = "charset=";
      int index = contentType.toLowerCase(Locale.ENGLISH).indexOf(charset);
      if (index > 0) {
        String encoding = contentType.substring(index + charset.length());
        // Sometimes charsets arrive with an ending semicolon.
        int semicolon = encoding.indexOf(';');
        if (semicolon > 0)
          encoding = encoding.substring(0, semicolon).trim();
        // Sometimes charsets are quoted.
        int lastIndex = encoding.length() - 1;
        if (encoding.charAt(0) == '"' && encoding.charAt(lastIndex) == '"')
          encoding = encoding.substring(1, lastIndex).trim();
        return encoding;
      }
    }
    return null;
  }

  private void setBasicAuthHeader(SolrRequest solrRequest, Request req) {
    if (solrRequest.getBasicAuthUser() != null && solrRequest.getBasicAuthPassword() != null) {
      String userPass = solrRequest.getBasicAuthUser() + ":" + solrRequest.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(StandardCharsets.UTF_8));
      req.header("Authorization", "Basic " + encoded);
    }
  }

  private Request makeRequest(SolrRequest solrRequest, String collection)
      throws SolrServerException, IOException {
    Request req = createRequest(solrRequest, collection);
    setListeners(solrRequest, req);
    if (solrRequest.getUserPrincipal() != null) {
      req.attribute(REQ_PRINCIPAL_KEY, solrRequest.getUserPrincipal());
    }

    return req;
  }

  private void setListeners(SolrRequest solrRequest, Request req) {
    setBasicAuthHeader(solrRequest, req);
    for (HttpListenerFactory factory : listenerFactory) {
      HttpListenerFactory.RequestResponseListener listener = factory.get();
      req.onRequestQueued(listener);
      req.onRequestBegin(listener);
      req.onComplete(listener);
    }
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
      req.content(new FormContentProvider(fields, StandardCharsets.UTF_8));
    }

    return req;
  }

  private boolean wantStream(final ResponseParser processor) {
    return processor == null || processor instanceof InputStreamResponseParser;
  }

  private NamedList<Object> processErrorsAndResponse(Response response,
                                                     final ResponseParser processor,
                                                     InputStream is,
                                                     String encoding,
                                                     final boolean isV2Api)
      throws SolrServerException {
    boolean shouldClose = true;
    try {
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

      if (wantStream(parser)) {
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
        rsp.add("stream", is);
        // Only case where stream should not be closed
        shouldClose = false;
        return rsp;
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
        rsp = processor.processResponse(is, encoding);
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
        } catch (Exception ex) {}
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
    private static final int MAX_OUTSTANDING_REQUESTS = 1000;

    // wait for async requests
    private final Phaser phaser;
    // maximum outstanding requests left
    private final Semaphore available;
    private final Request.QueuedListener queuedListener;
    private final Response.CompleteListener completeListener;

    AsyncTracker() {
      // TODO: what about shared instances?
      phaser = new Phaser(1);
      available = new Semaphore(MAX_OUTSTANDING_REQUESTS, false);
      queuedListener = request -> {
        phaser.register();
        try {
          available.acquire();
        } catch (InterruptedException ignored) {

        }
      };
      completeListener = result -> {
        phaser.arriveAndDeregister();
        available.release();
      };
    }

    int getMaxRequestsQueuedPerDestination() {
      // comfortably above max outstanding requests
      return MAX_OUTSTANDING_REQUESTS * 3;
    }

    public void waitForComplete() {
      phaser.arriveAndAwaitAdvance();
      phaser.arriveAndDeregister();
    }
  }

  public static class Builder {

    private HttpClient httpClient;
    private SSLConfig sslConfig = defaultSSLConfig;
    private Integer idleTimeout;
    private Integer connectionTimeout;
    private Integer maxConnectionsPerHost;
    private boolean useHttp1_1 = Boolean.getBoolean("solr.http1");
    protected String baseSolrUrl;

    public Builder() {

    }

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    public Builder withHttpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
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
    if (checkPeerNameStr == null || "false".equalsIgnoreCase(checkPeerNameStr)) {
      sslCheckPeerName = false;
    }

    if (System.getProperty("tests.jettySsl.clientAuth") != null) {
      sslCheckPeerName = sslCheckPeerName || Boolean.getBoolean("tests.jettySsl.clientAuth");
    }

    sslContextFactory.setNeedClientAuth(sslCheckPeerName);
    return sslContextFactory;
  }

}
