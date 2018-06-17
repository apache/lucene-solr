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

import static org.apache.solr.common.util.Utils.getObjectByPath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.SolrInternalHttpClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.AuthenticationStore;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Response.CompleteListener;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.MultiPartContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: finish error handling, multiple streams, small HttpSolrClient features, basic auth, security, ssl, apiV2 ...

/**
 * @lucene.experimental
 */
public class Http2SolrClient extends SolrClient {

  private static final String AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String UTF_8 = StandardCharsets.UTF_8.name();
  private static final String POST = "POST";
  private static final String PUT = "PUT";
  private static final String GET = "GET";
  private static final String DELETE = "DELETE";
  private static final String HEAD = "HEAD";
  
  private static final String DEFAULT_PATH = "/select";

  private static final List<String> errPath = Arrays.asList("metadata", "error-class");

  private final SolrInternalHttpClient httpClient;

  private volatile Set<String> queryParams = Collections.emptySet();

  private final Phaser phaser = new Phaser(1) {
    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  };
 // private final Semaphore available = new Semaphore(SolrInternalHttpClient.MAX_OUTSTANDING_REQUESTS, false); // nocommit: what about shared
                                                                                      // instances?

  private volatile ResponseParser parser = new BinaryResponseParser();
  private volatile RequestWriter requestWriter = new BinaryRequestWriter();

  private Request.QueuedListener requestQueuedListener = new Request.QueuedListener() {

    @Override
    public void onQueued(Request request) {
      
      phaser.register();
      
  //    try {
  //      available.acquire();
  //    } catch (InterruptedException e) {

   //   }

    }
  };
  
  private CompleteListener completeListener = new CompleteListener() {
    
    @Override
    public void onComplete(Result arg0) {
    //  phaser.arriveAndDeregister();
    }
  };

  private Request.BeginListener requestBeginListener = new Request.BeginListener() {

    @Override
    public void onBegin(Request arg0) {
     
    }
  };

  /**
   * The URL of the Solr server.
   */
  private volatile String serverBaseUrl;

  private boolean closeClient;

  private Builder builder;

  private volatile String closed = null;

  protected Http2SolrClient(String serverBaseUrl, Builder builder) {
    this.builder = builder;

    if (!serverBaseUrl.equals("/") && serverBaseUrl.endsWith("/")) {
      serverBaseUrl = serverBaseUrl.substring(0, serverBaseUrl.length() - 1);
    }

    if (serverBaseUrl.startsWith("//")) {
      serverBaseUrl = serverBaseUrl.substring(1, serverBaseUrl.length());
    }

    this.serverBaseUrl = serverBaseUrl;

    if (builder.responseParser != null) {
      this.parser = builder.responseParser;
    }

    if (builder.httpClient == null) {
      httpClient = new SolrInternalHttpClient(getClass().getSimpleName() + "-internal");
      closeClient = true;
    } else {
      httpClient = builder.httpClient;
    }
    if (!httpClient.isStarted()) {
      // Start HttpClient
      try {
        httpClient.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    assert ObjectReleaseTracker.track(this);
  }

  // public static HttpClientTransport createClientTransport() {
  // HTTP2Client transport = new HTTP2Client();
  // // TODO
  // transport.setSelectors(2);
  // HttpClientTransportOverHTTP2 clientTransport = new HttpClientTransportOverHTTP2(transport);
  // return clientTransport;
  // }

  public void close() throws IOException {
    // TODO: cleanup
    
    
    if (this.closed != null) {
      throw new IllegalStateException("Already closed! " + this.closed);
    }

    waitForAsyncRequests(false);
    
    phaser.arriveAndDeregister();
    
    if (closeClient) {
      IOUtils.closeQuietly(httpClient);
    }

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    new RuntimeException("Already closed at: ").printStackTrace(pw);
    this.closed = sw.toString();
    
    assert ObjectReleaseTracker.release(this);
  }
  
  public SolrInternalHttpClient getHttpClient() {
    return httpClient;
  }
  
  public void waitForAsyncRequests() {
    waitForAsyncRequests(true);
  }

  public void waitForAsyncRequests(boolean checkClosed) {
    if (checkClosed && this.closed != null) {
      throw new IllegalStateException("Already closed! " + this.closed);
    }

    // we wait for async requests, so far devs don't want to give sugar for this
    logPhaser("waitForAsyncRequests-start");
    try {

      phaser.arriveAndAwaitAdvance();

      // phaser.awaitAdvance(phaser.arriveAndDeregister());
      // phaser.register();
    } catch (IllegalStateException e) {
      // nocommit
      // work around something I don't understand - 1 party, 1 arrived, and then we arrive again - why isn't it a new
      // phase?
    }
    logPhaser("waitForAsyncRequests-end");
  }

  private void logPhaser(String location) {
    if (log.isDebugEnabled()) {
      log.debug(" ------------ phaser -> {} {} t:{} i:{}", location, phaser, Thread.currentThread().getId(), this);
    }
  }

  private boolean isV2ApiRequest(final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  public Http2ClientResponse request(SolrRequest request, String collection, OnComplete onComplete)
      throws SolrServerException, IOException {
    return request(request, collection, onComplete, false);
  }

  private Http2ClientResponse request(SolrRequest request, String collection, OnComplete onComplete,
      boolean returnStream)
      throws SolrServerException, IOException {
    long startNanos = System.nanoTime();

    ResponseParser parser = request.getResponseParser();

    if (request instanceof V2RequestSupport) {
      request = ((V2RequestSupport) request).getV2Request();
      if (parser != null) {
        request.setResponseParser(parser);
      }
    }

    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(request);

    Collection<ContentStream> streams = contentWriter == null ? requestWriter.getContentStreams(request) : null;

    boolean isV2Api = isV2ApiRequest(request);

    SolrParams params = request.getParams();

    if (parser == null) {
      parser = this.parser;
    }

    ModifiableSolrParams wparams = new ModifiableSolrParams(params);
    if (parser != null) {
      if (wparams.get(CommonParams.WT) == null) {
        wparams.set(CommonParams.WT, parser.getWriterType());
      }
      wparams.set(CommonParams.VERSION, parser.getVersion());
    }

    String path = requestWriter.getPath(request);
    if (path == null || !path.startsWith("/")) {
      path = DEFAULT_PATH;
    }

    String basePath;
    String requestBasePath = request.getBasePath();
    if (requestBasePath != null) {
      basePath = requestBasePath;
    } else {
      basePath = serverBaseUrl;
    }

    if (collection != null)
      basePath += "/" + collection;

    if (request instanceof V2Request) {
      if (System.getProperty("solr.v2RealPath") == null) {
        basePath = serverBaseUrl.replace("/solr", "/api");
      } else {
        basePath = serverBaseUrl + "/____v2";
      }
    }

    try {
      if (SolrRequest.METHOD.GET == request.getMethod() || SolrRequest.METHOD.DELETE == request.getMethod()) {
        if (streams != null || contentWriter != null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET and DELETE can't send streams!");
        }
        return getOrDelete(request, onComplete, startNanos, wparams, parser, path, basePath, returnStream, isV2Api);
      } else if (SolrRequest.METHOD.POST == request.getMethod()) {

        if (contentWriter != null) {
          // "Content-Type", contentWriter.getContentType() request.
          String url = basePath + path;
          String fullQueryUrl = url + wparams.toQueryString();
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          contentWriter.write(baos);
          Request req = newRequest(fullQueryUrl);
          req.header("Content-Type", contentWriter.getContentType());
          return postWithContent(request, req, onComplete, startNanos, fullQueryUrl, baos, parser, isV2Api);
        }

        String url = basePath + path;
        // Collection<ContentStream> streams = requestWriter.getContentStreams(request);
        // String url = basePath + path;
        // boolean hasNullStreamName = false;
        // if (streams != null) {
        // for (ContentStream cs : streams) {
        // if (cs.getName() == null) {
        // hasNullStreamName = true;
        // break;
        // }
        // }
        // }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        requestWriter.write(request, baos);

        if (baos.size() == 0) {
          // System.out.println("query params:" + request.getQueryParams());
          // System.out.println("reg params:" + request.getParams());
          // send server list and request list as query string params
          ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
          queryParams.add(calculateQueryParams(request.getQueryParams(), wparams));
          String fullQueryUrl = url + queryParams.toQueryString();
          // System.out.println("POST WITH NO CONTENT:" + fullQueryUrl + " wparams:" + wparams);
          // return postWithNoContentStream(request, onComplete, startNanos, wparams, fullQueryUrl, isV2Api);
          Request req = newRequest(fullQueryUrl);
          for (Entry<String,String[]> param : wparams) {
            String key = param.getKey();
            for (String value : param.getValue()) {
              req.param(key, value);
            }
          }

          return postWithContent(request, req, onComplete, startNanos, fullQueryUrl, baos, parser, isV2Api);
        }

        String fullQueryUrl = url + wparams.toQueryString();
        Request req = newRequest(fullQueryUrl);

        return postWithContent(request, req, onComplete, startNanos, fullQueryUrl, baos, parser, isV2Api);
      }

    } catch (InterruptedException e) { // TODO: finish error handling
      throw new RuntimeException(e);
    } catch (TimeoutException e) { // TODO: finish error handling
      throw new SolrServerException(e.getMessage(), e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // cause.printStackTrace();
      // e.printStackTrace();
      if (cause instanceof ConnectException) {
        throw new SolrServerException("Server refused connection at: "
            + getBaseURL(), cause);
      }
      if (cause instanceof SolrServerException) {
        throw (SolrServerException) cause;
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new SolrServerException(cause.getMessage(), cause);
    }

    throw new UnsupportedOperationException(request.getMethod().toString());
  }

  private void addBasicAuth(String url, SolrRequest request) throws UnsupportedEncodingException {
    if (request.getBasicAuthUser() != null && request.getBasicAuthPassword() != null) {
      AuthenticationStore a = httpClient.getAuthenticationStore();
      a.addAuthentication(
          new BasicAuthentication(URI.create(url), Authentication.ANY_REALM, request.getBasicAuthUser(),
              request.getBasicAuthPassword()));
      // nocommit method.setHeader(new BasicHeader("Authorization", "Basic " + encoded));
    }
  }

  private Request newRequest(String url) throws SolrServerException {
    Request req;
    try {
      req = httpClient.newRequest(url);
      for (Entry<String,String> entry : builder.headers.entrySet()) {
        req.header(entry.getKey(), entry.getValue());
      }
    } catch (IllegalArgumentException e) {
      throw new SolrServerException("Error parsing URL: " + url, e);
    }
    return req;
  }

  private Http2ClientResponse getOrDelete(SolrRequest request, OnComplete<SolrResponse> onComplete, long startNanos,
      ModifiableSolrParams wparams, ResponseParser parser, String path, String basePath, boolean returnStream,
      boolean isV2Api)
      throws InterruptedException, TimeoutException, ExecutionException, SolrServerException {

    String url = basePath + path + wparams.toQueryString();
    Request req = newRequest(url);
    req.header("User-Agent", AGENT);
    req.idleTimeout(builder.idleConnectionTimeout, TimeUnit.MILLISECONDS);

    if (SolrRequest.METHOD.GET == request.getMethod()) {
      req.method(GET);
    } else if (SolrRequest.METHOD.DELETE == request.getMethod()) {
      req.method(DELETE);
    }

    if (onComplete == null) {
      if (!returnStream) {

        ContentResponse response = req.send();
   
        ByteArrayInputStream is = new ByteArrayInputStream(response.getContent());
        NamedList<Object> rsp = processErrorsAndResponse(url, response, is, response.getEncoding(), parser, isV2Api);
        Http2ClientResponse arsp = new Http2ClientResponse();
        arsp.response = rsp;
        return arsp;
      } else {
        InputStreamResponseListener listener = new InputStreamResponseListener();
        req.send(listener);
        // Wait for the response headers to arrive
        Response response = listener.get(5, TimeUnit.SECONDS);
        // nocommit : process response
        Http2ClientResponse arsp = new Http2ClientResponse();

        arsp.stream = listener.getInputStream();
        return arsp;
      }
    } else {
   
      CountDownLatch latch = new CountDownLatch(1);

      Http2ClientResponse arsp = new Http2ClientResponse();
   
      arsp.abortable = new Abortable() {
        volatile SolrResponse rsp = null;

        @Override
        public void abort() {
          req.abort(new RuntimeException("Aborted"));
        }

        @Override
        public SolrResponse get() {
          try {
            latch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return rsp;
        }

        @Override
        void setResponse(SolrResponse response) {
          this.rsp = response;
        }
      };

      BufferingResponseListener listener = new BufferingResponseListener() {

        @Override
        public void onComplete(Result result) {
          // TODO: should we stream this?
          try (InputStream ris = getContentAsInputStream()) {
            NamedList<Object> rsp;
            try {
              rsp = processErrorsAndResponse(url, result.getResponse(), ris, getEncoding(), parser, isV2Api);
            } catch (Exception e) {
              onComplete.onFailure(e);
              return;
            }
            SolrResponse res = makeResponse(request, rsp, result.getResponse().getStatus());
            arsp.abortable.setResponse(res);
            onComplete.onSuccess(res);
          } catch (IOException e1) {
            // TODO: handle right
            throw new RuntimeException(e1);
          } finally {
            phaser.arriveAndDeregister();
            latch.countDown();
          }
        }

      };
  
      req.onRequestBegin(requestBeginListener).onRequestQueued(requestQueuedListener).onComplete(completeListener).send(listener);

      return arsp;
    }

  }

  private NamedList<Object> processErrorsAndResponse(String url, Response response, InputStream is, String encoding,
      ResponseParser parser, boolean isV2Api)
      throws SolrServerException {
    // System.out.println("response headers:" + response.getHeaders());
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
          throw new SolrServerException("Server at " + getBaseURL()
              + " sent back a redirect (" + httpStatus + ").");
        }
        break;
      default:
        if (encoding == null) {
          encoding = parser.getContentType();
        }
        String msg = null;
        if (!encoding.equalsIgnoreCase(parser.getContentType())) {
          try {
            is.reset();
            msg = IOUtils.toString(is, encoding);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        if (encoding == null || encoding.length() == 0) {
          try {
            is.reset();
            msg = IOUtils.toString(is, "UTF-8");

          } catch (IOException e1) {
            throw new RuntimeException(e1);
          }
        }
        
        if (msg != null) {
          throw new RemoteSolrException(serverBaseUrl, httpStatus, "non ok status: " + httpStatus
              + ", message:" + response.getReason() + " " + msg,
              null);
        }
    }
    NamedList<Object> rsp;
    try {
      rsp = parser.processResponse(is, encoding);
    } catch (Exception e) {
      try {
        // nocommit
        e.printStackTrace();
        is.reset();
        String msg = IOUtils.toString(is);
        System.out.println("fail:" + msg + " " + response.getReason() + " " + httpStatus);
      } catch (IOException e1) {
        throw new RuntimeException(e1);
      }
      throw new RemoteSolrException(serverBaseUrl, httpStatus, "Parser: "+ parser.getClass().getSimpleName() + " Parse error: " + e.getMessage(), e);
    }

    Object error = rsp == null ? null : rsp.get("error");
    if (error != null
        && (isV2Api || String.valueOf(getObjectByPath(error, true, errPath)).endsWith("ExceptionWithErrObject"))) {
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
            .append(url);
        try {
          reason = java.net.URLDecoder.decode(msg.toString(), UTF_8);
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException(e);
        }
      }
      RemoteSolrException rss = new RemoteSolrException(serverBaseUrl, httpStatus, reason, null);
      if (metadata != null) rss.setMetadata(metadata);
      throw rss;
    }

    return rsp;
  }

  private Http2ClientResponse postWithContent(SolrRequest request, Request req, OnComplete<SolrResponse> onComplete,
      long startNanos,
      String fullUrl, ByteArrayOutputStream baos, ResponseParser parser, boolean isV2Api)
      throws InterruptedException, TimeoutException, ExecutionException, SolrServerException {
    BytesContentProvider contentProvider = new BytesContentProvider(baos.toByteArray());

    req.method(POST).agent(AGENT);
    if (baos.size() > 0) {
      req.content(contentProvider, requestWriter.getUpdateContentType());
    } else {
      req.header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
    }
    req.idleTimeout(builder.idleConnectionTimeout, TimeUnit.MILLISECONDS);
    if (onComplete == null) {
      ContentResponse response = req.send();
      ByteArrayInputStream is = new ByteArrayInputStream(response.getContent());
      NamedList<Object> rsp = processErrorsAndResponse(fullUrl, response, is, response.getEncoding(), parser, isV2Api);
      Http2ClientResponse arsp = new Http2ClientResponse();
      arsp.response = rsp;
      return arsp;
    } else {

      CountDownLatch latch = new CountDownLatch(1);

      Http2ClientResponse arsp = new Http2ClientResponse();
 
      arsp.abortable = new Abortable() {
        volatile SolrResponse rsp = null;

        @Override
        public void abort() {
          boolean abort = req.abort(new RuntimeException("Aborted"));
        }

        @Override
        public SolrResponse get() {
          try {
            latch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return rsp;
        }

        @Override
        void setResponse(SolrResponse response) {
          this.rsp = response;
        }
      };
      req.onRequestBegin(requestBeginListener).onRequestQueued(requestQueuedListener)
      .onComplete(completeListener).send(new BufferingResponseListener() {

            @Override
            public void onComplete(Result result) {
              NamedList<Object> rsp;
              SolrResponse res = null;
              try {
                if (result.isFailed()) {
                  onComplete.onFailure(result.getFailure());
                  return;
                }

                try (InputStream ris = getContentAsInputStream()) {
                  try {
                    rsp = processErrorsAndResponse(fullUrl, result.getResponse(), ris, getEncoding(), parser, isV2Api);
                    res = makeResponse(request, rsp, result.getResponse().getStatus());
                    arsp.abortable.setResponse(res);
                  } catch (Exception e) {
                    onComplete.onFailure(e);
                    return;
                  }
                  onComplete.onSuccess(res);

                } catch (IOException e1) {
                  log.warn("Could not close InputStream.", e1);
                }
              } finally {
                latch.countDown();
                phaser.arriveAndDeregister();
              }
            }

          });
      return arsp;
    }
  }

  private Http2ClientResponse postWithMultipleContentStreams(SolrRequest request, OnComplete<SolrResponse> onComplete,
      long startNanos,
      ModifiableSolrParams wparams, String url, boolean isV2Api)
      throws InterruptedException, TimeoutException, ExecutionException, SolrServerException {

    Request req = newRequest(url);
    req.agent(AGENT).method(POST);
    req.idleTimeout(builder.idleConnectionTimeout, TimeUnit.MILLISECONDS);
    Iterator<String> iter = wparams.getParameterNamesIterator();

    // encode params in request content
    // TODO: support leaving some params as query params like HttpSolrClient
    // nocommit SolrRequestParsers puts this in a tmp file!!
    MultiPartContentProvider multiPart = new MultiPartContentProvider();
    try {
      while (iter.hasNext()) {
        String p = iter.next();
        String[] vals = wparams.getParams(p);
        if (vals != null) {
          for (String v : vals) {
            // nocommit System.out.println("Add multi-part: " + p + " : " + v);
            multiPart.addFieldPart(p, new StringContentProvider(v), null);
          }
        }
      }
      if (multiPart.getLength() > 1) {
        req.header("Content-Type", "multipart/form-data");
      }
    } finally {
      multiPart.close();
    }

    req = req.content(multiPart);
    if (onComplete == null) {
      ContentResponse response;
      response = req.send();

      NamedList<Object> rsp = processErrorsAndResponse(serverBaseUrl, response,
          new ByteArrayInputStream(response.getContent()),
          response.getEncoding(), parser, isV2Api);
      Http2ClientResponse arsp = new Http2ClientResponse();
      arsp.response = rsp;
      return arsp;

    } else {
      // TODO: there is like a 2mb buffer limit?
      req.onRequestBegin(requestBeginListener).onRequestQueued(requestQueuedListener)
      .onComplete(completeListener).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
              try {
                if (result.isFailed()) { // nocommit how to handle right
                  onComplete.onFailure(result.getFailure());
                  return;
                }

                try (InputStream ris = getContentAsInputStream()) {
                  NamedList<Object> rsp;
                  try {
                    rsp = processErrorsAndResponse(serverBaseUrl, result.getResponse(), ris, getEncoding(), parser,
                        isV2Api);
                  } catch (Exception e) {
                    onComplete.onFailure(e);
                    return;
                  }

                  SolrResponse resp = makeResponse(request, rsp, result.getResponse().getStatus());
                  onComplete.onSuccess(resp);
                } catch (IOException e1) {
                  // TODO: handle right
                  throw new RuntimeException(e1);
                }
              } finally {
                phaser.arriveAndDeregister();
             //   available.release();
              }
            }
          });
      return null;
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
    return request(request, collection, null).response;
  }

  // sample new async method
  public void add(String collection, SolrInputDocument doc, int commitWithinMs, OnComplete<UpdateResponse> onComplete)
      throws SolrServerException, IOException {

    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    request(req, collection, onComplete);
  }

  public static int HEAD(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response;
    try (SolrInternalHttpClient httpClient = new SolrInternalHttpClient(Http2SolrClient.class.getSimpleName() + "-HEAD-internal", true)) {
      Request req = httpClient.newRequest(URI.create(url));
      response = req.method(HEAD).send();
      if (response.getStatus() != 200) {
        throw new RemoteSolrException(url, response.getStatus(), response.getReason(), null);
      }
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

  // inefficient
  public static SimpleResponse GET(String url) throws InterruptedException, ExecutionException, TimeoutException {
    try (SolrInternalHttpClient httpClient = new SolrInternalHttpClient(Http2SolrClient.class.getSimpleName() + "-GET-internal", true)) {
      SimpleResponse resp = GET(url, httpClient);
      return resp;
    }
  }

  public static SimpleResponse GET(String url, HttpClient httpClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient);
  }

  public static SimpleResponse POST(String url, HttpClient httpClient, byte[] bytes, String contentType)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, Collections.emptyMap());
  }

  public SimpleResponse httpGet(String url) throws InterruptedException, ExecutionException, TimeoutException {
    return doGet(url, httpClient);
  }

  private static SimpleResponse doGet(String url, HttpClient httpClient)
      throws InterruptedException, ExecutionException, TimeoutException {
    // nocommit
    assert url != null;
    ContentResponse response = httpClient.GET(url);
    SimpleResponse sResponse = new SimpleResponse();
    sResponse.asString = response.getContentAsString();
    sResponse.contentType = response.getEncoding();
    sResponse.size = response.getContent().length;
    sResponse.bytes = response.getContent();
    sResponse.status = response.getStatus();
    return sResponse;
  }

  public String httpDelete(String url) throws InterruptedException, ExecutionException, TimeoutException {
    ContentResponse response = httpClient.newRequest(URI.create(url)).method(DELETE).send();
    return response.getContentAsString();
  }

  public SimpleResponse httpPost(String url, byte[] bytes, String contentType)
      throws InterruptedException, ExecutionException, TimeoutException {
    return httpPost(url, bytes, contentType, Collections.emptyMap());
  }

  public SimpleResponse httpPost(String url, byte[] bytes, String contentType, Map<String,String> headers)
      throws InterruptedException, ExecutionException, TimeoutException {
    return doPost(url, httpClient, bytes, contentType, headers);
  }

  private static SimpleResponse doPost(String url, HttpClient httpClient, byte[] bytes, String contentType,
      Map<String,String> headers) throws InterruptedException, ExecutionException, TimeoutException {
    Request req = httpClient.newRequest(url).method(POST).content(new BytesContentProvider(contentType, bytes));
    for (Entry<String,String> entry : headers.entrySet()) {
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

  public String httpPut(String url, byte[] bytes, String contentType)
      throws InterruptedException, ExecutionException, TimeoutException, SolrServerException {
    ContentResponse response = newRequest(url).method(PUT).content(new BytesContentProvider(bytes), contentType).send();
    return response.getContentAsString();
  }

  // sample new async method
  public void query(String collection, SolrParams params, OnComplete<QueryResponse> onComplete)
      throws SolrServerException, IOException {
    QueryRequest queryRequest = new QueryRequest(params);
    request(queryRequest, collection, onComplete);
  }

  public abstract class Abortable {
    public abstract void abort();

    public abstract SolrResponse get();

    abstract void setResponse(SolrResponse response);
  }

  public Abortable abortableRequest(SolrRequest request, OnComplete<SolrResponseBase> onComplete)
      throws SolrServerException, IOException {
    assert onComplete != null;
    Http2ClientResponse rsp = request(request, null, onComplete);
    return rsp.abortable;
  }

  public InputStream queryAndStreamResponse(String collection, SolrParams params)
      throws SolrServerException, IOException {
    QueryRequest queryRequest = new QueryRequest(params);
    Http2ClientResponse resp = request(queryRequest, collection, null, true);
    assert resp.stream != null;
    return resp.stream;
  }

  public void commit(String collection, boolean softCommit, boolean waitSearcher, OnComplete<SolrResponse> onComplete)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(UpdateParams.COMMIT, "true");
    params.set(UpdateParams.SOFT_COMMIT, String.valueOf(softCommit));

    params.set(UpdateParams.WAIT_SEARCHER, String.valueOf(waitSearcher));
    req.setParams(params);

    request(req, collection, onComplete);
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  private SolrResponse makeResponse(SolrRequest request, NamedList<Object> rsp, int status) {
    long startNanos = System.nanoTime();
    SolrResponse res = request.createResponse(Http2SolrClient.this);
    if (rsp.get("responseHeader") != null) {
      int index = ((NamedList) rsp.get("responseHeader")).indexOf("status", 0);
      ((NamedList) rsp.get("responseHeader")).setVal(index, status);
    }
    res.setResponse(rsp);
    long endNanos = System.nanoTime();
    res.setElapsedTime(TimeUnit.NANOSECONDS.toMillis(endNanos - startNanos));
    return res;
  }

  public static interface OnComplete<SolrResponse> {
    public void onSuccess(SolrResponse result);

    public void onFailure(Throwable e);
  }

  public void setFollowRedirects(boolean follow) {
    httpClient.setFollowRedirects(follow);
  }

  public String getBaseURL() {
    return serverBaseUrl;
  }

  public void setBaseURL(String serverBaseUrl) {
    this.serverBaseUrl = serverBaseUrl;
  }

  public static class Builder {

    private SolrInternalHttpClient httpClient;
    private int idleConnectionTimeout = SolrInternalHttpClient.DEFAULT_IDLE_TIMEOUT;
    private boolean useHttp1_1 = false;
    private String baseSolrUrl;
    private Map<String,String> headers = new HashMap<>();
    private ResponseParser responseParser;

    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
    }

    public Http2SolrClient build() {
      return new Http2SolrClient(baseSolrUrl, this);
    }

    public Builder withHttpClient(SolrInternalHttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    public Builder withHeader(String header, String value) {
      this.headers.put(header, value);
      return this;
    }

    public Builder withHeaders(Map<String,String> headers) {
      this.headers.putAll(headers);
      return this;
    }

    public Builder solrInternal() {
      this.headers.put(QoSParams.REQUEST_SOURCE, QoSParams.INTERNAL);
      return this;
    }

    public Builder withResponseParser(ResponseParser parser) {
      this.responseParser = parser;
      return this;
    }

    public Builder idleConnectionTimeout(int idleConnectionTimeout) {
      this.idleConnectionTimeout = idleConnectionTimeout;
      return this;
    }

    public Builder useHttp1_1(boolean useHttp1_1) {
      this.useHttp1_1 = useHttp1_1;
      return this;
    }
  }

  /**
   * Subclass of SolrException that allows us to capture an arbitrary HTTP status code that may have been returned by
   * the remote server or a proxy along the way.
   */
  public static class RemoteSolrException extends SolrException {
    /**
     * @param remoteHost
     *          the host the error was received from
     * @param code
     *          Arbitrary HTTP status code
     * @param msg
     *          Exception Message
     * @param th
     *          Throwable to wrap with this Exception
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

  private static class Http2ClientResponse {
    NamedList response;
    InputStream stream;
    Abortable abortable;
  }

  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }

  protected ModifiableSolrParams calculateQueryParams(Set<String> queryParamNames,
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

  public void setParser(ResponseParser parser) {
    this.parser = parser;
  }
}
