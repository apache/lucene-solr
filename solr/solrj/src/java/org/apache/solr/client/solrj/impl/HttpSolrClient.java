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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.util.Utils.getObjectByPath;

/**
 * A SolrClient implementation that talks directly to a Solr server via HTTP
 */
public class HttpSolrClient extends BaseHttpSolrClient {

  private static final Charset FALLBACK_CHARSET = StandardCharsets.UTF_8;
  private static final String DEFAULT_PATH = "/select";
  private static final long serialVersionUID = -946812319974801896L;
  
  /**
   * User-Agent String.
   */
  public static final String AGENT = "Solr[" + HttpSolrClient.class.getName() + "] 1.0";
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  static final Class<HttpSolrClient> cacheKey = HttpSolrClient.class;
  
  /**
   * The URL of the Solr server.
   */
  protected volatile String baseUrl;
  
  /**
   * Default value: null / empty.
   * <p>
   * Parameters that are added to every request regardless. This may be a place
   * to add something like an authentication token.
   */
  protected ModifiableSolrParams invariantParams;
  
  /**
   * Default response parser is BinaryResponseParser
   * <p>
   * This parser represents the default Response Parser chosen to parse the
   * response if the parser were not specified as part of the request.
   * 
   * @see org.apache.solr.client.solrj.impl.BinaryResponseParser
   */
  protected volatile ResponseParser parser;
  
  /**
   * The RequestWriter used to write all requests to Solr
   * 
   * @see org.apache.solr.client.solrj.request.RequestWriter
   */
  protected volatile RequestWriter requestWriter = new BinaryRequestWriter();
  
  private final HttpClient httpClient;
  
  private volatile Boolean followRedirects = false;
  
  private volatile boolean useMultiPartPost;
  private final boolean internalClient;

  private volatile Set<String> queryParams = Collections.emptySet();
  private volatile Integer connectionTimeout;
  private volatile Integer soTimeout;
  
  /**
   * @deprecated use {@link HttpSolrClient#HttpSolrClient(Builder)} instead, as it is a more extension/subclassing-friendly alternative
   */
  @Deprecated
  protected HttpSolrClient(String baseURL, HttpClient client, ResponseParser parser, boolean allowCompression) {
    this(new Builder(baseURL)
        .withHttpClient(client)
        .withResponseParser(parser)
        .allowCompression(allowCompression));
  }

  /**
   * The constructor.
   *
   * @param baseURL The base url to communicate with the Solr server
   * @param client Http client instance to use for communication
   * @param parser Response parser instance to use to decode response from Solr server
   * @param allowCompression Should compression be allowed ?
   * @param invariantParams The parameters which should be included with every request.
   * 
   * @deprecated use {@link HttpSolrClient#HttpSolrClient(Builder)} instead, as it is a more extension/subclassing-friendly alternative
   */
  @Deprecated
  protected HttpSolrClient(String baseURL, HttpClient client, ResponseParser parser, boolean allowCompression,
      ModifiableSolrParams invariantParams) {
    this(new Builder(baseURL)
        .withHttpClient(client)
        .withResponseParser(parser)
        .allowCompression(allowCompression)
        .withInvariantParams(invariantParams));
  }
  
  protected HttpSolrClient(Builder builder) {
    this.baseUrl = builder.baseSolrUrl;
    if (baseUrl.endsWith("/")) {
      baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    }
    
    if (baseUrl.indexOf('?') >= 0) {
      throw new RuntimeException(
          "Invalid base url for solrj.  The base URL must not contain parameters: "
              + baseUrl);
    }
    
    if (builder.httpClient != null) {
      this.httpClient = builder.httpClient;
      this.internalClient = false;
    } else {
      this.internalClient = true;
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, followRedirects);
      params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, builder.compression);
      httpClient = HttpClientUtil.createClient(params);
    }
    
    this.parser = builder.responseParser;
    this.invariantParams = builder.invariantParams;
    this.connectionTimeout = builder.connectionTimeoutMillis;
    this.soTimeout = builder.socketTimeoutMillis;
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method
   * @param queryParams set of param keys to only send via the query string
   * Note that the param will be sent as a query string if the key is part
   * of this Set or the SolrRequest's query params.
   * @see org.apache.solr.client.solrj.SolrRequest#getQueryParams
   */
  public void setQueryParams(Set<String> queryParams) {
    this.queryParams = queryParams;
  }
  
  /**
   * Process the request. If
   * {@link org.apache.solr.client.solrj.SolrRequest#getResponseParser()} is
   * null, then use {@link #getParser()}
   * 
   * @param request
   *          The {@link org.apache.solr.client.solrj.SolrRequest} to process
   * @return The {@link org.apache.solr.common.util.NamedList} result
   * @throws IOException If there is a low-level I/O error.
   * 
   * @see #request(org.apache.solr.client.solrj.SolrRequest,
   *      org.apache.solr.client.solrj.ResponseParser)
   */
  @Override
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    ResponseParser responseParser = request.getResponseParser();
    if (responseParser == null) {
      responseParser = parser;
    }
    return request(request, responseParser, collection);
  }

  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request, final ResponseParser processor) throws SolrServerException, IOException {
    return request(request, processor, null);
  }
  
  public NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request, final ResponseParser processor, String collection)
      throws SolrServerException, IOException {
    HttpRequestBase method = createMethod(request, collection);
    setBasicAuthHeader(request, method);
    if (request.getHeaders() != null) {
      @SuppressWarnings({"unchecked"})
      Map<String, String> headers = request.getHeaders();
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        method.setHeader(entry.getKey(), entry.getValue());
      }
    }
    return executeMethod(method, request.getUserPrincipal(), processor, isV2ApiRequest(request));
  }

  private boolean isV2ApiRequest(@SuppressWarnings({"rawtypes"})final SolrRequest request) {
    return request instanceof V2Request || request.getPath().contains("/____v2");
  }

  private void setBasicAuthHeader(@SuppressWarnings({"rawtypes"})SolrRequest request, HttpRequestBase method) throws UnsupportedEncodingException {
    if (request.getBasicAuthUser() != null && request.getBasicAuthPassword() != null) {
      String userPass = request.getBasicAuthUser() + ":" + request.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(FALLBACK_CHARSET));
      method.setHeader(new BasicHeader("Authorization", "Basic " + encoded));
    }
  }

  /**
   * @lucene.experimental
   */
  public static class HttpUriRequestResponse {
    public HttpUriRequest httpUriRequest;
    public Future<NamedList<Object>> future;
  }
  
  /**
   * @lucene.experimental
   */
  public HttpUriRequestResponse httpUriRequest(@SuppressWarnings({"rawtypes"})final SolrRequest request)
      throws SolrServerException, IOException {
    ResponseParser responseParser = request.getResponseParser();
    if (responseParser == null) {
      responseParser = parser;
    }
    return httpUriRequest(request, responseParser);
  }
  
  /**
   * @lucene.experimental
   */
  public HttpUriRequestResponse httpUriRequest(@SuppressWarnings({"rawtypes"})final SolrRequest request, final ResponseParser processor) throws SolrServerException, IOException {
    HttpUriRequestResponse mrr = new HttpUriRequestResponse();
    final HttpRequestBase method = createMethod(request, null);
    ExecutorService pool = ExecutorUtil.newMDCAwareFixedThreadPool(1, new SolrNamedThreadFactory("httpUriRequest"));
    try {
      MDC.put("HttpSolrClient.url", baseUrl);
      mrr.future = pool.submit(() -> executeMethod(method, request.getUserPrincipal(), processor, isV2ApiRequest(request)));
 
    } finally {
      pool.shutdown();
      MDC.remove("HttpSolrClient.url");
    }
    assert method != null;
    mrr.httpUriRequest = method;
    return mrr;
  }

  protected ModifiableSolrParams calculateQueryParams(Set<String> queryParamNames,
      ModifiableSolrParams wparams) {
    ModifiableSolrParams queryModParams = new ModifiableSolrParams();
    if (queryParamNames != null) {
      for (String param : queryParamNames) {
        String[] value = wparams.getParams(param) ;
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
  
  static String changeV2RequestEndpoint(String basePath) throws MalformedURLException {
    URL oldURL = new URL(basePath);
    String newPath = oldURL.getPath().replaceFirst("/solr", "/api");
    return new URL(oldURL.getProtocol(), oldURL.getHost(), oldURL.getPort(), newPath).toString();
  }

  @SuppressWarnings({"unchecked"})
  protected HttpRequestBase createMethod(@SuppressWarnings({"rawtypes"})SolrRequest request, String collection) throws IOException, SolrServerException {
    if (request instanceof V2RequestSupport) {
      request = ((V2RequestSupport) request).getV2Request();
    }
    SolrParams params = request.getParams();
    RequestWriter.ContentWriter contentWriter = requestWriter.getContentWriter(request);
    Collection<ContentStream> streams = contentWriter == null ? requestWriter.getContentStreams(request) : null;
    String path = requestWriter.getPath(request);
    if (path == null || !path.startsWith("/")) {
      path = DEFAULT_PATH;
    }
    
    ResponseParser parser = request.getResponseParser();
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
    if (invariantParams != null) {
      wparams.add(invariantParams);
    }

    String basePath = baseUrl;
    if (collection != null)
      basePath += "/" + collection;

    if (request instanceof V2Request) {
      if (System.getProperty("solr.v2RealPath") == null || ((V2Request) request).isForceV2()) {
        basePath = baseUrl.replace("/solr", "/api");
      } else {
        basePath = baseUrl + "/____v2";
      }
    }

    if (SolrRequest.METHOD.GET == request.getMethod()) {
      if (streams != null || contentWriter != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      return new HttpGet(basePath + path + wparams.toQueryString());
    }

    if (SolrRequest.METHOD.DELETE == request.getMethod()) {
      return new HttpDelete(basePath + path + wparams.toQueryString());
    }

    if (SolrRequest.METHOD.POST == request.getMethod() || SolrRequest.METHOD.PUT == request.getMethod()) {

      String url = basePath + path;
      boolean hasNullStreamName = false;
      if (streams != null) {
        for (ContentStream cs : streams) {
          if (cs.getName() == null) {
            hasNullStreamName = true;
            break;
          }
        }
      }
      boolean isMultipart = ((this.useMultiPartPost && SolrRequest.METHOD.POST == request.getMethod())
          || (streams != null && streams.size() > 1)) && !hasNullStreamName;

      LinkedList<NameValuePair> postOrPutParams = new LinkedList<>();

      if(contentWriter != null) {
        String fullQueryUrl = url + wparams.toQueryString();
        HttpEntityEnclosingRequestBase postOrPut = SolrRequest.METHOD.POST == request.getMethod() ?
            new HttpPost(fullQueryUrl) : new HttpPut(fullQueryUrl);
        postOrPut.addHeader("Content-Type",
            contentWriter.getContentType());
        postOrPut.setEntity(new BasicHttpEntity(){
          @Override
          public boolean isStreaming() {
            return true;
          }

          @Override
          public void writeTo(OutputStream outstream) throws IOException {
            contentWriter.write(outstream);
          }
        });
        return postOrPut;

      } else if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(request.getQueryParams(), wparams));
        String fullQueryUrl = url + queryParams.toQueryString();
        HttpEntityEnclosingRequestBase postOrPut = fillContentStream(request, streams, wparams, isMultipart, postOrPutParams, fullQueryUrl);
        return postOrPut;
      }
      // It is has one stream, it is the post body, put the params in the URL
      else {
        String fullQueryUrl = url + wparams.toQueryString();
        HttpEntityEnclosingRequestBase postOrPut = SolrRequest.METHOD.POST == request.getMethod() ?
            new HttpPost(fullQueryUrl) : new HttpPut(fullQueryUrl);
        fillSingleContentStream(streams, postOrPut);

        return postOrPut;
      }
    }

    throw new SolrServerException("Unsupported method: " + request.getMethod());

  }

  private void fillSingleContentStream(Collection<ContentStream> streams, HttpEntityEnclosingRequestBase postOrPut) throws IOException {
    // Single stream as body
    // Using a loop just to get the first one
    final ContentStream[] contentStream = new ContentStream[1];
    for (ContentStream content : streams) {
      contentStream[0] = content;
      break;
    }
    Long size = contentStream[0].getSize();
    postOrPut.setEntity(new InputStreamEntity(contentStream[0].getStream(), size == null ? -1 : size) {
      @Override
      public Header getContentType() {
        return new BasicHeader("Content-Type", contentStream[0].getContentType());
      }

      @Override
      public boolean isRepeatable() {
        return false;
      }
    });

  }

  private HttpEntityEnclosingRequestBase fillContentStream(
          @SuppressWarnings({"rawtypes"})SolrRequest request,
          Collection<ContentStream> streams, ModifiableSolrParams wparams,
          boolean isMultipart, LinkedList<NameValuePair> postOrPutParams,
          String fullQueryUrl) throws IOException {
    HttpEntityEnclosingRequestBase postOrPut = SolrRequest.METHOD.POST == request.getMethod() ?
        new HttpPost(fullQueryUrl) : new HttpPut(fullQueryUrl);

    if (!isMultipart) {
      postOrPut.addHeader("Content-Type",
          "application/x-www-form-urlencoded; charset=UTF-8");
    }

    List<FormBodyPart> parts = new LinkedList<>();
    Iterator<String> iter = wparams.getParameterNamesIterator();
    while (iter.hasNext()) {
      String p = iter.next();
      String[] vals = wparams.getParams(p);
      if (vals != null) {
        for (String v : vals) {
          if (isMultipart) {
            parts.add(new FormBodyPart(p, new StringBody(v, StandardCharsets.UTF_8)));
          } else {
            postOrPutParams.add(new BasicNameValuePair(p, v));
          }
        }
      }
    }

    // TODO: remove deprecated - first simple attempt failed, see {@link MultipartEntityBuilder}
    if (isMultipart && streams != null) {
      for (ContentStream content : streams) {
        String contentType = content.getContentType();
        if (contentType == null) {
          contentType = BinaryResponseParser.BINARY_CONTENT_TYPE; // default
        }
        String name = content.getName();
        if (name == null) {
          name = "";
        }
        parts.add(new FormBodyPart(name,
            new InputStreamBody(
                content.getStream(),
                ContentType.parse(contentType),
                content.getName())));
      }
    }

    if (parts.size() > 0) {
      MultipartEntity entity = new MultipartEntity(HttpMultipartMode.STRICT);
      for (FormBodyPart p : parts) {
        entity.addPart(p);
      }
      postOrPut.setEntity(entity);
    } else {
      //not using multipart
      postOrPut.setEntity(new UrlEncodedFormEntity(postOrPutParams, StandardCharsets.UTF_8));
    }
    return postOrPut;
  }

  private static final List<String> errPath = Arrays.asList("metadata", "error-class");//Utils.getObjectByPath(err, false,"metadata/error-class")

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected NamedList<Object> executeMethod(HttpRequestBase method, Principal userPrincipal, final ResponseParser processor, final boolean isV2Api) throws SolrServerException {
    method.addHeader("User-Agent", AGENT);
 
    org.apache.http.client.config.RequestConfig.Builder requestConfigBuilder = HttpClientUtil.createDefaultRequestConfigBuilder();
    if (soTimeout != null) {
      requestConfigBuilder.setSocketTimeout(soTimeout);
    }
    if (connectionTimeout != null) {
      requestConfigBuilder.setConnectTimeout(connectionTimeout);
    }
    if (followRedirects != null) {
      requestConfigBuilder.setRedirectsEnabled(followRedirects);
    }

    method.setConfig(requestConfigBuilder.build());
    
    HttpEntity entity = null;
    InputStream respBody = null;
    boolean shouldClose = true;
    try {
      // Execute the method.
      HttpClientContext httpClientRequestContext = HttpClientUtil.createNewHttpClientRequestContext();
      if (userPrincipal != null) {
        // Normally the context contains a static userToken to enable reuse resources.
        // However, if a personal Principal object exists, we use that instead, also as a means
        // to transfer authentication information to Auth plugins that wish to intercept the request later
        httpClientRequestContext.setUserToken(userPrincipal);
      }
      final HttpResponse response = httpClient.execute(method, httpClientRequestContext);

      int httpStatus = response.getStatusLine().getStatusCode();
      
      // Read the contents
      entity = response.getEntity();
      respBody = entity.getContent();
      String mimeType = null;
      Charset charset = null;
      String charsetName = null;

      ContentType contentType = ContentType.get(entity);
      if (contentType != null) {
        mimeType = contentType.getMimeType().trim().toLowerCase(Locale.ROOT);
        charset = contentType.getCharset();

        if (charset != null) {
          charsetName = charset.name();
        }
      }

      // handle some http level checks before trying to parse the response
      switch (httpStatus) {
        case HttpStatus.SC_OK:
        case HttpStatus.SC_BAD_REQUEST:
        case HttpStatus.SC_CONFLICT:  // 409
          break;
        case HttpStatus.SC_MOVED_PERMANENTLY:
        case HttpStatus.SC_MOVED_TEMPORARILY:
          if (!followRedirects) {
            throw new SolrServerException("Server at " + getBaseURL()
                + " sent back a redirect (" + httpStatus + ").");
          }
          break;
        default:
          if (processor == null || contentType == null) {
            throw new RemoteSolrException(baseUrl, httpStatus, "non ok status: " + httpStatus
                + ", message:" + response.getStatusLine().getReasonPhrase(),
                null);
          }
      }
      if (processor == null || processor instanceof InputStreamResponseParser) {
        
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
        rsp.add("stream", respBody);
        rsp.add("closeableResponse", response);
        // Only case where stream should not be closed
        shouldClose = false;
        return rsp;
      }

      String procCt = processor.getContentType();
      if (procCt != null) {
        String procMimeType = ContentType.parse(procCt).getMimeType().trim().toLowerCase(Locale.ROOT);
        if (!procMimeType.equals(mimeType)) {
          // unexpected mime type
          String msg = "Expected mime type " + procMimeType + " but got " + mimeType + ".";
          Charset exceptionCharset = charset != null? charset : FALLBACK_CHARSET;
          try {
            msg = msg + " " + IOUtils.toString(respBody, exceptionCharset);
          } catch (IOException e) {
            throw new RemoteSolrException(baseUrl, httpStatus, "Could not parse response with encoding " + exceptionCharset, e);
          }
          throw new RemoteSolrException(baseUrl, httpStatus, msg, null);
        }
      }
      
      NamedList<Object> rsp = null;
      try {
        rsp = processor.processResponse(respBody, charsetName);
      } catch (Exception e) {
        throw new RemoteSolrException(baseUrl, httpStatus, e.getMessage(), e);
      }
      Object error = rsp == null ? null : rsp.get("error");
      if (error != null && (isV2Api || String.valueOf(getObjectByPath(error, true, errPath)).endsWith("ExceptionWithErrObject"))) {
          throw RemoteExecutionException.create(baseUrl, rsp);
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
          }
        } catch (Exception ex) {}
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getStatusLine().getReasonPhrase())
            .append("\n\n")
            .append("request: ")
            .append(method.getURI());
          reason = java.net.URLDecoder.decode(msg.toString(), FALLBACK_CHARSET.name());
        }
        RemoteSolrException rss = new RemoteSolrException(baseUrl, httpStatus, reason, null);
        if (metadata != null) rss.setMetadata(metadata);
        throw rss;
      }
      return rsp;
    } catch (ConnectException e) {
      throw new SolrServerException("Server refused connection at: "
          + getBaseURL(), e);
    } catch (SocketTimeoutException e) {
      throw new SolrServerException(
          "Timeout occurred while waiting response from server at: "
              + getBaseURL(), e);
    } catch (IOException e) {
      throw new SolrServerException(
          "IOException occurred when talking to server at: " + getBaseURL(), e);
    } finally {
      if (shouldClose) {
        Utils.consumeFully(entity);
      }
    }
  }
  
  // -------------------------------------------------------------------
  // -------------------------------------------------------------------
  
  /**
   * Retrieve the default list of parameters are added to every request
   * regardless.
   * 
   * @see #invariantParams
   */
  public ModifiableSolrParams getInvariantParams() {
    return invariantParams;
  }
  
  public String getBaseURL() {
    return baseUrl;
  }
  
  /**
   * Change the base-url used when sending requests to Solr.
   * 
   * Two different paths can be specified as a part of this URL:
   * 
   * 1) A path pointing directly at a particular core
   * <pre>
   *   httpSolrClient.setBaseURL("http://my-solr-server:8983/solr/core1");
   *   QueryResponse resp = httpSolrClient.query(new SolrQuery("*:*"));
   * </pre>
   * Note that when a core is provided in the base URL, queries and other requests can be made without mentioning the
   * core explicitly.  However, the client can only send requests to that core.
   * 
   * 2) The path of the root Solr path ("/solr")
   * <pre>
   *   httpSolrClient.setBaseURL("http://my-solr-server:8983/solr");
   *   QueryResponse resp = httpSolrClient.query("core1", new SolrQuery("*:*"));
   * </pre>
   * In this case the client is more flexible and can be used to send requests to any cores.  The cost of this is that
   * the core must be specified on each request.
   */
  public void setBaseURL(String baseURL) {
    this.baseUrl = baseURL;
  }
  
  public ResponseParser getParser() {
    return parser;
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
    parser = processor;
  }
  
  /**
   * Return the HttpClient this instance uses.
   */
  public HttpClient getHttpClient() {
    return httpClient;
  }
  
  /**
   * HttpConnectionParams.setConnectionTimeout
   * 
   * @param timeout
   *          Timeout in milliseconds
   *          
   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setConnectionTimeout(int timeout) {
    this.connectionTimeout = timeout;
  }
  
  /**
   * Set SoTimeout (read timeout). This is desirable
   * for queries, but probably not for indexing.
   * 
   * @param timeout
   *          Timeout in milliseconds
   *          
s   * @deprecated since 7.0  Use {@link Builder} methods instead. 
   */
  @Deprecated
  public void setSoTimeout(int timeout) {
    this.soTimeout = timeout;
  }
  
  /**
   * Configure whether the client should follow redirects or not.
   * <p>
   * This defaults to false under the assumption that if you are following a
   * redirect to get to a Solr installation, something is misconfigured
   * somewhere.
   * </p>
   */
  public void setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
  }
  
  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }
  
  /**
   * Close the {@link HttpClientConnectionManager} from the internal client.
   */
  @Override
  public void close() throws IOException {
    if (httpClient != null && internalClient) {
      HttpClientUtil.close(httpClient);
    }
  }
  
  public boolean isUseMultiPartPost() {
    return useMultiPartPost;
  }

  /**
   * Set the multipart connection properties
   */
  public void setUseMultiPartPost(boolean useMultiPartPost) {
    this.useMultiPartPost = useMultiPartPost;
  }


  /**
   * @deprecated since 8.0, catch {@link BaseHttpSolrClient.RemoteSolrException} instead
   */
  @Deprecated
  public static class RemoteSolrException extends BaseHttpSolrClient.RemoteSolrException {

    public RemoteSolrException(String remoteHost, int code, String msg, Throwable th) {
      super(remoteHost, code, msg, th);
    }
  }

  /**
   * @deprecated since 8.0, catch {@link BaseHttpSolrClient.RemoteExecutionException} instead
   */
  @Deprecated
  public static class RemoteExecutionException extends BaseHttpSolrClient.RemoteExecutionException {

    public RemoteExecutionException(String remoteHost, int code, String msg, NamedList meta) {
      super(remoteHost, code, msg, meta);
    }
  }

  /**
   * Constructs {@link HttpSolrClient} instances from provided configuration.
   */
  public static class Builder extends SolrClientBuilder<Builder> {
    protected String baseSolrUrl;
    protected boolean compression;
    protected ModifiableSolrParams invariantParams = new ModifiableSolrParams();

    public Builder() {
      this.responseParser = new BinaryResponseParser();
    }

    /**
     * Specify the base-url for the created client to use when sending requests to Solr.
     * 
     * Two different paths can be specified as a part of this URL:
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
      this.baseSolrUrl = baseSolrUrl;
      return this;
    }

    /**
     * Create a Builder object, based on the provided Solr URL.
     * 
     * Two different paths can be specified as a part of this URL:
     * 
     * 1) A path pointing directly at a particular core
     * <pre>
     *   SolrClient client = new HttpSolrClient.Builder("http://my-solr-server:8983/solr/core1").build();
     *   QueryResponse resp = client.query(new SolrQuery("*:*"));
     * </pre>
     * Note that when a core is provided in the base URL, queries and other requests can be made without mentioning the
     * core explicitly.  However, the client can only send requests to that core.
     * 
     * 2) The path of the root Solr path ("/solr")
     * <pre>
     *   SolrClient client = new HttpSolrClient.Builder("http://my-solr-server:8983/solr").build();
     *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
     * </pre>
     * In this case the client is more flexible and can be used to send requests to any cores.  This flexibility though
     * requires that the core be specified on all requests.
     * 
     * By default, compression is not enabled on created HttpSolrClient objects.
     */
    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
      this.responseParser = new BinaryResponseParser();
    }

    /**
     * Chooses whether created {@link HttpSolrClient}s use compression by default.
     */
    public Builder allowCompression(boolean compression) {
      this.compression = compression;
      return this;
    }

    /**
     * Use a delegation token for authenticating via the KerberosPlugin
     */
    public Builder withKerberosDelegationToken(String delegationToken) {
      if (this.invariantParams.get(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM) != null) {
        throw new IllegalStateException(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM + " is already defined!");
      }
      this.invariantParams.add(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM, delegationToken);
      return this;
    }

    /**
     * Adds to the set of params that the created {@link HttpSolrClient} will add on all requests
     *
     * @param params a set of parameters to add to the invariant-params list.  These params must be unique and may not
     *               duplicate a param already in the invariant list.
     */
    public Builder withInvariantParams(ModifiableSolrParams params) {
      Objects.requireNonNull(params, "params must be non null!");

      for (String name : params.getParameterNames()) {
        if (this.invariantParams.get(name) != null) {
          throw new IllegalStateException("parameter " + name + " is redefined.");
        }
      }

      this.invariantParams.add(params);
      return this;
    }

    /**
     * Create a {@link HttpSolrClient} based on provided configuration.
     */
    public HttpSolrClient build() {
      if (baseSolrUrl == null) {
        throw new IllegalArgumentException("Cannot create HttpSolrClient without a valid baseSolrUrl!");
      }

      if (this.invariantParams.get(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM) == null) {
        return new HttpSolrClient(this);
      } else {
        return new DelegationTokenHttpSolrClient(this);
      }
    }

    @Override
    public Builder getThis() {
      return this;
    }
  }
}
