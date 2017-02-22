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
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
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
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Base64;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A SolrClient implementation that talks directly to a Solr server via HTTP
 *
 * There are two ways to use an HttpSolrClient:
 *
 * 1) Pass a URL to the constructor that points directly at a particular core
 * <pre>
 *   SolrClient client = new HttpSolrClient("http://my-solr-server:8983/solr/core1");
 *   QueryResponse resp = client.query(new SolrQuery("*:*"));
 * </pre>
 * In this case, you can query the given core directly, but you cannot query any other
 * cores or issue CoreAdmin requests with this client.
 *
 * 2) Pass the base URL of the node to the constructor
 * <pre>
 *   SolrClient client = new HttpSolrClient("http://my-solr-server:8983/solr");
 *   QueryResponse resp = client.query("core1", new SolrQuery("*:*"));
 * </pre>
 * In this case, you must pass the name of the required core for all queries and updates,
 * but you may use the same client for all cores, and for CoreAdmin requests.
 */
public class HttpSolrClient extends SolrClient {

  private static final String UTF_8 = StandardCharsets.UTF_8.name();
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
   * @param baseURL
   *          The URL of the Solr server. For example, "
   *          <code>http://localhost:8983/solr/</code>" if you are using the
   *          standard distribution Solr webapp on your local machine.
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public HttpSolrClient(String baseURL) {
    this(baseURL, null, new BinaryResponseParser());
  }
  
  /**
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public HttpSolrClient(String baseURL, HttpClient client) {
    this(baseURL, client, new BinaryResponseParser());
  }
  
  /**
   * @deprecated use {@link Builder} instead.
   */
  @Deprecated
  public HttpSolrClient(String baseURL, HttpClient client, ResponseParser parser) {
    this(baseURL, client, parser, false);
  }
  
  /**
   * @deprecated use {@link Builder} instead.  This will soon be a 'protected'
   * method, and will only be available for use in implementing subclasses.
   */
  @Deprecated
  public HttpSolrClient(String baseURL, HttpClient client, ResponseParser parser, boolean allowCompression) {
    this.baseUrl = baseURL;
    if (baseUrl.endsWith("/")) {
      baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
    }
    if (baseUrl.indexOf('?') >= 0) {
      throw new RuntimeException(
          "Invalid base url for solrj.  The base URL must not contain parameters: "
              + baseUrl);
    }
    
    if (client != null) {
      httpClient = client;
      internalClient = false;
    } else {
      internalClient = true;
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, followRedirects);
      params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, allowCompression);
      httpClient = HttpClientUtil.createClient(params);
    }
    
    this.parser = parser;
  }

  /**
   * The consturctor.
   *
   * @param baseURL The base url to communicate with the Solr server
   * @param client Http client instance to use for communication
   * @param parser Response parser instance to use to decode response from Solr server
   * @param allowCompression Should compression be allowed ?
   * @param invariantParams The parameters which should be included with every request.
   */
  protected HttpSolrClient(String baseURL, HttpClient client, ResponseParser parser, boolean allowCompression,
      ModifiableSolrParams invariantParams) {
    this(baseURL, client, parser, allowCompression);

    this.invariantParams = invariantParams;
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
  public NamedList<Object> request(final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    ResponseParser responseParser = request.getResponseParser();
    if (responseParser == null) {
      responseParser = parser;
    }
    return request(request, responseParser, collection);
  }

  public NamedList<Object> request(final SolrRequest request, final ResponseParser processor) throws SolrServerException, IOException {
    return request(request, processor, null);
  }
  
  public NamedList<Object> request(final SolrRequest request, final ResponseParser processor, String collection)
      throws SolrServerException, IOException {
    HttpRequestBase method = createMethod(request, collection);
    setBasicAuthHeader(request, method);
    return executeMethod(method, processor);
  }

  private void setBasicAuthHeader(SolrRequest request, HttpRequestBase method) throws UnsupportedEncodingException {
    if (request.getBasicAuthUser() != null && request.getBasicAuthPassword() != null) {
      String userPass = request.getBasicAuthUser() + ":" + request.getBasicAuthPassword();
      String encoded = Base64.byteArrayToBase64(userPass.getBytes(UTF_8));
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
  public HttpUriRequestResponse httpUriRequest(final SolrRequest request)
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
  public HttpUriRequestResponse httpUriRequest(final SolrRequest request, final ResponseParser processor) throws SolrServerException, IOException {
    HttpUriRequestResponse mrr = new HttpUriRequestResponse();
    final HttpRequestBase method = createMethod(request, null);
    ExecutorService pool = ExecutorUtil.newMDCAwareFixedThreadPool(1, new SolrjNamedThreadFactory("httpUriRequest"));
    try {
      MDC.put("HttpSolrClient.url", baseUrl);
      mrr.future = pool.submit(() -> executeMethod(method, processor));
 
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

  protected HttpRequestBase createMethod(final SolrRequest request, String collection) throws IOException, SolrServerException {

    SolrParams params = request.getParams();
    Collection<ContentStream> streams = requestWriter.getContentStreams(request);
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

    if (SolrRequest.METHOD.GET == request.getMethod()) {
      if (streams != null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
      }

      return new HttpGet(basePath + path + wparams.toQueryString());
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
      if (streams == null || isMultipart) {
        // send server list and request list as query string params
        ModifiableSolrParams queryParams = calculateQueryParams(this.queryParams, wparams);
        queryParams.add(calculateQueryParams(request.getQueryParams(), wparams));
        String fullQueryUrl = url + queryParams.toQueryString();
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
                    contentType,
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
      // It is has one stream, it is the post body, put the params in the URL
      else {
        String fullQueryUrl = url + wparams.toQueryString();
        HttpEntityEnclosingRequestBase postOrPut = SolrRequest.METHOD.POST == request.getMethod() ?
            new HttpPost(fullQueryUrl) : new HttpPut(fullQueryUrl);

        // Single stream as body
        // Using a loop just to get the first one
        final ContentStream[] contentStream = new ContentStream[1];
        for (ContentStream content : streams) {
          contentStream[0] = content;
          break;
        }
        if (contentStream[0] instanceof RequestWriter.LazyContentStream) {
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
        } else {
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
        return postOrPut;
      }
    }

    throw new SolrServerException("Unsupported method: " + request.getMethod());

  }
  
  protected NamedList<Object> executeMethod(HttpRequestBase method, final ResponseParser processor) throws SolrServerException {
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
      final HttpResponse response = httpClient.execute(method, httpClientRequestContext);
      int httpStatus = response.getStatusLine().getStatusCode();
      
      // Read the contents
      entity = response.getEntity();
      respBody = entity.getContent();
      Header ctHeader = response.getLastHeader("content-type");
      String contentType;
      if (ctHeader != null) {
        contentType = ctHeader.getValue();
      } else {
        contentType = "";
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
          if (processor == null || "".equals(contentType)) {
            throw new RemoteSolrException(baseUrl, httpStatus, "non ok status: " + httpStatus
                + ", message:" + response.getStatusLine().getReasonPhrase(),
                null);
          }
      }
      if (processor == null || processor instanceof InputStreamResponseParser) {
        
        // no processor specified, return raw stream
        NamedList<Object> rsp = new NamedList<>();
        rsp.add("stream", respBody);
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
          Header encodingHeader = response.getEntity().getContentEncoding();
          String encoding;
          if (encodingHeader != null) {
            encoding = encodingHeader.getValue();
          } else {
            encoding = "UTF-8"; // try UTF-8
          }
          try {
            msg = msg + " " + IOUtils.toString(respBody, encoding);
          } catch (IOException e) {
            throw new RemoteSolrException(baseUrl, httpStatus, "Could not parse response with encoding " + encoding, e);
          }
          throw new RemoteSolrException(baseUrl, httpStatus, msg, null);
        }
      }
      
      NamedList<Object> rsp = null;
      String charset = EntityUtils.getContentCharSet(response.getEntity());
      try {
        rsp = processor.processResponse(respBody, charset);
      } catch (Exception e) {
        throw new RemoteSolrException(baseUrl, httpStatus, e.getMessage(), e);
      }
      if (httpStatus != HttpStatus.SC_OK) {
        NamedList<String> metadata = null;
        String reason = null;
        try {
          NamedList err = (NamedList) rsp.get("error");
          if (err != null) {
            reason = (String) err.get("msg");
            if(reason == null) {
              reason = (String) err.get("trace");
            }
            metadata = (NamedList<String>)err.get("metadata");
          }
        } catch (Exception ex) {}
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getStatusLine().getReasonPhrase())
            .append("\n\n")
            .append("request: ")
            .append(method.getURI());
          reason = java.net.URLDecoder.decode(msg.toString(), UTF_8);
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
          "Timeout occured while waiting response from server at: "
              + getBaseURL(), e);
    } catch (IOException e) {
      throw new SolrServerException(
          "IOException occured when talking to server at: " + getBaseURL(), e);
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
   **/
  public void setConnectionTimeout(int timeout) {
    this.connectionTimeout = timeout;
  }
  
  /**
   * Set SoTimeout (read timeout). This is desirable
   * for queries, but probably not for indexing.
   * 
   * @param timeout
   *          Timeout in milliseconds
   **/
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
   * Subclass of SolrException that allows us to capture an arbitrary HTTP
   * status code that may have been returned by the remote server or a 
   * proxy along the way.
   */
  public static class RemoteSolrException extends SolrException {
    /**
     * @param remoteHost the host the error was received from
     * @param code Arbitrary HTTP status code
     * @param msg Exception Message
     * @param th Throwable to wrap with this Exception
     */
    public RemoteSolrException(String remoteHost, int code, String msg, Throwable th) {
      super(code, "Error from server at " + remoteHost + ": " + msg, th);
    }
  }

  /**
   * Constructs {@link HttpSolrClient} instances from provided configuration.
   */
  public static class Builder {
    private String baseSolrUrl;
    private HttpClient httpClient;
    private ResponseParser responseParser;
    private boolean compression;
    private ModifiableSolrParams invariantParams = new ModifiableSolrParams();

    public Builder() {
      this.responseParser = new BinaryResponseParser();
    }

    public Builder withBaseSolrUrl(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
      return this;
    }

    /**
     * Create a Builder object, based on the provided Solr URL.
     * 
     * By default, compression is not enabled in created HttpSolrClient objects.
     * 
     * @param baseSolrUrl the base URL of the Solr server that will be targeted by any created clients.
     */
    public Builder(String baseSolrUrl) {
      this.baseSolrUrl = baseSolrUrl;
      this.responseParser = new BinaryResponseParser();
    }

    /**
     * Provides a {@link HttpClient} for the builder to use when creating clients.
     */
    public Builder withHttpClient(HttpClient httpClient) {
      this.httpClient = httpClient;
      return this;
    }

    /**
     * Provides a {@link ResponseParser} for created clients to use when handling requests.
     */
    public Builder withResponseParser(ResponseParser responseParser) {
      this.responseParser = responseParser;
      return this;
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
      return withDelegationToken(delegationToken);
    }

    @Deprecated
    /**
     * @deprecated use {@link withKerberosDelegationToken(String)} instead
     */
    public Builder withDelegationToken(String delegationToken) {
      if (this.invariantParams.get(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM) != null) {
        throw new IllegalStateException(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM + " is already defined!");
      }
      this.invariantParams.add(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM, delegationToken);
      return this;
    }

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
        return new HttpSolrClient(baseSolrUrl, httpClient, responseParser, compression, invariantParams);
      } else {
        return new DelegationTokenHttpSolrClient(baseSolrUrl, httpClient, responseParser, compression, invariantParams);
      }
    }
  }
}
