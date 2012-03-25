/**
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
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Iterator;
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
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.ClientParamBean;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.entity.mime.FormBodyPart;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link HttpSolrServer} uses the Apache HTTP Client 4.x to connect to solr. 
 * <pre class="prettyprint" >SolrServer server = new HttpSolrServer( url );</pre>
 */
public class HttpSolrServer extends SolrServer {
  private static final String UTF_8 = "UTF-8";
  private static final String DEFAULT_PATH = "/select";
  private static final long serialVersionUID = 1L;
  /**
   * User-Agent String.
   */
  public static final String AGENT = "Solr[" + HttpSolrServer.class.getName()
      + "] 1.0";
  
  private static Logger log = LoggerFactory.getLogger(HttpSolrServer.class);
  
  /**
   * The URL of the Solr server.
   */
  protected String baseUrl;
  
  /**
   * Default value: null / empty.
   * <p/>
   * Parameters that are added to every request regardless. This may be a place
   * to add something like an authentication token.
   */
  protected ModifiableSolrParams invariantParams;
  
  /**
   * Default response parser is BinaryResponseParser
   * <p/>
   * This parser represents the default Response Parser chosen to parse the
   * response if the parser were not specified as part of the request.
   * 
   * @see org.apache.solr.client.solrj.impl.BinaryResponseParser
   */
  protected ResponseParser parser;
  
  /**
   * The RequestWriter used to write all requests to Solr
   * 
   * @see org.apache.solr.client.solrj.request.RequestWriter
   */
  protected RequestWriter requestWriter = new RequestWriter();
  
  private final HttpClient httpClient;
  
  /**
   * This defaults to false under the assumption that if you are following a
   * redirect to get to a Solr installation, something is misconfigured
   * somewhere.
   */
  private boolean followRedirects = false;
  
  /**
   * Maximum number of retries to attempt in the event of transient errors.
   * Default: 0 (no) retries. No more than 1 recommended.
   */
  private int maxRetries = 0;
  
  private ThreadSafeClientConnManager ccm;
  
  /**
   * @param baseURL
   *          The URL of the Solr server. For example, "
   *          <code>http://localhost:8983/solr/</code>" if you are using the
   *          standard distribution Solr webapp on your local machine.
   */
  public HttpSolrServer(String baseURL) {
    this(baseURL, null, new BinaryResponseParser());
  }
  
  public HttpSolrServer(String baseURL, HttpClient client) {
    this(baseURL, client, new BinaryResponseParser());
  }
  
  public HttpSolrServer(String baseURL, HttpClient client, ResponseParser parser) {
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
    } else {
      httpClient = createClient();
    }
    
    this.parser = parser;
  }
  
  private DefaultHttpClient createClient() {
    SchemeRegistry schemeRegistry = new SchemeRegistry();
    schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory
        .getSocketFactory()));
    schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory
        .getSocketFactory()));
    
    ccm = new ThreadSafeClientConnManager(schemeRegistry);
    // Increase default max connection per route to 32
    ccm.setDefaultMaxPerRoute(32);
    // Increase max total connection to 128
    ccm.setMaxTotal(128);
    DefaultHttpClient httpClient = new DefaultHttpClient(ccm);
    httpClient.getParams().setParameter(ClientPNames.HANDLE_REDIRECTS,
        followRedirects);
    return httpClient;
  }
  
  /**
   * Process the request. If
   * {@link org.apache.solr.client.solrj.SolrRequest#getResponseParser()} is
   * null, then use {@link #getParser()}
   * 
   * @param request
   *          The {@link org.apache.solr.client.solrj.SolrRequest} to process
   * @return The {@link org.apache.solr.common.util.NamedList} result
   * @throws SolrServerException
   * @throws IOException
   * 
   * @see #request(org.apache.solr.client.solrj.SolrRequest,
   *      org.apache.solr.client.solrj.ResponseParser)
   */
  public NamedList<Object> request(final SolrRequest request)
      throws SolrServerException, IOException {
    ResponseParser responseParser = request.getResponseParser();
    if (responseParser == null) {
      responseParser = parser;
    }
    return request(request, responseParser);
  }
  
  public NamedList<Object> request(final SolrRequest request,
      final ResponseParser processor) throws SolrServerException {
    HttpRequestBase method = null;
    SolrParams params = request.getParams();
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
    wparams.set(CommonParams.WT, parser.getWriterType());
    wparams.set(CommonParams.VERSION, parser.getVersion());
    if (invariantParams != null) {
      wparams.add(invariantParams);
    }
    params = wparams;
    
    int tries = maxRetries + 1;
    try {
      while (tries-- > 0) { // XXX this retry thing seems noop to me
        Collection<ContentStream> streams = requestWriter
            .getContentStreams(request);
        // Note: since we aren't doing intermittent time keeping
        // ourselves, the potential non-timeout latency could be as
        // much as tries-times (plus scheduling effects) the given
        // timeAllowed.
        try {
          if (SolrRequest.METHOD.GET == request.getMethod()) {
            if (streams != null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                  "GET can't send streams!");
            }
            method = new HttpGet(baseUrl + path
                + ClientUtils.toQueryString(params, false));
          } else if (SolrRequest.METHOD.POST == request.getMethod()) {
            String url = baseUrl + path;
            
            MultipartEntity entity = new MultipartEntity(
                HttpMultipartMode.BROWSER_COMPATIBLE);
            
            final HttpPost post = new HttpPost(url);
            
            final Iterator<String> iter = params.getParameterNamesIterator();
            if (iter.hasNext()) {
              
              while (iter.hasNext()) {
                final String name = iter.next();
                final String[] vals = params.getParams(name);
                if (vals != null) {
                  for (String value : vals) {
                    entity.addPart(name, new StringBody(value));
                  }
                }
              }
            }
            addParts(streams, entity);
            post.setEntity(entity);
            method = post;
          } else {
            throw new SolrServerException("Unsupported method: "
                + request.getMethod());
          }
        } catch (RuntimeException r) {
          // If out of tries then just rethrow (as normal error).
          if ((tries < 1)) {
            throw r;
          }
          // log.warn( "Caught: " + r + ". Retrying..." );
        }
      }
    } catch (IOException ex) {
      throw new SolrServerException("error reading streams", ex);
    }
    
    // TODO: move to a interceptor?
    method.getParams().setParameter(ClientPNames.HANDLE_REDIRECTS,
        followRedirects);
    method.addHeader("User-Agent", AGENT);
    method.setHeader("Content-Charset", UTF_8);
    method.setHeader("Accept-Charset", UTF_8);
    
    InputStream respBody = null;
    
    try {
      // Execute the method.
      final HttpResponse response = httpClient.execute(method);
      int httpStatus = response.getStatusLine().getStatusCode();
      
      // Read the contents
      String charset = EntityUtils.getContentCharSet(response.getEntity());
      respBody = response.getEntity().getContent();
      
      // handle some http level checks before trying to parse the response
      switch (httpStatus) {
        case HttpStatus.SC_OK:
          break;
        case HttpStatus.SC_MOVED_PERMANENTLY:
        case HttpStatus.SC_MOVED_TEMPORARILY:
          if (!followRedirects) {
            throw new SolrServerException("Server at " + getBaseURL()
                + " sent back a redirect (" + httpStatus + ").");
          }
          break;
        case HttpStatus.SC_NOT_FOUND:
          throw new SolrServerException("Server at " + getBaseURL()
              + " was not found (404).");
        default:
          throw new SolrServerException("Server at " + getBaseURL()
              + " returned non ok status:" + httpStatus + ", message:"
              + response.getStatusLine().getReasonPhrase());
          
      }
      NamedList<Object> rsp = processor.processResponse(respBody, charset);
      if (httpStatus != HttpStatus.SC_OK) {
        String reason = null;
        try {
          NamedList err = (NamedList) rsp.get("error");
          if (err != null) {
            reason = (String) err.get("msg");
            // TODO? get the trace?
          }
        } catch (Exception ex) {}
        if (reason == null) {
          StringBuilder msg = new StringBuilder();
          msg.append(response.getStatusLine().getReasonPhrase());
          msg.append("\n\n");
          msg.append("request: " + method.getURI());
          reason = java.net.URLDecoder.decode(msg.toString(), UTF_8);
        }
        throw new SolrException(
            SolrException.ErrorCode.getErrorCode(httpStatus), reason);
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
      if (respBody != null) {
        try {
          respBody.close();
        } catch (Throwable t) {} // ignore
      }
    }
  }
  
  private void addParts(Collection<ContentStream> streams,
      MultipartEntity entity) throws IOException {
    if (streams != null) {
      for (ContentStream content : streams) {
        entity.addPart(new FormBodyPart(CommonParams.STREAM_BODY,
            new InputStreamBody(content.getStream(), "")));
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
    HttpConnectionParams.setConnectionTimeout(httpClient.getParams(), timeout);
  }
  
  /**
   * Sets HttpConnectionParams.setSoTimeout (read timeout). This is desirable
   * for queries, but probably not for indexing.
   * 
   * @param timeout
   *          Timeout in milliseconds
   **/
  public void setSoTimeout(int timeout) {
    HttpConnectionParams.setSoTimeout(httpClient.getParams(), timeout);
  }
  
  /**
   * HttpClientParams.setRedirecting
   * 
   * @see #followRedirects
   */
  public void setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    new ClientParamBean(httpClient.getParams())
        .setHandleRedirects(followRedirects);
  }
  
  private static class UseCompressionRequestInterceptor implements
      HttpRequestInterceptor {
    
    public void process(HttpRequest request, HttpContext context)
        throws HttpException, IOException {
      if (!request.containsHeader("Accept-Encoding")) {
        request.addHeader("Accept-Encoding", "gzip, deflate");
      }
    }
  }
  
  private static class UseCompressionResponseInterceptor implements
      HttpResponseInterceptor {
    
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
    
    public InputStream getContent() throws IOException, IllegalStateException {
      return new GZIPInputStream(wrappedEntity.getContent());
    }
    
    public long getContentLength() {
      return -1;
    }
  }
  
  private static class DeflateDecompressingEntity extends
      GzipDecompressingEntity {
    public DeflateDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }
    
    public InputStream getContent() throws IOException, IllegalStateException {
      return new InflaterInputStream(wrappedEntity.getContent());
    }
  }
  
  /**
   * Allow server->client communication to be compressed. Currently gzip and
   * deflate are supported. If the server supports compression the response will
   * be compressed.
   */
  public void setAllowCompression(boolean allowCompression) {
    if (httpClient instanceof DefaultHttpClient) {
      final DefaultHttpClient client = (DefaultHttpClient) httpClient;
      client
          .removeRequestInterceptorByClass(UseCompressionRequestInterceptor.class);
      client
          .removeResponseInterceptorByClass(UseCompressionResponseInterceptor.class);
      if (allowCompression) {
        client.addRequestInterceptor(new UseCompressionRequestInterceptor());
        client.addResponseInterceptor(new UseCompressionResponseInterceptor());
      }
    } else {
      throw new UnsupportedOperationException(
          "HttpClient instance was not of type DefaultHttpClient");
    }
  }
  
  /**
   * Set maximum number of retries to attempt in the event of transient errors.
   * 
   * @param maxRetries
   *          No more than 1 recommended
   * @see #maxRetries
   */
  public void setMaxRetries(int maxRetries) {
    if (maxRetries > 1) {
      log.warn("CommonsHttpSolrServer: maximum Retries " + maxRetries
          + " > 1. Maximum recommended retries is 1.");
    }
    this.maxRetries = maxRetries;
  }
  
  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }
  
  /**
   * Adds the documents supplied by the given iterator.
   * 
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   * 
   * @return the response from the SolrServer
   */
  public UpdateResponse add(Iterator<SolrInputDocument> docIterator)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(docIterator);
    return req.process(this);
  }
  
  /**
   * Adds the beans supplied by the given iterator.
   * 
   * @param beanIterator
   *          the iterator which returns Beans
   * 
   * @return the response from the SolrServer
   */
  public UpdateResponse addBeans(final Iterator<?> beanIterator)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(new Iterator<SolrInputDocument>() {
      
      public boolean hasNext() {
        return beanIterator.hasNext();
      }
      
      public SolrInputDocument next() {
        Object o = beanIterator.next();
        if (o == null) return null;
        return getBinder().toSolrInputDocument(o);
      }
      
      public void remove() {
        beanIterator.remove();
      }
    });
    return req.process(this);
  }
  
  public void shutdown() {
    if (httpClient != null) {
      httpClient.getConnectionManager().shutdown();
    }
  }
  
  public void setDefaultMaxConnectionsPerHost(int max) {
    if (ccm != null) {
      ccm.setDefaultMaxPerRoute(max);
    } else {
      throw new UnsupportedOperationException(
          "Client was created outside of HttpSolrServer");
    }
  }
  
  /**
   * Set the maximum number of connections that can be open at any given time.
   */
  public void setMaxTotalConnections(int max) {
    if (ccm != null) {
      ccm.setMaxTotal(max);
    } else {
      throw new UnsupportedOperationException(
          "Client was created outside of HttpSolrServer");
    }
  }
}
