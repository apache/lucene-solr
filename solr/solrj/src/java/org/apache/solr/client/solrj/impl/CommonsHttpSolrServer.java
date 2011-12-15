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
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
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
 * The {@link CommonsHttpSolrServer} uses the Apache Commons HTTP Client to connect to solr. 
 * <pre class="prettyprint" >SolrServer server = new CommonsHttpSolrServer( url );</pre>
 * 
 *
 * @since solr 1.3
 */
public class CommonsHttpSolrServer extends SolrServer 
{
  private static final long serialVersionUID = 1L;

  /**
   * User-Agent String as identified by the HTTP request by the {@link
   * org.apache.commons.httpclient.HttpClient HttpClient} to the Solr
   * server from the client.
   */
  public static final String AGENT = "Solr["+CommonsHttpSolrServer.class.getName()+"] 1.0"; 
  
  private static Logger log = LoggerFactory.getLogger(CommonsHttpSolrServer.class);

  /**
   * The URL of the Solr server.
   */
  protected String _baseURL;
  
  /**
   * Default value: null / empty. <p/>
   * Parameters that are added to every request regardless.  This may be a place to add 
   * something like an authentication token.
   */
  protected ModifiableSolrParams _invariantParams;
  
  /**
   * Default response parser is BinaryResponseParser <p/>
   * This parser represents the default Response Parser chosen to
   * parse the response if the parser were not specified as part of
   * the request.
   * @see org.apache.solr.client.solrj.impl.BinaryResponseParser
   */
  protected ResponseParser _parser;

  /**
   * The RequestWriter used to write all requests to Solr
   * @see org.apache.solr.client.solrj.request.RequestWriter
   */
  protected RequestWriter requestWriter = new RequestWriter();
  
  private final HttpClient _httpClient;
  
  /**
   * This defaults to false under the
   * assumption that if you are following a redirect to get to a Solr
   * installation, something is misconfigured somewhere.
   */
  private boolean _followRedirects = false;
  
  /**
   * If compression is enabled, both gzip and deflate compression will
   * be accepted in the HTTP response.
   */
  private boolean _allowCompression = false;
  
  /**
   * Maximum number of retries to attempt in the event of transient
   * errors.  Default: 0 (no) retries. No more than 1 recommended.
   */
  private int _maxRetries = 0;
  
  /**
   * Default value: <b> false </b> 
   * <p>
   * If set to false, add the query parameters as URL-encoded parameters to the
   * POST request in a single part. If set to true, create a new part of a
   * multi-part request for each parameter.
   * 
   * The reason for adding all parameters as parts of a multi-part request is
   * that this allows us to specify the charset -- standards for single-part
   * requests specify that non-ASCII characters should be URL-encoded, but don't
   * specify the charset of the characters to be URL-encoded (cf.
   * http://www.w3.org/TR/html401/interact/forms.html#form-content-type).
   * Therefore you have to rely on your servlet container doing the right thing
   * with single-part requests.
   */
  private boolean useMultiPartPost;
  
  /**  
   * @param solrServerUrl The URL of the Solr server.  For 
   * example, "<code>http://localhost:8983/solr/</code>"
   * if you are using the standard distribution Solr webapp 
   * on your local machine.
   */
  public CommonsHttpSolrServer(String solrServerUrl) throws MalformedURLException {
    this(new URL(solrServerUrl));
  }

  /**
   * Talk to the Solr server via the given HttpClient.  The connection manager
   * for the client should be a MultiThreadedHttpConnectionManager if this
   * client is being reused across SolrServer instances, or of multiple threads
   * will use this SolrServer.
   */
  public CommonsHttpSolrServer(String solrServerUrl, HttpClient httpClient) throws MalformedURLException {
    this(new URL(solrServerUrl), httpClient, new BinaryResponseParser(), false);
  }
  
  public CommonsHttpSolrServer(String solrServerUrl, HttpClient httpClient, boolean useMultiPartPost) throws MalformedURLException {
    this(new URL(solrServerUrl), httpClient, new BinaryResponseParser(), useMultiPartPost);
  }

  public CommonsHttpSolrServer(String solrServerUrl, HttpClient httpClient, ResponseParser parser) throws MalformedURLException {
    this(new URL(solrServerUrl), httpClient, parser, false);
  }

  /**
   * @param baseURL The URL of the Solr server.  For example,
   * "<code>http://localhost:8983/solr/</code>" if you are using the
   * standard distribution Solr webapp on your local machine.
   */
  public CommonsHttpSolrServer(URL baseURL) 
  {
    this(baseURL, null, new BinaryResponseParser(), false);
  }

  public CommonsHttpSolrServer(URL baseURL, HttpClient client) {
    this(baseURL, client, new BinaryResponseParser(), false);
  }

  /**
   * 
   * @see #useMultiPartPost
   */
  public CommonsHttpSolrServer(URL baseURL, HttpClient client, boolean useMultiPartPost) {
    this(baseURL, client, new BinaryResponseParser(), useMultiPartPost);
  }

  /**
   * @see #useMultiPartPost
   * @see #_parser
   */
  public CommonsHttpSolrServer(URL baseURL, HttpClient client, ResponseParser parser, boolean useMultiPartPost) {
    _baseURL = baseURL.toExternalForm();
    if( _baseURL.endsWith( "/" ) ) {
      _baseURL = _baseURL.substring( 0, _baseURL.length()-1 );
    }
    if( _baseURL.indexOf( '?' ) >=0 ) {
      throw new RuntimeException( "Invalid base url for solrj.  The base URL must not contain parameters: "+_baseURL );
    }

    if (client == null) {
      _httpClient = new HttpClient(new MultiThreadedHttpConnectionManager()) ;

      // prevent retries  (note: this didn't work when set on mgr.. needed to be set on client)
      DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(0, false);
      _httpClient.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, retryhandler);

      // set some better defaults if we created a new connection manager and client

      // increase the default connections
      this.setDefaultMaxConnectionsPerHost( 32 );  // 2
      this.setMaxTotalConnections( 128 ); // 20
    } else {
      _httpClient = client;
    }

    _parser = parser;
    
    this.useMultiPartPost = useMultiPartPost;
  }


  //------------------------------------------------------------------------
  //------------------------------------------------------------------------

  /**
   * Process the request.  If {@link org.apache.solr.client.solrj.SolrRequest#getResponseParser()} is null, then use
   * {@link #getParser()}
   * @param request The {@link org.apache.solr.client.solrj.SolrRequest} to process
   * @return The {@link org.apache.solr.common.util.NamedList} result
   * @throws SolrServerException
   * @throws IOException
   *
   * @see #request(org.apache.solr.client.solrj.SolrRequest, org.apache.solr.client.solrj.ResponseParser)
   */
  @Override
  public NamedList<Object> request( final SolrRequest request ) throws SolrServerException, IOException
  {
    ResponseParser responseParser = request.getResponseParser();
    if (responseParser == null) {
      responseParser = _parser;
    }
    return request(request, responseParser);
  }

  
  public NamedList<Object> request(final SolrRequest request, ResponseParser processor) throws SolrServerException, IOException {
    HttpMethod method = null;
    InputStream is = null;
    SolrParams params = request.getParams();
    Collection<ContentStream> streams = requestWriter.getContentStreams(request);
    String path = requestWriter.getPath(request);
    if( path == null || !path.startsWith( "/" ) ) {
      path = "/select";
    }
    
    ResponseParser parser = request.getResponseParser();
    if( parser == null ) {
      parser = _parser;
    }
    
    // The parser 'wt=' and 'version=' params are used instead of the original params
    ModifiableSolrParams wparams = new ModifiableSolrParams();
    wparams.set( CommonParams.WT, parser.getWriterType() );
    wparams.set( CommonParams.VERSION, parser.getVersion());
    params = SolrParams.wrapDefaults(wparams, params);
    params = SolrParams.wrapDefaults(_invariantParams, params);

    int tries = _maxRetries + 1;
    try {
      while( tries-- > 0 ) {
        // Note: since we aren't doing intermittent time keeping
        // ourselves, the potential non-timeout latency could be as
        // much as tries-times (plus scheduling effects) the given
        // timeAllowed.
        try {
          if( SolrRequest.METHOD.GET == request.getMethod() ) {
            if( streams != null ) {
              throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!" );
            }
            method = new GetMethod( _baseURL + path + ClientUtils.toQueryString( params, false ) );
          }
          else if( SolrRequest.METHOD.POST == request.getMethod() ) {

            String url = _baseURL + path;
            boolean isMultipart = ( streams != null && streams.size() > 1 );

            if (streams == null || isMultipart) {
              PostMethod post = new PostMethod(url);
              post.getParams().setContentCharset("UTF-8");
              if (!this.useMultiPartPost && !isMultipart) {
                post.addRequestHeader("Content-Type",
                    "application/x-www-form-urlencoded; charset=UTF-8");
              }

              List<Part> parts = new LinkedList<Part>();
              Iterator<String> iter = params.getParameterNamesIterator();
              while (iter.hasNext()) {
                String p = iter.next();
                String[] vals = params.getParams(p);
                if (vals != null) {
                  for (String v : vals) {
                    if (this.useMultiPartPost || isMultipart) {
                      parts.add(new StringPart(p, v, "UTF-8"));
                    } else {
                      post.addParameter(p, v);
                    }
                  }
                }
              }

              if (isMultipart) {
                int i = 0;
                for (ContentStream content : streams) {
                  final ContentStream c = content;

                  String charSet = null;
                  PartSource source = new PartSource() {
                    @Override
                    public long getLength() {
                      return c.getSize();
                    }
                      
                    public String getFileName() {
                      return c.getName();
                    }

                    @Override
                    public InputStream createInputStream() throws IOException {
                      return c.getStream();
                    }
                  };
                
                  parts.add(new FilePart(c.getName(), source, 
                                         c.getContentType(), charSet));
                }
              }
              if (parts.size() > 0) {
                post.setRequestEntity(new MultipartRequestEntity(parts
                    .toArray(new Part[parts.size()]), post.getParams()));
              }

              method = post;
            }
            // It is has one stream, it is the post body, put the params in the URL
            else {
              String pstr = ClientUtils.toQueryString(params, false);
              PostMethod post = new PostMethod(url + pstr);

              // Single stream as body
              // Using a loop just to get the first one
              final ContentStream[] contentStream = new ContentStream[1];
              for (ContentStream content : streams) {
                contentStream[0] = content;
                break;
              }
              if (contentStream[0] instanceof RequestWriter.LazyContentStream) {
                post.setRequestEntity(new RequestEntity() {
                  public long getContentLength() {
                    return -1;
                  }

                  public String getContentType() {
                    return contentStream[0].getContentType();
                  }

                  public boolean isRepeatable() {
                    return false;
                  }

                  public void writeRequest(OutputStream outputStream) throws IOException {
                    ((RequestWriter.LazyContentStream) contentStream[0]).writeTo(outputStream);
                  }
                }
                );

              } else {
                is = contentStream[0].getStream();
                post.setRequestEntity(new InputStreamRequestEntity(is, contentStream[0].getContentType()));
              }
              method = post;
            }
          }
          else {
            throw new SolrServerException("Unsupported method: "+request.getMethod() );
          }
        }
        catch( NoHttpResponseException r ) {
          // This is generally safe to retry on
          method.releaseConnection();
          method = null;
          if(is != null) {
            is.close();
          }
          // If out of tries then just rethrow (as normal error).
          if( ( tries < 1 ) ) {
            throw r;
          }
          //log.warn( "Caught: " + r + ". Retrying..." );
        }
      }
    }
    catch( IOException ex ) {
      throw new SolrServerException("error reading streams", ex );
    }

    method.setFollowRedirects( _followRedirects );
    method.addRequestHeader( "User-Agent", AGENT );
    if( _allowCompression ) {
      method.setRequestHeader( new Header( "Accept-Encoding", "gzip,deflate" ) );
    }

    try {
      // Execute the method.
      //System.out.println( "EXECUTE:"+method.getURI() );

      int statusCode = _httpClient.executeMethod(method);
      if (statusCode != HttpStatus.SC_OK) {
        StringBuilder msg = new StringBuilder();
        msg.append( method.getStatusLine().getReasonPhrase() );
        msg.append( "\n\n" );
        msg.append( method.getStatusText() );
        msg.append( "\n\n" );
        msg.append( "request: "+method.getURI() );
        throw new SolrException(statusCode, java.net.URLDecoder.decode(msg.toString(), "UTF-8") );
      }

      // Read the contents
      String charset = "UTF-8";
      if( method instanceof HttpMethodBase ) {
        charset = ((HttpMethodBase)method).getResponseCharSet();
      }
      InputStream respBody = method.getResponseBodyAsStream();
      // Jakarta Commons HTTPClient doesn't handle any
      // compression natively.  Handle gzip or deflate
      // here if applicable.
      if( _allowCompression ) {
        Header contentEncodingHeader = method.getResponseHeader( "Content-Encoding" );
        if( contentEncodingHeader != null ) {
          String contentEncoding = contentEncodingHeader.getValue();
          if( contentEncoding.contains( "gzip" ) ) {
            //log.debug( "wrapping response in GZIPInputStream" );
            respBody = new GZIPInputStream( respBody );
          }
          else if( contentEncoding.contains( "deflate" ) ) {
            //log.debug( "wrapping response in InflaterInputStream" );
            respBody = new InflaterInputStream(respBody);
          }
        }
        else {
          Header contentTypeHeader = method.getResponseHeader( "Content-Type" );
          if( contentTypeHeader != null ) {
            String contentType = contentTypeHeader.getValue();
            if( contentType != null ) {
              if( contentType.startsWith( "application/x-gzip-compressed" ) ) {
                //log.debug( "wrapping response in GZIPInputStream" );
                respBody = new GZIPInputStream( respBody );
              }
              else if ( contentType.startsWith("application/x-deflate") ) {
                //log.debug( "wrapping response in InflaterInputStream" );
                respBody = new InflaterInputStream(respBody);
              }
            }
          }
        }
      }
      return processor.processResponse(respBody, charset);
    }
    catch (HttpException e) {
      throw new SolrServerException( e );
    }
    catch (IOException e) {
      throw new SolrServerException( e );
    }
    finally {
      method.releaseConnection();
      if(is != null) {
        is.close();
      }
    }
  }

  //-------------------------------------------------------------------
  //-------------------------------------------------------------------
  
  /**
   * Retrieve the default list of parameters are added to every request regardless.
   * 
   * @see #_invariantParams
   */
  public ModifiableSolrParams getInvariantParams()
  {
    return _invariantParams;
  }

  public String getBaseURL() {
    return _baseURL;
  }

  public void setBaseURL(String baseURL) {
    this._baseURL = baseURL;
  }

  public ResponseParser getParser() {
    return _parser;
  }

  /**
   * Note: This setter method is <b>not thread-safe</b>.
   * @param processor Default Response Parser chosen to parse the response if the parser were not specified as part of the request.
   * @see  org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser processor) {
    _parser = processor;
  }

  public HttpClient getHttpClient() {
    return _httpClient;
  }

  private HttpConnectionManager getConnectionManager() {
    return _httpClient.getHttpConnectionManager();
  }
  
  /** set connectionTimeout on the underlying HttpConnectionManager
   * @param timeout Timeout in milliseconds 
   **/ 
  public void setConnectionTimeout(int timeout) {
    getConnectionManager().getParams().setConnectionTimeout(timeout);
  }
  
  /** set connectionManagerTimeout on the HttpClient.
   * @param timeout Timeout in milliseconds
   * @deprecated Use {@link #setConnectionManagerTimeout(long)} **/
  @Deprecated
  public void setConnectionManagerTimeout(int timeout) {
    _httpClient.getParams().setConnectionManagerTimeout(timeout);
  }
  
  /**
   * Sets soTimeout (read timeout) on the underlying
   * HttpConnectionManager.  This is desirable for queries, but
   * probably not for indexing.
   * 
   * @param timeout Timeout in milliseconds
   */
  public void setConnectionManagerTimeout(long timeout) {
    _httpClient.getParams().setConnectionManagerTimeout(timeout);
  }
  
  
  /**
   * Sets soTimeout (read timeout) on the underlying
   * HttpConnectionManager.  This is desirable for queries, but
   * probably not for indexing.
   * @param timeout Timeout in milliseconds  
   **/
  public void setSoTimeout(int timeout) {
    getConnectionManager().getParams().setSoTimeout(timeout);
  }
  
  /** set maxConnectionsPerHost on the underlying HttpConnectionManager */
  public void setDefaultMaxConnectionsPerHost(int connections) {
    getConnectionManager().getParams().setDefaultMaxConnectionsPerHost(connections);
  }
  
  /** set maxTotalConnection on the underlying HttpConnectionManager */
  public void setMaxTotalConnections(int connections) {
    getConnectionManager().getParams().setMaxTotalConnections(connections);
  }

  /**
   * set followRedirects.  
   * @see #_followRedirects
   */
  public void setFollowRedirects( boolean followRedirects ) {
    _followRedirects = followRedirects;
  }

  /**
   * set allowCompression.  
   * @see #_allowCompression
   */
  public void setAllowCompression( boolean allowCompression ) {
    _allowCompression = allowCompression;
  }

  /**
   * set maximum number of retries to attempt in the event of
   * transient errors.
   * @param maxRetries No more than 1 recommended
   * @see #_maxRetries
   */
  public void setMaxRetries( int maxRetries ) {
    if (maxRetries > 1) { 
      log.warn("CommonsHttpSolrServer: maximum Retries " + maxRetries + " > 1. Maximum recommended retries is 1.");
    }
    _maxRetries = maxRetries;
  }

  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  /**
   * Adds the documents supplied by the given iterator.
   * @param docIterator  the iterator which returns SolrInputDocument instances
   * @return the response from the SolrServer
   */
  public UpdateResponse add(Iterator<SolrInputDocument> docIterator)
          throws SolrServerException, IOException {
    return add(docIterator, -1);
  }
  
  /**
   * Adds the documents supplied by the given iterator, specifying max time before they become committed
   * @param docIterator  the iterator which returns SolrInputDocument instances
   * @param commitWithinMs  the time in milliseconds before a commit automatically is triggered
   * @return the response from the SolrServer
   */
  public UpdateResponse add(Iterator<SolrInputDocument> docIterator, int commitWithinMs)
          throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(docIterator);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Adds the beans supplied by the given iterator
   * @param beanIterator  the iterator which returns Beans
   * @return the response from the SolrServer
   */
  public UpdateResponse addBeans(final Iterator<?> beanIterator)
          throws SolrServerException, IOException {
    return addBeans(beanIterator, -1);
  }
  
  /**
   * Adds the beans supplied by the given iterator, specifying max time before they become committed
   * @param commitWithinMs  the time in milliseconds before a commit automatically is triggered
   * @param beanIterator  the iterator which returns Beans
   * @return the response from the SolrServer
   */
  public UpdateResponse addBeans(final Iterator<?> beanIterator, int commitWithinMs)
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
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }
}
