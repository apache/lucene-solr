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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartBase;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;

/**
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class CommonsHttpSolrServer extends BaseSolrServer 
{
  public static final String AGENT = "Solr["+CommonsHttpSolrServer.class.getName()+"] 1.0"; 
  
  /**
   * The URL of the Solr server.
   */
  protected String _baseURL;
  protected ModifiableSolrParams _invariantParams;
  protected ResponseParser _processor;
  MultiThreadedHttpConnectionManager _connectionManager = new MultiThreadedHttpConnectionManager();
  
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
   * @param baseURL The URL of the Solr server.  For 
   * example, "<code>http://localhost:8983/solr/</code>"
   * if you are using the standard distribution Solr webapp 
   * on your local machine.
   */
  public CommonsHttpSolrServer(URL baseURL) 
  {
    this._baseURL = baseURL.toExternalForm();
    if( this._baseURL.endsWith( "/" ) ) {
      this._baseURL = this._baseURL.substring( 0, this._baseURL.length()-1 );
    }
    
    // by default use the XML one
    _processor = new XMLResponseParser();

    // TODO -- expose these so that people can add things like 'u' & 'p'
    _invariantParams = new ModifiableSolrParams();
    _invariantParams.set( CommonParams.WT, _processor.getWriterType() );
    _invariantParams.set( CommonParams.VERSION, "2.2" );
  }

  //------------------------------------------------------------------------
  //------------------------------------------------------------------------

  public NamedList<Object> request( final SolrRequest request ) throws SolrServerException, IOException
  {
    // TODO -- need to set the WRITER TYPE!!!
    // params.set( SolrParams.WT, wt );
   
    HttpMethod method = null;
    SolrParams params = request.getParams();
    Collection<ContentStream> streams = request.getContentStreams();
    String path = request.getPath();
    if( path == null || !path.startsWith( "/" ) ) {
      path = "/select";
    }
    
    if( params == null ) {
      params = new ModifiableSolrParams();
    }
    if( _invariantParams != null ) {
      params = new DefaultSolrParams( _invariantParams, params );
    }
    
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
        
        if( streams == null || isMultipart ) {
          // Without streams, just post the parameters
          PostMethod post = new PostMethod( url );

          Iterator<String> iter = params.getParameterNamesIterator();
          while( iter.hasNext() ) {
            String p = iter.next();
            String[] vals = params.getParams( p );
            if( vals != null && vals.length > 0 ) {
              for( String v : vals ) {
                post.addParameter( p, (v==null)?null:v );
              }
            }
            else {
              post.addParameter( p, null );
            }
          }

          post.getParams().setContentCharset("UTF-8");   

          if( isMultipart ) {
            int i=0;
              Part[] parts = new Part[streams.size()];
              
              for( ContentStream content : streams ) {
                final ContentStream c = content;
                
                String charSet = null;
                String transferEncoding = null;
                parts[i++] = new PartBase( c.getName(), c.getContentType(), charSet, transferEncoding ) {
                  @Override
                  protected long lengthOfData() throws IOException {
                    return c.getSize();
                  }

                  @Override
                  protected void sendData(OutputStream out) throws IOException {
                    IOUtils.copy( c.getReader(), out );
                  }
                }; 
              }
              
              // Set the multi-part request
              post.setRequestEntity(
                  new MultipartRequestEntity(
                    parts,
                    post.getParams() )
              );
              method = post;
          }

          method = post;
        }
        // It is has one stream, it is the post body, put the params in the URL
        else {
          String pstr = ClientUtils.toQueryString( params, false );
          PostMethod post = new PostMethod( url+pstr );
            
          // Single stream as body
          // Using a loop just to get the first one
          for( ContentStream content : streams ) {
            post.setRequestEntity( 
              new InputStreamRequestEntity(
                content.getStream(),
                content.getContentType()) );
            break;
          }
          method = post;
        }
      }
      else {
        throw new SolrServerException("Unsupported method: "+request.getMethod() );
      }
    }
    catch( IOException ex ) {
      throw new SolrServerException("error reading streams", ex );
    }
    
    method.addRequestHeader( "User-Agent", AGENT );
    
    try {
      // Execute the method.
      //System.out.println( "EXECUTE:"+method.getURI() );
    
      int statusCode = getHttpConnection().executeMethod(method);
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
      Reader reader = new InputStreamReader( method.getResponseBodyAsStream(), charset ); 
      return _processor.processResponse( reader );
    } 
    catch (HttpException e) {
      throw new SolrServerException( e );
    }
    catch (IOException e) {
      throw new SolrServerException( e );
    }
    finally {
      method.releaseConnection();
    }
  }

  //-------------------------------------------------------------------
  //-------------------------------------------------------------------
  
  /**
   * Parameters are added to ever request regardless.  This may be a place to add 
   * something like an authentication token.
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

  public ResponseParser getProcessor() {
    return _processor;
  }

  public void setProcessor(ResponseParser processor) {
    _processor = processor;
  }

  protected HttpClient getHttpConnection() {
    return new HttpClient(_connectionManager);
  }

  public MultiThreadedHttpConnectionManager getConnectionManager() {
    return _connectionManager;
  }
  
  /** set connectionTimeout on the underlying MultiThreadedHttpConnectionManager */
  public void setConnectionTimeout(int timeout) {
    _connectionManager.getParams().setConnectionTimeout(timeout);
  }
  
  /** set maxConnectionsPerHost on the underlying MultiThreadedHttpConnectionManager */
  public void setDefaultMaxConnectionsPerHost(int connections) {
    _connectionManager.getParams().setDefaultMaxConnectionsPerHost(connections);
  }
  
  /** set maxTotalConnection on the underlying MultiThreadedHttpConnectionManager */
  public void setMaxTotalConnections(int connections) {
    _connectionManager.getParams().setMaxTotalConnections(connections);
  }

}
