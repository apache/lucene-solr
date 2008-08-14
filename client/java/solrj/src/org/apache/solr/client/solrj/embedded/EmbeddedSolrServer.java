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

package org.apache.solr.client.solrj.embedded;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.BinaryResponseWriter;
import org.apache.solr.request.QueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.SolrRequestParsers;

/**
 * SolrServer that connects directly to SolrCore
 * 
 * TODO -- this implementation sends the response to XML and then parses it.  
 * It *should* be able to convert the response directly into a named list.
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class EmbeddedSolrServer extends SolrServer
{
  
  protected final CoreContainer multicore; // either cores
  protected final SolrCore core; // or single core
  protected final String coreName;  // use CoreContainer registry

  private final SolrRequestParsers _parser;
  
  public EmbeddedSolrServer( SolrCore core )
  {
    if ( core == null ) {
      throw new NullPointerException("SolrCore instance required");
    }
    this.core = core;
    if (core.getCoreDescriptor() != null) {
      this.multicore = core.getCoreDescriptor().getMultiCore();
      this.coreName = core.getCoreDescriptor().getName();
    } else {
      this.multicore = null;
      this.coreName = null;
    }
    _parser = new SolrRequestParsers( null );
  }
    
  public EmbeddedSolrServer(  CoreContainer multicore, String coreName )
  {
    if ( multicore == null ) {
      throw new NullPointerException("CoreContainer instance required");
    }
    this.core = null;
    this.multicore = multicore;
    this.coreName = coreName == null? "" : coreName;

    _parser = new SolrRequestParsers( null );
  }
  
  @Override
  public NamedList<Object> request(SolrRequest request) throws SolrServerException, IOException 
  {
    String path = request.getPath();
    if( path == null || !path.startsWith( "/" ) ) {
      path = "/select";
    }

    // Check for cores action
    SolrCore core = this.core;
    if( core == null )
      core = multicore.getCore( coreName );
    // solr-647
    //else
    //  core = core.open();
    if( core == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          coreName == null? "No core": "No such core: " + coreName );
    }
    
    SolrParams params = request.getParams();
    if( params == null ) {
      params = new ModifiableSolrParams();
    }
    
    // Extract the handler from the path or params
    SolrRequestHandler handler = core.getRequestHandler( path );
    if( handler == null ) {
      if( "/select".equals( path ) || "/select/".equalsIgnoreCase( path) ) {
        String qt = params.get( CommonParams.QT );
        handler = core.getRequestHandler( qt );
        if( handler == null ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
        }
      }
      // Perhaps the path is to manage the cores
      if( handler == null &&
          multicore != null &&
          path.equals( multicore.getAdminPath() ) ) {
        handler = multicore.getMultiCoreHandler();
      }
    }
    if( handler == null ) {
      // solr-647
      // core.close();
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+path );
    }

    try {
      SolrQueryRequest req = _parser.buildRequestFrom( core, params, request.getContentStreams() );
      req.getContext().put( "path", path );
      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute( handler, req, rsp );
      if( rsp.getException() != null ) {
        throw new SolrServerException( rsp.getException() );
      }
      
      // Now write it out
      NamedList<Object> normalized = getParsedResponse(req, rsp);
      req.close();
      return normalized;
    }
    catch( IOException iox ) {
      throw iox;
    }
    catch( Exception ex ) {
      throw new SolrServerException( ex );
    }
    finally {
      // solr-647
      // core.close();
    }
  }
  
  /**
   * TODO -- in the future, this could perhaps transform the NamedList without serializing it
   * then parsing it from the serialized form.
   * 
   * @param req
   * @param rsp
   * @return a response object equivalent to what you get from the XML/JSON/javabin parser. Documents
   * become SolrDocuments, DocList becomes SolrDocumentList etc.
   */
  public NamedList<Object> getParsedResponse( SolrQueryRequest req, SolrQueryResponse rsp )
  {
    try {
      BinaryResponseWriter writer = new BinaryResponseWriter();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      writer.write( bos, req, rsp );
      BinaryResponseParser parser = new BinaryResponseParser();
      return parser.processResponse( new ByteArrayInputStream( bos.toByteArray() ), "UTF-8" );
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
}
