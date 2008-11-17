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

import java.io.IOException;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.BinaryResponseWriter;
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
  protected final CoreContainer coreContainer;
  protected final String coreName;
  private final SolrRequestParsers _parser;
  
  /**
   * Use the other constructor using a CoreContainer and a name.
   * @param core
   * @deprecated
   */
  @Deprecated
  public EmbeddedSolrServer( SolrCore core )
  {
    if ( core == null ) {
      throw new NullPointerException("SolrCore instance required");
    }
    CoreDescriptor dcore = core.getCoreDescriptor();
    if (dcore == null)
      throw new NullPointerException("CoreDescriptor required");
    
    CoreContainer cores = dcore.getCoreContainer();
    if (cores == null)
      throw new NullPointerException("CoreContainer required");
    
    coreName = dcore.getName();
    coreContainer = cores;
    _parser = new SolrRequestParsers( null );
  }
    
  /**
   * Creates a SolrServer.
   * @param coreContainer the core container
   * @param coreName the core name
   */
  public EmbeddedSolrServer(  CoreContainer coreContainer, String coreName )
  {
    if ( coreContainer == null ) {
      throw new NullPointerException("CoreContainer instance required");
    }
    this.coreContainer = coreContainer;
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
    SolrCore core =  coreContainer.getCore( coreName );
    if( core == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
                               "No such core: " + coreName );
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
          coreContainer != null &&
          path.equals( coreContainer.getAdminPath() ) ) {
        handler = coreContainer.getMultiCoreHandler();
      }
    }
    if( handler == null ) {
      core.close();
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
      core.close();
    }
  }
  
  /**
   * @param req
   * @param rsp
   * @return a response object equivalent to what you get from the XML/JSON/javabin parser. Documents
   * become SolrDocuments, DocList becomes SolrDocumentList etc.
   * 
   * @deprecated use {@link BinaryResponseWriter#getParsedResponse(SolrQueryRequest, SolrQueryResponse)}
   */
  public NamedList<Object> getParsedResponse( SolrQueryRequest req, SolrQueryResponse rsp )
  {
    return BinaryResponseWriter.getParsedResponse(req, rsp);
  }
}
