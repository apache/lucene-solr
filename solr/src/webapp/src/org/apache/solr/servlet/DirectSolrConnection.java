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

package org.apache.solr.servlet;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;

/**
 * DirectSolrConnection provides an interface to Solr that is similar to
 * the the HTTP interface, but does not require an HTTP connection.
 * 
 * This class is designed to be as simple as possible and allow for more flexibility
 * in how you interface to Solr.
 * 
 * @version $Id$
 * @since solr 1.2
 */
public class DirectSolrConnection 
{
  final SolrCore core;
  final SolrRequestParsers parser;
  
  /**
   * Initialize using the static singleton SolrCore.getSolrCore().
   * 
   * @deprecated use {@link #DirectSolrConnection(SolrCore)}
   */
  @Deprecated
  public DirectSolrConnection()
  {
    this( SolrCore.getSolrCore() );
  }

  /**
   * Initialize using an explicit SolrCore
   */
  public DirectSolrConnection( SolrCore c )
  {
    core = c;
    parser = new SolrRequestParsers( c.getSolrConfig() );
  }

  /**
   * This constructor is designed to make it easy for JNI embedded applications 
   * to setup the entire solr environment with a simple interface.  It takes three parameters:
   * 
   * <code>instanceDir:</code> The solr instance directory.  If null, it will check the standard 
   * places first (JNDI,properties,"solr" directory)
   * 
   * <code>dataDir:</code> where the index is stored. 
   * 
   * <code>loggingPath:</code> Path to a java.util.logging.config.file.  If the path represents
   * an absolute path or is relative to the CWD, it will use that.  Next it will try a path 
   * relative to the instanceDir.  If none of these files exist, it will error.
   */
  public DirectSolrConnection( String instanceDir, String dataDir, String loggingPath )
  {
    // If a loggingPath is specified, try using that (this needs to happen first)
    if( loggingPath != null ) {
      File loggingConfig = new File( loggingPath );
      if( !loggingConfig.exists() && instanceDir != null ) {
        loggingConfig = new File( new File(instanceDir), loggingPath  );
      }
      if( loggingConfig.exists() ) {
        System.setProperty("java.util.logging.config.file", loggingConfig.getAbsolutePath() ); 
      }
      else {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, "can not find logging file: "+loggingConfig );
      }
    }
    
    if( instanceDir == null ) {
      instanceDir = SolrResourceLoader.locateInstanceDir();
    }
    
    // Initialize 
    try {
      CoreContainer cores = new CoreContainer(new SolrResourceLoader(instanceDir));
      SolrConfig solrConfig = new SolrConfig(instanceDir, SolrConfig.DEFAULT_CONF_FILE, null);
      CoreDescriptor dcore = new CoreDescriptor(cores, "", solrConfig.getResourceLoader().getInstanceDir());
      IndexSchema indexSchema = new IndexSchema(solrConfig, instanceDir+"/conf/schema.xml", null);
      core = new SolrCore( null, dataDir, solrConfig, indexSchema, dcore);
      cores.register("", core, false);
      parser = new SolrRequestParsers( solrConfig );
    } 
    catch (Exception ee) {
      throw new RuntimeException(ee);
    }
  }
  

  /**
   * For example:
   * 
   * String json = solr.request( "/select?qt=dismax&wt=json&q=...", null );
   * String xml = solr.request( "/update", "&lt;add><doc><field ..." );
   */
  public String request( String pathAndParams, String body ) throws Exception
  {
    String path = null;
    SolrParams params = null;
    int idx = pathAndParams.indexOf( '?' );
    if( idx > 0 ) {
      path = pathAndParams.substring( 0, idx );
      params = SolrRequestParsers.parseQueryString( pathAndParams.substring(idx+1) );
    }
    else {
      path= pathAndParams;
      params = new MapSolrParams( new HashMap<String, String>() );
    }

    return request(path, params, body);
  }


  public String request(String path, SolrParams params, String body) throws Exception
  {
    // Extract the handler from the path or params
    SolrRequestHandler handler = core.getRequestHandler( path );
    if( handler == null ) {
      if( "/select".equals( path ) || "/select/".equalsIgnoreCase( path) ) {
        if (params == null)
          params = new MapSolrParams( new HashMap<String, String>() );        
        String qt = params.get( CommonParams.QT );
        handler = core.getRequestHandler( qt );
        if( handler == null ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
        }
      }
    }
    if( handler == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+path );
    }

    return request(handler, params, body);
  }

  public String request(SolrRequestHandler handler, SolrParams params, String body) throws Exception
  {
    if (params == null)
      params = new MapSolrParams( new HashMap<String, String>() );

    // Make a stream for the 'body' content
    List<ContentStream> streams = new ArrayList<ContentStream>( 1 );
    if( body != null && body.length() > 0 ) {
      streams.add( new ContentStreamBase.StringStream( body ) );
    }

    SolrQueryRequest req = null;
    try {
      req = parser.buildRequestFrom( core, params, streams );
      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute( handler, req, rsp );
      if( rsp.getException() != null ) {
        throw rsp.getException();
      }

      // Now write it out
      QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
      StringWriter out = new StringWriter();
      responseWriter.write(out, req, rsp);
      return out.toString();
    } finally {
      if (req != null) {
        req.close();
      }
    }
  }



  /**
   * Use this method to close the underlying SolrCore.
   * 
   * @since solr 1.3
   */
  public void close() {
    core.close();
  }
}
