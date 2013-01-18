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

package org.apache.solr.handler.admin;

import java.net.URL;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * A special Handler that registers all standard admin handlers
 * 
 * @since solr 1.3
 */
public class AdminHandlers implements SolrCoreAware, SolrRequestHandler
{
  NamedList initArgs = null;
  
  private static class StandardHandler {
    final String name;
    final SolrRequestHandler handler;
    
    public StandardHandler( String n, SolrRequestHandler h )
    {
      this.name = n;
      this.handler = h;
    }
  }
  
  /**
   * Save the args and pass them to each standard handler
   */
  @Override
  public void init(NamedList args) {
    this.initArgs = args;
  }
  
  @Override
  public void inform(SolrCore core) 
  {
    String path = null;
    for( Map.Entry<String, SolrRequestHandler> entry : core.getRequestHandlers().entrySet() ) {
      if( entry.getValue() == this ) {
        path = entry.getKey();
        break;
      }
    }
    if( path == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "The AdminHandler is not registered with the current core." );
    }
    if( !path.startsWith( "/" ) ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
        "The AdminHandler needs to be registered to a path.  Typically this is '/admin'" );
    }
    // Remove the parent handler 
    core.registerRequestHandler(path, null);
    if( !path.endsWith( "/" ) ) {
      path += "/";
    }
    
    StandardHandler[] list = new StandardHandler[] {
      new StandardHandler( "luke", new LukeRequestHandler() ),
      new StandardHandler( "system", new SystemInfoHandler() ),
      new StandardHandler( "mbeans", new SolrInfoMBeanHandler() ),
      new StandardHandler( "plugins", new PluginInfoHandler() ),
      new StandardHandler( "threads", new ThreadDumpHandler() ),
      new StandardHandler( "properties", new PropertiesRequestHandler() ),
      new StandardHandler( "logging", new LoggingHandler() ),
      new StandardHandler( "file", new ShowFileRequestHandler() )
    };
    
    for( StandardHandler handler : list ) {
      if( core.getRequestHandler( path+handler.name ) == null ) {
        handler.handler.init( initArgs );
        core.registerRequestHandler( path+handler.name, handler.handler );
        if( handler.handler instanceof SolrCoreAware ) {
          ((SolrCoreAware)handler.handler).inform(core);
        }
      }
    }
  }

  
  @Override
  public void handleRequest(SolrQueryRequest req, SolrQueryResponse rsp) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
        "The AdminHandler should never be called directly" );
  }
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Register Standard Admin Handlers";
  }
  
  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  @Override
  public Category getCategory() {
    return Category.QUERYHANDLER;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }
}
