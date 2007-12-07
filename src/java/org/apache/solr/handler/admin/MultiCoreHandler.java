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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Date;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiCoreParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.MultiCoreParams.MultiCoreAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.MultiCore;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

/**
 * @version $Id$
 * @since solr 1.3
 */
public class MultiCoreHandler extends RequestHandlerBase
{
  public MultiCoreHandler()
  {
    super();
    // Unlike most request handlers, MultiCore initialization 
    // should happen in the constructor...  
  }
  
  
  @Override
  final public void init(NamedList args) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
        "MultiCoreHandler should not be configured in solrconf.xml\n"+
        "it is a special Handler configured directly by the RequestDispatcher" );
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    // Make sure the manager is enabled
    MultiCore manager = MultiCore.getRegistry();
    if( !manager.isEnabled() ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "MultiCore support must be enabled at startup." );
    }
    
    // Pick the action
    SolrParams params = req.getParams();
    MultiCoreAction action = MultiCoreAction.STATUS;
    String a = params.get( MultiCoreParams.ACTION );
    if( a != null ) {
      action = MultiCoreAction.get( a );
      if( action == null ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Unknown 'action' value.  Use: "+MultiCoreAction.values() );
      }
    }
    
    // Select the core
    SolrCore core = null;
    String cname = params.get( MultiCoreParams.CORE );
    if( cname != null ) {
      core = manager.getCore( cname );
      if( core == null ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "Unknown core: "+cname );
      }
    }
    
    // Handle a Status Request
    //---------------------------------------------------------
    if( action == MultiCoreAction.STATUS ) {
      SolrCore defaultCore = manager.getDefaultCore();
      NamedList<Object> status = new SimpleOrderedMap<Object>();
      if( core == null ) {
        for( SolrCore c : manager.getCores() ) {
          status.add( c.getName(), getCoreStatus( c, c==defaultCore ) );
        }
      }
      else {
        status.add( core.getName(), getCoreStatus( core, core==defaultCore ) );
      }
      rsp.add( "status", status );
    }
    else if( core == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
        "Action '"+action+"' requires a core name." );
    }
    else {
      switch( action ) {
      case SETASDEFAULT:
        manager.setDefaultCore( core );
        rsp.add( "default", core.getName() );
        break;
        
      case RELOAD: {
        manager.reload( core );
        break;
      } 
        
      default:
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "TODO: IMPLEMENT: " + action );
      }

      // Should we persist the changes?
      if( params.getBool( MultiCoreParams.PERSISTENT, manager.isPersistent() ) ) {
        rsp.add( "TODO", "SAVE THE CHANGES: "+manager.getConfigFile().getAbsolutePath() );
      }
    }
  }
  
  private static NamedList<Object> getCoreStatus( SolrCore core, boolean isDefault ) throws IOException
  {
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    info.add( "name", core.getName() );
    info.add( "instanceDir", core.getResourceLoader().getInstanceDir() );
    info.add( "dataDir", core.getDataDir() );
    info.add( "startTime", new Date( core.getStartTime() ) );
    info.add( "uptime", System.currentTimeMillis()-core.getStartTime() );
    info.add( "isDefault", isDefault );
    RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
    info.add( "index", LukeRequestHandler.getIndexInfo( searcher.get().getReader(), false ) );
    searcher.decref();
    return info;
  }
  
  
  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage Multiple Solr Cores";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }
}
