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
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

/**
 * @version $Id$
 * @since solr 1.3
 */
public abstract class MultiCoreHandler extends RequestHandlerBase
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
  
  /**
   * The instance of multicore this handler handles.
   * This should be the MultiCore instance that created this handler.
   * @return a MultiCore instance
   */
  public abstract MultiCore getMultiCore();
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    // Make sure the manager is enabled
    MultiCore manager = getMultiCore();
    if( !manager.isEnabled() ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "MultiCore support must be enabled at startup." );
    }
    boolean do_persist = false;
    
    // Pick the action
    SolrParams params = req.getParams();
    SolrParams required = params.required();
    MultiCoreAction action = MultiCoreAction.STATUS;
    String a = params.get( MultiCoreParams.ACTION );
    if( a != null ) {
      action = MultiCoreAction.get( a );
      if( action == null ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Unknown 'action' value.  Use: "+MultiCoreAction.values() );
      }
    }
    
    SolrCore core = null;
    // Handle a core creation
    //---------------------------------------------------------
    if (action == MultiCoreAction.CREATE) {
      CoreDescriptor dcore = new CoreDescriptor();
      dcore.init(params.get(MultiCoreParams.NAME),
                params.get(MultiCoreParams.INSTANCE_DIR));
      
      // fillup optional parameters
      String opts = params.get(MultiCoreParams.CONFIG);
      if (opts != null)
        dcore.setConfigName(opts);
      
      opts = params.get(MultiCoreParams.SCHEMA);
      if (opts != null)
        dcore.setSchemaName(opts);
      
      core = manager.create(dcore);
      rsp.add("core", core.getName());
      do_persist = manager.isPersistent();
    }
    else {
      // Select the core
      String cname = params.get( MultiCoreParams.CORE );
      if( cname != null ) {
        core = manager.getCore(cname);
        if( core == null ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
              "Unknown core: "+cname );
        }
      }

      // Handle a Status Request
      //---------------------------------------------------------
      if( action == MultiCoreAction.STATUS ) {
        do_persist = false; // no state change
        NamedList<Object> status = new SimpleOrderedMap<Object>();
        if( core == null ) {
          for (CoreDescriptor d : manager.getDescriptors()) {
            status.add(d.getName(), getCoreStatus( d.getCore() ) );
          }
        } 
        else {
          status.add(core.getName(), getCoreStatus(core) );
        }
        rsp.add( "status", status );
      } 
      else if (core == null) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Action '"+action+"' requires a core name." );
      } 
      else {
        // Handle all other
        //---------------------------------------------------------
        do_persist = params.getBool(MultiCoreParams.PERSISTENT, manager.isPersistent());
        switch( action ) {
          case RELOAD: {
            manager.reload( manager.getDescriptor( core.getName() ) );
            do_persist = false; // no change on reload
            break;
          }
  
          case SWAP: {
            String name = required.get( MultiCoreParams.WITH );
            CoreDescriptor swap = manager.getDescriptor( name );
            
            if( swap == null ) {
              throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                  "Unknown core: "+name );
            }
            manager.swap( manager.getDescriptor( core.getName() ), swap );
            break;
          } 
        
          case PERSIST: {
            do_persist = true;
            break;
          } 
          
          default: {
            throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
                "TODO: IMPLEMENT: " + action );
          }
        } // switch
      }
    }
    
    // Should we persist the changes?
    if (do_persist) {
      manager.persist();
      rsp.add("saved", manager.getConfigFile().getAbsolutePath());
    }
  }
  
  private static NamedList<Object> getCoreStatus( SolrCore core ) throws IOException
  {
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    info.add( "name", core.getName() );
    info.add( "instanceDir", core.getResourceLoader().getInstanceDir() );
    info.add( "dataDir", core.getDataDir() );
    info.add( "startTime", new Date( core.getStartTime() ) );
    info.add( "uptime", System.currentTimeMillis()-core.getStartTime() );
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
