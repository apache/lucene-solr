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
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
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
public abstract class CoreAdminHandler extends RequestHandlerBase
{
  public CoreAdminHandler()
  {
    super();
    // Unlike most request handlers, CoreContainer initialization 
    // should happen in the constructor...  
  }
  
  
  @Override
  final public void init(NamedList args) {
    throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
        "CoreAdminHandler should not be configured in solrconf.xml\n"+
        "it is a special Handler configured directly by the RequestDispatcher" );
  }
  
  /**
   * The instance of CoreContainer this handler handles.
   * This should be the CoreContainer instance that created this handler.
   * @return a CoreContainer instance
   */
  public abstract CoreContainer getCoreContainer();
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception 
  {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if( cores == null ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Core container instance missing" );
    }
    boolean do_persist = false;
    
    // Pick the action
    SolrParams params = req.getParams();
    SolrParams required = params.required();
    CoreAdminAction action = CoreAdminAction.STATUS;
    String a = params.get( CoreAdminParams.ACTION );
    if( a != null ) {
      action = CoreAdminAction.get( a );
      if( action == null ) {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "Unknown 'action' value.  Use: "+CoreAdminAction.values() );
      }
    }
    String cname = params.get( CoreAdminParams.CORE );
    
    switch(action) {
      case CREATE: {
        CoreDescriptor dcore = new CoreDescriptor(cores);
        dcore.init(params.get(CoreAdminParams.NAME),
                  params.get(CoreAdminParams.INSTANCE_DIR));

        // fillup optional parameters
        String opts = params.get(CoreAdminParams.CONFIG);
        if (opts != null)
          dcore.setConfigName(opts);

        opts = params.get(CoreAdminParams.SCHEMA);
        if (opts != null)
          dcore.setSchemaName(opts);

        SolrCore core = cores.create(dcore);
        rsp.add("core", core.getName());
        do_persist = cores.isPersistent();
        break;
      }
      
      case STATUS: {
        NamedList<Object> status = new SimpleOrderedMap<Object>();
        if( cname == null ) {
          for (CoreDescriptor d : cores.getDescriptors()) {
            cname = d.getName();
            status.add(d.getName(), getCoreStatus( cores, cname  ) );
          }
        } 
        else {
          status.add(cname, getCoreStatus( cores, cname  ) );
        }
        rsp.add( "status", status );
        do_persist = false; // no state change
        break;
       
      }
      
      case PERSIST: {
        do_persist = true;
        break;
      }

      case RELOAD: {
        cores.reload( cname  );
        do_persist = false; // no change on reload
        break;
      }

      case SWAP: {
        do_persist = params.getBool(CoreAdminParams.PERSISTENT, cores.isPersistent());
        String with = required.get( CoreAdminParams.WITH );
        cores.swap( cname, with );
        break;
      } 

      default: {
        throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
            "TODO: IMPLEMENT: " + action );
      }
    } // switch
      
    // Should we persist the changes?
    if (do_persist) {
      cores.persist();
      rsp.add("saved", cores.getConfigFile().getAbsolutePath());
    }
  }
  
  private static NamedList<Object> getCoreStatus( CoreContainer cores, String cname ) throws IOException
  {
    NamedList<Object> info = new SimpleOrderedMap<Object>();
    SolrCore core = cores.getCore(cname);
    if (core != null) {
      try {
        info.add("name", core.getName());
        info.add("instanceDir", core.getResourceLoader().getInstanceDir());
        info.add("dataDir", core.getDataDir());
        info.add("startTime", new Date(core.getStartTime()));
        info.add("uptime", System.currentTimeMillis() - core.getStartTime());
        RefCounted<SolrIndexSearcher> searcher = core.getSearcher();
        info.add("index", LukeRequestHandler.getIndexInfo(searcher.get().getReader(), false));
        searcher.decref();
      } finally {
        // solr-647
        // core.close();
      }
    }
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
