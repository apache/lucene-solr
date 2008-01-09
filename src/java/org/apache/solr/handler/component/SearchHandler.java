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

package org.apache.solr.handler.component;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * Refer SOLR-281
 *
 * @since solr 1.3
 */
public class SearchHandler extends RequestHandlerBase implements SolrCoreAware
{
  static final String RESPONSE_BUILDER_CONTEXT_KEY = "ResponseBuilder";
  
  static final String INIT_COMPONENTS = "components";
  static final String INIT_FISRT_COMPONENTS = "first-components";
  static final String INIT_LAST_COMPONENTS = "last-components";
  
  protected static Logger log = Logger.getLogger(SearchHandler.class.getName());
  
  protected List<SearchComponent> components = null;
  protected NamedList initArgs = null;
  
  @Override
  public void init(NamedList args) {
    super.init( args );
    initArgs = args;
  }

  protected List<String> getDefaultComponets()
  {
    ArrayList<String> names = new ArrayList<String>(5);
    names.add( QueryComponent.COMPONENT_NAME );
    names.add( FacetComponent.COMPONENT_NAME );
    names.add( MoreLikeThisComponent.COMPONENT_NAME );
    names.add( HighlightComponent.COMPONENT_NAME );
    names.add( DebugComponent.COMPONENT_NAME );
    return names;
  }

  /**
   * Initialize the components based on name
   */
  @SuppressWarnings("unchecked")
  public void inform(SolrCore core) 
  {
    Object declaredComponents = initArgs.get(INIT_COMPONENTS);
    List<String> first = (List<String>) initArgs.get(INIT_FISRT_COMPONENTS);
    List<String> last  = (List<String>) initArgs.get(INIT_LAST_COMPONENTS);

    List<String> list = null;
    if( declaredComponents == null ) {
      // Use the default component list
      list = getDefaultComponets();
      
      if( first != null ) {
        List<String> clist = first;
        clist.addAll( list );
        list = clist;
      }
      
      if( last != null ) {
        list.addAll( last );
      }
    }
    else {
      list = (List<String>)declaredComponents;
      if( first != null || last != null ) {
        throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
            "First/Last components only valid if you do not declare 'components'");
      }
    }
    
    // Build the component list
    components = new ArrayList<SearchComponent>( list.size() );
    for(String c : list){
      SearchComponent comp = core.getSearchComponent( c );
      components.add(comp);
      log.info("Adding  component:"+comp);
    }
  }

  public List<SearchComponent> getComponents() {
    return components;
  }
  
  public static ResponseBuilder getResponseBuilder(SolrQueryRequest req) 
  {
    return (ResponseBuilder) req.getContext().get( RESPONSE_BUILDER_CONTEXT_KEY );
  }
  
  //---------------------------------------------------------------------------------------
  // SolrRequestHandler
  //---------------------------------------------------------------------------------------
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, ParseException, InstantiationException, IllegalAccessException 
  {
    ResponseBuilder builder = new ResponseBuilder();
    req.getContext().put( RESPONSE_BUILDER_CONTEXT_KEY, builder );
    
    if( components == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR,
          "SearchHandler not initialized properly.  No components registered." );
    }
    
    // The semantics of debugging vs not debugging are distinct enough 
    // to justify two control loops
    if( !req.getParams().getBool( CommonParams.DEBUG_QUERY, false ) ) {
      // Prepare
      for( SearchComponent c : components ) {
        c.prepare( req, rsp );
      }
  
      // Process
      for( SearchComponent c : components ) {
        c.process( req, rsp );
      }
    }
    else {
      builder.setDebug( true );
      RTimer timer = new RTimer();
      
      // Prepare
      RTimer subt = timer.sub( "prepare" );
      for( SearchComponent c : components ) {
        builder.setTimer( subt.sub( c.getName() ) );
        c.prepare( req, rsp );
        builder.getTimer().stop();
      }
      subt.stop();
  
      // Process
      subt = timer.sub( "process" );
      for( SearchComponent c : components ) {
        builder.setTimer( subt.sub( c.getName() ) );
        c.process( req, rsp );
        builder.getTimer().stop();
      }
      subt.stop();
      timer.stop();
      
      // add the timing info
      builder.addDebugInfo( "timing", timer.asNamedList() );
    }
  }

  //---------------------------------------------------------------------------------------
  // SolrInfoMBeans
  //---------------------------------------------------------------------------------------
  
  @Override
  public String getDescription() {
    StringBuilder sb = new StringBuilder();
    sb.append("Search using components: ");
    for(SearchComponent c : components){
      sb.append(c.getName());
      sb.append(",");
    }
    return sb.toString();
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
