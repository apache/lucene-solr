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

package org.apache.solr.handler;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RTimer;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * Refer SOLR-281
 *
 */
public class SearchHandler extends RequestHandlerBase
{
  private static final String RESPONSE_BUILDER_CONTEXT_KEY = "ResponseBuilder";
  
  protected static Logger log = Logger.getLogger(SearchHandler.class.getName());
  
  protected Collection<SearchComponent> components;
  
  @Override
  public void init(NamedList args) {
    super.init( args );
    initComponents(args);
  }
  
  // TODO: should there be a way to append components from solrconfig w/o having to
  // know the complete standard list (which may expand over time?)
  protected void initComponents(NamedList args){
    if( args != null ) {
      try {
        Object declaredComponents = args.get("components");
        if (declaredComponents != null && declaredComponents instanceof List) {
          List list = (List) declaredComponents;
          components = new ArrayList<SearchComponent>(list.size());
          for(Object c : list){
            // TODO: an init() with args for components?
            SearchComponent comp = (SearchComponent) Class.forName((String) c).newInstance();
            components.add(comp);
            log.info("Adding  component:"+comp);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } 
    }
  }

  public static ResponseBuilder getResponseBuilder(SolrQueryRequest req) 
  {
    return (ResponseBuilder) req.getContext().get( RESPONSE_BUILDER_CONTEXT_KEY );
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, ParseException, InstantiationException, IllegalAccessException 
  {
    ResponseBuilder builder = new ResponseBuilder();
    req.getContext().put( RESPONSE_BUILDER_CONTEXT_KEY, builder );
    
    // The semantics of debugging vs not debugging are different enough that 
    // it makes sense to have two control loops
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
      if( builder.getDebugInfo() == null ) {
        builder.setDebugInfo( new SimpleOrderedMap<Object>() );
      }
      builder.getDebugInfo().add( "timing", timer.asNamedList() );
    }
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

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
