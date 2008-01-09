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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.MoreLikeThisComponent;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.util.AbstractSolrTestCase;


public class SearchHandlerTest extends AbstractSolrTestCase 
{
  @Override public String getSchemaFile() { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }
  
  @SuppressWarnings("unchecked")
  public void testInitalization()
  {
    SolrCore core = h.getCore();
    
    // Build an explicit list
    //-----------------------------------------------
    List<String> names0 = new ArrayList<String>();
    names0.add( MoreLikeThisComponent.COMPONENT_NAME );
    
    NamedList args = new NamedList();
    args.add( SearchHandler.INIT_COMPONENTS, names0 );
    SearchHandler handler = new SearchHandler();
    handler.init( args );
    handler.inform( core );
    
    assertEquals( 1, handler.getComponents().size() );
    assertEquals( core.getSearchComponent( MoreLikeThisComponent.COMPONENT_NAME ), 
        handler.getComponents().get( 0 ) );
    

    // First/Last list
    //-----------------------------------------------
    names0 = new ArrayList<String>();
    names0.add( MoreLikeThisComponent.COMPONENT_NAME );
    
    List<String> names1 = new ArrayList<String>();
    names1.add( FacetComponent.COMPONENT_NAME );
    
    args = new NamedList();
    args.add( SearchHandler.INIT_FISRT_COMPONENTS, names0 );
    args.add( SearchHandler.INIT_LAST_COMPONENTS, names1 );
    handler = new SearchHandler();
    handler.init( args );
    handler.inform( core );
    
    List<SearchComponent> comps = handler.getComponents();
    assertEquals( 2+handler.getDefaultComponets().size(), comps.size() );
    assertEquals( core.getSearchComponent( MoreLikeThisComponent.COMPONENT_NAME ), comps.get( 0 ) );
    assertEquals( core.getSearchComponent( FacetComponent.COMPONENT_NAME ), comps.get( comps.size()-1 ) );
  }
}
