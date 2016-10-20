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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchHandlerTest extends SolrTestCaseJ4 
{
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  
  @SuppressWarnings("unchecked")
  @Test
  public void testInitialization()
  {
    SolrCore core = h.getCore();
    
    // Build an explicit list
    //-----------------------------------------------
    List<String> names0 = new ArrayList<>();
    names0.add( MoreLikeThisComponent.COMPONENT_NAME );
    
    NamedList args = new NamedList();
    args.add( SearchHandler.INIT_COMPONENTS, names0 );
    SearchHandler handler = new SearchHandler();
    handler.init( args );
    handler.inform( core );
    
    assertEquals( 1, handler.getComponents().size() );
    assertEquals( core.getSearchComponent( MoreLikeThisComponent.COMPONENT_NAME ), 
        handler.getComponents().get( 0 ) );

    // Build an explicit list that includes the debug comp.
    //-----------------------------------------------
    names0 = new ArrayList<>();
    names0.add( FacetComponent.COMPONENT_NAME );
    names0.add( DebugComponent.COMPONENT_NAME );
    names0.add( MoreLikeThisComponent.COMPONENT_NAME );

    args = new NamedList();
    args.add( SearchHandler.INIT_COMPONENTS, names0 );
    handler = new SearchHandler();
    handler.init( args );
    handler.inform( core );

    assertEquals( 3, handler.getComponents().size() );
    assertEquals( core.getSearchComponent( FacetComponent.COMPONENT_NAME ),
        handler.getComponents().get( 0 ) );
    assertEquals( core.getSearchComponent( DebugComponent.COMPONENT_NAME ),
        handler.getComponents().get( 1 ) );
    assertEquals( core.getSearchComponent( MoreLikeThisComponent.COMPONENT_NAME ), 
        handler.getComponents().get( 2 ) );
    

    // First/Last list
    //-----------------------------------------------
    names0 = new ArrayList<>();
    names0.add( MoreLikeThisComponent.COMPONENT_NAME );
    
    List<String> names1 = new ArrayList<>();
    names1.add( FacetComponent.COMPONENT_NAME );
    
    args = new NamedList();
    args.add( SearchHandler.INIT_FIRST_COMPONENTS, names0 );
    args.add( SearchHandler.INIT_LAST_COMPONENTS, names1 );
    handler = new SearchHandler();
    handler.init( args );
    handler.inform( core );
    
    List<SearchComponent> comps = handler.getComponents();
    assertEquals( 2+handler.getDefaultComponents().size(), comps.size() );
    assertEquals( core.getSearchComponent( MoreLikeThisComponent.COMPONENT_NAME ), comps.get( 0 ) );
    assertEquals( core.getSearchComponent( FacetComponent.COMPONENT_NAME ), comps.get( comps.size()-2 ) );
    //Debug component is always last in this case
    assertEquals( core.getSearchComponent( DebugComponent.COMPONENT_NAME ), comps.get( comps.size()-1 ) );
  }
  
  @Test
  public void testZkConnected() throws Exception{
    MiniSolrCloudCluster miniCluster = new MiniSolrCloudCluster(5, createTempDir(), buildJettyConfig("/solr"));

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    try {
      assertNotNull(miniCluster.getZkServer());
      List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
      assertEquals(5, jettys.size());
      for (JettySolrRunner jetty : jettys) {
        assertTrue(jetty.isRunning());
      }

      // create collection
      String collectionName = "testSolrCloudCollection";
      String configName = "solrCloudCollectionConfig";
      miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

      CollectionAdminRequest.createCollection(collectionName, configName, 2, 2)
          .process(miniCluster.getSolrClient());
    
      QueryRequest req = new QueryRequest();
      QueryResponse rsp = req.process(cloudSolrClient, collectionName);
      assertTrue(rsp.getResponseHeader().getBooleanArg("zkConnected"));

    }
    finally {
      miniCluster.shutdown();
    }
  }
}
