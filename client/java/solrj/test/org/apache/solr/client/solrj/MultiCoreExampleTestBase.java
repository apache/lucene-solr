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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.request.MultiCoreRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest.ACTION;
import org.apache.solr.client.solrj.response.MultiCoreResponse;
import org.apache.solr.common.SolrInputDocument;


/**
 * @version $Id$
 * @since solr 1.3
 */
public abstract class MultiCoreExampleTestBase extends SolrExampleTestBase 
{
  @Override public String getSolrHome() { return "../../../example/multicore/"; }
  
  @Override public String getSchemaFile()     { return getSolrHome()+"core0/conf/schema.xml";     }
  @Override public String getSolrConfigFile() { return getSolrHome()+"core0/conf/solrconfig.xml"; }
  

  public void testMultiCore() throws Exception
  {
    SolrServer solr = getSolrServer();
    
    UpdateRequest up = new UpdateRequest();
    up.setAction( ACTION.COMMIT, true, true );
    up.setCore( "core0" );
    up.deleteByQuery( "*:*" );
    up.process( solr );
    up.setCore( "core1" );
    up.process( solr );
    up.clear();
    
    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField( "id", "AAA" );
    doc.setField( "core0", "yup" );
   
    // Add to core0
    up.setCore( "core0" );
    up.add( doc );
    up.process( solr );

    // You can't add it to core1
    try {
      up.setCore( "core1" );
      up.process( solr );
      fail( "Can't add core0 field to core1!" );
    }
    catch( Exception ex ) {}

    // Add to core1
    up.setCore( "core1" );
    doc.setField( "id", "BBB" );
    doc.setField( "core1", "yup" );
    doc.removeField( "core0" );
    up.add( doc );
    up.process( solr );

    // You can't add it to core1
    try {
      up.setCore( "core0" );
      up.process( solr );
      fail( "Can't add core1 field to core0!" );
    }
    catch( Exception ex ) {}
    
    // now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest( q );
    r.setCore( "core0" );
    q.setQuery( "id:AAA" );
    assertEquals( 1, r.process( solr ).getResults().size() );
    r.setCore( "core1" );
    assertEquals( 0, r.process( solr ).getResults().size() );
    
    // Now test Changing the default core
    solr.setDefaultCore( "core0" );
    assertEquals( 1, solr.query( new SolrQuery( "id:AAA" ) ).getResults().size() );
    assertEquals( 0, solr.query( new SolrQuery( "id:BBB" ) ).getResults().size() );

    solr.setDefaultCore( "core1" );
    assertEquals( 0, solr.query( new SolrQuery( "id:AAA" ) ).getResults().size() );
    assertEquals( 1, solr.query( new SolrQuery( "id:BBB" ) ).getResults().size() );
  
    // Now test reloading it should have a newer open time
    String name = "core0";
    MultiCoreResponse mcr = MultiCoreRequest.getStatus( name, solr );
    long before = mcr.getStartTime( name ).getTime();
    MultiCoreRequest.reloadCore( name, solr );
    
    mcr = MultiCoreRequest.getStatus( name, solr );
    long after = mcr.getStartTime( name ).getTime();
    assertTrue( "should have more recent time: "+after+","+before, after > before );
  }
}
