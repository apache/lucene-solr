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

package org.apache.solr.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.similarities.MockConfigurableSimilarityProvider;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.junit.BeforeClass;
import org.junit.Test;


public class IndexSchemaTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }    

  /**
   * This test assumes the schema includes:
   * <dynamicField name="dynamic_*" type="string" indexed="true" stored="true"/>
   * <dynamicField name="*_dynamic" type="string" indexed="true" stored="true"/>
   */
  @Test
  public void testDynamicCopy() 
  {
    SolrCore core = h.getCore();
    assertU(adoc("id", "10", "title", "test", "aaa_dynamic", "aaa"));
    assertU(commit());
    
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "title:test" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    
    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            );
    
    args = new HashMap<String, String>();
    args.put( CommonParams.Q, "aaa_dynamic:aaa" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("dynamic source", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            );

    args = new HashMap<String, String>();
    args.put( CommonParams.Q, "dynamic_aaa:aaa" );
    args.put( "indent", "true" );
    req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    assertQ("dynamic destination", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            );
    clearIndex();
  }
  
  @Test
  public void testRuntimeFieldCreation()
  {
    // any field manipulation needs to happen when you know the core will not 
    // be accepting any requests.  Typically this is done within the inform() 
    // method.  Since this is a single threaded test, we can change the fields
    // willi-nilly

    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    final String fieldName = "runtimefield";
    SchemaField sf = new SchemaField( fieldName, schema.getFieldTypes().get( "string" ) );
    schema.getFields().put( fieldName, sf );
    
    // also register a new copy field (from our new field)
    schema.registerCopyField( fieldName, "dynamic_runtime" );
    schema.refreshAnalyzers();
    
    assertU(adoc("id", "10", "title", "test", fieldName, "aaa"));
    assertU(commit());

    SolrQuery query = new SolrQuery( fieldName+":aaa" );
    query.set( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, query );
    
    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            );
    
    // Check to see if our copy field made it out safely
    query.setQuery( "dynamic_runtime:aaa" );
    assertQ("Make sure they got in", req
            ,"//*[@numFound='1']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            );
    clearIndex();
  }
  
  @Test
  public void testIsDynamicField() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse( schema.isDynamicField( "id" ) );
    assertTrue( schema.isDynamicField( "aaa_i" ) );
    assertFalse( schema.isDynamicField( "no_such_field" ) );
  }

  @Test
  public void testProperties() throws Exception{
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    assertFalse(schema.getField("id").multiValued());

  }
}
