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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.AbstractSolrTestCase;

/**
 * Most of the tests for StandardRequestHandler are in ConvertedLegacyTest
 * 
 */
public class StandardRequestHandlerTest extends AbstractSolrTestCase {

  @Override public String getSchemaFile() { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }
  @Override public void setUp() throws Exception {
    super.setUp();
    lrf = h.getRequestFactory("standard", 0, 20 );
  }
  
  public void testSorting() throws Exception {
    SolrCore core = h.getCore();
    assertU(adoc("id", "10", "title", "test", "val_s1", "aaa"));
    assertU(adoc("id", "11", "title", "test", "val_s1", "bbb"));
    assertU(adoc("id", "12", "title", "test", "val_s1", "ccc"));
    assertU(commit());
    
    Map<String,String> args = new HashMap<String, String>();
    args.put( CommonParams.Q, "title:test" );
    args.put( "indent", "true" );
    SolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    
    
    assertQ("Make sure they got in", req
            ,"//*[@numFound='3']"
            );
    
    args.put( CommonParams.SORT, "val_s1 asc" );
    assertQ("with sort param [asc]", req
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='12']"
            );

    args.put( CommonParams.SORT, "val_s1 desc" );
    assertQ("with sort param [desc]", req
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='12']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='10']"
            );
    
    // Make sure score parsing works
    args.put( CommonParams.SORT, "score desc" );
    assertQ("with sort param [desc]", req,"//*[@numFound='3']" );

    args.put( CommonParams.SORT, "score asc" );
    assertQ("with sort param [desc]", req,"//*[@numFound='3']" );
    
    // Using legacy ';' param
    args.remove( CommonParams.SORT );
    args.put( QueryParsing.DEFTYPE, "lucenePlusSort" );
    args.put( CommonParams.Q, "title:test; val_s1 desc" );
    assertQ("with sort param [desc]", req
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='12']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='10']"
            );

    args.put( CommonParams.Q, "title:test; val_s1 asc" );
    assertQ("with sort param [asc]", req
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='12']"
            );
  }
}
