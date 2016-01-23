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
import org.junit.BeforeClass;

/**
 * Most of the tests for StandardRequestHandler are in ConvertedLegacyTest
 * 
 */
public class StandardRequestHandlerTest extends AbstractSolrTestCase {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
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

    assertQ(req("q", "title:test")
            ,"//*[@numFound='3']"
            );
    
    assertQ(req("q", "title:test", "sort","val_s1 asc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='12']"
            );

    assertQ(req("q", "title:test", "sort","val_s1 desc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='12']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='10']"
            );
    
    // Make sure score parsing works
    assertQ(req("q", "title:test", "sort","score desc")
        ,"//*[@numFound='3']"
    );

    assertQ(req("q", "title:test", "sort","score asc")
        ,"//*[@numFound='3']"
    );
    
    // Using legacy ';' param
    assertQ(req("q", "title:test; val_s1 desc", "defType","lucenePlusSort")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='12']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='10']"
            );

    assertQ(req("q", "title:test; val_s1 asc", "defType","lucenePlusSort")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='10']"
            ,"//result/doc[2]/int[@name='id'][.='11']"
            ,"//result/doc[3]/int[@name='id'][.='12']"
            );
  }
}
