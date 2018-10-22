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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

/**
 * Most of the tests for {@link org.apache.solr.handler.component.SearchHandler} are in {@link org.apache.solr.ConvertedLegacyTest}.
 */
public class SearchHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  public void testSorting() throws Exception {
    assertU(adoc("id", "10", "title", "test", "val_s1", "aaa"));
    assertU(adoc("id", "11", "title", "test", "val_s1", "bbb"));
    assertU(adoc("id", "12", "title", "test", "val_s1", "ccc"));
    assertU(commit());

    assertQ(req("q", "title:test")
            ,"//*[@numFound='3']"
            );
    
    assertQ(req("q", "title:test", "sort","val_s1 asc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            ,"//result/doc[2]/str[@name='id'][.='11']"
            ,"//result/doc[3]/str[@name='id'][.='12']"
            );

    assertQ(req("q", "title:test", "sort","val_s1 desc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/str[@name='id'][.='12']"
            ,"//result/doc[2]/str[@name='id'][.='11']"
            ,"//result/doc[3]/str[@name='id'][.='10']"
            );
    
    // Make sure score parsing works
    assertQ(req("q", "title:test", "sort","score desc")
        ,"//*[@numFound='3']"
    );

    assertQ(req("q", "title:test", "sort","score asc")
        ,"//*[@numFound='3']"
    );
    
    // Using legacy ';' param
    assertQ(req("q", "title:test", "sort","val_s1 desc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/str[@name='id'][.='12']"
            ,"//result/doc[2]/str[@name='id'][.='11']"
            ,"//result/doc[3]/str[@name='id'][.='10']"
            );

    assertQ(req("q", "title:test", "sort", "val_s1 asc")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/str[@name='id'][.='10']"
            ,"//result/doc[2]/str[@name='id'][.='11']"
            ,"//result/doc[3]/str[@name='id'][.='12']"
            );
  }
}
