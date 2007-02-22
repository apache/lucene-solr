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

package org.apache.solr;

import org.apache.solr.request.*;
import org.apache.solr.util.*;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.HashMap;

/**
 * Tests some basic functionality of the DisMaxRequestHandler
 */
public class DisMaxRequestHandlerTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; }
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  public void setUp() throws Exception {
    super.setUp();
    lrf = h.getRequestFactory
      ("dismax", 0, 20,
       "version","2.0",
       "facet", "true",
       "facet.field","t_s"
       );
  }
  public void testSomeStuff() throws Exception {

    assertU(adoc("id", "666",
                 "features_t", "cool and scary stuff",
                 "subject", "traveling in hell",
                 "t_s", "movie",
                 "title", "The Omen",
                 "weight", "87.9",
                 "iind", "666"));
    assertU(adoc("id", "42",
                 "features_t", "cool stuff",
                 "subject", "traveling the galaxy",
                 "t_s", "movie", "t_s", "book",
                 "title", "Hitch Hiker's Guide to the Galaxy",
                 "weight", "99.45",
                 "iind", "42"));
    assertU(adoc("id", "1",
                 "features_t", "nothing",
                 "subject", "garbage",
                 "t_s", "book",
                 "title", "Most Boring Guide Ever",
                 "weight", "77",
                 "iind", "4"));
    assertU(adoc("id", "8675309",
                 "features_t", "Wikedly memorable chorus and stuff",
                 "subject", "One Cool Hot Chick",
                 "t_s", "song",
                 "title", "Jenny",
                 "weight", "97.3",
                 "iind", "8675309"));
    assertU(commit());
    
    assertQ("basic match",
            req("guide")
            ,"//*[@numFound='2']"
            ,"//lst[@name='facet_fields']/lst[@name='t_s']"
            ,"*[count(//lst[@name='t_s']/int)=3]"
            ,"//lst[@name='t_s']/int[@name='book'][.='2']"
            ,"//lst[@name='t_s']/int[@name='movie'][.='1']"
            );
    
    assertQ("basic cross field matching, boost on same field matching",
            req("cool stuff")
            ,"//*[@numFound='3']"
            ,"//result/doc[1]/int[@name='id'][.='42']"
            ,"//result/doc[2]/int[@name='id'][.='666']"
            ,"//result/doc[3]/int[@name='id'][.='8675309']"
            );
    
    assertQ("minimum mm is three",
            req("cool stuff traveling")
            ,"//*[@numFound='2']"
            ,"//result/doc[1]/int[@name='id'][. ='42']"
            ,"//result/doc[2]/int[@name='id'][. ='666']"
            );
    
    assertQ("at 4 mm allows one missing ",
            req("cool stuff traveling jenny")
            ,"//*[@numFound='3']"
            );

    assertQ("relying on ALTQ from config",
            req( "qt", "dismax",
                 "fq", "id:666",
                 "facet", "false" )
            ,"//*[@numFound='1']"
            );
    
    assertQ("explicit ALTQ",
            req( "qt", "dismax",
                 "q.alt", "id:blahbalh",
                 "fq", "id:666",
                 "facet", "false" )
            ,"//*[@numFound='0']"
            );

    assertQ("no query slop == no match",
            req( "qt", "dismax",
                 "q", "\"cool chick\"" )
            ,"//*[@numFound='0']"
            );
    assertQ("query slop == match",
            req( "qt", "dismax",
                 "qs", "2",
                 "q", "\"cool chick\"" )
            ,"//*[@numFound='1']"
            );
  }

  public void testOldStyleDefaults() throws Exception {

    lrf = h.getRequestFactory
      ("dismax", 0, 20,
       "version","2.0",
       "facet", "true",
       "facet.field","t_s"
       );
    testSomeStuff();
  }


  
}
