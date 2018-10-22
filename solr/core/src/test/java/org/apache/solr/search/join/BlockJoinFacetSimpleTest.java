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
package org.apache.solr.search.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockJoinFacetSimpleTest extends SolrTestCaseJ4 {
  private static String handler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-blockjoinfacetcomponent.xml", "schema-blockjoinfacetcomponent.xml");
    handler = random().nextBoolean() ? "/blockJoinDocSetFacetRH":"/blockJoinFacetRH";
    createIndex();
  }

  public static void createIndex() throws Exception {
    
    final String match;
    List<String> docs = Arrays.asList(
// match
    match = adoc("id", "10","type_s", "parent","BRAND_s", "Nike").replace("</doc>", 
    ""+
    doc("id", "11","type_s", "child","COLOR_s", "Red","SIZE_s", "XL")+// matches child filter 
    doc("id", "12","type_s", "child","COLOR_s", "Red","SIZE_s", "XL")+// matches child filter
    doc("id", "13","type_s", "child","COLOR_s", "Blue","SIZE_s", "XL")+"</doc>"),
// mismatch
    adoc("id", "100","type_s", "parent","BRAND_s", "Reebok").replace("</doc>", 
    ""+doc("id", "101","type_s", "child","COLOR_s", "Red","SIZE_s", "M")+
    doc("id", "102","type_s", "child","COLOR_s", "Blue","SIZE_s", "XL")+
    doc("id", "104","type_s", "child","COLOR_s", "While","SIZE_s", "XL")+
    doc("id", "105","type_s", "child","COLOR_s", "Green","SIZE_s", "XXXL")+
    "</doc>"));
    
    Collections.shuffle(docs, random());
    for(String d : docs){
      assertU(d);
    }
    if(random().nextBoolean()){// let's have a deleted doc
      if(random().nextBoolean()){
        assertU("let's have two segs",commit());
      }
      assertU("overriding matching doc",match);
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + 9 + "']");
  }

  @Test
  public void testSimple() throws Exception {
    //query
    // parents
    assertQ(req("q", "type_s:parent"), "//*[@numFound='" + 2 + "']");
    
    String alt[][] ={ {"q", "{!parent which=\"type_s:parent\"}+COLOR_s:Red +SIZE_s:XL"},
        {"q", "+{!parent which=\"type_s:parent\"}+COLOR_s:Red +BRAND_s:Nike"},
        {"q", "{!parent which=\"type_s:parent\"}+COLOR_s:Red", "fq", "BRAND_s:Nike"}};
    
    for(String param[] : alt){
      final List<String> reqParams = new ArrayList<>(Arrays.asList(param));
      reqParams.addAll(Arrays.asList("qt",handler,
          "facet", (random().nextBoolean() ? "true":"false"),// it's indifferent to 
              "child.facet.field", "COLOR_s",
              "child.facet.field", "SIZE_s"));
      assertQ(req(reqParams.toArray(new String[0])),
          "//*[@numFound='" + 1 + "']",
          "//lst[@name='COLOR_s']/int[@name='Red'][.='1']",
        //  "//lst[@name='COLOR_s']/int[@name='Blue'][.='1']",
          "count(//lst[@name='COLOR_s']/int)=1",
          "//lst[@name='SIZE_s']/int[@name='XL'][.='1']",
          "count(//lst[@name='SIZE_s']/int)=1");
      
    }
  }

  @Test
  public void testParentLevelFQExclusion() {
    SolrQueryRequest req = req(
        "qt", handler,
        "q", "{!parent which=type_s:parent}+SIZE_s:XL",
        "fq", "{!term f=BRAND_s tag=rbrand}Nike",
        "facet", "true",
        "facet.field", "BRAND_s",
        "child.facet.field", "COLOR_s");
    assertQ("no exclusion, brand facet got only one Nike",req, "//*[@numFound='" + 1 + "']",
        "count(//lst[@name='BRAND_s']/int[.='1'])=1");
  
    assertQ("nike filter is excluded, expecting both brand in facet",req(
        "qt", handler,
        "q", "{!parent which=type_s:parent}+SIZE_s:XL",
        "fq", "{!term f=BRAND_s tag=rbrand}Nike",
        "facet", "true",
        "facet.field", "{!ex=rbrand}BRAND_s",
        "child.facet.field", "COLOR_s"),
     "//*[@numFound='" + 1 + "']",
        "count(//lst[@name='BRAND_s']/int[.='1'])=2");
  
  }
}
