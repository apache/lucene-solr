
package org.apache.solr.search;

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

import org.apache.solr.util.AbstractSolrTestCase;

public class TestSurroundQueryParser extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() { return "schemasurround.xml"; }
  @Override
  public String getSolrConfigFile() { return "solrconfig.xml"; }
  // public String getCoreName() { return "collection1"; }

  @Override
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
   } 
  
  @Override
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }
  
  public void testQueryParser() {
    String v = "a b c d e a b c f g h i j k l m l k j z z z";
    assertU(adoc("id","1", "text",v,  "text_np",v));
    
    v="abc abxy cde efg ef e  ";
    assertU(adoc("id","2", "text",v,  "text_np",v));
    
    v="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 1001 1002 1003 1004 1005 1006 1007 1008 1009";
    assertU(adoc("id","3", "text",v,  "text_np",v));
    assertU(commit());
 
  
    // run through a series of syntax tests, not exhaustive yet
    String localP = "{!surround df=text}";
    String t1;

    t1  = localP+"1 N 2";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='1']");
    // but ordered search should fail
    t1 = localP +"2 W 1";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='0']");

    // alternate syntax
    t1 = localP + "3n(a,e)";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='1']");

    // wildcards
    t1 =localP + "100* w 20";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='0']");
    t1 =localP + "100* n 20";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='1']");

    // nested
    t1 = localP + "(1003 2n 1001) 3N 1006";
    assertQ(req("q", t1, "indent","true")
        ,"//*[@numFound='1']");
  }
  
}
