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

package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class BooleanFieldTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema15.xml");
  }

  // Note, docValues-based boolean tests are tested elsewhere refering to more appropriate schemas
  @Test
  public void testBoolField() {

    // found an odd case when adding booleans to docValues and noticed that we didn't have any boolean
    // specific tests. Only caught the odd case by accident so let's have a place for explicit tests
    assertU(adoc("id", "0")); // missing
    assertU(adoc("id", "1", "bind", "true", "bsto", "true", "bindsto", "true", "bindstom", "true", "bindstom", "false"));
    assertU(adoc("id", "2", "bind", "false", "bsto", "false", "bindsto", "false", "bindstom", "false", "bindstom", "true"));
    assertU(adoc("id", "3", "bind", "false"));
    assertU(adoc("id", "4", "bsto", "false"));
    assertU(adoc("id", "5", "bindsto", "true"));
    assertU(adoc("id", "6", "bindstom", "true"));
    assertU(commit());

    assertQ(req("q", "*:*", "sort", "id asc", "fl", "id,bind,bsto,bindsto,bindstom")
        ,"count(//result/doc[1]/bool[@name='bind'])=0"
        ,"count(//result/doc[1]/bool[@name='bsto'])=0"
        ,"count(//result/doc[1]/bool[@name='bindsto'])=0"
        ,"count(//result/doc[2]/bool[@name='bind'])=0"
        ,"count(//result/doc[3]/bool[@name='bind'])=0"
        ,"//result/doc[2]/bool[@name='bsto'][.='true']"
        ,"//result/doc[2]/bool[@name='bindsto'][.='true']"
        ,"//result/doc[3]/bool[@name='bsto'][.='false']"
        ,"//result/doc[3]/bool[@name='bindsto'][.='false']"
        ,"//result/doc[2]/arr[@name='bindstom']/bool[1][.='true']"
        ,"//result/doc[2]/arr[@name='bindstom']/bool[2][.='false']"
        ,"//result/doc[3]/arr[@name='bindstom']/bool[1][.='false']"
        ,"//result/doc[3]/arr[@name='bindstom']/bool[2][.='true']"

    );
    
    // Make sure faceting is behaving.
    assertQ(req("q", "*:*", "facet", "true", 
        "facet.field", "bind", 
        "facet.field", "bsto",
        "facet.field", "bindsto",
        "facet.field", "bindstom"),
        "//lst[@name='bind']/int[@name='false'][.='2']",
        "//lst[@name='bind']/int[@name='true'][.='1']",
        "//lst[@name='bsto'][not(node())]",
        "//lst[@name='bsto'][not(node())]",
        "//lst[@name='bindsto']/int[@name='false'][.='1']",
        "//lst[@name='bindsto']/int[@name='true'][.='2']",
        "//lst[@name='bindstom']/int[@name='false'][.='2']",
        "//lst[@name='bindstom']/int[@name='true'][.='3']");
  }

}
