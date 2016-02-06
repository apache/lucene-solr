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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.component.DebugComponent;
import org.apache.solr.handler.component.FacetComponent;
import org.apache.solr.handler.component.MoreLikeThisComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.StatsComponent;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestComponentsName extends SolrTestCaseJ4{
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-components-name.xml","schema.xml");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    assertU((commit()));
  }
  
  
  @Test
  public void testComponentsName() {
    assertU(adoc("id", "0", "name", "Zapp Brannigan"));
    assertU(adoc("id", "1", "name", "The Zapper"));
    assertU((commit()));
    
    assertQ("match all docs query",
        req("q","*:*")
        ,"//result[@numFound='2']",
        "/response/str[@name='component1'][.='foo']", 
        "/response/str[@name='component2'][.='bar']");
    
    assertQ("use debugQuery",
        req("q","*:*",
            "debugQuery", "true")
        ,"//result[@numFound='2']",
        "/response/str[@name='component1'][.='foo']", 
        "/response/str[@name='component2'][.='bar']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='component1']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + QueryComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + FacetComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + MoreLikeThisComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + StatsComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='" + DebugComponent.COMPONENT_NAME + "']",
        "/response/lst[@name='debug']/lst[@name='timing']/lst[@name='prepare']/lst[@name='component2']");
  }
  
}


