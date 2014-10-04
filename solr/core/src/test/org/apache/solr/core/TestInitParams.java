package org.apache.solr.core;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class TestInitParams extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-paramset.xml","schema.xml");
  }
  @Test
  public void testComponentWithParamSet(){

    for (String s : Arrays.asList("/dump1", "/dump3","/root/dump5" , "/root1/anotherlevel/dump6")) {
      SolrRequestHandler handler = h.getCore().getRequestHandler(s);
      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(req("initArgs", "true"), rsp);
      NamedList nl = (NamedList) rsp.getValues().get("initArgs");
      NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
      assertEquals("A", def.get("a"));
      def = (NamedList) nl.get(PluginInfo.INVARIANTS);
      assertEquals("B", def.get("b"));
      def = (NamedList) nl.get(PluginInfo.APPENDS);
      assertEquals("C", def.get("c"));
    }
  }

  @Test
  public void testMultiParamSet(){
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump6");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A", def.get("a"));
    assertEquals("P", def.get("p"));
    def = (NamedList) nl.get(PluginInfo.INVARIANTS);
    assertEquals("B", def.get("b"));
    def = (NamedList) nl.get(PluginInfo.APPENDS);
    assertEquals("C", def.get("c"));

  }

  @Test
  public void testComponentWithParamSetRequestParam(){
    for (String s : Arrays.asList("/dump4")) {
      SolrRequestHandler handler = h.getCore().getRequestHandler(s);
      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(req("param", "a","param","b" ,"param","c", "useParam","a"), rsp);
      NamedList def = (NamedList) rsp.getValues().get("params");
      assertEquals("A", def.get("a"));
      assertEquals("B", def.get("b"));
      assertEquals("C", def.get("c"));
    }
  }
  @Test
  public void testComponentWithConflictingParamSet(){
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump2");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A" ,def.get("a"));
    def = (NamedList) nl.get(PluginInfo.INVARIANTS);
    assertEquals("B1" ,def.get("b"));
    def = (NamedList) nl.get(PluginInfo.APPENDS);
    assertEquals(Arrays.asList("C1","C") ,def.getAll("c"));
  }

  public void testNestedRequestHandler() {
    assertNotNull(h.getCore().getRequestHandler("/greedypath"));
    assertNotNull(h.getCore().getRequestHandler("/greedypath/some/path"));
    assertNotNull( h.getCore().getRequestHandler("/greedypath/some/other/path"));
    assertNull(h.getCore().getRequestHandler("/greedypath/unknownpath"));
  }




}
