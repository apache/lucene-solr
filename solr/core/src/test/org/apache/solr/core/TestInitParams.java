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
package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static java.util.Collections.singletonMap;

public class TestInitParams extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-paramset.xml","schema.xml");
  }
  @Test
  public void testComponentWithInitParams(){

    for (String s : Arrays.asList("/dump1", "/dump3","/root/dump5" , "/root1/anotherlevel/dump6")) {
      SolrRequestHandler handler = h.getCore().getRequestHandler(s);
      SolrQueryResponse rsp = new SolrQueryResponse();
      handler.handleRequest(req("initArgs", "true"), rsp);
      @SuppressWarnings({"rawtypes"})
      NamedList nl = (NamedList) rsp.getValues().get("initArgs");
      @SuppressWarnings({"rawtypes"})
      NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
      assertEquals("A", def.get("a"));
      def = (NamedList) nl.get(PluginInfo.INVARIANTS);
      assertEquals("B", def.get("b"));
      def = (NamedList) nl.get(PluginInfo.APPENDS);
      assertEquals("C", def.get("c"));
    }

    InitParams initParams = h.getCore().getSolrConfig().getInitParams().get("a");

    @SuppressWarnings({"unchecked", "rawtypes"})
    PluginInfo pluginInfo = new PluginInfo("requestHandler",
        new HashMap<>(),
        new NamedList<>(singletonMap("defaults", new NamedList(Utils.makeMap("a", "A1")))), null);
    initParams.apply(pluginInfo);
    assertEquals( "A",initParams.defaults.get("a"));
  }

  @Test
  public void testMultiInitParams(){
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump6");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    @SuppressWarnings({"rawtypes"})
    NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A", def.get("a"));
    assertEquals("P", def.get("p"));
    def = (NamedList) nl.get(PluginInfo.INVARIANTS);
    assertEquals("B", def.get("b"));
    def = (NamedList) nl.get(PluginInfo.APPENDS);
    assertEquals("C", def.get("c"));

  }


  @Test
  public void testComponentWithConflictingInitParams(){
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump2");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    @SuppressWarnings({"rawtypes"})
    NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
    assertEquals("A1" ,def.get("a"));
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

  public void testElevateExample(){
    SolrRequestHandler handler = h.getCore().getRequestHandler("/elevate");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    @SuppressWarnings({"rawtypes"})
    NamedList def = (NamedList) nl.get(PluginInfo.DEFAULTS);
    assertEquals("text" ,def.get("df"));

  }

  public void testArbitraryAttributes() {
    SolrRequestHandler handler = h.getCore().getRequestHandler("/dump7");
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequest(req("initArgs", "true"), rsp);
    @SuppressWarnings({"rawtypes"})
    NamedList nl = (NamedList) rsp.getValues().get("initArgs");
    assertEquals("server-enabled.txt", nl.get("healthcheckFile"));
  }

  public void testMatchPath(){
    InitParams initParams = new InitParams(new PluginInfo(InitParams.TYPE, Utils.makeMap("path", "/update/json/docs")));
    assertFalse(initParams.matchPath("/update"));
    assertTrue(initParams.matchPath("/update/json/docs"));
    initParams = new InitParams(new PluginInfo(InitParams.TYPE, Utils.makeMap("path", "/update/**")));
    assertTrue(initParams.matchPath("/update/json/docs"));
    assertTrue(initParams.matchPath("/update/json"));
    assertTrue(initParams.matchPath("/update"));
    initParams = new InitParams(new PluginInfo(InitParams.TYPE, Utils.makeMap("path", "/update/*")));
    assertFalse(initParams.matchPath("/update/json/docs"));
    assertTrue(initParams.matchPath("/update/json"));
    assertTrue(initParams.matchPath("/update"));
  }

}
