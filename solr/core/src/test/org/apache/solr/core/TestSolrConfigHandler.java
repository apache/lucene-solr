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


import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.handler.TestSolrConfigHandlerConcurrent;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.restlet.ext.servlet.ServerServlet;

import static org.apache.solr.core.ConfigOverlay.getObjectByPath;

public class TestSolrConfigHandler extends RestTestBase {
  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";


  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder,String> extraServlets = new TreeMap<>();
    final ServletHolder solrRestApi = new ServletHolder("SolrSchemaRestApi", ServerServlet.class);
    solrRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi");
    extraServlets.put(solrRestApi, "/schema/*");  // '/schema/*' matches '/schema', '/schema/', and '/schema/whatever...'

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
        "/solr", true, extraServlets);
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    server = null;
    restTestHarness = null;
  }


  public void testProperty() throws Exception{
    RestTestHarness harness = restTestHarness;
    String payload= "{\n" +
        " 'set-property' : { 'updateHandler.autoCommit.maxDocs':100, 'updateHandler.autoCommit.maxTime':10 } \n" +
        " }";
    runConfigCommand( harness,"/config?wt=json", payload);

    Map m = (Map) getRespMap("/config/overlay?wt=json" ,harness).get("overlay");
    Map props = (Map) m.get("props");
    assertNotNull(props);
    assertEquals("100",  String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs")) ));
    assertEquals("10",  String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime")) ));

    m = (Map) getRespMap("/config?wt=json" ,harness).get("solrConfig");
    assertNotNull(m);

    assertEquals( "100",String.valueOf(getObjectByPath(m, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs"))));
    assertEquals( "10",String.valueOf(getObjectByPath(m, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime"))));
    payload= "{\n" +
        " 'unset-property' :  'updateHandler.autoCommit.maxDocs'} \n" +
        " }";
    runConfigCommand(harness, "/config?wt=json", payload);

    m = (Map) getRespMap("/config/overlay?wt=json" ,harness).get("overlay");
    props = (Map) m.get("props");
    assertNotNull(props);
    assertNull(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs")));
    assertEquals("10",  String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime"))));
  }

  public void testUserProp() throws Exception{
    RestTestHarness harness = restTestHarness;
    String payload= "{\n" +
        " 'set-user-property' : { 'my.custom.variable.a':'MODIFIEDA'," +
        " 'my.custom.variable.b':'MODIFIEDB' } \n" +
        " }";
    runConfigCommand(harness,"/config?wt=json", payload);

    Map m = (Map) getRespMap("/config/overlay?wt=json" ,harness).get("overlay");
    Map props = (Map) m.get("userProps");
    assertNotNull(props);
    assertEquals(props.get("my.custom.variable.a"), "MODIFIEDA");
    assertEquals(props.get("my.custom.variable.b"),"MODIFIEDB");

    m = (Map) getRespMap("/dump?wt=json&json.nl=map&initArgs=true" ,harness).get("initArgs");

    m = (Map) m.get(PluginInfo.DEFAULTS);
    assertEquals("MODIFIEDA", m.get("a"));
    assertEquals("MODIFIEDB", m.get("b"));

  }

  public void testReqHandlerAPIs() throws Exception {
    reqhandlertests(restTestHarness, null,null);
  }

  public static void runConfigCommand(RestTestHarness harness, String uri,  String payload) throws IOException {
    String response = harness.post(uri, SolrTestCaseJ4.json(payload));
    Map map = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    assertNull(response,  map.get("errors"));
  }


  public static void reqhandlertests(RestTestHarness writeHarness,String testServerBaseUrl, CloudSolrServer cloudSolrServer) throws Exception {
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy'}\n" +
        "}";
    runConfigCommand(writeHarness,"/config?wt=json", payload);

    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config/overlay?wt=json",
        cloudSolrServer,
        Arrays.asList("overlay", "requestHandler", "/x", "startup"),
        "lazy",
        10);

    payload = "{\n" +
        "'update-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy' , 'a':'b'}\n" +
        "}";
    runConfigCommand(writeHarness,"/config?wt=json", payload);

    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config/overlay?wt=json",
        cloudSolrServer,
        Arrays.asList("overlay", "requestHandler", "/x", "a"),
        "b",
        10);

    payload = "{\n" +
        "'delete-requesthandler' : '/x'" +
        "}";
    runConfigCommand(writeHarness,"/config?wt=json", payload);
    boolean success = false;
    long startTime = System.nanoTime();
    int maxTimeoutSeconds = 10;
    while ( TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
      String uri = "/config/overlay?wt=json";
      Map m = testServerBaseUrl ==null?  getRespMap(uri,writeHarness) : TestSolrConfigHandlerConcurrent.getAsMap(testServerBaseUrl+uri ,cloudSolrServer) ;
      if(null == ConfigOverlay.getObjectByPath(m,  true, Arrays.asList("overlay", "requestHandler", "/x","a"))) {
        success = true;
        break;
      }
      Thread.sleep(100);

    }
    assertTrue( "Could not delete requestHandler  ", success);

  }

  public static void testForResponseElement(RestTestHarness harness,
                                            String testServerBaseUrl,
                                            String uri,
                                            CloudSolrServer cloudSolrServer,List<String> jsonPath,
                                            String expected,
                                            long maxTimeoutSeconds ) throws Exception {

    boolean success = false;
    long startTime = System.nanoTime();
    while ( TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
      Map m = testServerBaseUrl ==null?  getRespMap(uri,harness) : TestSolrConfigHandlerConcurrent.getAsMap(testServerBaseUrl + uri, cloudSolrServer) ;
      if(Objects.equals(expected,ConfigOverlay.getObjectByPath(m, true, jsonPath))) {
        success = true;
        break;
        /*Map map = getRespMap("/x?wt=json",harness);
        if(map.containsKey("params")) {
          success = true;
          break;
        }*/
      }
      Thread.sleep(100);

    }

    assertTrue( "Could not add/change requestHandler  ", success);
  }


  public static Map getRespMap(String path, RestTestHarness restHarness) throws Exception {
    String response = restHarness.query(path);
    try {
      return (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    } catch (JSONParser.ParseException e) {
      return Collections.emptyMap();
    }
  }
}
