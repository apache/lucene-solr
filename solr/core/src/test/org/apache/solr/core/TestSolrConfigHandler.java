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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.handler.TestSolrConfigHandlerConcurrent;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.ConfigOverlay.getObjectByPath;
import static org.apache.solr.handler.TestBlobHandler.getAsString;

public class TestSolrConfigHandler extends RestTestBase {
  public static final Logger log = LoggerFactory.getLogger(TestSolrConfigHandler.class);

  private static File tmpSolrHome;
  private static File tmpConfDir;

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";


  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    final SortedMap<ServletHolder, String> extraServlets = new TreeMap<>();
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
    client = null;
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }


  public void testProperty() throws Exception {
    RestTestHarness harness = restTestHarness;
    Map confMap = getRespMap("/config?wt=json", harness);
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/luke")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/system")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/mbeans")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/plugins")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/threads")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/properties")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/logging")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/file")));
    assertNotNull(getObjectByPath(confMap, false, Arrays.asList("config", "requestHandler", "/admin/ping")));

    String payload = "{\n" +
        " 'set-property' : { 'updateHandler.autoCommit.maxDocs':100, 'updateHandler.autoCommit.maxTime':10 , 'requestDispatcher.requestParsers.addHttpRequestToContext':true} \n" +
        " }";
    runConfigCommand(harness, "/config?wt=json", payload);

    Map m = (Map) getRespMap("/config/overlay?wt=json", harness).get("overlay");
    Map props = (Map) m.get("props");
    assertNotNull(props);
    assertEquals("100", String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs"))));
    assertEquals("10", String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime"))));

    m =  getRespMap("/config/updateHandler?wt=json", harness);
    assertNotNull(getObjectByPath(m, true, ImmutableList.of("config","updateHandler", "commitWithin", "softCommit")));

    m = (Map) getRespMap("/config?wt=json", harness).get("config");
    assertNotNull(m);

    assertEquals("100", String.valueOf(getObjectByPath(m, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs"))));
    assertEquals("10", String.valueOf(getObjectByPath(m, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime"))));
    assertEquals("true", String.valueOf(getObjectByPath(m, true, ImmutableList.of("requestDispatcher", "requestParsers", "addHttpRequestToContext"))));
    payload = "{\n" +
        " 'unset-property' :  'updateHandler.autoCommit.maxDocs'} \n" +
        " }";
    runConfigCommand(harness, "/config?wt=json", payload);

    m = (Map) getRespMap("/config/overlay?wt=json", harness).get("overlay");
    props = (Map) m.get("props");
    assertNotNull(props);
    assertNull(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxDocs")));
    assertEquals("10", String.valueOf(getObjectByPath(props, true, ImmutableList.of("updateHandler", "autoCommit", "maxTime"))));
  }

  public void testUserProp() throws Exception {
    RestTestHarness harness = restTestHarness;
    String payload = "{\n" +
        " 'set-user-property' : { 'my.custom.variable.a':'MODIFIEDA'," +
        " 'my.custom.variable.b':'MODIFIEDB' } \n" +
        " }";
    runConfigCommand(harness, "/config?wt=json", payload);

    Map m = (Map) getRespMap("/config/overlay?wt=json", harness).get("overlay");
    Map props = (Map) m.get("userProps");
    assertNotNull(props);
    assertEquals(props.get("my.custom.variable.a"), "MODIFIEDA");
    assertEquals(props.get("my.custom.variable.b"), "MODIFIEDB");

    m = (Map) getRespMap("/dump?wt=json&json.nl=map&initArgs=true", harness).get("initArgs");

    m = (Map) m.get(PluginInfo.DEFAULTS);
    assertEquals("MODIFIEDA", m.get("a"));
    assertEquals("MODIFIEDB", m.get("b"));

  }

  public void testReqHandlerAPIs() throws Exception {
    reqhandlertests(restTestHarness, null, null);
  }

  public static void runConfigCommand(RestTestHarness harness, String uri, String payload) throws IOException {
    String response = harness.post(uri, SolrTestCaseJ4.json(payload));
    Map map = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    assertNull(response, map.get("errors"));
  }


  public static void reqhandlertests(RestTestHarness writeHarness, String testServerBaseUrl, CloudSolrClient cloudSolrServer) throws Exception {
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);

    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config/overlay?wt=json",
        cloudSolrServer,
        Arrays.asList("overlay", "requestHandler", "/x", "startup"),
        "lazy",
        10);

    payload = "{\n" +
        "'update-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy' , 'a':'b' , 'defaults': {'def_a':'def A val'}}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);

    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config/overlay?wt=json",
        cloudSolrServer,
        Arrays.asList("overlay", "requestHandler", "/x", "a"),
        "b",
        10);

    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/x?wt=json&getdefaults=true&json.nl=map",
        cloudSolrServer,
        Arrays.asList("getdefaults", "def_a"),
        "def A val",
        10);

    payload = "{\n" +
        "'delete-requesthandler' : '/x'" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    boolean success = false;
    long startTime = System.nanoTime();
    int maxTimeoutSeconds = 10;
    while (TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
      String uri = "/config/overlay?wt=json";
      Map m = testServerBaseUrl == null ? getRespMap(uri, writeHarness) : TestSolrConfigHandlerConcurrent.getAsMap(testServerBaseUrl + uri, cloudSolrServer);
      if (null == ConfigOverlay.getObjectByPath(m, true, Arrays.asList("overlay", "requestHandler", "/x", "a"))) {
        success = true;
        break;
      }
      Thread.sleep(100);

    }
    assertTrue("Could not delete requestHandler  ", success);

    payload = "{\n" +
        "'create-queryconverter' : { 'name' : 'qc', 'class': 'org.apache.solr.spelling.SpellingQueryConverter'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "queryConverter", "qc", "class"),
        "org.apache.solr.spelling.SpellingQueryConverter",
        10);
    payload = "{\n" +
        "'update-queryconverter' : { 'name' : 'qc', 'class': 'org.apache.solr.spelling.SuggestQueryConverter'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "queryConverter", "qc", "class"),
        "org.apache.solr.spelling.SuggestQueryConverter",
        10);

    payload = "{\n" +
        "'delete-queryconverter' : 'qc'" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "queryConverter", "qc"),
        null,
        10);

    payload = "{\n" +
        "'create-searchcomponent' : { 'name' : 'tc', 'class': 'org.apache.solr.handler.component.TermsComponent'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "searchComponent", "tc", "class"),
        "org.apache.solr.handler.component.TermsComponent",
        10);
    payload = "{\n" +
        "'update-searchcomponent' : { 'name' : 'tc', 'class': 'org.apache.solr.handler.component.TermVectorComponent' }\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "searchComponent", "tc", "class"),
        "org.apache.solr.handler.component.TermVectorComponent",
        10);

    payload = "{\n" +
        "'delete-searchcomponent' : 'tc'" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "searchComponent", "tc"),
        null,
        10);
    //<valueSourceParser name="countUsage" class="org.apache.solr.core.CountUsageValueSourceParser"/>
    payload = "{\n" +
        "'create-valuesourceparser' : { 'name' : 'cu', 'class': 'org.apache.solr.core.CountUsageValueSourceParser'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "valueSourceParser", "cu", "class"),
        "org.apache.solr.core.CountUsageValueSourceParser",
        10);
    //  <valueSourceParser name="nvl" class="org.apache.solr.search.function.NvlValueSourceParser">
//    <float name="nvlFloatValue">0.0</float>
//    </valueSourceParser>
    payload = "{\n" +
        "'update-valuesourceparser' : { 'name' : 'cu', 'class': 'org.apache.solr.search.function.NvlValueSourceParser'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "valueSourceParser", "cu", "class"),
        "org.apache.solr.search.function.NvlValueSourceParser",
        10);

    payload = "{\n" +
        "'delete-valuesourceparser' : 'cu'" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "valueSourceParser", "cu"),
        null,
        10);
//    <transformer name="mytrans2" class="org.apache.solr.response.transform.ValueAugmenterFactory" >
//    <int name="value">5</int>
//    </transformer>
    payload = "{\n" +
        "'create-transformer' : { 'name' : 'mytrans', 'class': 'org.apache.solr.response.transform.ValueAugmenterFactory', 'value':'5'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "transformer", "mytrans", "class"),
        "org.apache.solr.response.transform.ValueAugmenterFactory",
        10);

    payload = "{\n" +
        "'update-transformer' : { 'name' : 'mytrans', 'class': 'org.apache.solr.response.transform.ValueAugmenterFactory', 'value':'6'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "transformer", "mytrans", "value"),
        "6",
        10);

    payload = "{\n" +
        "'delete-transformer' : 'mytrans'," +
        "'create-initparams' : { 'name' : 'hello', 'key':'val'}\n" +
        "}";
    runConfigCommand(writeHarness, "/config?wt=json", payload);
    Map map = testForResponseElement(writeHarness,
        testServerBaseUrl,
        "/config?wt=json",
        cloudSolrServer,
        Arrays.asList("config", "transformer", "mytrans"),
        null,
        10);

    List l = (List) ConfigOverlay.getObjectByPath(map,false, Arrays.asList("config", "initParams"));
    assertNotNull("no object /config/initParams : "+ TestBlobHandler.getAsString(map) , l);
    assertEquals( 1, l.size());
    assertEquals( "val", ((Map)l.get(0)).get("key") );
  }

  public static Map testForResponseElement(RestTestHarness harness,
                                           String testServerBaseUrl,
                                           String uri,
                                           CloudSolrClient cloudSolrServer, List<String> jsonPath,
                                           Object expected,
                                           long maxTimeoutSeconds) throws Exception {

    boolean success = false;
    long startTime = System.nanoTime();
    Map m = null;

    while (TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS) < maxTimeoutSeconds) {
      try {
        m = testServerBaseUrl == null ? getRespMap(uri, harness) : TestSolrConfigHandlerConcurrent.getAsMap(testServerBaseUrl + uri, cloudSolrServer);
      } catch (Exception e) {
        Thread.sleep(100);
        continue;

      }
      if (Objects.equals(expected, ConfigOverlay.getObjectByPath(m, false, jsonPath))) {
        success = true;
        break;
      }
      Thread.sleep(100);

    }

    assertTrue(StrUtils.formatString("Could not get expected value  ''{0}'' for path ''{1}'' full output: {2}", expected, StrUtils.join(jsonPath, '/'), getAsString(m)), success);
    return m;
  }

  public void testReqParams() throws Exception {
    RestTestHarness harness = restTestHarness;
    String payload = " {\n" +
        "  'set' : {'x': {" +
        "                    'a':'A val',\n" +
        "                    'b': 'B val'}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(harness, "/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "x", "a"),
        "A val",
        10);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "x", "b"),
        "B val",
        10);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump', 'class': 'org.apache.solr.handler.DumpRequestHandler' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(harness, "/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/overlay?wt=json",
        null,
        Arrays.asList("overlay", "requestHandler", "/dump", "name"),
        "/dump",
        10);

    TestSolrConfigHandler.testForResponseElement(harness,
        null,
        "/dump?wt=json&useParams=x",
        null,
        Arrays.asList("params", "a"),
        "A val",
        5);
    TestSolrConfigHandler.testForResponseElement(harness,
        null,
        "/dump?wt=json&useParams=x&a=fomrequest",
        null,
        Arrays.asList("params", "a"),
        "fomrequest",
        5);

    payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/dump1', 'class': 'org.apache.solr.handler.DumpRequestHandler', 'useParams':'x' }\n" +
        "}";

    TestSolrConfigHandler.runConfigCommand(harness, "/config?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(harness,
        null,
        "/config/overlay?wt=json",
        null,
        Arrays.asList("overlay", "requestHandler", "/dump1", "name"),
        "/dump1",
        10);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/dump1?wt=json",
        null,
        Arrays.asList("params", "a"),
        "A val",
        5);


    payload = " {\n" +
        "  'set' : {'y':{\n" +
        "                'c':'CY val',\n" +
        "                'b': 'BY val', " +
        "                'd': ['val 1', 'val 2']}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(harness, "/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "c"),
        "CY val",
        10);

    TestSolrConfigHandler.testForResponseElement(harness,
        null,
        "/dump?wt=json&useParams=y",
        null,
        Arrays.asList("params", "c"),
        "CY val",
        5);


    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/dump1?wt=json&useParams=y",
        null,
        Arrays.asList("params", "b"),
        "BY val",
        5);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/dump1?wt=json&useParams=y",
        null,
        Arrays.asList("params", "a"),
        null,
        5);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/dump1?wt=json&useParams=y",
        null,
        Arrays.asList("params", "d"),
        Arrays.asList("val 1", "val 2"),
        5);

    payload = " {\n" +
        "  'update' : {'y': {\n" +
        "                'c':'CY val modified',\n" +
        "                'e':'EY val',\n" +
        "                'b': 'BY val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(harness, "/config/params?wt=json", payload);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "c"),
        "CY val modified",
        10);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "e"),
        "EY val",
        10);

    payload = " {\n" +
        "  'set' : {'y': {\n" +
        "                'p':'P val',\n" +
        "                'q': 'Q val'" +
        "}\n" +
        "             }\n" +
        "  }";


    TestSolrConfigHandler.runConfigCommand(harness, "/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "p"),
        "P val",
        10);

    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "c"),
        null,
        10);
    payload = " {'delete' : 'y'}";
    TestSolrConfigHandler.runConfigCommand(harness, "/config/params?wt=json", payload);
    TestSolrConfigHandler.testForResponseElement(
        harness,
        null,
        "/config/params?wt=json",
        null,
        Arrays.asList("response", "params", "y", "p"),
        null,
        10);


  }


  public static Map getRespMap(String path, RestTestHarness restHarness) throws Exception {
    String response = restHarness.query(path);
    try {
      return (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    } catch (JSONParser.ParseException e) {
      log.error(response);
      return Collections.emptyMap();
    }
  }
}
