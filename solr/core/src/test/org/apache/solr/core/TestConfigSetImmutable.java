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

import java.io.File;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RestTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that a ConfigSet marked as immutable cannot be modified via
 * the known APIs, i.e. SolrConfigHandler and SchemaHandler.
 */
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestConfigSetImmutable extends RestTestBase {

  private static final String collection = "collection1";
  private static final String confDir = collection + "/conf";

  @Before
  public void before() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    File tmpConfDir = new File(tmpSolrHome, confDir);
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());
    // make the ConfigSet immutable
    FileUtils.write(new File(tmpConfDir, "configsetprops.json"), new StringBuilder("{\"immutable\":\"true\"}"), StandardCharsets.UTF_8);

    System.setProperty("managed.schema.mutable", "true");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-schemaless.xml", "schema-rest.xml",
        "/solr", true, null);
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

  @Test
  public void testSolrConfigHandlerImmutable() throws Exception {
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/x', 'class': 'org.apache.solr.handler.DumpRequestHandler' , 'startup' : 'lazy'}\n" +
        "}";
    String uri = "/config";
    String response = restTestHarness.post(uri, SolrTestCaseJ4.json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) Utils.fromJSONString(response);
    assertNotNull(map.get("error"));
    assertTrue(map.get("error").toString().contains("immutable"));
  }

  @Test
  public void testSchemaHandlerImmutable() throws Exception {
    String payload = "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'a1',\n" +
        "                 'type': 'string',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':false\n" +
        "                 },\n" +
        "    }";

    String response = restTestHarness.post("/schema", json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) Utils.fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map error = (Map)map.get("error");
    assertNotNull("No errors", error);
    String msg = (String)error.get("msg");
    assertTrue(msg.contains("immutable"));
  }

  @Test
  public void testAddSchemaFieldsImmutable() throws Exception {
    final String error = "error";

    // check writing an existing field is okay
    String updateXMLSafe = "<add><doc><field name=\"id\">\"testdoc\"</field></doc></add>";
    String response = restTestHarness.update(updateXMLSafe);
    XMLResponseParser parser = new XMLResponseParser();
    NamedList<Object> listResponse = parser.processResponse(new StringReader(response));
    assertNull(listResponse.get(error));

    // check writing a new field is not okay
    String updateXMLNotSafe = "<add><doc><field name=\"id\">\"testdoc\"</field>" +
        "<field name=\"newField67\">\"foobar\"</field></doc></add>";
    response = restTestHarness.update(updateXMLNotSafe);
    listResponse = parser.processResponse(new StringReader(response));
    assertNotNull(listResponse.get(error));
    assertTrue(listResponse.get(error).toString().contains("immutable"));
  }
}
