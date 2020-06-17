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

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.rest.schema.TestBulkSchemaAPI;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.junit.After;
import org.junit.Before;

/**
 * Tests the useDocValuesAsStored functionality.
 */
// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestUseDocValuesAsStored2 extends RestTestBase {

  @Before
  public void before() throws Exception {
    File tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
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
  

  public void testSchemaAPI() throws Exception {
    RestTestHarness harness = restTestHarness;
    String payload = "{\n" +
        "          'add-field' : {\n" +
        "                       'name':'a1',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':false,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':false\n" +
        "                       },\n" +
        "          'add-field' : {\n" +
        "                       'name':'a2',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':false,\n" +
        "                       'useDocValuesAsStored':true,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'add-field' : {\n" +
        "                       'name':'a3',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':false,\n" +
        "                       'useDocValuesAsStored':false,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':true\n" +
        "                       }\n" +
        "          }\n";

    String response = harness.post("/schema", json(payload));

    @SuppressWarnings({"rawtypes"})
    Map m = (Map) Utils.fromJSONString(response);
    assertNull(response, m.get("errors"));

    // default value of useDocValuesAsStored
    m = TestBulkSchemaAPI.getObj(harness, "a1", "fields");
    assertNotNull("field a1 not created", m);
    assertNull(m.get("useDocValuesAsStored"));

    // useDocValuesAsStored=true
    m = TestBulkSchemaAPI.getObj(harness,"a2", "fields");
    assertNotNull("field a2 not created", m);
    assertEquals(Boolean.TRUE, m.get("useDocValuesAsStored"));

    // useDocValuesAsStored=false
    m = TestBulkSchemaAPI.getObj(harness,"a3", "fields");
    assertNotNull("field a3 not created", m);
    assertEquals(Boolean.FALSE, m.get("useDocValuesAsStored"));

    // Index documents to check the effect
    assertU(adoc("id", "myid1", "a1", "1", "a2", "2", "a3", "3"));
    assertU(commit());

    RestTestBase.assertJQ("/select?q=id:myid*&fl=*",
        "/response/docs==[{'id':'myid1', 'a1':'1', 'a2':'2'}]");

    RestTestBase.assertJQ("/select?q=id:myid*&fl=id,a1,a2,a3",
        "/response/docs==[{'id':'myid1', 'a1':'1', 'a2':'2', 'a3':'3'}]");

    RestTestBase.assertJQ("/select?q=id:myid*&fl=a3",
        "/response/docs==[{'a3':'3'}]");

    // this will return a3 because it is explicitly requested even if '*' is specified
    RestTestBase.assertJQ("/select?q=id:myid*&fl=*,a3",
        "/response/docs==[{'id':'myid1', 'a1':'1', 'a2':'2', 'a3':'3'}]");

    // this will not return a3 because the glob 'a*' will match only stored + useDocValuesAsStored=true fields
    RestTestBase.assertJQ("/select?q=id:myid*&fl=id,a*",
        "/response/docs==[{'id':'myid1', 'a1':'1', 'a2':'2'}]");
    
    // Test replace-field
    // Explicitly set useDocValuesAsStored to false
    payload = "{\n" +
        "          'replace-field' : {\n" +
        "                       'name':'a1',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':false,\n" +
        "                       'useDocValuesAsStored':false,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':false\n" +
        "                       }}";
    response = harness.post("/schema", json(payload));
    m = TestBulkSchemaAPI.getObj(harness, "a1", "fields");
    assertNotNull("field a1 doesn't exist any more", m);
    assertEquals(Boolean.FALSE, m.get("useDocValuesAsStored"));

    // Explicitly set useDocValuesAsStored to true
    payload = "{\n" +
        "          'replace-field' : {\n" +
        "                       'name':'a1',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':false,\n" +
        "                       'useDocValuesAsStored':true,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':false\n" +
        "                       }}";
    response = harness.post("/schema", json(payload));
    m = TestBulkSchemaAPI.getObj(harness, "a1", "fields");
    assertNotNull("field a1 doesn't exist any more", m);
    assertEquals(Boolean.TRUE, m.get("useDocValuesAsStored"));

    // add a field which is stored as well as docvalues
    payload = "{          'add-field' : {\n" +
        "                       'name':'a4',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'useDocValuesAsStored':true,\n" +
        "                       'docValues':true,\n" +
        "                       'indexed':true\n" +
        "                       }}";
    response = harness.post("/schema", json(payload));
    m = TestBulkSchemaAPI.getObj(harness, "a4", "fields");
    assertNotNull("field a4 not found", m);
    assertEquals(Boolean.TRUE, m.get("useDocValuesAsStored"));

    assertU(adoc("id", "myid1", "a1", "1", "a2", "2", "a3", "3", "a4", "4"));
    assertU(commit());

    RestTestBase.assertJQ("/select?q=id:myid*&fl=*",
        "/response/docs==[{'id':'myid1', 'a1':'1', 'a2':'2', 'a4':'4'}]");

  }
}
