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
package org.apache.solr.rest.schema;

import java.io.File;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.util.RestTestBase;
import org.junit.After;
import org.junit.Before;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

public class TestRemoveLastDynamicCopyField extends RestTestBase {
  private static File tmpSolrHome;

  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-single-dynamic-copy-field.xml",
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

  public void test() throws Exception {
    List copyFields = getCopyFields();
    assertEquals("There is more than one copyField directive", 1, copyFields.size());
    assertEquals("The copyField source is not '*'", "*", ((Map)copyFields.get(0)).get("source"));
    assertEquals("The copyField dest is not 'text'", "text", ((Map)copyFields.get(0)).get("dest"));

    String payload = "{ 'delete-copy-field': { 'source': '*', 'dest': 'text' } }";

    String response = restTestHarness.post("/schema?wt=json", json(payload));
    Map map = (Map)ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    assertNull(response, map.get("errors"));
    
    assertEquals(0, getCopyFields().size());
  }
  
  private List getCopyFields() throws Exception {
    String response = restTestHarness.query("/schema?wt=json");
    System.err.println(response);
    Map map = (Map)ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    return (List)((Map)map.get("schema")).get("copyFields");
  }
}
