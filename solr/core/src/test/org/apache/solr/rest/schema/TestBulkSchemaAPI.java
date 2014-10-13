package org.apache.solr.rest.schema;

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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.restlet.ext.servlet.ServerServlet;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;


public class TestBulkSchemaAPI extends RestTestBase {

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

  public void testMultipleAddFieldWithErrors() throws Exception {

    String payload = SolrTestCaseJ4.json( "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'a1',\n" +
        "                 'type': 'string1',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':false\n" +
        "                 },\n" +
        "    'add-field' : {\n" +
        "                 'type': 'string',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':true\n" +
        "                 }\n" +
        "   \n" +
        "    }");

    String response = restTestHarness.post("/schema?wt=json", payload);
    Map map = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    List l = (List) map.get("errors");

    List errorList = (List) ((Map) l.get(0)).get("errorMessages");
    assertEquals(1, errorList.size());
    assertTrue (((String)errorList.get(0)).contains("No such field type"));
    errorList = (List) ((Map) l.get(1)).get("errorMessages");
    assertEquals(1, errorList.size());
    assertTrue (((String)errorList.get(0)).contains("is a required field"));

  }


  public void testMultipleCommands() throws Exception{
    String payload = "{\n" +
        "          'add-field' : {\n" +
        "                       'name':'a1',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':false\n" +
        "                       },\n" +
        "          'add-field' : {\n" +
        "                       'name':'a2',\n" +
        "                       'type': 'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'add-dynamic-field' : {\n" +
        "                       'name' :'*_lol',\n" +
        "                        'type':'string',\n" +
        "                        'stored':true,\n" +
        "                        'indexed':true\n" +
        "                        },\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'a1',\n" +
        "                        'dest':['a2','hello_lol']\n" +
        "                        },\n" +
        "          'add-field-type' : {\n" +
        "                       'name' :'mystr',\n" +
        "                       'class' : 'solr.StrField',\n" +
        "                        'sortMissingLast':'true'\n" +
        "                        },\n" +
        "          'add-field-type' : {" +
        "                     'name' : 'myNewTxtField',\n" +
        "                     'class':'solr.TextField','positionIncrementGap':'100',\n" +
        "                     'analyzer' : {\n" +
        "                                  'charFilters':[\n" +
        "                                            {'class':'solr.PatternReplaceCharFilterFactory','replacement':'$1$1','pattern':'([a-zA-Z])\\\\\\\\1+'}\n" +
        "                                         ],\n" +
        "                     'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},\n" +
        "                     'filters':[\n" +
        "                             {'class':'solr.WordDelimiterFilterFactory','preserveOriginal':'0'},\n" +
        "                             {'class':'solr.StopFilterFactory','words':'stopwords.txt','ignoreCase':'true'},\n" +
        "                             {'class':'solr.LowerCaseFilterFactory'},\n" +
        "                             {'class':'solr.ASCIIFoldingFilterFactory'},\n" +
        "                             {'class':'solr.KStemFilterFactory'}\n" +
        "                  ]\n" +
        "                }\n" +
        "              }"+
        "          }";

    RestTestHarness harness = restTestHarness;


    String response = harness.post("/schema?wt=json", SolrTestCaseJ4.json( payload));

    Map map = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
    assertNull(response,  map.get("errors"));


    Map m = getObj(harness, "a1", "fields");
    assertNotNull("field a1 not created", m);

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.FALSE, m.get("indexed"));

    m = getObj(harness,"a2", "fields");
    assertNotNull("field a2 not created", m);

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.TRUE, m.get("indexed"));

    m = getObj(harness,"*_lol", "dynamicFields");
    assertNotNull("field *_lol not created",m );

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.TRUE, m.get("indexed"));

    List l = getCopyFields(harness,"a1");
    Set s =new HashSet();
    assertEquals(2,l.size());
    s.add(((Map) l.get(0)).get("dest"));
    s.add(((Map) l.get(1)).get("dest"));
    assertTrue(s.contains("hello_lol"));
    assertTrue(s.contains("a2"));

    m = getObj(harness,"mystr", "fieldTypes");
    assertNotNull(m);
    assertEquals("solr.StrField",m.get("class"));
    assertEquals("true",String.valueOf(m.get("sortMissingLast")));

    m = getObj(harness,"myNewTxtField", "fieldTypes");
    assertNotNull(m);


  }

  public static Map getObj(RestTestHarness restHarness, String fld, String key) throws Exception {
    Map map = getRespMap(restHarness);
    List l = (List) ((Map)map.get("schema")).get(key);
    for (Object o : l) {
      Map m = (Map) o;
      if(fld.equals(m.get("name"))) return m;
    }
    return null;
  }

  public static Map getRespMap(RestTestHarness restHarness) throws Exception {
    String response = restHarness.query("/schema?wt=json");
    return (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
  }

  public static List getCopyFields(RestTestHarness harness, String src) throws Exception {
    Map map = getRespMap(harness);
    List l = (List) ((Map)map.get("schema")).get("copyFields");
    List result = new ArrayList();
    for (Object o : l) {
      Map m = (Map) o;
      if(src.equals(m.get("source"))) result.add(m);
    }
    return result;

  }


}
