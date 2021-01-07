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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.misc.SweetSpotSimilarity;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.similarities.SchemaSimilarityFactory;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestBase;
import org.apache.solr.util.RestTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.fromJSONString;


public class TestBulkSchemaAPI extends RestTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  private static File tmpSolrHome;

  @Before
  public void before() throws Exception {
    tmpSolrHome = createTempDir().toFile();
    FileUtils.copyDirectory(new File(TEST_HOME()), tmpSolrHome.getAbsoluteFile());

    System.setProperty("managed.schema.mutable", "true");
    System.setProperty("enable.update.log", "false");

    createJettyAndHarness(tmpSolrHome.getAbsolutePath(), "solrconfig-managed-schema.xml", "schema-rest.xml",
        "/solr", true, null);
    if (random().nextBoolean()) {
      log.info("These tests are run with V2 API");
      restTestHarness.setServerProvider(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return jetty.getBaseUrl().toString() + "/____v2/cores/" + DEFAULT_TEST_CORENAME;
        }
      });
    }
  }

  @After
  public void after() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }

  public void testMultipleAddFieldWithErrors() throws Exception {

    String payload = "{\n" +
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
        "    }";

    String response = restTestHarness.post("/schema", json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map error = (Map)map.get("error");
    assertNotNull("No errors", error);
    @SuppressWarnings({"rawtypes"})
    List details = (List)error.get("details");
    assertNotNull("No details", details);
    assertEquals("Wrong number of details", 2, details.size());
    @SuppressWarnings({"rawtypes"})
    List firstErrorList = (List)((Map)details.get(0)).get("errorMessages");
    assertEquals(1, firstErrorList.size());
    assertTrue (((String)firstErrorList.get(0)).contains("Field 'a1': Field type 'string1' not found.\n"));
    @SuppressWarnings({"rawtypes"})
    List secondErrorList = (List)((Map)details.get(1)).get("errorMessages");
    assertEquals(1, secondErrorList.size());
    assertTrue (((String)secondErrorList.get(0)).contains("is a required field"));
  }
  
  public void testAnalyzerClass() throws Exception {

    String addFieldTypeAnalyzerWithClass = "{\n" +
        "'add-field-type' : {" +
        "    'name' : 'myNewTextFieldWithAnalyzerClass',\n" +
        "    'class':'solr.TextField',\n" +
        "    'analyzer' : {\n" +
        "        'luceneMatchVersion':'5.0.0',\n" +
        "        'class':'org.apache.lucene.analysis.core.WhitespaceAnalyzer'\n";
    String charFilters =
        "        'charFilters' : [{\n" +
        "            'class':'solr.PatternReplaceCharFilterFactory',\n" +
        "            'replacement':'$1$1',\n" +
        "            'pattern':'([a-zA-Z])\\\\\\\\1+'\n" +
        "        }],\n";
    String tokenizer =
        "        'tokenizer' : { 'class':'solr.WhitespaceTokenizerFactory' },\n";
    String filters =
        "        'filters' : [{ 'class':'solr.ASCIIFoldingFilterFactory' }]\n";
    String suffix =
        "    }\n"+
        "}}";

    String response = restTestHarness.post("/schema",
        json(addFieldTypeAnalyzerWithClass + ',' + charFilters + tokenizer + filters + suffix));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    @SuppressWarnings({"rawtypes"})
    Map error = (Map)map.get("error");
    assertNotNull("No errors", error);
    @SuppressWarnings({"rawtypes"})
    List details = (List)error.get("details");
    assertNotNull("No details", details);
    assertEquals("Wrong number of details", 1, details.size());
    @SuppressWarnings({"rawtypes"})
    List errorList = (List)((Map)details.get(0)).get("errorMessages");
    assertEquals(1, errorList.size());
    assertTrue (((String)errorList.get(0)).contains
        ("An analyzer with a class property may not define any char filters!"));

    response = restTestHarness.post("/schema",
        json(addFieldTypeAnalyzerWithClass + ',' + tokenizer + filters + suffix));
    map = (Map) fromJSONString(response);
    error = (Map)map.get("error");
    assertNotNull("No errors", error);
    details = (List)error.get("details");
    assertNotNull("No details", details);
    assertEquals("Wrong number of details", 1, details.size());
    errorList = (List)((Map)details.get(0)).get("errorMessages");
    assertEquals(1, errorList.size());
    assertTrue (((String)errorList.get(0)).contains
        ("An analyzer with a class property may not define a tokenizer!"));

    response = restTestHarness.post("/schema",
        json(addFieldTypeAnalyzerWithClass + ',' + filters + suffix));
    map = (Map) fromJSONString(response);
    error = (Map)map.get("error");
    assertNotNull("No errors", error);
    details = (List)error.get("details");
    assertNotNull("No details", details);
    assertEquals("Wrong number of details", 1, details.size());
    errorList = (List)((Map)details.get(0)).get("errorMessages");
    assertEquals(1, errorList.size());
    assertTrue (((String)errorList.get(0)).contains
        ("An analyzer with a class property may not define any filters!"));

    response = restTestHarness.post("/schema", json(addFieldTypeAnalyzerWithClass + suffix));
    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(restTestHarness, "myNewTextFieldWithAnalyzerClass", "fieldTypes");
    assertNotNull(map);
    @SuppressWarnings({"rawtypes"})
    Map analyzer = (Map)map.get("analyzer");
    assertEquals("org.apache.lucene.analysis.core.WhitespaceAnalyzer", String.valueOf(analyzer.get("class")));
    assertEquals("5.0.0", String.valueOf(analyzer.get(IndexSchema.LUCENE_MATCH_VERSION_PARAM)));
  }

  public void testAddFieldMatchingExistingDynamicField() throws Exception {
    RestTestHarness harness = restTestHarness;

    String newFieldName = "attr_non_dynamic";

    @SuppressWarnings({"rawtypes"})
    Map map = getObj(harness, newFieldName, "fields");
    assertNull("Field '" + newFieldName + "' already exists in the schema", map);

    map = getObj(harness, "attr_*", "dynamicFields");
    assertNotNull("'attr_*' dynamic field does not exist in the schema", map);

    map = getObj(harness, "boolean", "fieldTypes");
    assertNotNull("'boolean' field type does not exist in the schema", map);

    String payload = "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'" + newFieldName + "',\n" +
        "                 'type':'boolean',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':true\n" +
        "                 }\n" +
        "    }";

    String response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(harness, newFieldName, "fields");
    assertNotNull("Field '" + newFieldName + "' is not in the schema", map);
  }

  public void testAddIllegalDynamicField() throws Exception {
    RestTestHarness harness = restTestHarness;

    String newFieldName = "illegal";

    String payload = "{\n" +
        "    'add-dynamic-field' : {\n" +
        "                 'name':'" + newFieldName + "',\n" +
        "                 'type':'string',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':true\n" +
        "                 }\n" +
        "    }";

    String response = harness.post("/schema", json(payload));
    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    assertNotNull(response, map.get("error"));

    map = getObj(harness, newFieldName, "dynamicFields");
    assertNull(newFieldName + " illegal dynamic field should not have been added to schema", map);
  }

  @SuppressWarnings({"rawtypes"})
  public void testAddIllegalFields() throws Exception {
    RestTestHarness harness = restTestHarness;

    // 1. Make sure you can't create a new field with an asterisk in its name
    String newFieldName = "asterisk*";

    String payload = "{\n" +
        "    'add-field' : {\n" +
        "         'name':'" + newFieldName + "',\n" +
        "         'type':'string',\n" +
        "         'stored':true,\n" +
        "         'indexed':true\n" +
        "     }\n" +
        "}";

    String response = harness.post("/schema", json(payload));
    Map map = (Map) fromJSONString(response);
    assertNotNull(response, map.get("error"));

    map = getObj(harness, newFieldName, "fields");
    assertNull(newFieldName + " illegal dynamic field should not have been added to schema", map);

    // 2. Make sure you get an error when you try to create a field that already exists
    // Make sure 'wdf_nocase' field exists
    newFieldName = "wdf_nocase";
    Map m = getObj(harness, newFieldName, "fields");
    assertNotNull("'" + newFieldName + "' field does not exist in the schema", m);

    payload = "{\n" +
        "    'add-field' : {\n" +
        "         'name':'" + newFieldName + "',\n" +
        "         'type':'string',\n" +
        "         'stored':true,\n" +
        "         'indexed':true\n" +
        "     }\n" +
        "}";

    response = harness.post("/schema", json(payload));
    map = (Map) fromJSONString(response);
    assertNotNull(response, map.get("error"));
  }

  @SuppressWarnings({"rawtypes"})
  public void testAddFieldWithExistingCatchallDynamicField() throws Exception {
    RestTestHarness harness = restTestHarness;

    String newFieldName = "NewField1";

    Map map = getObj(harness, newFieldName, "fields");
    assertNull("Field '" + newFieldName + "' already exists in the schema", map);

    map = getObj(harness, "*", "dynamicFields");
    assertNull("'*' dynamic field already exists in the schema", map);

    map = getObj(harness, "string", "fieldTypes");
    assertNotNull("'boolean' field type does not exist in the schema", map);

    map = getObj(harness, "boolean", "fieldTypes");
    assertNotNull("'boolean' field type does not exist in the schema", map);

    String payload = "{\n" +
        "    'add-dynamic-field' : {\n" +
        "         'name':'*',\n" +
        "         'type':'string',\n" +
        "         'stored':true,\n" +
        "         'indexed':true\n" +
        "    }\n" +
        "}";

    String response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(harness, "*", "dynamicFields");
    assertNotNull("Dynamic field '*' is not in the schema", map);

    payload = "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'" + newFieldName + "',\n" +
        "                 'type':'boolean',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':true\n" +
        "                 }\n" +
        "    }";

    response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(harness, newFieldName, "fields");
    assertNotNull("Field '" + newFieldName + "' is not in the schema", map);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testMultipleCommands() throws Exception{
    RestTestHarness harness = restTestHarness;

    Map m = getObj(harness, "wdf_nocase", "fields");
    assertNotNull("'wdf_nocase' field does not exist in the schema", m);
    
    m = getObj(harness, "wdf_nocase", "fieldTypes");
    assertNotNull("'wdf_nocase' field type does not exist in the schema", m);
    
    m = getObj(harness, "boolean", "fieldTypes");
    assertNotNull("'boolean' field type does not exist in the schema", m);
    assertNull(m.get("sortMissingFirst"));
    assertTrue((Boolean)m.get("sortMissingLast"));
    
    m = getObj(harness, "name", "fields");
    assertNotNull("'name' field does not exist in the schema", m);
    assertEquals("nametext", m.get("type"));

    m = getObj(harness, "bind", "fields");
    assertNotNull("'bind' field does not exist in the schema", m);
    assertEquals("boolean", m.get("type"));

    m = getObj(harness, "attr_*", "dynamicFields");
    assertNotNull("'attr_*' dynamic field does not exist in the schema", m);
    assertEquals("text", m.get("type"));

    List l = getSourceCopyFields(harness, "*_i");
    Set s = new HashSet();
    assertEquals(4, l.size());
    s.add(((Map)l.get(0)).get("dest"));
    s.add(((Map)l.get(1)).get("dest"));
    s.add(((Map) l.get(2)).get("dest"));
    s.add(((Map) l.get(3)).get("dest"));
    assertTrue(s.contains("title"));
    assertTrue(s.contains("*_s"));

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
        "                       'indexed':true,\n" +
        "                       'uninvertible':true,\n" +
        "                       },\n" +
        "          'add-dynamic-field' : {\n" +
        "                       'name' :'*_lol',\n" +
        "                       'type':'string',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true,\n" +
        "                       'uninvertible':false,\n" +
        "                       },\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'a1',\n" +
        "                       'dest':['a2','hello_lol']\n" +
        "                       },\n" +
        "          'add-field-type' : {\n" +
        "                       'name' :'mystr',\n" +
        "                       'class' : 'solr.StrField',\n" +
        "                       'sortMissingLast':'true'\n" +
        "                       },\n" +
        "          'add-field-type' : {" +
        "                       'name' : 'myNewTxtField',\n" +
        "                       'class':'solr.TextField',\n" +
        "                       'positionIncrementGap':'100',\n" +
        "                       'indexAnalyzer' : {\n" +
        "                               'charFilters':[\n" +
        "                                          {\n" +
        "                                           'class':'solr.PatternReplaceCharFilterFactory',\n" +
        "                                           'replacement':'$1$1',\n" +
        "                                           'pattern':'([a-zA-Z])\\\\\\\\1+'\n" +
        "                                          }\n" +
        "                                         ],\n" +
        "                               'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},\n" +
        "                               'filters':[\n" +
        "                                          {\n" +
        "                                           'class':'solr.WordDelimiterGraphFilterFactory',\n" +
        "                                           'preserveOriginal':'0'\n" +
        "                                          },\n" +
        "                                          {\n" +
        "                                           'class':'solr.StopFilterFactory',\n" +
        "                                           'words':'stopwords.txt',\n" +
        "                                           'ignoreCase':'true'\n" +
        "                                          },\n" +
        "                                          {'class':'solr.LowerCaseFilterFactory'},\n" +
        "                                          {'class':'solr.ASCIIFoldingFilterFactory'},\n" +
        "                                          {'class':'solr.KStemFilterFactory'},\n" +
        "                                          {'class':'solr.FlattenGraphFilterFactory'}\n" +
        "                                         ]\n" +
        "                               },\n" +
        "                       'queryAnalyzer' : {\n" +
        "                               'charFilters':[\n" +
        "                                          {\n" +
        "                                           'class':'solr.PatternReplaceCharFilterFactory',\n" +
        "                                           'replacement':'$1$1',\n" +
        "                                           'pattern':'([a-zA-Z])\\\\\\\\1+'\n" +
        "                                          }\n" +
        "                                         ],\n" +
        "                               'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},\n" +
        "                               'filters':[\n" +
        "                                          {\n" +
        "                                           'class':'solr.WordDelimiterGraphFilterFactory',\n" +
        "                                           'preserveOriginal':'0'\n" +
        "                                          },\n" +
        "                                          {\n" +
        "                                           'class':'solr.StopFilterFactory',\n" +
        "                                           'words':'stopwords.txt',\n" +
        "                                           'ignoreCase':'true'\n" +
        "                                          },\n" +
        "                                          {'class':'solr.LowerCaseFilterFactory'},\n" +
        "                                          {'class':'solr.ASCIIFoldingFilterFactory'},\n" +
        "                                          {'class':'solr.KStemFilterFactory'}\n" +
        "                                         ]\n" +
        "                               }\n" +
        "                       },\n"+
        "          'add-field' : {\n" +
        "                       'name':'a3',\n" +
        "                       'type': 'myNewTxtField',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'add-field-type' : {" +
        "                       'name' : 'myWhitespaceTxtField',\n" +
        "                       'class':'solr.TextField',\n" +
        "                       'uninvertible':false,\n" +
        "                       'analyzer' : {'class' : 'org.apache.lucene.analysis.core.WhitespaceAnalyzer'}\n" +
        "                       },\n"+
        "          'add-field' : {\n" +
        "                       'name':'a5',\n" +
        "                       'type': 'myWhitespaceTxtField',\n" +
        "                       'stored':true\n" +
        "                       },\n" +
        "          'add-field-type' : {" +
        "                       'name' : 'mySimField',\n" +
        "                       'class':'solr.TextField',\n" +
        "                       'analyzer' : {'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'}},\n" +
        "                       'similarity' : {'class':'org.apache.lucene.misc.SweetSpotSimilarity'}\n" +
        "                       },\n"+
        "          'add-field' : {\n" +
        "                       'name':'a4',\n" +
        "                       'type': 'mySimField',\n" +
        "                       'stored':true,\n" +
        "                       'indexed':true\n" +
        "                       },\n" +
        "          'delete-field' : {'name':'wdf_nocase'},\n" +
        "          'delete-field-type' : {'name':'wdf_nocase'},\n" +
        "          'delete-dynamic-field' : {'name':'*_tt'},\n" +
        "          'delete-copy-field' : {'source':'a1', 'dest':'a2'},\n" +
        "          'delete-copy-field' : {'source':'*_i', 'dest':['title', '*_s']},\n" +
        "          'replace-field-type' : {\n" +
        "                       'name':'boolean',\n" +
        "                       'class':'solr.BoolField',\n" +
        "                       'sortMissingFirst':true\n" +
        "                       },\n" +
        "          'replace-field' : {\n" +
        "                       'name':'name',\n" +
        "                       'type':'string',\n" +
        "                       'indexed':true,\n" +
        "                       'stored':true\n" +
        "                       },\n" +
        "          'replace-dynamic-field' : {\n" +
        "                       'name':'attr_*',\n" +
        "                       'type':'string',\n" +
        "                       'indexed':true,\n" +
        "                       'stored':true,\n" +
        "                       'multiValued':true\n" +
        "                       }\n" +
        "          }\n";
    
    String response = harness.post("/schema", json(payload));

    Map map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    m = getObj(harness, "a1", "fields");
    assertNotNull("field a1 not created", m);

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.FALSE, m.get("indexed"));

    m = getObj(harness,"a2", "fields");
    assertNotNull("field a2 not created", m);

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.TRUE, m.get("indexed"));
    assertEquals(Boolean.TRUE, m.get("uninvertible"));

    m = getObj(harness,"*_lol", "dynamicFields");
    assertNotNull("field *_lol not created", m);

    assertEquals("string", m.get("type"));
    assertEquals(Boolean.TRUE, m.get("stored"));
    assertEquals(Boolean.TRUE, m.get("indexed"));
    assertEquals(Boolean.FALSE, m.get("uninvertible"));

    l = getSourceCopyFields(harness, "a1");
    s = new HashSet();
    assertEquals(1, l.size());
    s.add(((Map) l.get(0)).get("dest"));
    assertTrue(s.contains("hello_lol"));

    l = getSourceCopyFields(harness, "*_i");
    s = new HashSet();
    assertEquals(2, l.size());
    s.add(((Map)l.get(0)).get("dest"));
    s.add(((Map) l.get(1)).get("dest"));
    assertFalse(s.contains("title"));
    assertFalse(s.contains("*_s"));

    m = getObj(harness, "mystr", "fieldTypes");
    assertNotNull(m);
    assertEquals("solr.StrField", m.get("class"));
    assertEquals("true", String.valueOf(m.get("sortMissingLast")));

    m = getObj(harness, "myNewTxtField", "fieldTypes");
    assertNotNull(m);

    m = getObj(harness, "a3", "fields");
    assertNotNull("field a3 not created", m);
    assertEquals("myNewTxtField", m.get("type"));

    m = getObj(harness, "mySimField", "fieldTypes");
    assertNotNull(m);
    m = (Map)m.get("similarity");
    assertNotNull(m);
    assertEquals(SweetSpotSimilarity.class.getName(), m.get("class"));

    m = getObj(harness, "a4", "fields");
    assertNotNull("field a4 not created", m);
    assertEquals("mySimField", m.get("type"));
    assertFieldSimilarity("a4", SweetSpotSimilarity.class);
    
    m = getObj(harness, "myWhitespaceTxtField", "fieldTypes");
    assertNotNull(m);
    assertEquals(Boolean.FALSE, m.get("uninvertible"));
    assertNull(m.get("similarity")); // unspecified, expect default

    m = getObj(harness, "a5", "fields");
    assertNotNull("field a5 not created", m);
    assertEquals("myWhitespaceTxtField", m.get("type"));
    assertNull(m.get("uninvertible")); // inherited, but API shouldn't return w/o explicit showDefaults
    assertFieldSimilarity("a5", BM25Similarity.class); // unspecified, expect default

    m = getObj(harness, "wdf_nocase", "fields");
    assertNull("field 'wdf_nocase' not deleted", m);

    m = getObj(harness, "wdf_nocase", "fieldTypes");
    assertNull("field type 'wdf_nocase' not deleted", m);

    m = getObj(harness, "*_tt", "dynamicFields");
    assertNull("dynamic field '*_tt' not deleted", m);

    m = getObj(harness, "boolean", "fieldTypes");
    assertNotNull("'boolean' field type does not exist in the schema", m);
    assertNull(m.get("sortMissingLast"));
    assertTrue((Boolean)m.get("sortMissingFirst"));

    m = getObj(harness, "bind", "fields"); // this field will be rebuilt when "boolean" field type is replaced
    assertNotNull("'bind' field does not exist in the schema", m);

    m = getObj(harness, "name", "fields");
    assertNotNull("'name' field does not exist in the schema", m);
    assertEquals("string", m.get("type"));

    m = getObj(harness, "attr_*", "dynamicFields");
    assertNotNull("'attr_*' dynamic field does not exist in the schema", m);
    assertEquals("string", m.get("type"));
  }

  public void testCopyFieldRules() throws Exception {
    RestTestHarness harness = restTestHarness;

    @SuppressWarnings({"rawtypes"})
    Map m = getObj(harness, "name", "fields");
    assertNotNull("'name' field does not exist in the schema", m);

    m = getObj(harness, "bind", "fields");
    assertNotNull("'bind' field does not exist in the schema", m);

    @SuppressWarnings({"rawtypes"})
    List l = getSourceCopyFields(harness, "bleh_s");
    assertTrue("'bleh_s' copyField rule exists in the schema", l.isEmpty());

    String payload = "{\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'bleh_s',\n" +
        "                       'dest':'name'\n" +
        "                       }\n" +
        "          }\n";
    String response = harness.post("/schema", json(payload));

    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    l = getSourceCopyFields(harness, "bleh_s");
    assertFalse("'bleh_s' copyField rule doesn't exist", l.isEmpty());
    assertEquals("bleh_s", ((Map)l.get(0)).get("source"));
    assertEquals("name", ((Map)l.get(0)).get("dest"));

    // delete copy field rule
    payload = "{\n" +
        "          'delete-copy-field' : {\n" +
        "                       'source' :'bleh_s',\n" +
        "                       'dest':'name'\n" +
        "                       }\n" +
        "          }\n";

    response = harness.post("/schema", json(payload));
    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));
    l = getSourceCopyFields(harness, "bleh_s");
    assertTrue("'bleh_s' copyField rule exists in the schema", l.isEmpty());

    // copy and delete with multiple destination
    payload = "{\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'bleh_s',\n" +
        "                       'dest':['name','bind']\n" +
        "                       }\n" +
        "          }\n";
    response = harness.post("/schema", json(payload));
    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    l = getSourceCopyFields(harness, "bleh_s");
    assertEquals(2, l.size());

    payload = "{\n" +
        "          'delete-copy-field' : {\n" +
        "                       'source' :'bleh_s',\n" +
        "                       'dest':['name','bind']\n" +
        "                       }\n" +
        "          }\n";

    response = harness.post("/schema", json(payload));
    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));
    l = getSourceCopyFields(harness, "bleh_s");
    assertTrue("'bleh_s' copyField rule exists in the schema", l.isEmpty());
  }

  @SuppressWarnings({"rawtypes"})
  public void testCopyFieldWithReplace() throws Exception {
    RestTestHarness harness = restTestHarness;
    String newFieldName = "test_solr_14950";

    // add-field-type
    String addFieldTypeAnalyzer = "{\n" +
        "'add-field-type' : {" +
        "    'name' : 'myNewTextField',\n" +
        "    'class':'solr.TextField',\n" +
        "    'analyzer' : {\n" +
        "        'charFilters' : [{\n" +
        "                'class':'solr.PatternReplaceCharFilterFactory',\n" +
        "                'replacement':'$1$1',\n" +
        "                'pattern':'([a-zA-Z])\\\\\\\\1+'\n" +
        "            }],\n" +
        "        'tokenizer' : { 'class':'solr.WhitespaceTokenizerFactory' },\n" +
        "        'filters' : [{ 'class':'solr.ASCIIFoldingFilterFactory' }]\n" +
        "    }\n"+
        "}}";

    String response = restTestHarness.post("/schema", json(addFieldTypeAnalyzer));
    Map map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));
    map = getObj(harness, "myNewTextField", "fieldTypes");
    assertNotNull("'myNewTextField' field type does not exist in the schema", map);

    // add-field
    String payload = "{\n" +
        "    'add-field' : {\n" +
        "                 'name':'" + newFieldName + "',\n" +
        "                 'type':'myNewTextField',\n" +
        "                 'stored':true,\n" +
        "                 'indexed':true\n" +
        "                 }\n" +
        "    }";

    response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    Map m = getObj(harness, newFieldName, "fields");
    assertNotNull("'"+ newFieldName + "' field does not exist in the schema", m);

    // add copy-field with explicit source and destination
    List l = getSourceCopyFields(harness, "bleh_s");
    assertTrue("'bleh_s' copyField rule exists in the schema", l.isEmpty());

    payload = "{\n" +
        "          'add-copy-field' : {\n" +
        "                       'source' :'bleh_s',\n" +
        "                       'dest':'"+ newFieldName + "'\n" +
        "                       }\n" +
        "          }\n";
    response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    l = getSourceCopyFields(harness, "bleh_s");
    assertFalse("'bleh_s' copyField rule doesn't exist", l.isEmpty());
    assertEquals("bleh_s", ((Map)l.get(0)).get("source"));
    assertEquals(newFieldName, ((Map)l.get(0)).get("dest"));

    // replace-field-type
    String replaceFieldTypeAnalyzer = "{\n" +
        "'replace-field-type' : {" +
        "    'name' : 'myNewTextField',\n" +
        "    'class':'solr.TextField',\n" +
        "    'analyzer' : {\n" +
        "        'tokenizer' : { 'class':'solr.WhitespaceTokenizerFactory' },\n" +
        "        'filters' : [{ 'class':'solr.ASCIIFoldingFilterFactory' }]\n" +
        "    }\n"+
        "}}";

    response = restTestHarness.post("/schema", json(replaceFieldTypeAnalyzer));
    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(restTestHarness, "myNewTextField", "fieldTypes");
    assertNotNull(map);
    Map analyzer = (Map)map.get("analyzer");
    assertNull("'myNewTextField' shouldn't contain charFilters", analyzer.get("charFilters"));

    l = getSourceCopyFields(harness, "bleh_s");
    assertFalse("'bleh_s' copyField rule doesn't exist", l.isEmpty());
    assertEquals("bleh_s", ((Map)l.get(0)).get("source"));
    assertEquals(newFieldName, ((Map)l.get(0)).get("dest"));

    // with replace-field
    String replaceField = "{'replace-field' : {'name':'" + newFieldName + "', 'type':'string'}}";
    response = harness.post("/schema", json(replaceField));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));

    l = getSourceCopyFields(harness, "bleh_s");
    assertFalse("'bleh_s' copyField rule doesn't exist", l.isEmpty());
    assertEquals("bleh_s", ((Map)l.get(0)).get("source"));
    assertEquals(newFieldName, ((Map)l.get(0)).get("dest"));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testDeleteAndReplace() throws Exception {
    RestTestHarness harness = restTestHarness;

    Map map = getObj(harness, "NewField1", "fields");
    assertNull("Field 'NewField1' already exists in the schema", map);

    map = getObj(harness, "NewField2", "fields");
    assertNull("Field 'NewField2' already exists in the schema", map);

    map = getObj(harness, "NewFieldType", "fieldTypes");
    assertNull("'NewFieldType' field type already exists in the schema", map);

    List list = getSourceCopyFields(harness, "NewField1");
    assertEquals("There is already a copy field with source 'NewField1' in the schema", 0, list.size());

    map = getObj(harness, "NewDynamicField1*", "dynamicFields");
    assertNull("Dynamic field 'NewDynamicField1*' already exists in the schema", map);

    map = getObj(harness, "NewDynamicField2*", "dynamicFields");
    assertNull("Dynamic field 'NewDynamicField2*' already exists in the schema", map);

    String cmds = "{\n" + 
        "     'add-field-type': {   'name':'NewFieldType',     'class':'solr.StrField'                    },\n" +
        "          'add-field': [{  'name':'NewField1',         'type':'NewFieldType'                    },\n" +
        "                        {  'name':'NewField2',         'type':'NewFieldType'                    },\n" +
        "                        {  'name':'NewField3',         'type':'NewFieldType'                    },\n" +
        "                        {  'name':'NewField4',         'type':'NewFieldType'                    }],\n" +
        "  'add-dynamic-field': [{  'name':'NewDynamicField1*', 'type':'NewFieldType'                    },\n" +
        "                        {  'name':'NewDynamicField2*', 'type':'NewFieldType'                    },\n" +
        "                        {  'name':'NewDynamicField3*', 'type':'NewFieldType'                    }],\n" +
        "     'add-copy-field': [{'source':'NewField1',         'dest':['NewField2', 'NewDynamicField1A']},\n" +
        "                        {'source':'NewDynamicField1*', 'dest':'NewField2'                       },\n" +
        "                        {'source':'NewDynamicField2*', 'dest':'NewField2'                       },\n" +
        "                        {'source':'NewDynamicField3*', 'dest':'NewField3'                       },\n" +
        "                        {'source':'NewField4',         'dest':'NewField3'                       },\n" +
        "                        {'source':'NewField4',         'dest':'NewField2', maxChars: 100        },\n" +
        "                        {'source':'NewField4',         'dest':['NewField1'], maxChars: 3333     }]\n" +
        "}\n";

    String response = harness.post("/schema", json(cmds));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    map = getObj(harness, "NewFieldType", "fieldTypes");
    assertNotNull("'NewFieldType' is not in the schema", map);

    map = getObj(harness, "NewField1", "fields");
    assertNotNull("Field 'NewField1' is not in the schema", map);

    map = getObj(harness, "NewField2", "fields");
    assertNotNull("Field 'NewField2' is not in the schema", map);

    map = getObj(harness, "NewField3", "fields");
    assertNotNull("Field 'NewField3' is not in the schema", map);

    map = getObj(harness, "NewField4", "fields");
    assertNotNull("Field 'NewField4' is not in the schema", map);

    list = getSourceCopyFields(harness, "NewField1");
    Set set = new HashSet();
    for (Object obj : list) {
      set.add(((Map)obj).get("dest"));
    }
    assertEquals(2, list.size());
    assertTrue(set.contains("NewField2"));
    assertTrue(set.contains("NewDynamicField1A"));

    list = getSourceCopyFields(harness, "NewDynamicField1*");
    assertEquals(1, list.size());
    assertEquals("NewField2", ((Map)list.get(0)).get("dest"));

    list = getSourceCopyFields(harness, "NewDynamicField2*");
    assertEquals(1, list.size());
    assertEquals("NewField2", ((Map)list.get(0)).get("dest"));

    list = getSourceCopyFields(harness, "NewDynamicField3*");
    assertEquals(1, list.size());
    assertEquals("NewField3", ((Map)list.get(0)).get("dest"));

    list = getSourceCopyFields(harness, "NewField4");
    assertEquals(3, list.size());
    map.clear();
    for (Object obj : list) { 
      map.put(((Map)obj).get("dest"), ((Map)obj).get("maxChars"));
    }
    assertTrue(map.containsKey("NewField1"));
    assertEquals(3333L, map.get("NewField1"));
    assertTrue(map.containsKey("NewField2"));
    assertEquals(100L, map.get("NewField2"));
    assertTrue(map.containsKey("NewField3"));
    assertNull(map.get("NewField3"));

    cmds = "{'delete-field-type' : {'name':'NewFieldType'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    Object errors = map.get("error");
    assertNotNull(errors);
    assertTrue(errors.toString().contains("Can't delete 'NewFieldType' because it's the field type of "));

    cmds = "{'delete-field' : {'name':'NewField1'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNotNull(errors);
    assertTrue(errors.toString().contains
        ("Can't delete field 'NewField1' because it's referred to by at least one copy field directive"));

    cmds = "{'delete-field' : {'name':'NewField2'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNotNull(errors);
    assertTrue(errors.toString().contains
        ("Can't delete field 'NewField2' because it's referred to by at least one copy field directive"));

    cmds = "{'replace-field' : {'name':'NewField1', 'type':'string'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));
    // Make sure the copy field directives with source NewField1 are preserved
    list = getSourceCopyFields(harness, "NewField1");
    set = new HashSet();
    for (Object obj : list) {
      set.add(((Map)obj).get("dest"));
    }
    assertEquals(2, list.size());
    assertTrue(set.contains("NewField2"));
    assertTrue(set.contains("NewDynamicField1A"));

    cmds = "{'delete-dynamic-field' : {'name':'NewDynamicField1*'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNotNull(errors);
    assertTrue(errors.toString().contains
        ("copyField dest :'NewDynamicField1A' is not an explicit field and doesn't match a dynamicField."));

    cmds = "{'replace-field' : {'name':'NewField2', 'type':'string'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNull(errors);
    // Make sure the copy field directives with destination NewField2 are preserved
    list = getDestCopyFields(harness, "NewField2");
    set = new HashSet();
    for (Object obj : list) {
      set.add(((Map)obj).get("source"));
    }
    assertEquals(4, list.size());
    assertTrue(set.contains("NewField1"));
    assertTrue(set.contains("NewField4"));
    assertTrue(set.contains("NewDynamicField1*"));
    assertTrue(set.contains("NewDynamicField2*"));

    cmds = "{'replace-dynamic-field' : {'name':'NewDynamicField2*', 'type':'string'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNull(errors);
    // Make sure the copy field directives with source NewDynamicField2* are preserved
    list = getSourceCopyFields(harness, "NewDynamicField2*");
    assertEquals(1, list.size());
    assertEquals("NewField2", ((Map) list.get(0)).get("dest"));

    cmds = "{'replace-dynamic-field' : {'name':'NewDynamicField1*', 'type':'string'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    errors = map.get("error");
    assertNull(errors);
    // Make sure the copy field directives with destinations matching NewDynamicField1* are preserved
    list = getDestCopyFields(harness, "NewDynamicField1A");
    assertEquals(1, list.size());
    assertEquals("NewField1", ((Map) list.get(0)).get("source"));

    cmds = "{'replace-field-type': {'name':'NewFieldType', 'class':'solr.BinaryField'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));
    // Make sure the copy field directives with sources and destinations of type NewFieldType are preserved
    list = getDestCopyFields(harness, "NewField3");
    assertEquals(2, list.size());
    set = new HashSet();
    for (Object obj : list) {
      set.add(((Map)obj).get("source"));
    }
    assertTrue(set.contains("NewField4"));
    assertTrue(set.contains("NewDynamicField3*"));

    cmds = "{\n" +
        "  'delete-copy-field': [{'source':'NewField1',         'dest':['NewField2', 'NewDynamicField1A']     },\n" +
        "                        {'source':'NewDynamicField1*', 'dest':'NewField2'                            },\n" +
        "                        {'source':'NewDynamicField2*', 'dest':'NewField2'                            },\n" +
        "                        {'source':'NewDynamicField3*', 'dest':'NewField3'                            },\n" +
        "                        {'source':'NewField4',         'dest':['NewField1', 'NewField2', 'NewField3']}]\n" +
        "}\n";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));
    list = getSourceCopyFields(harness, "NewField1");
    assertEquals(0, list.size());
    list = getSourceCopyFields(harness, "NewDynamicField1*");
    assertEquals(0, list.size());
    list = getSourceCopyFields(harness, "NewDynamicField2*");
    assertEquals(0, list.size());
    list = getSourceCopyFields(harness, "NewDynamicField3*");
    assertEquals(0, list.size());
    list = getSourceCopyFields(harness, "NewField4");
    assertEquals(0, list.size());
    
    cmds = "{'delete-field': [{'name':'NewField1'},{'name':'NewField2'},{'name':'NewField3'},{'name':'NewField4'}]}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));

    cmds = "{'delete-dynamic-field': [{'name':'NewDynamicField1*'}," +
        "                             {'name':'NewDynamicField2*'},\n" +
        "                             {'name':'NewDynamicField3*'}]\n" +
        "}\n";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));
    
    cmds = "{'delete-field-type':{'name':'NewFieldType'}}";
    response = harness.post("/schema", json(cmds));
    map = (Map) fromJSONString(response);
    assertNull(map.get("error"));
  }

  public void testSortableTextFieldWithAnalyzer() throws Exception {
    String fieldTypeName = "sort_text_type";
    String fieldName = "sort_text";
    String payload = "{\n" +
        "  'add-field-type' : {" +
        "    'name' : '" + fieldTypeName + "',\n" +
        "    'stored':true,\n" +
        "    'indexed':true\n" +
        "    'maxCharsForDocValues':6\n" +
        "    'class':'solr.SortableTextField',\n" +
        "    'analyzer' : {'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'}},\n" +
        "  },\n"+
        "  'add-field' : {\n" +
        "    'name':'" + fieldName + "',\n" +
        "    'type': '"+fieldTypeName+"',\n" +
        "  }\n" +
        "}\n";

    String response = restTestHarness.post("/schema", json(payload));

    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    assertNull(response, map.get("errors"));

    @SuppressWarnings({"rawtypes"})
    Map fields = getObj(restTestHarness, fieldName, "fields");
    assertNotNull("field " + fieldName + " not created", fields);

    assertEquals(0,
                 getSolrClient().add(Arrays.asList(sdoc("id","1",fieldName,"xxx aaa"),
                                                   sdoc("id","2",fieldName,"xxx bbb aaa"),
                                                   sdoc("id","3",fieldName,"xxx bbb zzz"))).getStatus());
                                                   
    assertEquals(0, getSolrClient().commit().getStatus());
    {
      SolrDocumentList docs = getSolrClient().query
        (params("q",fieldName+":xxx","sort", fieldName + " asc, id desc")).getResults();
         
      assertEquals(3L, docs.getNumFound());
      assertEquals(3L, docs.size());
      assertEquals("1", docs.get(0).getFieldValue("id"));
      assertEquals("3", docs.get(1).getFieldValue("id"));
      assertEquals("2", docs.get(2).getFieldValue("id"));
    }
    {
      SolrDocumentList docs = getSolrClient().query
        (params("q",fieldName+":xxx", "sort", fieldName + " desc, id asc")).getResults();
                                                           
      assertEquals(3L, docs.getNumFound());
      assertEquals(3L, docs.size());
      assertEquals("2", docs.get(0).getFieldValue("id"));
      assertEquals("3", docs.get(1).getFieldValue("id"));
      assertEquals("1", docs.get(2).getFieldValue("id"));
    }
    
  }

  @Test
  public void testAddNewFieldAndQuery() throws Exception {
    getSolrClient().add(Arrays.asList(
        sdoc("id", "1", "term_s", "tux")));

    getSolrClient().commit(true, true);
    Map<String,Object> attrs = new HashMap<>();
    attrs.put("name", "newstringtestfield");
    attrs.put("type", "string");

    new SchemaRequest.AddField(attrs).process(getSolrClient());

    SolrQuery query = new SolrQuery("*:*");
    query.addFacetField("newstringtestfield");
    int size = getSolrClient().query(query).getResults().size();
    assertEquals(1, size);
  }

  public void testSimilarityParser() throws Exception {
    RestTestHarness harness = restTestHarness;

    final float k1 = 2.25f;
    final float b = 0.33f;

    String fieldTypeName = "MySimilarityField";
    String fieldName = "similarityTestField";
    String payload = "{\n" +
        "  'add-field-type' : {" +
        "    'name' : '" + fieldTypeName + "',\n" +
        "    'class':'solr.TextField',\n" +
        "    'analyzer' : {'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'}},\n" +
        "    'similarity' : {'class':'org.apache.solr.search.similarities.BM25SimilarityFactory', 'k1':"+k1+", 'b':"+b+" }\n" +
        "  },\n"+
        "  'add-field' : {\n" +
        "    'name':'" + fieldName + "',\n" +
        "    'type': 'MySimilarityField',\n" +
        "    'stored':true,\n" +
        "    'indexed':true\n" +
        "  }\n" +
        "}\n";

    String response = harness.post("/schema", json(payload));

    @SuppressWarnings({"rawtypes"})
    Map map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));

    @SuppressWarnings({"rawtypes"})
    Map fields = getObj(harness, fieldName, "fields");
    assertNotNull("field " + fieldName + " not created", fields);
    
    assertFieldSimilarity(fieldName, BM25Similarity.class,
       sim -> assertEquals("Unexpected k1", k1, sim.getK1(), .001),
       sim -> assertEquals("Unexpected b", b, sim.getB(), .001));

    final String independenceMeasure = "Saturated";
    final boolean discountOverlaps = false; 
    payload = "{\n" +
        "  'replace-field-type' : {" +
        "    'name' : '" + fieldTypeName + "',\n" +
        "    'class':'solr.TextField',\n" +
        "    'analyzer' : {'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'}},\n" +
        "    'similarity' : {\n" +
        "      'class':'org.apache.solr.search.similarities.DFISimilarityFactory',\n" +
        "      'independenceMeasure':'" + independenceMeasure + "',\n" +
        "      'discountOverlaps':" + discountOverlaps + "\n" +
        "     }\n" +
        "  }\n"+
        "}\n";

    response = harness.post("/schema", json(payload));

    map = (Map) fromJSONString(response);
    assertNull(response, map.get("error"));
    fields = getObj(harness, fieldName, "fields");
    assertNotNull("field " + fieldName + " not created", fields);

    assertFieldSimilarity(fieldName, DFISimilarity.class,
        sim -> assertEquals("Unexpected independenceMeasure", independenceMeasure, sim.getIndependence().toString()),
        sim -> assertEquals("Unexpected discountedOverlaps", discountOverlaps, sim.getDiscountOverlaps()));
  }

  @SuppressWarnings({"rawtypes"})
  public static Map getObj(RestTestHarness restHarness, String fld, String key) throws Exception {
    Map map = getRespMap(restHarness);
    List l = (List) ((Map)map.get("schema")).get(key);
    for (Object o : l) {
      @SuppressWarnings({"rawtypes"})Map m = (Map) o;
      if (fld.equals(m.get("name"))) 
        return m;
    }
    return null;
  }

  @SuppressWarnings({"rawtypes"})
  public static Map getRespMap(RestTestHarness restHarness) throws Exception {
    return getAsMap("/schema", restHarness);
  }

  @SuppressWarnings({"rawtypes"})
  public static Map getAsMap(String uri, RestTestHarness restHarness) throws Exception {
    String response = restHarness.query(uri);
    return (Map) fromJSONString(response);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List getSourceCopyFields(RestTestHarness harness, String src) throws Exception {
    Map map = getRespMap(harness);
    List l = (List) ((Map)map.get("schema")).get("copyFields");
    List result = new ArrayList();
    for (Object o : l) {
      Map m = (Map) o;
      if (src.equals(m.get("source"))) result.add(m);
    }
    return result;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List getDestCopyFields(RestTestHarness harness, String dest) throws Exception {
    Map map = getRespMap(harness);
    List l = (List) ((Map)map.get("schema")).get("copyFields");
    List result = new ArrayList();
    for (Object o : l) {
      Map m = (Map) o;
      if (dest.equals(m.get("dest"))) result.add(m);
    }
    return result;
  }

  /**
   * whitebox checks the Similarity for the specified field according to {@link SolrCore#getLatestSchema}
   * 
   * Executes each of the specified Similarity-accepting validators.
   */
  @SafeVarargs
  @SuppressWarnings({"unchecked", "varargs"})
  private static <T extends Similarity> void assertFieldSimilarity(String fieldname, Class<T> expected, Consumer<T>... validators) {
    CoreContainer cc = jetty.getCoreContainer();
    try (SolrCore core = cc.getCore("collection1")) {
      SimilarityFactory simfac = core.getLatestSchema().getSimilarityFactory();
      assertNotNull(simfac);
      assertTrue("test only works with SchemaSimilarityFactory",
                 simfac instanceof SchemaSimilarityFactory);
      
      Similarity mainSim = core.getLatestSchema().getSimilarity();
      assertNotNull(mainSim);
      
      // sanity check simfac vs sim in use - also verify infom called on simfac, otherwise exception
      assertEquals(mainSim, simfac.getSimilarity());
      
      assertTrue("test only works with PerFieldSimilarityWrapper, SchemaSimilarityFactory redefined?",
                 mainSim instanceof PerFieldSimilarityWrapper);
      Similarity fieldSim = ((PerFieldSimilarityWrapper)mainSim).get(fieldname);
      assertEquals("wrong sim for field=" + fieldname, expected, fieldSim.getClass());
      Arrays.asList(validators).forEach(v -> v.accept((T)fieldSim));
    }
  }
}
