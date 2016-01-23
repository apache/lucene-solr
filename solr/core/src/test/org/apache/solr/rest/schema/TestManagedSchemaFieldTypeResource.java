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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.util.RestTestBase;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.noggit.JSONUtil;
import org.restlet.ext.servlet.ServerServlet;

public class TestManagedSchemaFieldTypeResource extends RestTestBase {
  
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
  private void after() throws Exception {
    jetty.stop();
    jetty = null;
    System.clearProperty("managed.schema.mutable");
    System.clearProperty("enable.update.log");
    
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }
  
  @Test
  public void testAddFieldTypes() throws Exception {
    
    // name mismatch
    assertJPut("/schema/fieldtypes/myIntFieldType",
        json("{'name':'badNameEh','class':'solr.TrieIntField','stored':false}"),
        "/responseHeader/status==400");    
    
    // no class
    assertJPut("/schema/fieldtypes/myIntFieldType",
        json("{'stored':false}"),
        "/responseHeader/status==400");
    
    // invalid attribute
    assertJPut("/schema/fieldtypes/myIntFieldType",
        json("{'foo':'bar'}"),
        "/responseHeader/status==400");
    
    // empty analyzer
    String ftdef = "";
    ftdef += "{";
    ftdef += "  'class':'solr.TextField','positionIncrementGap':'100',";
    ftdef += "  'analyzer':''";
    ftdef += "}";    
    assertJPut("/schema/fieldtypes/emptyAnalyzerFieldType",
        json(ftdef),
        "/responseHeader/status==400");

    // basic field types
    assertJPut("/schema/fieldtypes/myIntFieldType",
        json("{'name':'myIntFieldType','class':'solr.TrieIntField','stored':false}"),
        "/responseHeader/status==0");    
    checkFieldTypeProps(getExpectedProps("myIntFieldType", "solr.TrieIntField", true, false), 16);
    
    assertJPut("/schema/fieldtypes/myDoubleFieldType",
        json("{'class':'solr.TrieDoubleField','precisionStep':'0','positionIncrementGap':'0'}"),
        "/responseHeader/status==0");
    Map<String,Object> expProps = 
        getExpectedProps("myDoubleFieldType", "solr.TrieDoubleField", true, true);
    // add some additional expected props for this type
    expProps.put("precisionStep", "0");
    expProps.put("positionIncrementGap", "0");
    checkFieldTypeProps(expProps, 18);
    
    assertJPut("/schema/fieldtypes/myBoolFieldType",
        json("{'class':'solr.BoolField','sortMissingLast':true}"),
        "/responseHeader/status==0");
    expProps = getExpectedProps("myBoolFieldType", "solr.BoolField", true, true);
    expProps.put("sortMissingLast", true);
    checkFieldTypeProps(expProps, 17);    
    
    // a text analyzing field type
    ftdef = "{";
    ftdef += "  'class':'solr.TextField','positionIncrementGap':'100',";
    ftdef += "  'analyzer':{";
    ftdef += "    'charFilters':[";
    ftdef += "       {'class':'solr.PatternReplaceCharFilterFactory','replacement':'$1$1','pattern':'([a-zA-Z])\\\\1+'}";
    ftdef += "    ],";
    ftdef += "    'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},";
    ftdef += "    'filters':[";
    ftdef += "       {'class':'solr.WordDelimiterFilterFactory','preserveOriginal':'0'},";
    ftdef += "       {'class':'solr.StopFilterFactory','words':'stopwords.txt','ignoreCase':'true'},";
    ftdef += "       {'class':'solr.LowerCaseFilterFactory'},";
    ftdef += "       {'class':'solr.ASCIIFoldingFilterFactory'},";
    ftdef += "       {'class':'solr.KStemFilterFactory'}";
    ftdef += "    ]";
    ftdef += "  }";
    ftdef += "}";
  
    assertJPut("/schema/fieldtypes/myTextFieldType", json(ftdef), "/responseHeader/status==0");
    
    expProps = getExpectedProps("myTextFieldType", "solr.TextField", true, true);
    expProps.put("autoGeneratePhraseQueries", false);
    expProps.put("omitNorms", false);
    expProps.put("omitTermFreqAndPositions", false);
    expProps.put("omitPositions", false);
    expProps.put("storeOffsetsWithPositions", false);
    expProps.put("tokenized", true);
            
    List<String> analyzerTests = new ArrayList<>();
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='charFilters']/lst[1]/str[@name='class'] = 'solr.PatternReplaceCharFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.WhitespaceTokenizerFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[1]/str[@name='class'] = 'solr.WordDelimiterFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[2]/str[@name='class'] = 'solr.StopFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[3]/str[@name='class'] = 'solr.LowerCaseFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[4]/str[@name='class'] = 'solr.ASCIIFoldingFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[5]/str[@name='class'] = 'solr.KStemFilterFactory'");
    checkFieldTypeProps(expProps, 19, analyzerTests);    
    
    // now add a field type that uses managed resources and a field that uses that type
    
    String piglatinStopWordEndpoint = "/schema/analysis/stopwords/piglatin";            
    String piglatinSynonymEndpoint = "/schema/analysis/synonyms/piglatin";    
    
    // now define a new FieldType that uses the managed piglatin endpoints
    // the managed endpoints will be autovivified as needed 
    ftdef = "{";
    ftdef += "  'class':'solr.TextField',";
    ftdef += "  'analyzer':{";
    ftdef += "    'tokenizer':{'class':'solr.StandardTokenizerFactory'},";
    ftdef += "    'filters':[";
    ftdef += "       {'class':'solr.ManagedStopFilterFactory','managed':'piglatin'},";
    ftdef += "       {'class':'solr.ManagedSynonymFilterFactory','managed':'piglatin'}";
    ftdef += "    ]";
    ftdef += "  }";
    ftdef += "}";
    assertJPut("/schema/fieldtypes/piglatinFieldType", json(ftdef), "/responseHeader/status==0");
    
    expProps = getExpectedProps("piglatinFieldType", "solr.TextField", true, true);
    expProps.put("autoGeneratePhraseQueries", false);
    expProps.put("omitNorms", false);
    expProps.put("omitTermFreqAndPositions", false);
    expProps.put("omitPositions", false);
    expProps.put("storeOffsetsWithPositions", false);
    expProps.put("tokenized", true);
            
    analyzerTests = new ArrayList<>();
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.StandardTokenizerFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[1]/str[@name='class'] = 'solr.ManagedStopFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[2]/str[@name='class'] = 'solr.ManagedSynonymFilterFactory'");
    checkFieldTypeProps(expProps, 18, analyzerTests);
    
    assertJQ(piglatinSynonymEndpoint, 
        "/synonymMappings/initArgs/ignoreCase==false",
        "/synonymMappings/managedMap=={}");

    // add some piglatin synonyms
    Map<String,List<String>> syns = new HashMap<>();
    syns.put("appyhay", Arrays.asList("ladgay","oyfuljay"));    
    assertJPut(piglatinSynonymEndpoint, 
              JSONUtil.toJSON(syns),
              "/responseHeader/status==0");    
    assertJQ(piglatinSynonymEndpoint, 
            "/synonymMappings/managedMap/appyhay==['ladgay','oyfuljay']");
    
    // add some piglatin stopwords
    assertJPut(piglatinStopWordEndpoint, 
        JSONUtil.toJSON(Arrays.asList("hetay")),
        "/responseHeader/status==0");
   
    assertJQ(piglatinStopWordEndpoint + "/hetay", "/hetay=='hetay'");
    
    // add a field that uses our new type
    assertJPut("/schema/fields/newManagedField",
        json("{'type':'piglatinFieldType','stored':false}"),
        "/responseHeader/status==0");   
    
    assertQ("/schema/fields/newManagedField?indent=on&wt=xml",
        "count(/response/lst[@name='field']) = 1",
        "/response/lst[@name='responseHeader']/int[@name='status'] = '0'");

    // try to delete the managed synonyms endpoint, which should fail because it is being used
    assertJDelete(piglatinSynonymEndpoint, "/responseHeader/status==403");
    
    // test adding multiple field types at once
    ftdef = "[";
    ftdef += "{";
    ftdef += "  'name':'textFieldType1',";
    ftdef += "  'class':'solr.TextField','positionIncrementGap':'100',";
    ftdef += "  'analyzer':{";
    ftdef += "    'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},";
    ftdef += "    'filters':[";
    ftdef += "       {'class':'solr.WordDelimiterFilterFactory','preserveOriginal':'0'},";
    ftdef += "       {'class':'solr.StopFilterFactory','words':'stopwords.txt','ignoreCase':'true'},";
    ftdef += "       {'class':'solr.LowerCaseFilterFactory'}";
    ftdef += "    ]";
    ftdef += "  }";
    ftdef += "},{";
    ftdef += "  'name':'textFieldType2',";
    ftdef += "  'class':'solr.TextField','positionIncrementGap':'100',";
    ftdef += "  'analyzer':{";
    ftdef += "    'tokenizer':{'class':'solr.WhitespaceTokenizerFactory'},";
    ftdef += "    'filters':[";
    ftdef += "       {'class':'solr.WordDelimiterFilterFactory','preserveOriginal':'0'},";
    ftdef += "       {'class':'solr.StopFilterFactory','words':'stopwords.txt','ignoreCase':'true'},";
    ftdef += "       {'class':'solr.LowerCaseFilterFactory'},";
    ftdef += "       {'class':'solr.ASCIIFoldingFilterFactory'}";
    ftdef += "    ]";
    ftdef += "  }";
    ftdef += "}";
    ftdef += "]";
  
    assertJPost("/schema/fieldtypes", json(ftdef), "/responseHeader/status==0");
    
    expProps = getExpectedProps("textFieldType1", "solr.TextField", true, true);
    expProps.put("autoGeneratePhraseQueries", false);
    expProps.put("omitNorms", false);
    expProps.put("omitTermFreqAndPositions", false);
    expProps.put("omitPositions", false);
    expProps.put("storeOffsetsWithPositions", false);
    expProps.put("tokenized", true);
            
    analyzerTests = new ArrayList<>();
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.WhitespaceTokenizerFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[1]/str[@name='class'] = 'solr.WordDelimiterFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[2]/str[@name='class'] = 'solr.StopFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[3]/str[@name='class'] = 'solr.LowerCaseFilterFactory'");
    checkFieldTypeProps(expProps, 19, analyzerTests);    

    expProps = getExpectedProps("textFieldType2", "solr.TextField", true, true);
    expProps.put("autoGeneratePhraseQueries", false);
    expProps.put("omitNorms", false);
    expProps.put("omitTermFreqAndPositions", false);
    expProps.put("omitPositions", false);
    expProps.put("storeOffsetsWithPositions", false);
    expProps.put("tokenized", true);
            
    analyzerTests = new ArrayList<>();
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/lst[@name='tokenizer']/str[@name='class'] = 'solr.WhitespaceTokenizerFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[1]/str[@name='class'] = 'solr.WordDelimiterFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[2]/str[@name='class'] = 'solr.StopFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[3]/str[@name='class'] = 'solr.LowerCaseFilterFactory'");
    analyzerTests.add("/response/lst[@name='fieldType']/lst[@name='analyzer']/arr[@name='filters']/lst[4]/str[@name='class'] = 'solr.ASCIIFoldingFilterFactory'");
    checkFieldTypeProps(expProps, 19, analyzerTests);        
  }
  
  /**
   * Helper function to check fieldType settings against a set of expected values.
   */
  protected void checkFieldTypeProps(Map<String,Object> expected, int expectedChildCount) {
    checkFieldTypeProps(expected, expectedChildCount, null);
  }
  
  protected void checkFieldTypeProps(Map<String,Object> expected, int expectedChildCount, List<String> addlTests) {
    String fieldTypeName = (String)expected.get("name");
    
    List<String> tests = new ArrayList<>();
    tests.add("count(/response/lst[@name='fieldType']) = 1");
    tests.add("count(/response/lst[@name='fieldType']/*) = "+expectedChildCount);
    tests.add("count(/response/lst[@name='fieldType']/arr[@name='fields']/*) = 0");
    tests.add("count(/response/lst[@name='fieldType']/arr[@name='dynamicFields']/*) = 0");
    for (Map.Entry<String,Object> next : expected.entrySet()) {
      Object val = next.getValue();
      String pathType = null;
      if (val instanceof Boolean)
        pathType = "bool";
      else if (val instanceof String)
        pathType = "str";
      else
        fail("Unexpected value type "+val.getClass().getName());
      // NOTE: it seems like the fieldtypes endpoint only returns strings or booleans
      
      String xpath = 
          "/response/lst[@name='fieldType']/"+pathType+"[@name='"+next.getKey()+"']";
      tests.add(xpath+" = '"+val+"'");
    }
    
    if (addlTests != null)
      tests.addAll(addlTests);
    
    assertQ("/schema/fieldtypes/"+fieldTypeName+"?indent=on&wt=xml&showDefaults=true",
        tests.toArray(new String[0]));
  }
  
  /**
   * Builds a map containing expected values for a field type created by this test. 
   */
  protected Map<String,Object> getExpectedProps(String name, String className, boolean indexed, boolean stored) {
    Map<String,Object> map = new HashMap<>();
    map.put("name", name);
    map.put("class", className);
    map.put("indexed", indexed);
    map.put("stored", stored);      
    map.put("docValues", false);
    map.put("termVectors", false);
    map.put("termPositions", false);
    map.put("termOffsets", false);
    map.put("omitNorms", true);
    map.put("omitTermFreqAndPositions", true);
    map.put("omitPositions", false);
    map.put("storeOffsetsWithPositions", false);
    map.put("multiValued", false);
    map.put("tokenized", false);
    return map;
  }
}
