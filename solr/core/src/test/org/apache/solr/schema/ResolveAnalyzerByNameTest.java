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

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This is a simple test to make sure the schema loads when
 * provided analyzers resolve tokenizer/tokenfilter/charfilter factories by (SPI) name.
 *
 */

public class ResolveAnalyzerByNameTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-analyzer-by-name.xml");
  }

  @Test
  public void testSchemaLoadingSimpleAnalyzer() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    assertTrue( schema.getFieldTypes().containsKey("text_ws") );
    SimpleOrderedMap<Object> analyzerProps =
        (SimpleOrderedMap<Object>)schema.getFieldTypeByName("text_ws")
        .getNamedPropertyValues(true).get("analyzer");
    checkTokenizerName(analyzerProps, "whitespace");

    assertNotNull(schema.getFieldTypeByName("text_ws").getIndexAnalyzer());
    assertNotNull(schema.getFieldTypeByName("text_ws").getQueryAnalyzer());
  }

  @Test
  public void testSchemaLoadingComplexAnalyzer() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    assertTrue( schema.getFieldTypes().containsKey("text") );

    SimpleOrderedMap<Object> indexAnalyzerProps =
        (SimpleOrderedMap<Object>)schema.getFieldTypeByName("text")
            .getNamedPropertyValues(true).get("indexAnalyzer");
    checkTokenizerName(indexAnalyzerProps, "whitespace");
    checkTokenFilterNames(indexAnalyzerProps, new String[]{"stop", "wordDelimiterGraph", "lowercase", "keywordMarker", "porterStem", "removeDuplicates", "flattenGraph"});

    SimpleOrderedMap<Object> queryAnalyzerProps =
        (SimpleOrderedMap<Object>)schema.getFieldTypeByName("text")
            .getNamedPropertyValues(true).get("queryAnalyzer");
    checkTokenizerName(queryAnalyzerProps, "whitespace");
    checkTokenFilterNames(queryAnalyzerProps, new String[]{"synonymGraph", "stop", "wordDelimiterGraph", "lowercase", "keywordMarker", "porterStem", "removeDuplicates"});

    assertNotNull(schema.getFieldTypeByName("text").getIndexAnalyzer());
    assertNotNull(schema.getFieldTypeByName("text").getQueryAnalyzer());
  }

  @Test
  public void testSchemaLoadingAnalyzerWithCharFilters() {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    assertTrue( schema.getFieldTypes().containsKey("charfilthtmlmap") );
    SimpleOrderedMap<Object> analyzerProps =
        (SimpleOrderedMap<Object>)schema.getFieldTypeByName("charfilthtmlmap")
            .getNamedPropertyValues(true).get("analyzer");
    checkTokenizerName(analyzerProps, "whitespace");
    checkCharFilterNames(analyzerProps, new String[]{"htmlStrip", "mapping"});

    assertNotNull(schema.getFieldTypeByName("charfilthtmlmap").getIndexAnalyzer());
    assertNotNull(schema.getFieldTypeByName("charfilthtmlmap").getQueryAnalyzer());
  }

  @Test(expected = SolrException.class)
  public void testSchemaLoadingBogus() throws Exception {
    initCore("solrconfig-basic.xml", "bad-schema-analyzer-by-name.xml");
  }

  @Test(expected = SolrException.class)
  public void testSchemaLoadingClassAndNameTokenizer() throws Exception {
    initCore("solrconfig-basic.xml", "bad-schema-analyzer-class-and-name-tok.xml");
  }

  @Test(expected = SolrException.class)
  public void testSchemaLoadingClassAndNameCharFilter() throws Exception {
    initCore("solrconfig-basic.xml", "bad-schema-analyzer-class-and-name-cf.xml");
  }

  @Test(expected = SolrException.class)
  public void testSchemaLoadingClassAndNameTokenFilter() throws Exception {
    initCore("solrconfig-basic.xml", "bad-schema-analyzer-class-and-name-tf.xml");
  }

  private void checkTokenizerName(SimpleOrderedMap<Object> analyzerProps, String name) {
    SimpleOrderedMap<Object> tokenizerProps = (SimpleOrderedMap<Object>)analyzerProps.get("tokenizer");
    assertNull(tokenizerProps.get("class"));
    assertEquals(name, tokenizerProps.get("name"));
  }

  private void checkTokenFilterNames(SimpleOrderedMap<Object> analyzerProps, String[] names) {
    List<SimpleOrderedMap<Object>> tokenFilterProps = (List<SimpleOrderedMap<Object>>)analyzerProps.get("filters");
    assertEquals(names.length, tokenFilterProps.size());
    for (int i = 0; i < names.length; i++) {
      assertNull(tokenFilterProps.get(i).get("class"));
      assertEquals(names[i], tokenFilterProps.get(i).get("name"));
    }
  }

  private void checkCharFilterNames(SimpleOrderedMap<Object> analyzerProps, String[] names) {
    List<SimpleOrderedMap<Object>> charFilterProps = (List<SimpleOrderedMap<Object>>)analyzerProps.get("charFilters");
    assertEquals(names.length, charFilterProps.size());
    for (int i = 0; i < names.length; i++) {
      assertNull(charFilterProps.get(i).get("class"));
      assertEquals(names[i], charFilterProps.get(i).get("name"));
    }
  }
}
