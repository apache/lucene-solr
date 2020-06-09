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
package org.apache.solr.handler.dataimport;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEphemeralCache extends AbstractDataImportHandlerTestCase {
  
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("dataimport-solrconfig.xml", "dataimport-schema.xml");
  }
  
  @Before
  public void reset() {
    DestroyCountCache.destroyed.clear();
    setupMockData();
  }
  
  @Test
  public void test() throws Exception {
    assertFullImport(getDataConfigDotXml());
  }

  @SuppressWarnings("unchecked")
  private void setupMockData() {
    @SuppressWarnings({"rawtypes"})
    List parentRows = new ArrayList();
    parentRows.add(createMap("id", new BigDecimal("1"), "parent_s", "one"));
    parentRows.add(createMap("id", new BigDecimal("2"), "parent_s", "two"));
    parentRows.add(createMap("id", new BigDecimal("3"), "parent_s", "three"));
    parentRows.add(createMap("id", new BigDecimal("4"), "parent_s", "four"));
    parentRows.add(createMap("id", new BigDecimal("5"), "parent_s", "five"));
    
    @SuppressWarnings({"rawtypes"})
    List child1Rows = new ArrayList();
    child1Rows.add(createMap("id", new BigDecimal("6"), "child1a_mult_s", "this is the number six."));
    child1Rows.add(createMap("id", new BigDecimal("5"), "child1a_mult_s", "this is the number five."));
    child1Rows.add(createMap("id", new BigDecimal("6"), "child1a_mult_s", "let's sing a song of six."));
    child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "three"));
    child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "III"));
    child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "3"));
    child1Rows.add(createMap("id", new BigDecimal("3"), "child1a_mult_s", "|||"));
    child1Rows.add(createMap("id", new BigDecimal("1"), "child1a_mult_s", "one"));
    child1Rows.add(createMap("id", new BigDecimal("1"), "child1a_mult_s", "uno"));
    child1Rows.add(createMap("id", new BigDecimal("2"), "child1b_s", "CHILD1B", "child1a_mult_s", "this is the number two."));
    
    @SuppressWarnings({"rawtypes"})
    List child2Rows = new ArrayList();
    child2Rows.add(createMap("id", new BigDecimal("6"), "child2a_mult_s", "Child 2 says, 'this is the number six.'"));
    child2Rows.add(createMap("id", new BigDecimal("5"), "child2a_mult_s", "Child 2 says, 'this is the number five.'"));
    child2Rows.add(createMap("id", new BigDecimal("6"), "child2a_mult_s", "Child 2 says, 'let's sing a song of six.'"));
    child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, 'three'"));
    child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, 'III'"));
    child2Rows.add(createMap("id", new BigDecimal("3"), "child2b_s", "CHILD2B", "child2a_mult_s", "Child 2 says, '3'"));
    child2Rows.add(createMap("id", new BigDecimal("3"), "child2a_mult_s", "Child 2 says, '|||'"));
    child2Rows.add(createMap("id", new BigDecimal("1"), "child2a_mult_s", "Child 2 says, 'one'"));
    child2Rows.add(createMap("id", new BigDecimal("1"), "child2a_mult_s", "Child 2 says, 'uno'"));
    child2Rows.add(createMap("id", new BigDecimal("2"), "child2a_mult_s", "Child 2 says, 'this is the number two.'"));
    
    MockDataSource.setIterator("SELECT * FROM PARENT", parentRows.iterator());
    MockDataSource.setIterator("SELECT * FROM CHILD_1", child1Rows.iterator());
    MockDataSource.setIterator("SELECT * FROM CHILD_2", child2Rows.iterator());
    
  }
  private String getDataConfigDotXml() {
    return
      "<dataConfig>" +
      " <dataSource type=\"MockDataSource\" />" +
      " <document>" +
      "   <entity " +
      "     name=\"PARENT\"" +
      "     processor=\"SqlEntityProcessor\"" +
      "     cacheImpl=\"org.apache.solr.handler.dataimport.DestroyCountCache\"" +
      "     cacheName=\"PARENT\"" +
      "     query=\"SELECT * FROM PARENT\"  " +
      "   >" +
      "     <entity" +
      "       name=\"CHILD_1\"" +
      "       processor=\"SqlEntityProcessor\"" +
      "       cacheImpl=\"org.apache.solr.handler.dataimport.DestroyCountCache\"" +
      "       cacheName=\"CHILD\"" +
      "       cacheKey=\"id\"" +
      "       cacheLookup=\"PARENT.id\"" +
      "       fieldNames=\"id,         child1a_mult_s, child1b_s\"" +
      "       fieldTypes=\"BIGDECIMAL, STRING,         STRING\"" +
      "       query=\"SELECT * FROM CHILD_1\"       " +
      "     />" +
      "     <entity" +
      "       name=\"CHILD_2\"" +
      "       processor=\"SqlEntityProcessor\"" +
      "       cacheImpl=\"org.apache.solr.handler.dataimport.DestroyCountCache\"" +
      "       cacheKey=\"id\"" +
      "       cacheLookup=\"PARENT.id\"" +
      "       query=\"SELECT * FROM CHILD_2\"       " +
      "     />" +
      "   </entity>" +
      " </document>" +
      "</dataConfig>"
    ;
  }
  
  private void assertFullImport(String dataConfig) throws Exception {
    runFullImport(dataConfig);
    
    assertQ(req("*:*"), "//*[@numFound='5']");
    assertQ(req("id:1"), "//*[@numFound='1']");
    assertQ(req("id:6"), "//*[@numFound='0']");
    assertQ(req("parent_s:four"), "//*[@numFound='1']");
    assertQ(req("child1a_mult_s:this\\ is\\ the\\ numbe*"), "//*[@numFound='2']");
    assertQ(req("child2a_mult_s:Child\\ 2\\ say*"), "//*[@numFound='4']");
    assertQ(req("child1b_s:CHILD1B"), "//*[@numFound='1']");
    assertQ(req("child2b_s:CHILD2B"), "//*[@numFound='1']");
    assertQ(req("child1a_mult_s:one"), "//*[@numFound='1']");
    assertQ(req("child1a_mult_s:uno"), "//*[@numFound='1']");
    assertQ(req("child1a_mult_s:(uno OR one)"), "//*[@numFound='1']");
    
    assertThat(DestroyCountCache.destroyed.size(), is(3));
  }
  
}
