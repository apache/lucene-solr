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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.junit.Test;

/**
 * Tests that defaults are set for Primitive (non-analyzed) fields
 */
public class PrimitiveFieldTypeTest extends SolrTestCaseJ4 {
  private final String testConfHome = TEST_HOME() + File.separator + "collection1" + File.separator + "conf"+ File.separator; 
  protected SolrConfig config;
  protected IndexSchema schema;
  protected HashMap<String,String> initMap;
  
  @Override
  public void setUp()  throws Exception {
    super.setUp();
    // set some system properties for use by tests
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    initMap = new HashMap<>();
    config = new SolrConfig(TEST_PATH().resolve("collection1"), testConfHome + "solrconfig.xml");
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDefaultOmitNorms() throws Exception {
    
    final List<Class<? extends FieldType>> types
      = Arrays.asList(TrieDateField.class, DatePointField.class,
                      TrieIntField.class, IntPointField.class,
                      TrieLongField.class, IntPointField.class,
                      TrieFloatField.class, FloatPointField.class,
                      TrieDoubleField.class, DoublePointField.class,
                      StrField.class, BoolField.class,
                      // Non-prims, omitNorms always defaults to false regardless of schema version...
                      TextField.class, BinaryField.class);
    
    // ***********************
    // With schema version 1.4:
    // ***********************
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema12.xml", config);


    for (Class<? extends FieldType> clazz : types) {
      FieldType ft = clazz.newInstance();
      ft.init(schema, initMap);
      assertFalse(ft.getClass().getName(), ft.hasProperty(FieldType.OMIT_NORMS));
    }
    
    // ***********************
    // With schema version 1.5
    // ***********************
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema15.xml", config);

    for (Class<? extends FieldType> clazz : types) {
      FieldType ft = clazz.newInstance();
      ft.init(schema, initMap);
      assertEquals(ft.getClass().getName(),
                   ft instanceof PrimitiveFieldType,
                   ft.hasProperty(FieldType.OMIT_NORMS));
    }
    
  }
  
  public void testDateField() { 
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema15.xml", config);
    
    final TrieDateField tdt = new TrieDateField();
    {
      final Map<String, String> args = new HashMap<>();
      args.put("sortMissingLast", "true");
      args.put("indexed", "true");
      args.put("stored", "false");
      args.put("docValues", "true");
      args.put("precisionStep", "16");
      tdt.setArgs(schema, args);
      assertEquals(16, tdt.getPrecisionStep());
    }
    final DatePointField pdt = new DatePointField();
    {
      final Map<String, String> args = new HashMap<>();
      args.put("sortMissingLast", "true");
      args.put("indexed", "true");
      args.put("stored", "false");
      args.put("docValues", "true");
      pdt.setArgs(schema, args);
    }
    
    for (FieldType ft : Arrays.asList(tdt, pdt)) {
      assertTrue(ft.getClass().getName(), ft.hasProperty(FieldType.OMIT_NORMS));
      assertTrue(ft.getClass().getName(), ft.hasProperty(FieldType.SORT_MISSING_LAST));
      assertTrue(ft.getClass().getName(), ft.hasProperty(FieldType.INDEXED));
      assertFalse(ft.getClass().getName(), ft.hasProperty(FieldType.STORED));
      assertTrue(ft.getClass().getName(), ft.hasProperty(FieldType.DOC_VALUES));
    }
  }
}
