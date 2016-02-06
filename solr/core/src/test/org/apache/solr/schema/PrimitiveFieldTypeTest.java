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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
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
    System.setProperty("solr.allow.unsafe.resourceloading", "true");

    initMap = new HashMap<>();
    config = new SolrConfig(new SolrResourceLoader(TEST_PATH().resolve("collection1")), testConfHome + "solrconfig.xml", null);
  }
  
  @Override
  public void tearDown() throws Exception {
    System.clearProperty("solr.allow.unsafe.resourceloading");
    super.tearDown();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDefaultOmitNorms() throws Exception {
    BinaryField bin;
    TextField t;
    TrieDateField dt;
    StrField s;
    TrieIntField ti;
    TrieLongField tl;
    TrieFloatField tf;
    TrieDoubleField td;
    BoolField b;
    
    
    // ***********************
    // With schema version 1.4:
    // ***********************
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema12.xml", config);
    
    dt = new TrieDateField();
    dt.init(schema, initMap);
    assertFalse(dt.hasProperty(FieldType.OMIT_NORMS));

    s = new StrField();
    s.init(schema, initMap);
    assertFalse(s.hasProperty(FieldType.OMIT_NORMS));

    ti = new TrieIntField();
    ti.init(schema, initMap);
    assertFalse(ti.hasProperty(FieldType.OMIT_NORMS));

    tl = new TrieLongField();
    tl.init(schema, initMap);
    assertFalse(tl.hasProperty(FieldType.OMIT_NORMS));

    tf = new TrieFloatField();
    tf.init(schema, initMap);
    assertFalse(tf.hasProperty(FieldType.OMIT_NORMS));

    td = new TrieDoubleField();
    td.init(schema, initMap);
    assertFalse(td.hasProperty(FieldType.OMIT_NORMS));

    b = new BoolField();
    b.init(schema, initMap);
    assertFalse(b.hasProperty(FieldType.OMIT_NORMS));

    // Non-primitive fields
    t = new TextField();
    t.init(schema, initMap);
    assertFalse(t.hasProperty(FieldType.OMIT_NORMS));

    bin = new BinaryField();
    bin.init(schema, initMap);
    assertFalse(bin.hasProperty(FieldType.OMIT_NORMS));

    // ***********************
    // With schema version 1.5
    // ***********************
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema15.xml", config);

    dt = new TrieDateField();
    dt.init(schema, initMap);
    assertTrue(dt.hasProperty(FieldType.OMIT_NORMS));

    s = new StrField();
    s.init(schema, initMap);
    assertTrue(s.hasProperty(FieldType.OMIT_NORMS));

    ti = new TrieIntField();
    ti.init(schema, initMap);
    assertTrue(ti.hasProperty(FieldType.OMIT_NORMS));

    tl = new TrieLongField();
    tl.init(schema, initMap);
    assertTrue(tl.hasProperty(FieldType.OMIT_NORMS));

    tf = new TrieFloatField();
    tf.init(schema, initMap);
    assertTrue(tf.hasProperty(FieldType.OMIT_NORMS));

    td = new TrieDoubleField();
    td.init(schema, initMap);
    assertTrue(td.hasProperty(FieldType.OMIT_NORMS));

    b = new BoolField();
    b.init(schema, initMap);
    assertTrue(b.hasProperty(FieldType.OMIT_NORMS));

    // Non-primitive fields
    t = new TextField();
    t.init(schema, initMap);
    assertFalse(t.hasProperty(FieldType.OMIT_NORMS));

    bin = new BinaryField();
    bin.init(schema, initMap);
    assertFalse(bin.hasProperty(FieldType.OMIT_NORMS));
  }
  
  public void testTrieDateField() {
    schema = IndexSchemaFactory.buildIndexSchema(testConfHome + "schema15.xml", config);
    TrieDateField tdt = new TrieDateField();
    Map<String, String> args = new HashMap<>();
    args.put("sortMissingLast", "true");
    args.put("indexed", "true");
    args.put("stored", "false");
    args.put("docValues", "true");
    args.put("precisionStep", "16");
    tdt.setArgs(schema, args);
    assertTrue(tdt.hasProperty(FieldType.OMIT_NORMS));
    assertTrue(tdt.hasProperty(FieldType.SORT_MISSING_LAST));
    assertTrue(tdt.hasProperty(FieldType.INDEXED));
    assertFalse(tdt.hasProperty(FieldType.STORED));
    assertTrue(tdt.hasProperty(FieldType.DOC_VALUES));
    assertEquals(16, tdt.getPrecisionStep());
  }
}
