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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.TimeZone;

/**
 * Tests that defaults are set for Primitive (non-analyzed) fields
 */
public class PrimitiveFieldTypeTest extends SolrTestCaseJ4 {
  private final String testConfHome = TEST_HOME() + File.separator + "collection1" + File.separator + "conf"+ File.separator; 
  public static TimeZone UTC = TimeZone.getTimeZone("UTC");
  protected SolrConfig config;
  protected IndexSchema schema;
  protected HashMap<String,String> initMap;
  
  @Override
  public void setUp()  throws Exception {
    super.setUp();
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    initMap = new HashMap<String,String>();
    config = new SolrConfig(new SolrResourceLoader("solr/collection1"), testConfHome + "solrconfig.xml", null);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDefaultOmitNorms() throws Exception {
    BinaryField bin;
    TextField t;
    DateField dt;
    StrField s;
    IntField i;
    TrieIntField ti;
    SortableIntField si;
    LongField l;
    ShortField sf;
    FloatField f;
    DoubleField d;
    BoolField b;
    ByteField bf;
    
    
    // ***********************
    // With schema version 1.4:
    // ***********************
    schema = new IndexSchema(config, testConfHome + "schema12.xml", null);
    
    dt = new DateField();
    dt.init(schema, initMap);
    assertFalse(dt.hasProperty(FieldType.OMIT_NORMS));

    s = new StrField();
    s.init(schema, initMap);
    assertFalse(s.hasProperty(FieldType.OMIT_NORMS));

    i = new IntField();
    i.init(schema, initMap);
    assertFalse(i.hasProperty(FieldType.OMIT_NORMS));

    ti = new TrieIntField();
    ti.init(schema, initMap);
    assertFalse(ti.hasProperty(FieldType.OMIT_NORMS));

    si = new SortableIntField();
    si.init(schema, initMap);
    assertFalse(si.hasProperty(FieldType.OMIT_NORMS));

    l = new LongField();
    l.init(schema, initMap);
    assertFalse(l.hasProperty(FieldType.OMIT_NORMS));

    sf = new ShortField();
    sf.init(schema, initMap);
    assertFalse(sf.hasProperty(FieldType.OMIT_NORMS));

    f = new FloatField();
    f.init(schema, initMap);
    assertFalse(f.hasProperty(FieldType.OMIT_NORMS));

    d = new DoubleField();
    d.init(schema, initMap);
    assertFalse(d.hasProperty(FieldType.OMIT_NORMS));

    dt = new DateField();
    dt.init(schema, initMap);
    assertFalse(dt.hasProperty(FieldType.OMIT_NORMS));

    b = new BoolField();
    b.init(schema, initMap);
    assertFalse(b.hasProperty(FieldType.OMIT_NORMS));

    bf = new ByteField();
    bf.init(schema, initMap);
    assertFalse(bf.hasProperty(FieldType.OMIT_NORMS));

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
    schema = new IndexSchema(config, testConfHome + "schema15.xml", null);

    dt = new DateField();
    dt.init(schema, initMap);
    assertTrue(dt.hasProperty(FieldType.OMIT_NORMS));

    s = new StrField();
    s.init(schema, initMap);
    assertTrue(s.hasProperty(FieldType.OMIT_NORMS));

    i = new IntField();
    i.init(schema, initMap);
    assertTrue(i.hasProperty(FieldType.OMIT_NORMS));

    ti = new TrieIntField();
    ti.init(schema, initMap);
    assertTrue(ti.hasProperty(FieldType.OMIT_NORMS));

    si = new SortableIntField();
    si.init(schema, initMap);
    assertTrue(si.hasProperty(FieldType.OMIT_NORMS));

    l = new LongField();
    l.init(schema, initMap);
    assertTrue(l.hasProperty(FieldType.OMIT_NORMS));

    sf = new ShortField();
    sf.init(schema, initMap);
    assertTrue(sf.hasProperty(FieldType.OMIT_NORMS));

    f = new FloatField();
    f.init(schema, initMap);
    assertTrue(f.hasProperty(FieldType.OMIT_NORMS));

    d = new DoubleField();
    d.init(schema, initMap);
    assertTrue(d.hasProperty(FieldType.OMIT_NORMS));

    dt = new DateField();
    dt.init(schema, initMap);
    assertTrue(dt.hasProperty(FieldType.OMIT_NORMS));

    b = new BoolField();
    b.init(schema, initMap);
    assertTrue(b.hasProperty(FieldType.OMIT_NORMS));

    bf = new ByteField();
    bf.init(schema, initMap);
    assertTrue(bf.hasProperty(FieldType.OMIT_NORMS));

    // Non-primitive fields
    t = new TextField();
    t.init(schema, initMap);
    assertFalse(t.hasProperty(FieldType.OMIT_NORMS));

    bin = new BinaryField();
    bin.init(schema, initMap);
    assertFalse(bin.hasProperty(FieldType.OMIT_NORMS));
  }
}
