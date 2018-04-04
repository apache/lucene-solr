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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test a whole slew of things related to PolyFields
 */
public class PolyFieldTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  @Test
  public void testSchemaBasics() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();


    SchemaField home = schema.getField("home");
    assertNotNull(home);
    assertTrue(home.isPolyField());
    
    String subFieldType = "double";
    SchemaField[] dynFields = schema.getDynamicFieldPrototypes();
    boolean seen = false;
    for (SchemaField dynField : dynFields) {
      if (dynField.getName().equals("*" + FieldType.POLY_FIELD_SEPARATOR + subFieldType)) {
        seen = true;
      }
    }
    assertTrue("Didn't find the expected dynamic field", seen);
    FieldType homeFT = schema.getFieldType("home");
    assertEquals(home.getType(), homeFT);
    FieldType xy = schema.getFieldTypeByName("xy");
    assertNotNull(xy);
    assertTrue(xy instanceof PointType);
    assertTrue(xy.isPolyField());
    home = schema.getFieldOrNull("home_0" + FieldType.POLY_FIELD_SEPARATOR + subFieldType);
    assertNotNull(home);
    home = schema.getField("home");
    assertNotNull(home);

    home = schema.getField("homed");//sub field suffix
    assertNotNull(home);
    assertTrue(home.isPolyField());
  }

  @Test
  public void testPointFieldType() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    SchemaField home = schema.getField("home");
    assertNotNull(home);
    assertTrue("home is not a poly field", home.isPolyField());
    FieldType tmp = home.getType();
    assertTrue(tmp instanceof PointType);
    PointType pt = (PointType) tmp;
    assertEquals(pt.getDimension(), 2);
    double[] xy = new double[]{35.0, -79.34};
    String point = xy[0] + "," + xy[1];
    List<IndexableField> fields = home.createFields(point);
    assertNotNull(pt.getSubType());
    int expectdNumFields = 3;//If DV=false, we expect one field per dimension plus a stored field
    if (pt.subField(home, 0, schema).hasDocValues()) {
      expectdNumFields+=2; // If docValues=true, then we expect two more fields
    }
    assertEquals("Unexpected fields created: " + Arrays.toString(fields.toArray()), expectdNumFields, fields.size());
    //first two/four fields contain the values, last one is just stored and contains the original
    for (int i = 0; i < expectdNumFields; i++) {
      boolean hasValue = fields.get(i).binaryValue() != null
          || fields.get(i).stringValue() != null
          || fields.get(i).numericValue() != null;
      assertTrue("Doesn't have a value: " + fields.get(i), hasValue);
    }
    /*assertTrue("first field " + fields[0].tokenStreamValue() +  " is not 35.0", pt.getSubType().toExternal(fields[0]).equals(String.valueOf(xy[0])));
    assertTrue("second field is not -79.34", pt.getSubType().toExternal(fields[1]).equals(String.valueOf(xy[1])));
    assertTrue("third field is not '35.0,-79.34'", pt.getSubType().toExternal(fields[2]).equals(point));*/


    home = schema.getField("home_ns");
    assertNotNull(home);
    fields = home.createFields(point);
    assertEquals(expectdNumFields - 1, fields.size(), 2);//one less field than with "home", since we aren't storing

    home = schema.getField("home_ns");
    assertNotNull(home);
    try {
      fields = home.createFields("35.0,foo");
      assertTrue(false);
    } catch (Exception e) {
      //
    }

    SchemaField s1 = schema.getField("test_p");
    SchemaField s2 = schema.getField("test_p");
    ValueSource v1 = s1.getType().getValueSource(s1, null);
    ValueSource v2 = s2.getType().getValueSource(s2, null);
    assertEquals(v1, v2);
    assertEquals(v1.hashCode(), v2.hashCode());
  }

  @Test
  public void testSearching() throws Exception {
    for (int i = 0; i < 50; i++) {
      assertU(adoc("id", "" + i, "home", i + "," + (i * 100), "homed", (i * 1000) + "," + (i * 10000)));
    }
    assertU(commit());

    assertQ(req("fl", "*,score", "q", "*:*"), "//*[@numFound='50']");
    assertQ(req("fl", "*,score", "q", "home:1,100"),
            "//*[@numFound='1']",
            "//str[@name='home'][.='1,100']");
    assertQ(req("fl", "*,score", "q", "homed:1000,10000"),
            "//*[@numFound='1']",
            "//str[@name='homed'][.='1000,10000']");
    assertQ(req("fl", "*,score", "q",
            "{!func}sqedist(home, vector(0, 0))"),
            "\"//*[@numFound='50']\"");
    assertQ(req("fl", "*,score", "q",
            "{!func}dist(2, home, vector(0, 0))"),
            "\"//*[@numFound='50']\"");

    assertQ(req("fl", "*,score", "q",
            "home:[10,10000 TO 30,30000]"),
            "\"//*[@numFound='3']\"");
    assertQ(req("fl", "*,score", "q",
            "homed:[1,1000 TO 2000,35000]"),
            "\"//*[@numFound='2']\"");
    //bad

    ignoreException("dimension");
    assertQEx("Query should throw an exception due to incorrect dimensions", req("fl", "*,score", "q",
            "homed:[1 TO 2000]"), SolrException.ErrorCode.BAD_REQUEST);
    resetExceptionIgnores();
    clearIndex();
  }

  @Test
  public void testSearchDetails() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getLatestSchema();
    double[] xy = new double[]{35.0, -79.34};
    String point = xy[0] + "," + xy[1];
    //How about some queries?
    //don't need a parser for this path currently.  This may change
    assertU(adoc("id", "0", "home_ns", point));
    assertU(commit());
    SchemaField home = schema.getField("home_ns");
    PointType pt = (PointType) home.getType();
    assertEquals(pt.getDimension(), 2);
    Query q = pt.getFieldQuery(null, home, point);
    assertNotNull(q);
    assertTrue(q instanceof BooleanQuery);
    //should have two clauses, one for 35.0 and the other for -79.34
    BooleanQuery bq = (BooleanQuery) q;
    assertEquals(2, bq.clauses().size());
    clearIndex();
  }

}
