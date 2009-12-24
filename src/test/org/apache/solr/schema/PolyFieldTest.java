package org.apache.solr.schema;
/**
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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.common.SolrException;

import java.util.Map;
import java.util.Random;


/**
 * Test a whole slew of things related to PolyFields
 */
public class PolyFieldTest extends AbstractSolrTestCase {

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void testSchemaBasics() throws Exception {
    IndexSchema schema = h.getCore().getSchema();


    SchemaField home = schema.getField("home");
    assertNotNull(home);
    assertTrue(home.isPolyField());

    SchemaField[] dynFields = schema.getDynamicFieldPrototypes();
    boolean seen = false;
    for (SchemaField dynField : dynFields) {
      if (dynField.getName().equals("*" + FieldType.POLY_FIELD_SEPARATOR + "double")) {
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
    home = schema.getFieldOrNull("home_0" + FieldType.POLY_FIELD_SEPARATOR + "double");
    assertNotNull(home);
    home = schema.getField("home");
    assertNotNull(home);
    homeFT = schema.getPolyFieldType("home");
    assertNotNull(homeFT);

    home = schema.getField("homed");//sub field suffix
    assertNotNull(home);
    assertTrue(home.isPolyField());

    try {
      FieldType bad = schema.getPolyFieldType("foo");
      assertTrue(false);
    } catch (Exception e) {
    }
    try {
      FieldType bad = schema.getPolyFieldTypeNoEx("foo");
      assertNull(bad);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  public void testPointFieldType() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
    SchemaField home = schema.getField("home");
    assertNotNull(home);
    assertTrue("home is not a poly field", home.isPolyField());
    FieldType tmp = home.getType();
    assertTrue(tmp instanceof PointType);
    PointType pt = (PointType) tmp;
    assertEquals(pt.getDimension(), 2);
    double[] xy = new double[]{35.0, -79.34};
    String point = xy[0] + "," + xy[1];
    Fieldable[] fields = home.createFields(point, 2);
    assertEquals(fields.length, 3);//should be 3, we have a stored field
    //first two fields contain the values, third is just stored and contains the original
    for (int i = 0; i < 3; i++) {
      boolean hasValue = fields[1].tokenStreamValue() != null
              || fields[1].getBinaryValue() != null
              || fields[1].stringValue() != null;
      assertTrue("Doesn't have a value: " + fields[1], hasValue);
    }
    /*assertTrue("first field " + fields[0].tokenStreamValue() +  " is not 35.0", pt.getSubType().toExternal(fields[0]).equals(String.valueOf(xy[0])));
    assertTrue("second field is not -79.34", pt.getSubType().toExternal(fields[1]).equals(String.valueOf(xy[1])));
    assertTrue("third field is not '35.0,-79.34'", pt.getSubType().toExternal(fields[2]).equals(point));*/


    home = schema.getField("home_ns");
    assertNotNull(home);
    fields = home.createFields(point, 2);
    assertEquals(fields.length, 2);//should be 2, since we aren't storing

    home = schema.getField("home_ns");
    assertNotNull(home);
    try {
      fields = home.createFields("35.0,foo", 2);
      assertTrue(false);
    } catch (Exception e) {
      //
    }
  }

  public void testSearching() throws Exception {
    for (int i = 0; i < 50; i++) {
      assertU(adoc("id", "" + i, "home", i + "," + (i * 100), "homed", (i * 1000) + "," + (i * 10000)));
    }
    assertU(commit());
    IndexReader reader = h.getCore().getSearcher().get().getReader();
    /*for (int i = 0; i < 50; i++){
      Document doc = reader.document(i);
      System.out.println("Doc: " + doc.get("homed_0___double"));
    }*/
    assertQ(req("fl", "*,score", "q", "*:*"), "//*[@numFound='50']");
    assertQ(req("fl", "*,score", "q", "home:1,100"),
            "//*[@numFound='1']",
            "//str[@name='home'][.='1,100']");
    assertQ(req("fl", "*,score", "q", "homed:1000,10000"),
            "//*[@numFound='1']",
            "//str[@name='homed'][.='1000,10000']");
    assertQ(req("fl", "*,score", "q",
            "{!func}sqedist(home, toMultiVS(0, 0))"),
            "\"//*[@numFound='50']\"");
    assertQ(req("fl", "*,score", "q",
            "{!func}dist(2, home, toMultiVS(0, 0))"),
            "\"//*[@numFound='50']\"");

    assertQ(req("fl", "*,score", "q",
            "home:[10,10000 TO 30,30000]"),
            "\"//*[@numFound='3']\"");
    assertQ(req("fl", "*,score", "q",
            "homed:[1,1000 TO 2000,35000]"),
            "\"//*[@numFound='2']\"");
    //bad
    
    assertQEx("Query should throw an exception due to incorrect dimensions", req("fl", "*,score", "q",
            "homed:[1 TO 2000]"), SolrException.ErrorCode.BAD_REQUEST);
  }

  

  public void testSearchDetails() throws Exception {
    SolrCore core = h.getCore();
    IndexSchema schema = core.getSchema();
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
    BooleanClause[] clauses = bq.getClauses();
    assertEquals(clauses.length, 2);

  }
}