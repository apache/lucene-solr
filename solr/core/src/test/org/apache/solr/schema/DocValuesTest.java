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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocValuesTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValues.xml");

    // sanity check our schema meets our expectations
    final IndexSchema schema = h.getCore().getLatestSchema();
    for (String f : new String[] {"floatdv", "intdv", "doubledv", "longdv", "datedv", "stringdv", "booldv"}) {
      final SchemaField sf = schema.getField(f);
      assertFalse(f + " is multiValued, test is useless, who changed the schema?",
                  sf.multiValued());
      assertFalse(f + " is indexed, test is useless, who changed the schema?",
                  sf.indexed());
      assertTrue(f + " has no docValues, test is useless, who changed the schema?",
                 sf.hasDocValues());
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  @Test
  public void testDocValues() throws IOException {
    assertU(adoc("id", "1"));
    assertU(commit());
    try (SolrCore core = h.getCoreInc()) {
      final RefCounted<SolrIndexSearcher> searcherRef = core.openNewSearcher(true, true);
      final SolrIndexSearcher searcher = searcherRef.get();
      try {
        final LeafReader reader = searcher.getSlowAtomicReader();
        assertEquals(1, reader.numDocs());
        final FieldInfos infos = reader.getFieldInfos();
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("floatdv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("intdv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("doubledv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("longdv").getDocValuesType());
        assertEquals(DocValuesType.SORTED, infos.fieldInfo("stringdv").getDocValuesType());
        assertEquals(DocValuesType.SORTED, infos.fieldInfo("booldv").getDocValuesType());

        NumericDocValues dvs = reader.getNumericDocValues("floatdv");
        assertEquals(0, dvs.nextDoc());
        assertEquals((long) Float.floatToIntBits(1), dvs.longValue());
        dvs = reader.getNumericDocValues("intdv");
        assertEquals(0, dvs.nextDoc());
        assertEquals(2L, dvs.longValue());
        dvs = reader.getNumericDocValues("doubledv");
        assertEquals(0, dvs.nextDoc());
        assertEquals(Double.doubleToLongBits(3), dvs.longValue());
        dvs = reader.getNumericDocValues("longdv");
        assertEquals(0, dvs.nextDoc());
        assertEquals(4L, dvs.longValue());
        SortedDocValues sdv = reader.getSortedDocValues("stringdv");
        assertEquals(0, sdv.nextDoc());
        assertEquals("solr", sdv.lookupOrd(sdv.ordValue()).utf8ToString());
        sdv = reader.getSortedDocValues("booldv");
        assertEquals(0, sdv.nextDoc());
        assertEquals("T", sdv.lookupOrd(sdv.ordValue()).utf8ToString());

        final IndexSchema schema = core.getLatestSchema();
        final SchemaField floatDv = schema.getField("floatdv");
        final SchemaField intDv = schema.getField("intdv");
        final SchemaField doubleDv = schema.getField("doubledv");
        final SchemaField longDv = schema.getField("longdv");
        final SchemaField boolDv = schema.getField("booldv");

        FunctionValues values = floatDv.getType().getValueSource(floatDv, null).getValues(null, searcher.getSlowAtomicReader().leaves().get(0));
        assertEquals(1f, values.floatVal(0), 0f);
        assertEquals(1f, values.objectVal(0));
        values = intDv.getType().getValueSource(intDv, null).getValues(null, searcher.getSlowAtomicReader().leaves().get(0));
        assertEquals(2, values.intVal(0));
        assertEquals(2, values.objectVal(0));
        values = doubleDv.getType().getValueSource(doubleDv, null).getValues(null, searcher.getSlowAtomicReader().leaves().get(0));
        assertEquals(3d, values.doubleVal(0), 0d);
        assertEquals(3d, values.objectVal(0));
        values = longDv.getType().getValueSource(longDv, null).getValues(null, searcher.getSlowAtomicReader().leaves().get(0));
        assertEquals(4L, values.longVal(0));
        assertEquals(4L, values.objectVal(0));
        
        values = boolDv.getType().getValueSource(boolDv, null).getValues(null, searcher.getSlowAtomicReader().leaves().get(0));
        assertEquals("true", values.strVal(0));
        assertEquals(true, values.objectVal(0));

        // check reversibility of created fields
        tstToObj(schema.getField("floatdv"), -1.5f);
        tstToObj(schema.getField("floatdvs"), -1.5f);
        tstToObj(schema.getField("doubledv"), -1.5d);
        tstToObj(schema.getField("doubledvs"), -1.5d);
        tstToObj(schema.getField("intdv"), -7);
        tstToObj(schema.getField("intdvs"), -7);
        tstToObj(schema.getField("longdv"), -11L);
        tstToObj(schema.getField("longdvs"), -11L);
        tstToObj(schema.getField("datedv"), new Date(1000));
        tstToObj(schema.getField("datedvs"), new Date(1000));
        tstToObj(schema.getField("stringdv"), "foo");
        tstToObj(schema.getField("stringdvs"), "foo");
        tstToObj(schema.getField("booldv"), true);
        tstToObj(schema.getField("booldvs"), true);

      } finally {
        searcherRef.decref();
      }
    }
  }

  private void tstToObj(SchemaField sf, Object o) {
    List<IndexableField> fields = sf.createFields(o);
    for (IndexableField field : fields) {
      assertEquals( sf.getType().toObject(field), o);
    }
  }

  @Test
  public void testDocValuesSorting() {
    assertU(adoc("id", "1", "floatdv", "2", "intdv", "3", "doubledv", "4", "longdv", "5", "datedv", "1995-12-31T23:59:59.999Z", "stringdv", "b", "booldv", "true"));
    assertU(adoc("id", "2", "floatdv", "5", "intdv", "4", "doubledv", "3", "longdv", "2", "datedv", "1997-12-31T23:59:59.999Z", "stringdv", "a", "booldv", "false"));
    assertU(adoc("id", "3", "floatdv", "3", "intdv", "1", "doubledv", "2", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "c", "booldv", "true"));
    assertU(adoc("id", "4"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "floatdv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='2']");
    assertQ(req("q", "*:*", "sort", "intdv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='2']");
    assertQ(req("q", "*:*", "sort", "doubledv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='1']");
    assertQ(req("q", "*:*", "sort", "longdv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='1']");
    assertQ(req("q", "*:*", "sort", "datedv desc", "rows", "1", "fl", "id,datedv"),
        "//str[@name='id'][.='2']",
        "//result/doc[1]/date[@name='datedv'][.='1997-12-31T23:59:59.999Z']"
        );
    assertQ(req("q", "*:*", "sort", "stringdv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='4']");
    assertQ(req("q", "*:*", "sort", "floatdv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='4']");
    assertQ(req("q", "*:*", "sort", "intdv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='3']");
    assertQ(req("q", "*:*", "sort", "doubledv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='3']");
    assertQ(req("q", "*:*", "sort", "longdv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='3']");
    assertQ(req("q", "*:*", "sort", "datedv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='1']");
    assertQ(req("q", "*:*", "sort", "stringdv asc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='2']");
    assertQ(req("q", "*:*", "sort", "booldv asc", "rows", "10", "fl", "booldv,stringdv"),
        "//result/doc[1]/bool[@name='booldv'][.='false']",
        "//result/doc[2]/bool[@name='booldv'][.='true']",
        "//result/doc[3]/bool[@name='booldv'][.='true']",
        "//result/doc[4]/bool[@name='booldv'][.='true']"
        );
        

  }
  
  @Test
  public void testDocValuesSorting2() {
    assertU(adoc("id", "1", "doubledv", "12"));
    assertU(adoc("id", "2", "doubledv", "50.567"));
    assertU(adoc("id", "3", "doubledv", "+0"));
    assertU(adoc("id", "4", "doubledv", "4.9E-324"));
    assertU(adoc("id", "5", "doubledv", "-0.1"));
    assertU(adoc("id", "6", "doubledv", "-5.123"));
    assertU(adoc("id", "7", "doubledv", "1.7976931348623157E308"));
    assertU(commit());
    assertQ(req("fl", "id", "q", "*:*", "sort", "doubledv asc"),
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='3']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='1']",
        "//result/doc[6]/str[@name='id'][.='2']",
        "//result/doc[7]/str[@name='id'][.='7']"
        );
  }

  @Test
  public void testDocValuesFaceting() {
    for (int i = 0; i < 50; ++i) {
      assertU(adoc("id", "" + i));
    }
    for (int i = 0; i < 50; ++i) {
      if (rarely()) {
        assertU(commit()); // to have several segments
      }
      switch (i % 3) {
        case 0:
          assertU(adoc("id", "1000" + i, "floatdv", "" + i, "intdv", "" + i, "doubledv", "" + i, "longdv", "" + i,
              "datedv", (1900 + i) + "-12-31T23:59:59.999Z", "stringdv", "abc" + i, "booldv", "false"));
          break;
        case 1:
          assertU(adoc("id", "1000" + i, "floatdv", "" + i, "intdv", "" + i, "doubledv", "" + i, "longdv", "" + i,
              "datedv", (1900 + i) + "-12-31T23:59:59.999Z", "stringdv", "abc" + i, "booldv", "true"));
          break;
        case 2:
          assertU(adoc("id", "1000" + i, "floatdv", "" + i, "intdv", "" + i, "doubledv", "" + i, "longdv", "" + i,
              "datedv", (1900 + i) + "-12-31T23:59:59.999Z", "stringdv", "abc" + i));
          break;
      }
    }
    assertU(commit());
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "longdv", "facet.sort", "count", "facet.limit", "1"),
        "//lst[@name='longdv']/int[@name='4'][.='51']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "longdv", "facet.sort", "count", "facet.offset", "1", "facet.limit", "1"),
        "//lst[@name='longdv']/int[@name='0'][.='1']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "longdv", "facet.sort", "index", "facet.offset", "33", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='longdv']/int[@name='33'][.='1']");

    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "floatdv", "facet.sort", "count", "facet.limit", "1"),
        "//lst[@name='floatdv']/int[@name='1.0'][.='51']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "floatdv", "facet.sort", "count", "facet.offset", "1", "facet.limit", "-1", "facet.mincount", "1"),
        "//lst[@name='floatdv']/int[@name='0.0'][.='1']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "floatdv", "facet.sort", "index", "facet.offset", "33", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='floatdv']/int[@name='33.0'][.='1']");

    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "doubledv", "facet.sort", "count", "facet.limit", "1"),
        "//lst[@name='doubledv']/int[@name='3.0'][.='51']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "doubledv", "facet.sort", "count", "facet.offset", "1", "facet.limit", "-1", "facet.mincount", "1"),
        "//lst[@name='doubledv']/int[@name='0.0'][.='1']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "doubledv", "facet.sort", "index", "facet.offset", "33", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='doubledv']/int[@name='33.0'][.='1']");

    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "intdv", "facet.sort", "count", "facet.limit", "1"),
        "//lst[@name='intdv']/int[@name='2'][.='51']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "intdv", "facet.sort", "count", "facet.offset", "1", "facet.limit", "-1", "facet.mincount", "1"),
        "//lst[@name='intdv']/int[@name='0'][.='1']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "intdv", "facet.sort", "index", "facet.offset", "33", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='intdv']/int[@name='33'][.='1']");

    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "datedv", "facet.sort", "count", "facet.limit", "1"),
        "//lst[@name='datedv']/int[@name='1995-12-31T23:59:59.999Z'][.='50']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "datedv", "facet.sort", "count", "facet.offset", "1", "facet.limit", "-1", "facet.mincount", "1"),
        "//lst[@name='datedv']/int[@name='1900-12-31T23:59:59.999Z'][.='1']");
    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "datedv", "facet.sort", "index", "facet.offset", "33", "facet.limit", "1", "facet.mincount", "1"),
        "//lst[@name='datedv']/int[@name='1933-12-31T23:59:59.999Z'][.='1']");

    assertQ(req("q", "booldv:true"),
        "//*[@numFound='83']");

    assertQ(req("q", "booldv:false"),
        "//*[@numFound='17']");

    assertQ(req("q", "*:*", "facet", "true", "rows", "0", "facet.field", "booldv", "facet.sort", "index", "facet.mincount", "1"),
        "//lst[@name='booldv']/int[@name='false'][.='17']",
        "//lst[@name='booldv']/int[@name='true'][.='83']");

  }

  @Test
  public void testDocValuesStats() {
    for (int i = 0; i < 50; ++i) {
      assertU(adoc("id", "1000" + i, "floatdv", "" + i%2, "intdv", "" + i%3, "doubledv", "" + i%4, "longdv", "" + i%5, "datedv", (1900+i%6) + "-12-31T23:59:59.999Z", "stringdv", "abc" + i%7));
      if (rarely()) {
        assertU(commit()); // to have several segments
      }
    }
    assertU(commit());

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "stringdv"),
        "//str[@name='min'][.='abc0']",
        "//str[@name='max'][.='abc6']",
        "//long[@name='count'][.='50']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "floatdv"),
        "//double[@name='min'][.='0.0']",
        "//double[@name='max'][.='1.0']",
        "//long[@name='count'][.='50']",
        "//double[@name='sum'][.='25.0']",
        "//double[@name='mean'][.='0.5']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "intdv"),
        "//double[@name='min'][.='0.0']",
        "//double[@name='max'][.='2.0']",
        "//long[@name='count'][.='50']",
        "//double[@name='sum'][.='49.0']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "doubledv"),
        "//double[@name='min'][.='0.0']",
        "//double[@name='max'][.='3.0']",
        "//long[@name='count'][.='50']",
        "//double[@name='sum'][.='73.0']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "longdv"),
        "//double[@name='min'][.='0.0']",
        "//double[@name='max'][.='4.0']",
        "//long[@name='count'][.='50']",
        "//double[@name='sum'][.='100.0']",
        "//double[@name='mean'][.='2.0']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "datedv"),
        "//date[@name='min'][.='1900-12-31T23:59:59.999Z']",
        "//date[@name='max'][.='1905-12-31T23:59:59.999Z']",
        "//long[@name='count'][.='50']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "floatdv", "stats.facet", "intdv"),
        "//lst[@name='intdv']/lst[@name='0']/long[@name='count'][.='17']",
        "//lst[@name='intdv']/lst[@name='1']/long[@name='count'][.='17']",
        "//lst[@name='intdv']/lst[@name='2']/long[@name='count'][.='16']");

    assertQ(req("q", "*:*", "stats", "true", "rows", "0", "stats.field", "floatdv", "stats.facet", "datedv"),
        "//lst[@name='datedv']/lst[@name='1900-12-31T23:59:59.999Z']/long[@name='count'][.='9']",
        "//lst[@name='datedv']/lst[@name='1901-12-31T23:59:59.999Z']/long[@name='count'][.='9']",
        "//lst[@name='datedv']/lst[@name='1902-12-31T23:59:59.999Z']/long[@name='count'][.='8']",
        "//lst[@name='datedv']/lst[@name='1903-12-31T23:59:59.999Z']/long[@name='count'][.='8']",
        "//lst[@name='datedv']/lst[@name='1904-12-31T23:59:59.999Z']/long[@name='count'][.='8']",
        "//lst[@name='datedv']/lst[@name='1905-12-31T23:59:59.999Z']/long[@name='count'][.='8']");
  }
  
  /** Tests the ability to do basic queries (without scoring, just match-only) on
   *  docvalues fields that are not inverted (indexed "forward" only)
   */
  @Test
  public void testDocValuesMatch() throws Exception {
    assertU(adoc("id", "1", "floatdv", "2", "intdv", "3", "doubledv", "3.1", "longdv", "5", "datedv", "1995-12-31T23:59:59.999Z", "stringdv", "b", "booldv", "false"));
    assertU(adoc("id", "2", "floatdv", "-5", "intdv", "4", "doubledv", "-4.3", "longdv", "2", "datedv", "1997-12-31T23:59:59.999Z", "stringdv", "a", "booldv", "true"));
    assertU(adoc("id", "3", "floatdv", "3", "intdv", "1", "doubledv", "2.1", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "c", "booldv", "false"));
    assertU(adoc("id", "4", "floatdv", "3", "intdv", "-1", "doubledv", "1.5", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "car"));
    assertU(commit());

    // string: termquery
    assertQ(req("q", "stringdv:car", "sort", "id_i asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // string: range query
    assertQ(req("q", "stringdv:[b TO d]", "sort", "id_i asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
    
    // string: prefix query
    assertQ(req("q", "stringdv:c*", "sort", "id_i asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // string: wildcard query
    assertQ(req("q", "stringdv:c?r", "sort", "id_i asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // string: regexp query
    assertQ(req("q", "stringdv:/c[a-b]r/", "sort", "id_i asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // float: termquery
    assertQ(req("q", "floatdv:3", "sort", "id_i asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // float: rangequery
    assertQ(req("q", "floatdv:[2 TO 3]", "sort", "id_i asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
    
    // (neg) float: termquery
    assertQ(req("q", "floatdv:\"-5\"", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=2]"
            );

    // (neg) float: rangequery
    assertQ(req("q", "floatdv:[-6 TO -4]", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=2]"
            );
    
    // (cross zero bounds) float: rangequery
    assertQ(req("q", "floatdv:[-6 TO 2.1]", "sort", "id_i asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.=1]",
            "//result/doc[2]/str[@name='id'][.=2]"
            );
    
    // int: termquery
    assertQ(req("q", "intdv:1", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=3]"
            );
    
    // int: rangequery
    assertQ(req("q", "intdv:[3 TO 4]", "sort", "id_i asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.=1]",
            "//result/doc[2]/str[@name='id'][.=2]"
            );
    
    // (neg) int: termquery
    assertQ(req("q", "intdv:\"-1\"", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=4]"
            );
    
    // (neg) int: rangequery
    assertQ(req("q", "intdv:[-1 TO 1]", "sort", "id_i asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.=3]",
            "//result/doc[2]/str[@name='id'][.=4]"
            );

    // long: termquery
    assertQ(req("q", "longdv:1", "sort", "id_i asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // long: rangequery
    assertQ(req("q", "longdv:[1 TO 2]", "sort", "id_i asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
    
    // double: termquery
    assertQ(req("q", "doubledv:3.1", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=1]"
            );
    
    // double: rangequery
    assertQ(req("q", "doubledv:[2 TO 3.3]", "sort", "id_i asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.=1]",
            "//result/doc[2]/str[@name='id'][.=3]"
            );
    
    // (neg) double: termquery
    assertQ(req("q", "doubledv:\"-4.3\"", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=2]"
            );
    
    // (neg) double: rangequery
    assertQ(req("q", "doubledv:[-6 TO -4]", "sort", "id_i asc"),
            "//*[@numFound='1']",
            "//result/doc[1]/str[@name='id'][.=2]"
            );
    
    // (cross zero bounds) double: rangequery
    assertQ(req("q", "doubledv:[-6 TO 2.0]", "sort", "id_i asc"),
            "//*[@numFound='2']",
            "//result/doc[1]/str[@name='id'][.=2]",
            "//result/doc[2]/str[@name='id'][.=4]"
            );
    // boolean basic queries:

    assertQ(req("q", "booldv:false", "sort", "id_i asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]"
    );

    assertQ(req("q", "booldv:true", "sort", "id_i asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );

  }

  @Test
  public void testFloatAndDoubleRangeQueryRandom() throws Exception {

    String fieldName[] = new String[] {"floatdv", "doubledv"};
    
    Number largestNegative[] = new Number[] {0f-Float.MIN_NORMAL, 0f-Double.MIN_NORMAL};
    Number smallestPositive[] = new Number[] {Float.MIN_NORMAL, Double.MIN_NORMAL};
    Number positiveInfinity[] = new Number[] {Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY};
    Number negativeInfinity[] = new Number[] {Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY};
    Number largestValue[] = new Number[] {Float.MAX_VALUE, Double.MAX_VALUE};
    Number zero[] = new Number[] {0f, 0d};
    Function<Supplier<Number>,Number> noNaN = (next)
        -> { Number num; while (String.valueOf(num = next.get()).equals("NaN")); return num; };
    List<Supplier<Number>> nextRandNoNaN = Arrays.asList(
        () -> noNaN.apply(() -> Float.intBitsToFloat(random().nextInt())),
        () -> noNaN.apply(() -> Double.longBitsToDouble(random().nextLong())));
    List<Function<Number,Long>> toSortableLong = Arrays.asList(
        (num) -> (long)NumericUtils.floatToSortableInt(num.floatValue()),
        (num) -> NumericUtils.doubleToSortableLong(num.doubleValue()));

    // Number minusZero[] = new Number[] {-0f, -0d}; // -0 == 0, so we should not treat them differently (and we should not guarantee that sign is preserved... we should be able to index both as 0)

    for (int i=0; i<fieldName.length; i++) {
      assertU(delQ("*:*"));
      commit();

      Number specialValues[] = new Number[] {largestNegative[i], smallestPositive[i], negativeInfinity[i], 
          largestValue[i], positiveInfinity[i], zero[i]};

      List<Number> values = new ArrayList<>();
      int numDocs = 1 + random().nextInt(10);
      for (int j=0; j<numDocs; j++) {
        
        if (random().nextInt(100) < 5) { // Add a boundary value with 5% probability
          values.add(specialValues[random().nextInt(specialValues.length)]);
        } else 
        {
          if (fieldName[i].equals("floatdv")) { // Add random values with 95% probability
            values.add(Float.intBitsToFloat(random().nextInt()));
          } else {
            values.add(Double.longBitsToDouble(random().nextLong()));
          }
        }
      }
      // Indexing
      for (int j=0; j<values.size(); j++) {
        assertU(adoc("id", String.valueOf(j+1), fieldName[i], String.valueOf(values.get(j))));
      }
      assertU(commit());

      log.info("Indexed values: {}", values);
      // Querying
      int numQueries = 10000;
      for (int j=0; j<numQueries; j++) {
        boolean minInclusive = random().nextBoolean();
        boolean maxInclusive = random().nextBoolean();

        Number minVal, maxVal;
        String min = String.valueOf(minVal = nextRandNoNaN.get(i).get());
        String max = String.valueOf(maxVal = nextRandNoNaN.get(i).get());

        // randomly use boundary values for min, 15% of the time
        int r = random().nextInt(100);
        if (r<5) {
          minVal = negativeInfinity[i]; min = "*";
        } else if (r<10) {
          minVal = specialValues[random().nextInt(specialValues.length)]; min = String.valueOf(minVal);
        } else if (r<15) {
          minVal = values.get(random().nextInt(values.size())); min = String.valueOf(minVal);
        }

        // randomly use boundary values for max, 15% of the time
        r = random().nextInt(100);
        if (r<5) {
          maxVal = positiveInfinity[i]; max = "*";
        } else if (r<10) {
            maxVal = specialValues[random().nextInt(specialValues.length)]; max = String.valueOf(maxVal);
        } else if (r<15) {
          // Don't pick a NaN for the range query
          Number tmp = values.get(random().nextInt(values.size()));
          if (!Double.isNaN(tmp.doubleValue()) && !Float.isNaN(tmp.floatValue())) {
            maxVal = tmp; max = String.valueOf(maxVal);
          }
        }

        List<String> tests = new ArrayList<>();
        int counter = 0;
        
        for (int k=0; k<values.size(); k++) {
          Number val = values.get(k);
          long valSortable = toSortableLong.get(i).apply(val);
          long minSortable = toSortableLong.get(i).apply(minVal);
          long maxSortable = toSortableLong.get(i).apply(maxVal);
          
          if((minInclusive && minSortable<=valSortable || !minInclusive && minSortable<valSortable || (min.equals("*") && val == negativeInfinity[i])) &&
              (maxInclusive && maxSortable>=valSortable || !maxInclusive && maxSortable>valSortable || (max.equals("*") && val == positiveInfinity[i]))) {
            counter++;
            tests.add("//result/doc["+counter+"]/str[@name='id'][.="+(k+1)+"]");
            tests.add("//result/doc["+counter+"]/float[@name='score'][.=1.0]");
          }
        }

        tests.add(0, "//*[@numFound='"+counter+"']");

        String testsArr[] = new String[tests.size()];
        for (int k=0; k<tests.size(); k++) {
          testsArr[k] = tests.get(k);
        }
        log.info("Expected: {}", tests);
        assertQ(req("q", fieldName[i] + ":" + (minInclusive? '[': '{') + min + " TO " + max + (maxInclusive? ']': '}'),
                         "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
            testsArr);
      }
    }
  }

  @Test
  public void testFloatAndDoubleRangeQuery() throws Exception {
    String fieldName[] = new String[] {"floatdv", "doubledv"};
    String largestNegative[] = new String[] {String.valueOf(0f-Float.MIN_NORMAL), String.valueOf(0f-Double.MIN_NORMAL)};
    String negativeInfinity[] = new String[] {String.valueOf(Float.NEGATIVE_INFINITY), String.valueOf(Double.NEGATIVE_INFINITY)};
    String largestValue[] = new String[] {String.valueOf(Float.MAX_VALUE), String.valueOf(Double.MAX_VALUE)};
    
    for (int i=0; i<fieldName.length; i++) {
      assertU(adoc("id", "1", fieldName[i], "2"));
      assertU(adoc("id", "2", fieldName[i], "-5"));
      assertU(adoc("id", "3", fieldName[i], "3"));
      assertU(adoc("id", "4", fieldName[i], "3"));
      assertU(adoc("id", "5", fieldName[i], largestNegative[i]));
      assertU(adoc("id", "6", fieldName[i], negativeInfinity[i]));
      assertU(adoc("id", "7", fieldName[i], largestValue[i]));
      assertU(commit());

      // Negative Zero to Positive
      assertQ(req("q", fieldName[i]+":[-0.0 TO 2.5]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='1']",
          "//result/doc[1]/str[@name='id'][.=1]"
          );

      // Negative to Positive Zero
      assertQ(req("q", fieldName[i]+":[-6 TO 0]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='2']",
          "//result/doc[1]/str[@name='id'][.=2]",
          "//result/doc[2]/str[@name='id'][.=5]"
          );

      // Negative to Positive
      assertQ(req("q", fieldName[i]+":[-6 TO 2.5]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=2]",
          "//result/doc[3]/str[@name='id'][.=5]"
          );

      // Positive to Positive
      assertQ(req("q", fieldName[i]+":[2 TO 3]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=3]",
          "//result/doc[3]/str[@name='id'][.=4]"
          );

      // Positive to POSITIVE_INF
      assertQ(req("q", fieldName[i]+":[2 TO *]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=3]",
          "//result/doc[3]/str[@name='id'][.=4]",
          "//result/doc[4]/str[@name='id'][.=7]"
          );

      // NEGATIVE_INF to Negative
      assertQ(req("q", fieldName[i]+":[* TO -1]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='2']",
          "//result/doc[1]/str[@name='id'][.=2]",
          "//result/doc[2]/str[@name='id'][.=6]"
          );

      // NEGATIVE_INF to Positive
      assertQ(req("q", fieldName[i]+":[* TO 2]", "sort", "id_i asc", "fl", "id,"+fieldName[i]),
          "//*[@numFound='4']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=2]",
          "//result/doc[3]/str[@name='id'][.=5]",
          "//result/doc[4]/str[@name='id'][.=6]"
          );

      // NEGATIVE_INF to Positive (non-inclusive)
      assertQ(req("q", fieldName[i]+":[* TO 2}", "sort", "id_i asc", "fl", "id,"+fieldName[i]),
          "//*[@numFound='3']",
          "//result/doc[1]/str[@name='id'][.=2]",
          "//result/doc[2]/str[@name='id'][.=5]",
          "//result/doc[3]/str[@name='id'][.=6]"
          );

      // Negative to POSITIVE_INF
      assertQ(req("q", fieldName[i]+":[-6 TO *]", "sort", "id_i asc", "fl", "id,"+fieldName[i]),
          "//*[@numFound='6']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=2]",
          "//result/doc[3]/str[@name='id'][.=3]",
          "//result/doc[4]/str[@name='id'][.=4]",
          "//result/doc[5]/str[@name='id'][.=5]",
          "//result/doc[6]/str[@name='id'][.=7]"
          );

      // NEGATIVE_INF to POSITIVE_INF
      assertQ(req("q", fieldName[i]+":[* TO *]", "sort", "id_i asc", "fl", "id,"+fieldName[i]+",score"),
          "//*[@numFound='7']",
          "//result/doc[1]/str[@name='id'][.=1]",
          "//result/doc[2]/str[@name='id'][.=2]",
          "//result/doc[3]/str[@name='id'][.=3]",
          "//result/doc[4]/str[@name='id'][.=4]",
          "//result/doc[5]/str[@name='id'][.=5]",
          "//result/doc[6]/str[@name='id'][.=6]",
          "//result/doc[7]/str[@name='id'][.=7]",
          "//result/doc[1]/float[@name='score'][.=1.0]",
          "//result/doc[2]/float[@name='score'][.=1.0]",
          "//result/doc[3]/float[@name='score'][.=1.0]",
          "//result/doc[4]/float[@name='score'][.=1.0]",
          "//result/doc[5]/float[@name='score'][.=1.0]",
          "//result/doc[6]/float[@name='score'][.=1.0]",
          "//result/doc[7]/float[@name='score'][.=1.0]"
          );
    }
  }
}
