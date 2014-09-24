package org.apache.solr.schema;

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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;

import java.io.IOException;

public class DocValuesTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValues.xml");
  }

  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  public void testDocValues() throws IOException {
    assertU(adoc("id", "1"));
    assertU(commit());
    try (SolrCore core = h.getCoreInc()) {
      final RefCounted<SolrIndexSearcher> searcherRef = core.openNewSearcher(true, true);
      final SolrIndexSearcher searcher = searcherRef.get();
      try {
        final LeafReader reader = searcher.getLeafReader();
        assertEquals(1, reader.numDocs());
        final FieldInfos infos = reader.getFieldInfos();
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("floatdv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("intdv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("doubledv").getDocValuesType());
        assertEquals(DocValuesType.NUMERIC, infos.fieldInfo("longdv").getDocValuesType());
        assertEquals(DocValuesType.SORTED, infos.fieldInfo("stringdv").getDocValuesType());

        assertEquals((long) Float.floatToIntBits(1), reader.getNumericDocValues("floatdv").get(0));
        assertEquals(2L, reader.getNumericDocValues("intdv").get(0));
        assertEquals(Double.doubleToLongBits(3), reader.getNumericDocValues("doubledv").get(0));
        assertEquals(4L, reader.getNumericDocValues("longdv").get(0));

        final IndexSchema schema = core.getLatestSchema();
        final SchemaField floatDv = schema.getField("floatdv");
        final SchemaField intDv = schema.getField("intdv");
        final SchemaField doubleDv = schema.getField("doubledv");
        final SchemaField longDv = schema.getField("longdv");

        FunctionValues values = floatDv.getType().getValueSource(floatDv, null).getValues(null, searcher.getLeafReader().leaves().get(0));
        assertEquals(1f, values.floatVal(0), 0f);
        assertEquals(1f, values.objectVal(0));
        values = intDv.getType().getValueSource(intDv, null).getValues(null, searcher.getLeafReader().leaves().get(0));
        assertEquals(2, values.intVal(0));
        assertEquals(2, values.objectVal(0));
        values = doubleDv.getType().getValueSource(doubleDv, null).getValues(null, searcher.getLeafReader().leaves().get(0));
        assertEquals(3d, values.doubleVal(0), 0d);
        assertEquals(3d, values.objectVal(0));
        values = longDv.getType().getValueSource(longDv, null).getValues(null, searcher.getLeafReader().leaves().get(0));
        assertEquals(4L, values.longVal(0));
        assertEquals(4L, values.objectVal(0));
      } finally {
        searcherRef.decref();
      }
    }
  }

  public void testDocValuesSorting() {
    assertU(adoc("id", "1", "floatdv", "2", "intdv", "3", "doubledv", "4", "longdv", "5", "datedv", "1995-12-31T23:59:59.999Z", "stringdv", "b"));
    assertU(adoc("id", "2", "floatdv", "5", "intdv", "4", "doubledv", "3", "longdv", "2", "datedv", "1997-12-31T23:59:59.999Z", "stringdv", "a"));
    assertU(adoc("id", "3", "floatdv", "3", "intdv", "1", "doubledv", "2", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "c"));
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
    assertQ(req("q", "*:*", "sort", "datedv desc", "rows", "1", "fl", "id"),
        "//str[@name='id'][.='2']");
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
  }
  
  public void testDocValuesSorting2() {
    assertU(adoc("id", "1", "doubledv", "12"));
    assertU(adoc("id", "2", "doubledv", "50.567"));
    assertU(adoc("id", "3", "doubledv", "+0"));
    assertU(adoc("id", "4", "doubledv", "4.9E-324"));
    assertU(adoc("id", "5", "doubledv", "-0"));
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

  public void testDocValuesFaceting() {
    for (int i = 0; i < 50; ++i) {
      assertU(adoc("id", "" + i));
    }
    for (int i = 0; i < 50; ++i) {
      if (rarely()) {
        assertU(commit()); // to have several segments
      }
      assertU(adoc("id", "1000" + i, "floatdv", "" + i, "intdv", "" + i, "doubledv", "" + i, "longdv", "" + i, "datedv", (1900+i) + "-12-31T23:59:59.999Z", "stringdv", "abc" + i));
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
  }

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
  public void testDocValuesMatch() throws Exception {
    assertU(adoc("id", "1", "floatdv", "2", "intdv", "3", "doubledv", "4", "longdv", "5", "datedv", "1995-12-31T23:59:59.999Z", "stringdv", "b"));
    assertU(adoc("id", "2", "floatdv", "5", "intdv", "4", "doubledv", "3", "longdv", "2", "datedv", "1997-12-31T23:59:59.999Z", "stringdv", "a"));
    assertU(adoc("id", "3", "floatdv", "3", "intdv", "1", "doubledv", "2", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "c"));
    assertU(adoc("id", "4", "floatdv", "3", "intdv", "1", "doubledv", "2", "longdv", "1", "datedv", "1996-12-31T23:59:59.999Z", "stringdv", "car"));
    assertU(commit());
    
    // string: termquery
    assertQ(req("q", "stringdv:car", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // string: range query
    assertQ(req("q", "stringdv:[b TO d]", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
    
    // string: prefix query
    assertQ(req("q", "stringdv:c*", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // string: wildcard query
    assertQ(req("q", "stringdv:c?r", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // string: regexp query
    assertQ(req("q", "stringdv:/c[a-b]r/", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // float: termquery
    assertQ(req("q", "floatdv:3", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // float: rangequery
    assertQ(req("q", "floatdv:[2 TO 3]", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
    
    // int: termquery
    assertQ(req("q", "intdv:1", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // int: rangequery
    assertQ(req("q", "intdv:[3 TO 4]", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=2]"
    );
    
    // long: termquery
    assertQ(req("q", "longdv:1", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // long: rangequery
    assertQ(req("q", "longdv:[1 TO 2]", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=2]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]"
    );
  }
}
