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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;

import java.io.IOException;

public class DocValuesMultiTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml", "schema-docValuesMulti.xml");
  }

  public void setUp() throws Exception {
    super.setUp();
    assertU(delQ("*:*"));
  }

  public void testDocValues() throws IOException {
    assertU(adoc("id", "1", "floatdv", "4.5", "intdv", "-1", "intdv", "3", "stringdv", "value1", "stringdv", "value2"));
    assertU(commit());
    try (SolrCore core = h.getCoreInc()) {
      final RefCounted<SolrIndexSearcher> searcherRef = core.openNewSearcher(true, true);
      final SolrIndexSearcher searcher = searcherRef.get();
      try {
        final LeafReader reader = searcher.getLeafReader();
        assertEquals(1, reader.numDocs());
        final FieldInfos infos = reader.getFieldInfos();
        assertEquals(DocValuesType.SORTED_SET, infos.fieldInfo("stringdv").getDocValuesType());
        assertEquals(DocValuesType.SORTED_SET, infos.fieldInfo("floatdv").getDocValuesType());
        assertEquals(DocValuesType.SORTED_SET, infos.fieldInfo("intdv").getDocValuesType());

        SortedSetDocValues dv = reader.getSortedSetDocValues("stringdv");
        dv.setDocument(0);
        assertEquals(0, dv.nextOrd());
        assertEquals(1, dv.nextOrd());
        assertEquals(SortedSetDocValues.NO_MORE_ORDS, dv.nextOrd());
      } finally {
        searcherRef.decref();
      }
    }
  }
  
  /** Tests the ability to do basic queries (without scoring, just match-only) on
   *  string docvalues fields that are not inverted (indexed "forward" only)
   */
  public void testStringDocValuesMatch() throws Exception {
    assertU(adoc("id", "1", "stringdv", "b"));
    assertU(adoc("id", "2", "stringdv", "a"));
    assertU(adoc("id", "3", "stringdv", "c"));
    assertU(adoc("id", "4", "stringdv", "car"));
    assertU(adoc("id", "5", "stringdv", "dog", "stringdv", "cat"));
    assertU(commit());
    
    // string: termquery
    assertQ(req("q", "stringdv:car", "sort", "id asc"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.=4]"
    );
    
    // string: range query
    assertQ(req("q", "stringdv:[b TO d]", "sort", "id asc"),
        "//*[@numFound='4']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]",
        "//result/doc[3]/str[@name='id'][.=4]",
        "//result/doc[4]/str[@name='id'][.=5]"
    );
    
    // string: prefix query
    assertQ(req("q", "stringdv:c*", "sort", "id asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]",
        "//result/doc[3]/str[@name='id'][.=5]"
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
  }
  
  /** Tests the ability to do basic queries (without scoring, just match-only) on
   *  float docvalues fields that are not inverted (indexed "forward" only)
   */
  public void testFloatDocValuesMatch() throws Exception {
    assertU(adoc("id", "1", "floatdv", "2"));
    assertU(adoc("id", "2", "floatdv", "5"));
    assertU(adoc("id", "3", "floatdv", "3.0", "floatdv", "-1.3", "floatdv", "2.2"));
    assertU(adoc("id", "4", "floatdv", "3"));
    assertU(commit());
    
    // float: termquery
    assertQ(req("q", "floatdv:3", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=3]",
        "//result/doc[2]/str[@name='id'][.=4]"
    );
    
    // float: rangequery
    assertQ(req("q", "floatdv:[-1 TO 2.5]", "sort", "id asc"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.=1]",
        "//result/doc[2]/str[@name='id'][.=3]"
    );
  }
  
  public void testDocValuesFacetingSimple() {
    // this is the random test verbatim from DocValuesTest, so it populates with the default values defined in its schema.
    for (int i = 0; i < 50; ++i) {
      assertU(adoc("id", "" + i, "floatdv", "1", "intdv", "2", "doubledv", "3", "longdv", "4", "datedv", "1995-12-31T23:59:59.999Z"));
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
}
