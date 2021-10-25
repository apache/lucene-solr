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
import java.nio.charset.StandardCharsets;

import javax.xml.xpath.XPathConstants;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.ErrorLogMuter;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.Ignore;

public class RankFieldTest extends SolrTestCaseJ4 {
  
  private static final String RANK_1 = "rank_1";
  private static final String RANK_2 = "rank_2";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml","schema-rank-fields.xml");
  }
  
  @Override
  public void setUp() throws Exception {
    clearIndex();
    assertU(commit());
    super.setUp();
  }
  
  public void testInternalFieldName() {
    assertEquals("RankField.INTERNAL_RANK_FIELD_NAME changed in an incompatible way",
        "_rank_", RankField.INTERNAL_RANK_FIELD_NAME);
  }

  public void testBasic() {
    assertNotNull(h.getCore().getLatestSchema().getFieldOrNull(RANK_1));
    assertEquals(RankField.class, h.getCore().getLatestSchema().getField(RANK_1).getType().getClass());
  }
  
  public void testBadFormat() {
    try (ErrorLogMuter errors = ErrorLogMuter.substring("Expecting float")) {
      assertFailedU(adoc(
        "id", "1",
        RANK_1, "foo"
        ));

      assertFailedU(adoc(
        "id", "1",
        RANK_1, "1.2.3"
        ));
      assertEquals(2, errors.getCount());
    }
    
    try (ErrorLogMuter errors = ErrorLogMuter.substring("must be finite")) {
      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(Float.POSITIVE_INFINITY)
        ));

      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(Float.NEGATIVE_INFINITY)
        ));
    
      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(Float.NaN)
        ));
      assertEquals(3, errors.getCount());
    }
    
    try (ErrorLogMuter errors = ErrorLogMuter.substring("must be a positive")) {
      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(-0.0f)
        ));

      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(-1f)
        ));

      assertFailedU(adoc(
        "id", "1",
        RANK_1, Float.toString(0.0f)
        ));
      assertEquals(3, errors.getCount());
    }
  }
  
  public void testAddRandom() {
    for (int i = 0 ; i < random().nextInt(TEST_NIGHTLY ? 10000 : 100); i++) {
      assertU(adoc(
          "id", String.valueOf(i),
          RANK_1, Float.toString(random().nextFloat())
          ));
    }
    assertU(commit());
  }
  
  public void testSkipEmpty() {
    assertU(adoc(
        "id", "1",
        RANK_1, ""
        ));
  }
  
  public void testBasicAdd() throws IOException {
    assertU(adoc(
        "id", "testBasicAdd",
        RANK_1, "1"
        ));
    assertU(commit());
    //assert that the document made it in
    assertQ(req("q", "id:testBasicAdd"), "//*[@numFound='1']");
    h.getCore().withSearcher((searcher) -> {
      LeafReader reader = searcher.getIndexReader().getContext().leaves().get(0).reader();
      // assert that the field made it in
      assertNotNull(reader.getFieldInfos().fieldInfo(RankField.INTERNAL_RANK_FIELD_NAME));
      // assert that the feature made it in
      assertTrue(reader.terms(RankField.INTERNAL_RANK_FIELD_NAME).iterator().seekExact(new BytesRef(RANK_1.getBytes(StandardCharsets.UTF_8))));
      return null;
    });
  }
  
  public void testMultipleRankFields() throws IOException {
    assertU(adoc(
        "id", "testMultiValueAdd",
        RANK_1, "1",
        RANK_2, "2"
        ));
    assertU(commit());
    //assert that the document made it in
    assertQ(req("q", "id:testMultiValueAdd"), "//*[@numFound='1']");
    h.getCore().withSearcher((searcher) -> {
      LeafReader reader = searcher.getIndexReader().getContext().leaves().get(0).reader();
      // assert that the field made it in
      assertNotNull(reader.getFieldInfos().fieldInfo(RankField.INTERNAL_RANK_FIELD_NAME));
      // assert that the features made it in
      assertTrue(reader.terms(RankField.INTERNAL_RANK_FIELD_NAME).iterator().seekExact(new BytesRef(RANK_2.getBytes(StandardCharsets.UTF_8))));
      assertTrue(reader.terms(RankField.INTERNAL_RANK_FIELD_NAME).iterator().seekExact(new BytesRef(RANK_1.getBytes(StandardCharsets.UTF_8))));
      return null;
    });
  }
  
  public void testSortFails() throws IOException {
    assertU(adoc(
        "id", "testSortFails",
        RANK_1, "1"
        ));
    assertU(commit());
    assertQEx("Can't sort on rank field", req(
        "q", "id:testSortFails",
        "sort", RANK_1 + " desc"), 400);
  }
  
  @Ignore("We currently don't fail these kinds of requests with other field types")
  public void testFacetFails() throws IOException {
    assertU(adoc(
        "id", "testFacetFails",
        RANK_1, "1"
        ));
    assertU(commit());
    assertQEx("Can't facet on rank field", req(
        "q", "id:testFacetFails",
        "facet", "true",
        "facet.field", RANK_1), 400);
  }
  
  public void testTermQuery() throws IOException {
    assertU(adoc(
        "id", "testTermQuery",
        RANK_1, "1",
        RANK_2, "1"
        ));
    assertU(adoc(
        "id", "testTermQuery2",
        RANK_1, "1"
        ));
    assertU(commit());
    assertQ(req("q", RANK_1 + ":*"), "//*[@numFound='2']");
    assertQ(req("q", RANK_1 + ":[* TO *]"), "//*[@numFound='2']");
    assertQ(req("q", RANK_2 + ":*"), "//*[@numFound='1']");
    assertQ(req("q", RANK_2 + ":[* TO *]"), "//*[@numFound='1']");
    
    assertQEx("Term queries not supported", req("q", RANK_1 + ":1"), 400);
    assertQEx("Range queries not supported", req("q", RANK_1 + ":[1 TO 10]"), 400);
  }
  
  
  public void testResponseQuery() throws IOException {
    assertU(adoc(
        "id", "testResponseQuery",
        RANK_1, "1"
        ));
    assertU(commit());
    // Ignore requests to retrieve rank
    assertQ(req("q", RANK_1 + ":*",
        "fl", "id," + RANK_1),
        "//*[@numFound='1']",
        "count(//result/doc[1]/str)=1");
  }
  
  public void testRankQParserQuery() throws IOException {
    assertU(adoc(
        "id", "1",
        "str_field", "foo",
        RANK_1, "1",
        RANK_2, "2"
        ));
    assertU(adoc(
        "id", "2",
        "str_field", "foo",
        RANK_1, "2",
        RANK_2, "1"
        ));
    assertU(commit());
    assertQ(req("q", "str_field:foo _query_:{!rank f='" + RANK_1 + "' function='log' scalingFactor='1'}"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
    
    assertQ(req("q", "str_field:foo _query_:{!rank f='" + RANK_2 + "' function='log' scalingFactor='1'}"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
    
    assertQ(req("q", "foo",
        "defType", "dismax",
        "qf", "str_field^10",
        "bq", "{!rank f='" + RANK_1 + "' function='log' scalingFactor='1'}"
        ),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
    
    assertQ(req("q", "foo",
        "defType", "dismax",
        "qf", "str_field^10",
        "bq", "{!rank f='" + RANK_2 + "' function='log' scalingFactor='1'}"
        ),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }
  
  public void testScoreChanges() throws Exception {
    assertU(adoc(
        "id", "1",
        "str_field", "foo",
        RANK_1, "1"
        ));
    assertU(commit());
    ModifiableSolrParams params = params("q", "foo",
        "defType", "dismax",
        "qf", "str_field^10",
        "fl", "id,score",
        "wt", "xml");
    
    double scoreBefore = (Double) TestHarness.evaluateXPath(h.query(req(params)), "//result/doc[1]/float[@name='score']", XPathConstants.NUMBER);
    params.add("bq", "{!rank f='" + RANK_1 + "' function='log' scalingFactor='1'}");
    double scoreAfter = (Double) TestHarness.evaluateXPath(h.query(req(params)), "//result/doc[1]/float[@name='score']", XPathConstants.NUMBER);
    assertNotEquals("Expecting score to change", scoreBefore, scoreAfter, 0f);

  }

}
