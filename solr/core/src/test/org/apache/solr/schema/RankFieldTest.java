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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Ignore;

public class RankFieldTest extends SolrTestCaseJ4 {
  
  private static final String RANK_1 = "rank_1";
  private static final String RANK_MV = "rank_mv";

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

  public void testBasic() {
    assertNotNull(h.getCore().getLatestSchema().getFieldOrNull(RANK_1));
    assertEquals(RankField.class, h.getCore().getLatestSchema().getField(RANK_1).getType().getClass());
  }
  
  public void testBadFormat() {
    ignoreException("Expecting format to be");
    assertFailedU(adoc(
        "id", "1",
        RANK_1, "pagerank"
        ));
    assertFailedU(adoc(
        "id", "1",
        RANK_1, "123"
        ));
    assertFailedU(adoc(
        "id", "1",
        RANK_1, "a:1:2"
        ));
    assertFailedU(adoc(
        "id", "1",
        RANK_1, "a:b"
        ));
    unIgnoreException("Expecting format to be");
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
        RANK_1, "pagerank:1"
        ));
    assertU(commit());
    //assert that the document made it in
    assertQ(req("q", "id:testBasicAdd"), "//*[@numFound='1']");
    h.getCore().withSearcher((searcher) -> {
      LeafReader reader = searcher.getIndexReader().getContext().leaves().get(0).reader();
      // assert that the field made it in
      assertNotNull(reader.getFieldInfos().fieldInfo(RANK_1));
      // assert that the feature made it in
      assertTrue(reader.terms(RANK_1).iterator().seekExact(new BytesRef("pagerank".getBytes(StandardCharsets.UTF_8))));
      return null;
    });
  }
  
  public void testMultiValueAdd() throws IOException {
    assertU(adoc(
        "id", "testMultiValueAdd",
        RANK_MV, "pagerank:1",
        RANK_MV, "foo:2"
        ));
    assertU(commit());
    //assert that the document made it in
    assertQ(req("q", "id:testMultiValueAdd"), "//*[@numFound='1']");
    h.getCore().withSearcher((searcher) -> {
      LeafReader reader = searcher.getIndexReader().getContext().leaves().get(0).reader();
      // assert that the field made it in
      assertNotNull(reader.getFieldInfos().fieldInfo(RANK_MV));
      // assert that the features made it in
      assertTrue(reader.terms(RANK_MV).iterator().seekExact(new BytesRef("pagerank".getBytes(StandardCharsets.UTF_8))));
      assertTrue(reader.terms(RANK_MV).iterator().seekExact(new BytesRef("foo".getBytes(StandardCharsets.UTF_8))));
      return null;
    });
  }
  
  public void testSortFails() throws IOException {
    assertU(adoc(
        "id", "testSortFails",
        RANK_1, "pagerank:1"
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
        RANK_1, "pagerank:1"
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
        RANK_1, "pagerank:1"
        ));
    assertU(commit());
    assertQ(req("q", RANK_1 + ":pagerank"), "//*[@numFound='1']");
  }
  
  public void testRankQParserQuery() throws IOException {
    assertU(adoc(
        "id", "1",
        "str_field", "foo",
        RANK_1, "pagerank:1",
        RANK_MV, "pagerank:2"
        ));
    assertU(adoc(
        "id", "2",
        "str_field", "foo",
        RANK_1, "pagerank:2",
        RANK_MV, "pagerank:1"
        ));
    assertU(commit());
    assertQ(req("q", "str_field:foo _query_:{!rank f='" + RANK_1 + "' function='log' scalingFactor='1'}pagerank"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
    
    assertQ(req("q", "str_field:foo _query_:{!rank f='" + RANK_MV + "' function='log' scalingFactor='1'}pagerank"),
        "//*[@numFound='2']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']");
  }

}
