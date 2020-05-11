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
package org.apache.solr.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

public class SolrIndexSearcherTest extends SolrTestCaseJ4 {
  
  private final static int NUM_DOCS = 20;

  @BeforeClass
  public static void setUpClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    for (int i = 0 ; i < NUM_DOCS ; i ++) {
      assertU(adoc("id", String.valueOf(i), "field1_s", "foo", "field2_s", String.valueOf(i % 2), "field3_s", String.valueOf(i)));
      assertU(commit());
    }
  }
  
  @Before
  public void setUp() throws Exception {
    assertU(adoc("id", "1", "field1_s", "foo", "field2_s", "1", "field3_s", "1"));
    assertU(commit());
    super.setUp();
  }
  
  public void testMinExactHitsLongValue() {
    assertQ("test query on empty index",
        req("q", "field1_s:foo", 
            "minExactHits", Long.toString(10L * Integer.MAX_VALUE),
            "rows", "2")
        ,"//*[@numFoundExact='true']"
        ,"//*[@numFound='" + NUM_DOCS + "']"
        );
  }
  
  public void testMinExactHits() {
    assertQ("minExactHits is lower than numFound,should produce approximated results",
            req("q", "field1_s:foo", 
                "minExactHits", "2",
                "rows", "2")
            ,"//*[@numFoundExact='false']"
            ,"//*[@numFound<='" + NUM_DOCS + "']"
            );
    assertQ("minExactHits is higher than numFound,should produce exact results",
        req("q", "field1_s:foo", 
            "minExactHits", "200",
            "rows", "2")
        ,"//*[@numFoundExact='true']"
        ,"//*[@numFound='" + NUM_DOCS + "']"
        );
  }
  
  private void assertMatchesEqual(int expectedCount, QueryResult qr) {
    assertEquals(expectedCount, qr.getDocList().matches());
    assertEquals(TotalHits.Relation.EQUAL_TO, qr.getDocList().hitCountRelation());
  }
  
  private void assertMatchesGraterThan(int expectedCount, QueryResult qr) {
    assertTrue("Expecting returned matches to be greater than " + expectedCount + " but got " + qr.getDocList().matches(),
        expectedCount >= qr.getDocList().matches());
    assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, qr.getDocList().hitCountRelation());
  }
  
  public void testLowMinExactHitsGeneratesApproximation() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(NUM_DOCS / 2);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesGraterThan(NUM_DOCS, qr);
      return null;
    });
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(1);
      cmd.setLen(1);
      // We need to disable cache, otherwise the search will be done for 20 docs (cache window size) which brings up the minExactHits
      cmd.setFlags(SolrIndexSearcher.NO_CHECK_QCACHE | SolrIndexSearcher.NO_SET_QCACHE);
      cmd.setQuery(new TermQuery(new Term("field2_s", "1")));
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesGraterThan(NUM_DOCS/2, qr);
      return null;
    });
  }
  
  public void testHighMinExactHitsGeneratesExactCount() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(NUM_DOCS);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesEqual(NUM_DOCS, qr);
      return null;
    });
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(NUM_DOCS);
      cmd.setQuery(new TermQuery(new Term("field2_s", "1")));
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesEqual(NUM_DOCS/2, qr);
      return null;
    });
  }
  
  public void testLowMinExactHitsWithQueryResultCache() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(NUM_DOCS / 2);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      searcher.search(new QueryResult(), cmd);
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesGraterThan(NUM_DOCS, qr);
      return null;
    });
  }
  
  public void testHighMinExactHitsWithQueryResultCache() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(NUM_DOCS);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      searcher.search(new QueryResult(), cmd);
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesEqual(NUM_DOCS, qr);
      return null;
    });
  }
  
  public void testMinExactHitsMoreRows() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(2);
      cmd.setLen(NUM_DOCS);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesEqual(NUM_DOCS, qr);
      return null;
    });
  }
  
  public void testMinExactHitsMatchWithDocSet() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setNeedDocSet(true);
      cmd.setMinExactHits(2);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      searcher.search(new QueryResult(), cmd);
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesEqual(NUM_DOCS, qr);
      return null;
    });
  }
  
  public void testMinExactHitsWithMaxScoreRequested() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = new QueryCommand();
      cmd.setMinExactHits(2);
      cmd.setFlags(SolrIndexSearcher.GET_SCORES);
      cmd.setQuery(new TermQuery(new Term("field1_s", "foo")));
      searcher.search(new QueryResult(), cmd);
      QueryResult qr = new QueryResult();
      searcher.search(qr, cmd);
      assertMatchesGraterThan(NUM_DOCS, qr);
      assertNotEquals(Float.NaN, qr.getDocList().maxScore());
      return null;
    });
  }
}
