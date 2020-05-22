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

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;

public class SolrIndexSearcherTest extends SolrTestCaseJ4 {

  private final static int NUM_DOCS = 20;

  @BeforeClass
  public static void setUpClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    for (int i = 0 ; i < NUM_DOCS ; i ++) {
      assertU(adoc("id", String.valueOf(i),
          "field1_s", "foo",
          "field2_s", String.valueOf(i % 2),
          "field3_i_dvo", String.valueOf(i),
          "field4_t", numbersTo(i)));
      assertU(commit()); //commit inside the loop to get multiple segments
    }
  }
  
  private static String numbersTo(int i) {
    StringBuilder numbers = new StringBuilder();
    for (int j = 0; j <= i ; j++) {
      numbers.append(String.valueOf(j) + " ");
    }
    return numbers.toString();
  }

  @Before
  public void setUp() throws Exception {
    assertU(adoc("id", "1",
        "field1_s", "foo",
        "field2_s", "1",
        "field3_i_dvo", "1",
        "field4_t", numbersTo(1)));
    assertU(commit());
    super.setUp();
  }
  
  public void testMinExactCountLongValue() {
    assertQ("test query on empty index",
        req("q", "field1_s:foo", 
            "minExactCount", Long.toString(10L * Integer.MAX_VALUE),
            "rows", "2")
        ,"//*[@numFoundExact='true']"
        ,"//*[@numFound='" + NUM_DOCS + "']"
        );
  }
  
  public void testMinExactCount() {
    assertQ("minExactCount is lower than numFound,should produce approximated results",
            req("q", "field1_s:foo", 
                "minExactCount", "2",
                "rows", "2")
            ,"//*[@numFoundExact='false']"
            ,"//*[@numFound<='" + NUM_DOCS + "']"
            );
    assertQ("minExactCount is higher than numFound,should produce exact results",
        req("q", "field1_s:foo", 
            "minExactCount", "200",
            "rows", "2")
        ,"//*[@numFoundExact='true']"
        ,"//*[@numFound='" + NUM_DOCS + "']"
        );
  }
  
  private void assertMatchesEqual(int expectedCount, SolrIndexSearcher searcher, QueryCommand cmd) throws IOException {
    QueryResult qr = new QueryResult();
    searcher.search(qr, cmd);
    assertEquals(expectedCount, qr.getDocList().matches());
    assertEquals(TotalHits.Relation.EQUAL_TO, qr.getDocList().hitCountRelation());
  }
  
  private QueryResult assertMatchesGreaterThan(int expectedCount, SolrIndexSearcher searcher, QueryCommand cmd) throws IOException {
    QueryResult qr = new QueryResult();
    searcher.search(qr, cmd);
    assertTrue("Expecting returned matches to be greater than " + expectedCount + " but got " + qr.getDocList().matches(),
        expectedCount >= qr.getDocList().matches());
    assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, qr.getDocList().hitCountRelation());
    return qr;
  }
  
  public void testLowMinExactCountGeneratesApproximation() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(NUM_DOCS / 2, 10, "field1_s", "foo");
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      return null;
    });
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field2_s", "1");
      assertMatchesGreaterThan(NUM_DOCS/2, searcher, cmd);
      return null;
    });
  }

  public void testHighMinExactCountGeneratesExactCount() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(NUM_DOCS, 10, "field1_s", "foo");
      assertMatchesEqual(NUM_DOCS, searcher, cmd);
      return null;
    });
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(NUM_DOCS, 10, "field2_s", "1");
      assertMatchesEqual(NUM_DOCS/2, searcher, cmd);
      return null;
    });
  }

  
  
  public void testLowMinExactCountWithQueryResultCache() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(NUM_DOCS / 2, 10, "field1_s", "foo");
      cmd.clearFlags(SolrIndexSearcher.NO_CHECK_QCACHE | SolrIndexSearcher.NO_SET_QCACHE);
      searcher.search(new QueryResult(), cmd);
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      return null;
    });
  }
  
  public void testHighMinExactCountWithQueryResultCache() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(NUM_DOCS, 2, "field1_s", "foo");
      cmd.clearFlags(SolrIndexSearcher.NO_CHECK_QCACHE | SolrIndexSearcher.NO_SET_QCACHE);
      searcher.search(new QueryResult(), cmd);
      assertMatchesEqual(NUM_DOCS, searcher, cmd);
      return null;
    });
  }
  
  public void testMinExactCountMoreRows() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(2, NUM_DOCS, "field1_s", "foo");
      assertMatchesEqual(NUM_DOCS, searcher, cmd);
      return null;
    });
  }
  
  public void testMinExactCountMatchWithDocSet() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(2, 2, "field1_s", "foo");
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      
      cmd.setNeedDocSet(true);
      assertMatchesEqual(NUM_DOCS, searcher, cmd);
      return null;
    });
  }
  
  public void testMinExactCountWithMaxScoreRequested() throws IOException {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(2, 2, "field1_s", "foo");
      cmd.setFlags(SolrIndexSearcher.GET_SCORES);
      QueryResult qr = assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      assertNotEquals(Float.NaN, qr.getDocList().maxScore());
      return null;
    });
  }
  
  public void testMinExactWithFilters() throws Exception {
    
    h.getCore().withSearcher(searcher -> {
      //Sanity Check - No Filter
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      return null;
    });
    
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      Query filterQuery = new TermQuery(new Term("field4_t", "19"));
      cmd.setFilterList(filterQuery);
      assertNull(searcher.getProcessedFilter(null, cmd.getFilterList()).postFilter);
      assertMatchesEqual(1, searcher, cmd);
      return null;
    });
  }
  
  public void testMinExactWithPostFilters() throws Exception {
    h.getCore().withSearcher(searcher -> {
      //Sanity Check - No Filter
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      return null;
    });
    
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      MockPostFilter filterQuery = new MockPostFilter(1, 101);
      cmd.setFilterList(filterQuery);
      assertNotNull(searcher.getProcessedFilter(null, cmd.getFilterList()).postFilter);
      assertMatchesEqual(1, searcher, cmd);
      return null;
    });
    
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      MockPostFilter filterQuery = new MockPostFilter(100, 101);
      cmd.setFilterList(filterQuery);
      assertNotNull(searcher.getProcessedFilter(null, cmd.getFilterList()).postFilter);
      assertMatchesGreaterThan(NUM_DOCS, searcher, cmd);
      return null;
    });
    
  }
  
  public void testMinExactWithPostFilterThatChangesScoreMode() throws Exception {
    h.getCore().withSearcher(searcher -> {
      QueryCommand cmd = createBasicQueryCommand(1, 1, "field4_t", "0");
      // Use ScoreMode.COMPLETE for the PostFilter
      MockPostFilter filterQuery = new MockPostFilter(100, 101, ScoreMode.COMPLETE);
      cmd.setFilterList(filterQuery);
      assertNotNull(searcher.getProcessedFilter(null, cmd.getFilterList()).postFilter);
      assertMatchesEqual(NUM_DOCS, searcher, cmd);
      return null;
    });
  }

  private QueryCommand createBasicQueryCommand(int minExactCount, int length, String field, String q) {
    QueryCommand cmd = new QueryCommand();
    cmd.setMinExactCount(minExactCount);
    cmd.setLen(length);
    cmd.setFlags(SolrIndexSearcher.NO_CHECK_QCACHE | SolrIndexSearcher.NO_SET_QCACHE);
    cmd.setQuery(new TermQuery(new Term(field, q)));
    return cmd;
  }
  
  private final static class MockPostFilter  extends TermQuery implements PostFilter {
    
    private final int cost;
    private final int maxDocsToCollect;
    private final ScoreMode scoreMode;
    
    public MockPostFilter(int maxDocsToCollect, int cost, ScoreMode scoreMode) {
      super(new Term("foo", "bar"));//The term won't really be used. just the collector
      assert cost > 100;
      this.cost = cost;
      this.maxDocsToCollect = maxDocsToCollect;
      this.scoreMode = scoreMode;
    }

    public MockPostFilter(int maxDocsToCollect, int cost) {
      this(maxDocsToCollect, cost, null);
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      throw new UnsupportedOperationException("This class is only intended to be used as a PostFilter");
    }

    @Override
    public boolean getCache() {
      return false;
    }

    @Override
    public void setCache(boolean cache) {}

    @Override
    public int getCost() {
      return cost;
    }

    @Override
    public void setCost(int cost) {}

    @Override
    public boolean getCacheSep() {
      return false;
    }

    @Override
    public void setCacheSep(boolean cacheSep) {
    }

    @Override
    public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
      return new DelegatingCollector() {
        private int collected = 0;
        @Override
        public void collect(int doc) throws IOException {
          if (++collected <= maxDocsToCollect) {
            super.collect(doc);
          }
        }
        
        @Override
        public ScoreMode scoreMode() {
          if (scoreMode != null) {
            return scoreMode;
          }
          return super.scoreMode();
        }
      };
    }
    
  }
}
