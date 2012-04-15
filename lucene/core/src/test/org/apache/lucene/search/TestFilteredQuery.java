package org.apache.lucene.search;

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

import java.util.BitSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.LuceneTestCase;

/**
 * FilteredQuery JUnit tests.
 *
 * <p>Created: Apr 21, 2004 1:21:46 PM
 *
 *
 * @since   1.4
 */
public class TestFilteredQuery extends LuceneTestCase {

  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;
  private Query query;
  private Filter filter;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter (random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    Document doc = new Document();
    doc.add (newField("field", "one two three four five", TextField.TYPE_STORED));
    doc.add (newField("sorter", "b", TextField.TYPE_STORED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two three four", TextField.TYPE_STORED));
    doc.add (newField("sorter", "d", TextField.TYPE_STORED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two three y", TextField.TYPE_STORED));
    doc.add (newField("sorter", "a", TextField.TYPE_STORED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two x", TextField.TYPE_STORED));
    doc.add (newField("sorter", "c", TextField.TYPE_STORED));
    writer.addDocument (doc);

    // tests here require single segment (eg try seed
    // 8239472272678419952L), because SingleDocTestFilter(x)
    // blindly accepts that docID in any sub-segment
    writer.forceMerge(1);

    reader = writer.getReader();
    writer.close ();

    searcher = newSearcher(reader);

    query = new TermQuery (new Term ("field", "three"));
    filter = newStaticFilterB();
  }

  // must be static for serialization tests
  private static Filter newStaticFilterB() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet (AtomicReaderContext context, Bits acceptDocs) {
        if (acceptDocs == null) acceptDocs = new Bits.MatchAllBits(5);
        BitSet bitset = new BitSet(5);
        if (acceptDocs.get(1)) bitset.set(1);
        if (acceptDocs.get(3)) bitset.set(3);
        return new DocIdBitSet(bitset);
      }
    };
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testFilteredQuery() throws Exception {
    // force the filter to be executed as bits
    tFilteredQuery(true);
    // force the filter to be executed as iterator
    tFilteredQuery(false);
  }

  private void tFilteredQuery(final boolean useRandomAccess) throws Exception {
    Query filteredquery = new FilteredQueryRA(query, filter, useRandomAccess);
    ScoreDoc[] hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    hits = searcher.search (filteredquery, null, 1000, new Sort(new SortField("sorter", SortField.Type.STRING))).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);

    filteredquery = new FilteredQueryRA(new TermQuery (new Term ("field", "one")), filter, useRandomAccess);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQueryRA(new MatchAllDocsQuery(), filter, useRandomAccess);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQueryRA(new TermQuery (new Term ("field", "x")), filter, useRandomAccess);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (3, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQueryRA(new TermQuery (new Term ("field", "y")), filter, useRandomAccess);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (0, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);
    
    // test boost
    Filter f = newStaticFilterA();
    
    float boost = 2.5f;
    BooleanQuery bq1 = new BooleanQuery();
    TermQuery tq = new TermQuery (new Term ("field", "one"));
    tq.setBoost(boost);
    bq1.add(tq, Occur.MUST);
    bq1.add(new TermQuery (new Term ("field", "five")), Occur.MUST);
    
    BooleanQuery bq2 = new BooleanQuery();
    tq = new TermQuery (new Term ("field", "one"));
    filteredquery = new FilteredQueryRA(tq, f, useRandomAccess);
    filteredquery.setBoost(boost);
    bq2.add(filteredquery, Occur.MUST);
    bq2.add(new TermQuery (new Term ("field", "five")), Occur.MUST);
    assertScoreEquals(bq1, bq2);
    
    assertEquals(boost, filteredquery.getBoost(), 0);
    assertEquals(1.0f, tq.getBoost(), 0); // the boost value of the underlying query shouldn't have changed 
  }

  // must be static for serialization tests 
  private static Filter newStaticFilterA() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet (AtomicReaderContext context, Bits acceptDocs) {
        assertNull("acceptDocs should be null, as we have an index without deletions", acceptDocs);
        BitSet bitset = new BitSet(5);
        bitset.set(0, 5);
        return new DocIdBitSet(bitset);
      }
    };
  }
  
  /**
   * Tests whether the scores of the two queries are the same.
   */
  public void assertScoreEquals(Query q1, Query q2) throws Exception {
    ScoreDoc[] hits1 = searcher.search (q1, null, 1000).scoreDocs;
    ScoreDoc[] hits2 = searcher.search (q2, null, 1000).scoreDocs;
      
    assertEquals(hits1.length, hits2.length);
    
    for (int i = 0; i < hits1.length; i++) {
      assertEquals(hits1[i].score, hits2[i].score, 0.000001f);
    }
  }

  /**
   * This tests FilteredQuery's rewrite correctness
   */
  public void testRangeQuery() throws Exception {
    // force the filter to be executed as bits
    tRangeQuery(true);
    tRangeQuery(false);
  }

  private void tRangeQuery(final boolean useRandomAccess) throws Exception {
    TermRangeQuery rq = TermRangeQuery.newStringRange(
        "sorter", "b", "d", true, true);

    Query filteredquery = new FilteredQueryRA(rq, filter, useRandomAccess);
    ScoreDoc[] hits = searcher.search(filteredquery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);
  }

  public void testBooleanMUST() throws Exception {
    // force the filter to be executed as bits
    tBooleanMUST(true);
    // force the filter to be executed as iterator
    tBooleanMUST(false);
  }

  private void tBooleanMUST(final boolean useRandomAccess) throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQueryRA(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), useRandomAccess);
    bq.add(query, BooleanClause.Occur.MUST);
    query = new FilteredQueryRA(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), useRandomAccess);
    bq.add(query, BooleanClause.Occur.MUST);
    ScoreDoc[] hits = searcher.search(bq, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    QueryUtils.check(random(), query,searcher);    
  }

  public void testBooleanSHOULD() throws Exception {
    // force the filter to be executed as bits
    tBooleanSHOULD(true);
    // force the filter to be executed as iterator
    tBooleanSHOULD(false);
  }

  private void tBooleanSHOULD(final boolean useRandomAccess) throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQueryRA(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), useRandomAccess);
    bq.add(query, BooleanClause.Occur.SHOULD);
    query = new FilteredQueryRA(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), useRandomAccess);
    bq.add(query, BooleanClause.Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(bq, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), query,searcher);    
  }

  // Make sure BooleanQuery, which does out-of-order
  // scoring, inside FilteredQuery, works
  public void testBoolean2() throws Exception {
    // force the filter to be executed as bits
    tBoolean2(true);
    // force the filter to be executed as iterator
    tBoolean2(false);
  }

  private void tBoolean2(final boolean useRandomAccess) throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQueryRA(bq, new SingleDocTestFilter(0), useRandomAccess);
    bq.add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query, searcher);    
  }
  
  public void testChainedFilters() throws Exception {
    // force the filter to be executed as bits
    tChainedFilters(true);
    // force the filter to be executed as iterator
    tChainedFilters(false);
  }
  
  private void tChainedFilters(final boolean useRandomAccess) throws Exception {
    Query query = new TestFilteredQuery.FilteredQueryRA(new TestFilteredQuery.FilteredQueryRA(
      new MatchAllDocsQuery(), new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "three")))), useRandomAccess),
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "four")))), useRandomAccess);
    ScoreDoc[] hits = searcher.search(query, 10).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), query, searcher);    

    // one more:
    query = new TestFilteredQuery.FilteredQueryRA(query,
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "five")))), useRandomAccess);
    hits = searcher.search(query, 10).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query, searcher);    
  }
  
  public void testEqualsHashcode() throws Exception {
    // some tests before, if the used queries and filters work:
    assertEquals(new PrefixFilter(new Term("field", "o")), new PrefixFilter(new Term("field", "o")));
    assertFalse(new PrefixFilter(new Term("field", "a")).equals(new PrefixFilter(new Term("field", "o"))));
    QueryUtils.checkHashEquals(new TermQuery(new Term("field", "one")));
    QueryUtils.checkUnequal(
      new TermQuery(new Term("field", "one")), new TermQuery(new Term("field", "two"))
    );
    // now test FilteredQuery equals/hashcode:
    QueryUtils.checkHashEquals(new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o"))));
    QueryUtils.checkUnequal(
      new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o"))), 
      new FilteredQuery(new TermQuery(new Term("field", "two")), new PrefixFilter(new Term("field", "o")))
    );
    QueryUtils.checkUnequal(
      new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "a"))), 
      new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o")))
    );
  }
  
  public void testInvalidArguments() throws Exception {
    try {
      new FilteredQuery(null, null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    try {
      new FilteredQuery(new TermQuery(new Term("field", "one")), null);
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
    try {
      new FilteredQuery(null, new PrefixFilter(new Term("field", "o")));
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // pass
    }
  }
  
  private void assertRewrite(FilteredQuery fq, Class<? extends Query> clazz) throws Exception {
    // assign crazy boost to FQ
    final float boost = random().nextFloat() * 100.f;
    fq.setBoost(boost);
    
    // assign crazy boost to inner
    final float innerBoost = random().nextFloat() * 100.f;
    fq.getQuery().setBoost(innerBoost);
    
    // check the class and boosts of rewritten query
    final Query rewritten = searcher.rewrite(fq);
    assertTrue("is not instance of " + clazz.getName(), clazz.isInstance(rewritten));
    if (rewritten instanceof FilteredQuery) {
      assertEquals(boost, rewritten.getBoost(), 1.E-5f);
      assertEquals(innerBoost, ((FilteredQuery) rewritten).getQuery().getBoost(), 1.E-5f);
    } else {
      assertEquals(boost * innerBoost, rewritten.getBoost(), 1.E-5f);
    }
    
    // check that the original query was not modified
    assertEquals(boost, fq.getBoost(), 1.E-5f);
    assertEquals(innerBoost, fq.getQuery().getBoost(), 1.E-5f);
  }

  public void testRewrite() throws Exception {
    assertRewrite(new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o"))), FilteredQuery.class);
    assertRewrite(new FilteredQuery(new MatchAllDocsQuery(), new PrefixFilter(new Term("field", "o"))), ConstantScoreQuery.class);
  }

  public static final class FilteredQueryRA extends FilteredQuery {
    private final boolean useRandomAccess;
  
    public FilteredQueryRA(Query q, Filter f, boolean useRandomAccess) {
      super(q,f);
      this.useRandomAccess = useRandomAccess;
    }
    
    @Override
    protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
      return useRandomAccess;
    }
  }
}




