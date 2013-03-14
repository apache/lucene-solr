package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FilteredQuery.FilterStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

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
    doc.add (newTextField("field", "one two three four five", Field.Store.YES));
    doc.add (newTextField("sorter", "b", Field.Store.YES));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two three four", Field.Store.YES));
    doc.add (newTextField("sorter", "d", Field.Store.YES));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two three y", Field.Store.YES));
    doc.add (newTextField("sorter", "a", Field.Store.YES));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two x", Field.Store.YES));
    doc.add (newTextField("sorter", "c", Field.Store.YES));
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
    Query filteredquery = new FilteredQuery(query, filter, randomFilterStrategy(random(), useRandomAccess));
    ScoreDoc[] hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    hits = searcher.search (filteredquery, null, 1000, new Sort(new SortField("sorter", SortField.Type.STRING))).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "one")), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new MatchAllDocsQuery(), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "x")), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (3, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "y")), filter, randomFilterStrategy(random(), useRandomAccess));
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
    filteredquery = new FilteredQuery(tq, f, randomFilterStrategy(random(), useRandomAccess));
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

    Query filteredquery = new FilteredQuery(rq, filter, randomFilterStrategy(random(), useRandomAccess));
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
    Query query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.MUST);
    query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), randomFilterStrategy(random(), useRandomAccess));
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
    Query query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.SHOULD);
    query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), randomFilterStrategy(random(), useRandomAccess));
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
    Query query = new FilteredQuery(bq, new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
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
    Query query = new FilteredQuery(new FilteredQuery(
      new MatchAllDocsQuery(), new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "three")))), randomFilterStrategy(random(), useRandomAccess)),
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "four")))), randomFilterStrategy(random(), useRandomAccess));
    ScoreDoc[] hits = searcher.search(query, 10).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), query, searcher);    

    // one more:
    query = new FilteredQuery(query,
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "five")))), randomFilterStrategy(random(), useRandomAccess));
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
  
  private FilterStrategy randomFilterStrategy() {
    return randomFilterStrategy(random(), true);
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
      assertEquals(fq.getFilterStrategy(), ((FilteredQuery) rewritten).getFilterStrategy());
    } else {
      assertEquals(boost * innerBoost, rewritten.getBoost(), 1.E-5f);
    }
    
    // check that the original query was not modified
    assertEquals(boost, fq.getBoost(), 1.E-5f);
    assertEquals(innerBoost, fq.getQuery().getBoost(), 1.E-5f);
  }

  public void testRewrite() throws Exception {
    assertRewrite(new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o")), randomFilterStrategy()), FilteredQuery.class);
    assertRewrite(new FilteredQuery(new PrefixQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o")), randomFilterStrategy()), FilteredQuery.class);
    assertRewrite(new FilteredQuery(new MatchAllDocsQuery(), new PrefixFilter(new Term("field", "o")), randomFilterStrategy()), ConstantScoreQuery.class);
  }
  
  public void testGetFilterStrategy() {
    FilterStrategy randomFilterStrategy = randomFilterStrategy();
    FilteredQuery filteredQuery = new FilteredQuery(new TermQuery(new Term("field", "one")), new PrefixFilter(new Term("field", "o")), randomFilterStrategy);
    assertSame(randomFilterStrategy, filteredQuery.getFilterStrategy());
  }
  
  private static FilteredQuery.FilterStrategy randomFilterStrategy(Random random, final boolean useRandomAccess) {
    if (useRandomAccess) {
      return  new FilteredQuery.RandomAccessFilterStrategy() {
        @Override
        protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
          return useRandomAccess;
        }
      };
    }
    return _TestUtil.randomFilterStrategy(random);
  }
  
  /*
   * Test if the QueryFirst strategy calls the bits only if the document has
   * been matched by the query and not otherwise
   */
  public void testQueryFirstFilterStrategy() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int numDocs = atLeast(50);
    int totalDocsWithZero = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int num = random().nextInt(5);
      if (num == 0) {
        totalDocsWithZero++;
      }
      doc.add(newTextField("field", "" + num, Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(reader);
    Query query = new FilteredQuery(new TermQuery(new Term("field", "0")),
        new Filter() {
          @Override
          public DocIdSet getDocIdSet(AtomicReaderContext context,
              Bits acceptDocs) throws IOException {
            final boolean nullBitset = random().nextInt(10) == 5;
            final AtomicReader reader = context.reader();
            DocsEnum termDocsEnum = reader.termDocsEnum(new Term("field", "0"));
            if (termDocsEnum == null) {
              return null; // no docs -- return null
            }
            final BitSet bitSet = new BitSet(reader.maxDoc());
            int d;
            while ((d = termDocsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
              bitSet.set(d, true);
            }
            return new DocIdSet() {
              
              @Override
              public Bits bits() throws IOException {
                if (nullBitset) {
                  return null;
                }
                return new Bits() {
                  
                  @Override
                  public boolean get(int index) {
                    assertTrue("filter was called for a non-matching doc",
                        bitSet.get(index));
                    return bitSet.get(index);
                  }
                  
                  @Override
                  public int length() {
                    return bitSet.length();
                  }
                  
                };
              }
              
              @Override
              public DocIdSetIterator iterator() throws IOException {
                assertTrue(
                    "iterator should not be called if bitset is present",
                    nullBitset);
                return reader.termDocsEnum(new Term("field", "0"));
              }
              
            };
          }
        }, FilteredQuery.QUERY_FIRST_FILTER_STRATEGY);
    
    TopDocs search = searcher.search(query, 10);
    assertEquals(totalDocsWithZero, search.totalHits);
    IOUtils.close(reader, writer, directory);
    
  }
  
  /*
   * Test if the leapfrog strategy works correctly in terms
   * of advancing / next the right thing first
   */
  public void testLeapFrogStrategy() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter (random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    int numDocs = atLeast(50);
    int totalDocsWithZero = 0;
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      int num = random().nextInt(10);
      if (num == 0) {
        totalDocsWithZero++;
      }
      doc.add (newTextField("field", ""+num, Field.Store.YES));
      writer.addDocument (doc);  
    }
    IndexReader reader = writer.getReader();
    writer.close ();
    final boolean queryFirst = random().nextBoolean();
    IndexSearcher searcher = newSearcher(reader);
    Query query = new FilteredQuery(new TermQuery(new Term("field", "0")), new Filter() {
      @Override
      public DocIdSet getDocIdSet(final AtomicReaderContext context, Bits acceptDocs)
          throws IOException {
        return new DocIdSet() {
          
          @Override
          public Bits bits() throws IOException {
             return null;
          }
          @Override
          public DocIdSetIterator iterator() throws IOException {
            final DocsEnum termDocsEnum = context.reader().termDocsEnum(new Term("field", "0"));
            if (termDocsEnum == null) {
              return null;
            }
            return new DocIdSetIterator() {
              boolean nextCalled;
              boolean advanceCalled;
              @Override
              public int nextDoc() throws IOException {
                assertTrue("queryFirst: "+ queryFirst + " advanced: " + advanceCalled + " next: "+ nextCalled, nextCalled || advanceCalled ^ !queryFirst);  
                nextCalled = true;
                return termDocsEnum.nextDoc();
              }
              
              @Override
              public int docID() {
                return termDocsEnum.docID();
              }
              
              @Override
              public int advance(int target) throws IOException {
                assertTrue("queryFirst: "+ queryFirst + " advanced: " + advanceCalled + " next: "+ nextCalled, advanceCalled || nextCalled ^ queryFirst);  
                advanceCalled = true;
                return termDocsEnum.advance(target);
              }
              
              @Override
              public long cost() {
                return termDocsEnum.cost();
              } 
            };
          }
          
        };
      }
        }, queryFirst ? FilteredQuery.LEAP_FROG_QUERY_FIRST_STRATEGY : random()
            .nextBoolean() ? FilteredQuery.RANDOM_ACCESS_FILTER_STRATEGY
            : FilteredQuery.LEAP_FROG_FILTER_FIRST_STRATEGY);  // if filterFirst, we can use random here since bits are null
    
    TopDocs search = searcher.search(query, 10);
    assertEquals(totalDocsWithZero, search.totalHits);
    IOUtils.close(reader, writer, directory);
     
  }
}




