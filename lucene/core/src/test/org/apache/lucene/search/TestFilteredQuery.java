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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.FilteredQuery.FilterStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

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
    RandomIndexWriter writer = new RandomIndexWriter (random(), directory, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    Document doc = new Document();
    doc.add (newTextField("field", "one two three four five", Field.Store.YES));
    doc.add (newTextField("sorter", "b", Field.Store.YES));
    doc.add (new SortedDocValuesField("sorter", new BytesRef("b")));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two three four", Field.Store.YES));
    doc.add (newTextField("sorter", "d", Field.Store.YES));
    doc.add (new SortedDocValuesField("sorter", new BytesRef("d")));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two three y", Field.Store.YES));
    doc.add (newTextField("sorter", "a", Field.Store.YES));
    doc.add (new SortedDocValuesField("sorter", new BytesRef("a")));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newTextField("field", "one two x", Field.Store.YES));
    doc.add (newTextField("sorter", "c", Field.Store.YES));
    doc.add (new SortedDocValuesField("sorter", new BytesRef("c")));
    writer.addDocument (doc);

    // tests here require single segment (eg try seed
    // 8239472272678419952L), because SingleDocTestFilter(x)
    // blindly accepts that docID in any sub-segment
    writer.forceMerge(1);

    reader = writer.getReader();
    writer.close();

    searcher = newSearcher(reader);

    query = new TermQuery (new Term ("field", "three"));
    filter = newStaticFilterB();
  }

  // must be static for serialization tests
  private static Filter newStaticFilterB() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet (LeafReaderContext context, Bits acceptDocs) {
        if (acceptDocs == null) acceptDocs = new Bits.MatchAllBits(5);
        FixedBitSet bitset = new FixedBitSet(context.reader().maxDoc());
        if (acceptDocs.get(1)) bitset.set(1);
        if (acceptDocs.get(3)) bitset.set(3);
        return new BitDocIdSet(bitset);
      }
      @Override
      public String toString(String field) {
        return "staticFilterB";
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
    ScoreDoc[] hits = searcher.search (filteredquery, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    hits = searcher.search (filteredquery, 1000, new Sort(new SortField("sorter", SortField.Type.STRING))).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "one")), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new MatchAllDocsQuery(), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "x")), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (3, hits[0].doc);
    QueryUtils.check(random(), filteredquery,searcher);

    filteredquery = new FilteredQuery(new TermQuery (new Term ("field", "y")), filter, randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search (filteredquery, 1000).scoreDocs;
    assertEquals (0, hits.length);
    QueryUtils.check(random(), filteredquery,searcher);
    
    // test boost
    Filter f = newStaticFilterA();
    
    float boost = 2.5f;
    BooleanQuery.Builder bq1 = new BooleanQuery.Builder();
    TermQuery tq = new TermQuery (new Term ("field", "one"));
    tq.setBoost(boost);
    bq1.add(tq, Occur.MUST);
    bq1.add(new TermQuery (new Term ("field", "five")), Occur.MUST);
    
    BooleanQuery.Builder bq2 = new BooleanQuery.Builder();
    tq = new TermQuery (new Term ("field", "one"));
    filteredquery = new FilteredQuery(tq, f, randomFilterStrategy(random(), useRandomAccess));
    filteredquery.setBoost(boost);
    bq2.add(filteredquery, Occur.MUST);
    bq2.add(new TermQuery (new Term ("field", "five")), Occur.MUST);
    assertScoreEquals(bq1.build(), bq2.build());
    
    assertEquals(boost, filteredquery.getBoost(), 0);
    assertEquals(1.0f, tq.getBoost(), 0); // the boost value of the underlying query shouldn't have changed 
  }

  // must be static for serialization tests 
  private static Filter newStaticFilterA() {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet (LeafReaderContext context, Bits acceptDocs) {
        assertNull("acceptDocs should be null, as we have an index without deletions", acceptDocs);
        FixedBitSet bitset = new FixedBitSet(context.reader().maxDoc());
        bitset.set(0, Math.min(5, bitset.length()));
        return new BitDocIdSet(bitset);
      }
      @Override
      public String toString(String field) {
        return "staticFilterA";
      }
    };
  }
  
  /**
   * Tests whether the scores of the two queries are the same.
   */
  public void assertScoreEquals(Query q1, Query q2) throws Exception {
    ScoreDoc[] hits1 = searcher.search (q1, 1000).scoreDocs;
    ScoreDoc[] hits2 = searcher.search (q2, 1000).scoreDocs;
      
    assertEquals(hits1.length, hits2.length);
    
    for (int i = 0; i < hits1.length; i++) {
      assertEquals(hits1[i].doc, hits2[i].doc);
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
    ScoreDoc[] hits = searcher.search(filteredquery, 1000).scoreDocs;
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
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    Query query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.MUST);
    query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.MUST);
    ScoreDoc[] hits = searcher.search(bq.build(), 1000).scoreDocs;
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
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    Query query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.SHOULD);
    query = new FilteredQuery(new TermQuery(new Term("field", "one")), new SingleDocTestFilter(1), randomFilterStrategy(random(), useRandomAccess));
    bq.add(query, BooleanClause.Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(bq.build(), 1000).scoreDocs;
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
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD);
    Query query = new FilteredQuery(bq.build(), new SingleDocTestFilter(0), randomFilterStrategy(random(), useRandomAccess));
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
  
  // a filter for which other queries don't have special rewrite rules
  private static class FilterWrapper extends Filter {

    private final Filter in;
    
    FilterWrapper(Filter in) {
      this.in = in;
    }
    
    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      return in.getDocIdSet(context, acceptDocs);
    }

    @Override
    public String toString(String field) {
      return in.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return in.equals(((FilterWrapper) obj).in);
    }
  }
  
  private void tChainedFilters(final boolean useRandomAccess) throws Exception {
    Query query = new FilteredQuery(new FilteredQuery(
      new MatchAllDocsQuery(), new FilterWrapper(new QueryWrapperFilter(new TermQuery(new Term("field", "three")))), randomFilterStrategy(random(), useRandomAccess)),
      new FilterWrapper(new QueryWrapperFilter(new TermQuery(new Term("field", "four")))), randomFilterStrategy(random(), useRandomAccess));
    ScoreDoc[] hits = searcher.search(query, 10).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), query, searcher);    

    // one more:
    query = new FilteredQuery(query,
      new FilterWrapper(new QueryWrapperFilter(new TermQuery(new Term("field", "five")))), randomFilterStrategy(random(), useRandomAccess));
    hits = searcher.search(query, 10).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query, searcher);    
  }
  
  public void testEqualsHashcode() throws Exception {
    // some tests before, if the used queries and filters work:
    assertEquals(new PrefixQuery(new Term("field", "o")), new PrefixQuery(new Term("field", "o")));
    assertFalse(new PrefixQuery(new Term("field", "a")).equals(new PrefixQuery(new Term("field", "o"))));
    QueryUtils.checkHashEquals(new TermQuery(new Term("field", "one")));
    QueryUtils.checkUnequal(
      new TermQuery(new Term("field", "one")), new TermQuery(new Term("field", "two"))
    );
    // now test FilteredQuery equals/hashcode:
    QueryUtils.checkHashEquals(new FilteredQuery(new TermQuery(new Term("field", "one")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "o")))));
    QueryUtils.checkUnequal(
      new FilteredQuery(new TermQuery(new Term("field", "one")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "o")))), 
      new FilteredQuery(new TermQuery(new Term("field", "two")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "o"))))
    );
    QueryUtils.checkUnequal(
      new FilteredQuery(new TermQuery(new Term("field", "one")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "a")))), 
      new FilteredQuery(new TermQuery(new Term("field", "one")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "o"))))
    );
  }
  
  public void testInvalidArguments() throws Exception {
    try {
      new FilteredQuery(null, null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException npe) {
      // pass
    }
    try {
      new FilteredQuery(new TermQuery(new Term("field", "one")), null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException npe) {
      // pass
    }
    try {
      new FilteredQuery(null, new QueryWrapperFilter(new PrefixQuery(new Term("field", "o"))));
      fail("Should throw NullPointerException");
    } catch (NullPointerException npe) {
      // pass
    }
  }
  
  private FilterStrategy randomFilterStrategy() {
    return randomFilterStrategy(random(), true);
  }
  
  public void testGetFilterStrategy() {
    FilterStrategy randomFilterStrategy = randomFilterStrategy();
    FilteredQuery filteredQuery = new FilteredQuery(new TermQuery(new Term("field", "one")), new QueryWrapperFilter(new PrefixQuery(new Term("field", "o"))), randomFilterStrategy);
    assertSame(randomFilterStrategy, filteredQuery.getFilterStrategy());
  }
  
  private static FilteredQuery.FilterStrategy randomFilterStrategy(Random random, final boolean useRandomAccess) {
    if (useRandomAccess) {
      return new FilteredQuery.RandomAccessFilterStrategy() {
        @Override
        protected boolean useRandomAccess(Bits bits, long filterCost) {
          return true;
        }
      };
    }
    return TestUtil.randomFilterStrategy(random);
  }
  
  /*
   * Test if the QueryFirst strategy calls the bits only if the document has
   * been matched by the query and not otherwise
   */
  public void testQueryFirstFilterStrategy() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random())));
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
    searcher.setQueryCache(null); // needed otherwise the iterator may be used
    Query query = new FilteredQuery(new TermQuery(new Term("field", "0")),
        new Filter() {
          @Override
          public DocIdSet getDocIdSet(LeafReaderContext context,
              Bits acceptDocs) throws IOException {
            final boolean nullBitset = random().nextInt(10) == 5;
            final LeafReader reader = context.reader();
            PostingsEnum termPostingsEnum = reader.postings(new Term("field", "0"));
            if (termPostingsEnum == null) {
              return null; // no docs -- return null
            }
            final BitSet bitSet = new BitSet(reader.maxDoc());
            int d;
            while ((d = termPostingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
              bitSet.set(d, true);
            }
            return new DocIdSet() {

              @Override
              public long ramBytesUsed() {
                return 0L;
              }

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
                return reader.postings(new Term("field", "0"));
              }
              
            };
          }
          @Override
          public String toString(String field) {
            return "filterField0";
          }
        }, FilteredQuery.QUERY_FIRST_FILTER_STRATEGY);
    
    TopDocs search = searcher.search(query, 10);
    assertEquals(totalDocsWithZero, search.totalHits);  
    IOUtils.close(reader, directory);
  }

  public void testPreservesScores() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Store.NO));
    writer.addDocument(doc);
    writer.commit();
    final IndexReader reader = writer.getReader();
    writer.close();
    final IndexSearcher searcher = new IndexSearcher(reader);
    final Query query = new TermQuery(new Term("foo", "bar"));
    query.setBoost(random().nextFloat());
    FilteredQuery fq = new FilteredQuery(query, new Filter() {
      @Override
      public DocIdSet getDocIdSet(final LeafReaderContext context, Bits acceptDocs)
          throws IOException {
        return new DocIdSet() {
          
          @Override
          public long ramBytesUsed() {
            return 0;
          }
          
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return DocIdSetIterator.all(context.reader().maxDoc());
          }
        };
      }
      @Override
      public String toString(String field) {
        return "dummy";
      }
    });
    assertEquals(searcher.search(query, 1).scoreDocs[0].score, searcher.search(fq, 1).scoreDocs[0].score, 0f);
    fq.setBoost(random().nextFloat());
    // QueryWrapperFilter has special rewrite rules
    FilteredQuery fq2 = new FilteredQuery(query, new QueryWrapperFilter(new MatchAllDocsQuery()));
    fq2.setBoost(fq.getBoost());
    assertEquals(searcher.search(fq, 1).scoreDocs[0].score, searcher.search(fq2, 1).scoreDocs[0].score, 10e-5);
    reader.close();
    dir.close();
  }
}




