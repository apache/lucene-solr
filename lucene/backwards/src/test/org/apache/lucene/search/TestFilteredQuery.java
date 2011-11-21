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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.LuceneTestCase;

import java.util.BitSet;

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
    RandomIndexWriter writer = new RandomIndexWriter (random, directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setMergePolicy(newLogMergePolicy()));

    Document doc = new Document();
    doc.add (newField("field", "one two three four five", Field.Store.YES, Field.Index.ANALYZED));
    doc.add (newField("sorter", "b", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two three four", Field.Store.YES, Field.Index.ANALYZED));
    doc.add (newField("sorter", "d", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two three y", Field.Store.YES, Field.Index.ANALYZED));
    doc.add (newField("sorter", "a", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (newField("field", "one two x", Field.Store.YES, Field.Index.ANALYZED));
    doc.add (newField("sorter", "c", Field.Store.YES, Field.Index.ANALYZED));
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
      public DocIdSet getDocIdSet (IndexReader reader) {
        BitSet bitset = new BitSet(5);
        bitset.set (1);
        bitset.set (3);
        return new DocIdBitSet(bitset);
      }
    };
  }

  @Override
  public void tearDown() throws Exception {
    searcher.close();
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testFilteredQuery() throws Exception {
    Query filteredquery = new FilteredQuery(query, filter);
    ScoreDoc[] hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);
    QueryUtils.check(random, filteredquery,searcher);

    hits = searcher.search (filteredquery, null, 1000, new Sort(new SortField("sorter", SortField.STRING))).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (1, hits[0].doc);

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "one")), filter);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (2, hits.length);
    QueryUtils.check(random, filteredquery,searcher);

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "x")), filter);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (1, hits.length);
    assertEquals (3, hits[0].doc);
    QueryUtils.check(random, filteredquery,searcher);

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "y")), filter);
    hits = searcher.search (filteredquery, null, 1000).scoreDocs;
    assertEquals (0, hits.length);
    QueryUtils.check(random, filteredquery,searcher);
    
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
    filteredquery = new FilteredQuery(tq, f);
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
      public DocIdSet getDocIdSet (IndexReader reader) {
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
      assertEquals(hits1[i].score, hits2[i].score, 0.0000001f);
    }
  }

  /**
   * This tests FilteredQuery's rewrite correctness
   */
  public void testRangeQuery() throws Exception {
    TermRangeQuery rq = new TermRangeQuery(
        "sorter", "b", "d", true, true);

    Query filteredquery = new FilteredQuery(rq, filter);
    ScoreDoc[] hits = searcher.search(filteredquery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random, filteredquery,searcher);
  }

  public void testBooleanMUST() throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(0));
    bq.add(query, BooleanClause.Occur.MUST);
    query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(1));
    bq.add(query, BooleanClause.Occur.MUST);
    ScoreDoc[] hits = searcher.search(bq, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    QueryUtils.check(random, query,searcher);    
  }

  public void testBooleanSHOULD() throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(0));
    bq.add(query, BooleanClause.Occur.SHOULD);
    query = new FilteredQuery(new MatchAllDocsQuery(), new SingleDocTestFilter(1));
    bq.add(query, BooleanClause.Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(bq, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random, query,searcher);    
  }

  // Make sure BooleanQuery, which does out-of-order
  // scoring, inside FilteredQuery, works
  public void testBoolean2() throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQuery(bq, new SingleDocTestFilter(0));
    bq.add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random, query,searcher);    
  }
  
  public void testChainedFilters() throws Exception {
    Query query = new FilteredQuery(new FilteredQuery(
      new MatchAllDocsQuery(), new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "three"))))),
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "four")))));
    ScoreDoc[] hits = searcher.search(query, 10).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random, query, searcher);    

    // one more:
    query = new FilteredQuery(query,
      new CachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("field", "five")))));
    hits = searcher.search(query, 10).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random, query, searcher);    
  }
}




