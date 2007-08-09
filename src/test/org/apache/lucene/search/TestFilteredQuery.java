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

import junit.framework.TestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.RAMDirectory;
import java.util.BitSet;

/**
 * FilteredQuery JUnit tests.
 *
 * <p>Created: Apr 21, 2004 1:21:46 PM
 *
 *
 * @version $Id$
 * @since   1.4
 */
public class TestFilteredQuery
extends TestCase {

  private IndexSearcher searcher;
  private RAMDirectory directory;
  private Query query;
  private Filter filter;

  public void setUp()
  throws Exception {
    directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter (directory, new WhitespaceAnalyzer(), true);

    Document doc = new Document();
    doc.add (new Field("field", "one two three four five", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add (new Field("sorter", "b", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (new Field("field", "one two three four", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add (new Field("sorter", "d", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (new Field("field", "one two three y", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add (new Field("sorter", "a", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument (doc);

    doc = new Document();
    doc.add (new Field("field", "one two x", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add (new Field("sorter", "c", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument (doc);

    writer.optimize ();
    writer.close ();

    searcher = new IndexSearcher (directory);
    query = new TermQuery (new Term ("field", "three"));
    filter = new Filter() {
      public BitSet bits (IndexReader reader) {
        BitSet bitset = new BitSet(5);
        bitset.set (1);
        bitset.set (3);
        return bitset;
      }
    };
  }

  public void tearDown()
  throws Exception {
    searcher.close();
    directory.close();
  }

  public void testFilteredQuery()
  throws Exception {
    Query filteredquery = new FilteredQuery (query, filter);
    Hits hits = searcher.search (filteredquery);
    assertEquals (1, hits.length());
    assertEquals (1, hits.id(0));
    QueryUtils.check(filteredquery,searcher);

    hits = searcher.search (filteredquery, new Sort("sorter"));
    assertEquals (1, hits.length());
    assertEquals (1, hits.id(0));

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "one")), filter);
    hits = searcher.search (filteredquery);
    assertEquals (2, hits.length());
    QueryUtils.check(filteredquery,searcher);

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "x")), filter);
    hits = searcher.search (filteredquery);
    assertEquals (1, hits.length());
    assertEquals (3, hits.id(0));
    QueryUtils.check(filteredquery,searcher);

    filteredquery = new FilteredQuery (new TermQuery (new Term ("field", "y")), filter);
    hits = searcher.search (filteredquery);
    assertEquals (0, hits.length());
    QueryUtils.check(filteredquery,searcher);
    
    // test boost
    Filter f = new Filter() {
      public BitSet bits (IndexReader reader) {
        BitSet bitset = new BitSet(5);
        bitset.set(0, 5);
        return bitset;
      }
    };
    
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
  
  /**
   * Tests whether the scores of the two queries are the same.
   */
  public void assertScoreEquals(Query q1, Query q2) throws Exception {
    Hits hits1 = searcher.search (q1);
    Hits hits2 = searcher.search (q2);
      
    assertEquals(hits1.length(), hits2.length());
    
    for (int i = 0; i < hits1.length(); i++) {
      assertEquals(hits1.score(i), hits2.score(i), 0.0000001f);
    }
  }

  /**
   * This tests FilteredQuery's rewrite correctness
   */
  public void testRangeQuery() throws Exception {
    RangeQuery rq = new RangeQuery(
        new Term("sorter", "b"), new Term("sorter", "d"), true);

    Query filteredquery = new FilteredQuery(rq, filter);
    Hits hits = searcher.search(filteredquery);
    assertEquals(2, hits.length());
    QueryUtils.check(filteredquery,searcher);
  }

  public void testBoolean() throws Exception {
    BooleanQuery bq = new BooleanQuery();
    Query query = new FilteredQuery(new MatchAllDocsQuery(),
        new SingleDocTestFilter(0));
    bq.add(query, BooleanClause.Occur.MUST);
    query = new FilteredQuery(new MatchAllDocsQuery(),
        new SingleDocTestFilter(1));
    bq.add(query, BooleanClause.Occur.MUST);
    Hits hits = searcher.search(bq);
    assertEquals(0, hits.length());
    QueryUtils.check(query,searcher);    
  }
}


