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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.io.Reader;

/**
 * Tests {@link PhraseQuery}.
 *
 * @see TestPositionIncrement
 */
public class TestPhraseQuery extends LuceneTestCase {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 1e-6f;
  
  private IndexSearcher searcher;
  private PhraseQuery query;
  private RAMDirectory directory;

  public void setUp() throws Exception {
    super.setUp();
    directory = new RAMDirectory();
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new WhitespaceTokenizer(reader);
      }

      public int getPositionIncrementGap(String fieldName) {
        return 100;
      }
    };
    IndexWriter writer = new IndexWriter(directory, analyzer, true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    
    Document doc = new Document();
    doc.add(new Field("field", "one two three four five", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("repeated", "this is a repeated field - first part", Field.Store.YES, Field.Index.ANALYZED));
    Fieldable repeatedField = new Field("repeated", "second part of a repeated field", Field.Store.YES, Field.Index.ANALYZED);
    doc.add(repeatedField);
    doc.add(new Field("palindrome", "one two three two one", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new Field("nonexist", "phrase exist notexist exist found", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new Field("nonexist", "phrase exist notexist exist found", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(directory);
    query = new PhraseQuery();
  }

  public void tearDown() throws Exception {
    super.tearDown();
    searcher.close();
    directory.close();
  }

  public void testNotCloseEnough() throws Exception {
    query.setSlop(2);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "five"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    QueryUtils.check(query,searcher);
  }

  public void testBarelyCloseEnough() throws Exception {
    query.setSlop(3);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "five"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(query,searcher);
  }

  /**
   * Ensures slop of 0 works for exact matches, but not reversed
   */
  public void testExact() throws Exception {
    // slop is zero by default
    query.add(new Term("field", "four"));
    query.add(new Term("field", "five"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("exact match", 1, hits.length);
    QueryUtils.check(query,searcher);


    query = new PhraseQuery();
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("reverse not exact", 0, hits.length);
    QueryUtils.check(query,searcher);
  }

  public void testSlop1() throws Exception {
    // Ensures slop of 1 works with terms in order.
    query.setSlop(1);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "two"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("in order", 1, hits.length);
    QueryUtils.check(query,searcher);


    // Ensures slop of 1 does not work for phrases out of order;
    // must be at least 2.
    query = new PhraseQuery();
    query.setSlop(1);
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("reversed, slop not 2 or more", 0, hits.length);
    QueryUtils.check(query,searcher);
  }

  /**
   * As long as slop is at least 2, terms can be reversed
   */
  public void testOrderDoesntMatter() throws Exception {
    query.setSlop(2); // must be at least two for reverse order match
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    QueryUtils.check(query,searcher);


    query = new PhraseQuery();
    query.setSlop(2);
    query.add(new Term("field", "three"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("not sloppy enough", 0, hits.length);
    QueryUtils.check(query,searcher);

  }

  /**
   * slop is the total number of positional moves allowed
   * to line up a phrase
   */
  public void testMulipleTerms() throws Exception {
    query.setSlop(2);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "three"));
    query.add(new Term("field", "five"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("two total moves", 1, hits.length);
    QueryUtils.check(query,searcher);


    query = new PhraseQuery();
    query.setSlop(5); // it takes six moves to match this phrase
    query.add(new Term("field", "five"));
    query.add(new Term("field", "three"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("slop of 5 not close enough", 0, hits.length);
    QueryUtils.check(query,searcher);


    query.setSlop(6);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("slop of 6 just right", 1, hits.length);
    QueryUtils.check(query,searcher);

  }
  
  public void testPhraseQueryWithStopAnalyzer() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    StopAnalyzer stopAnalyzer = new StopAnalyzer();
    IndexWriter writer = new IndexWriter(directory, stopAnalyzer, true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("field", "the stop words are here", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);

    // valid exact phrase query
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field","stop"));
    query.add(new Term("field","words"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(query,searcher);


    // currently StopAnalyzer does not leave "holes", so this matches.
    query = new PhraseQuery();
    query.add(new Term("field", "words"));
    query.add(new Term("field", "here"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(query,searcher);


    searcher.close();
  }
  
  public void testPhraseQueryInConjunctionScorer() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);
    
    Document doc = new Document();
    doc.add(new Field("source", "marketing info", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new Field("contents", "foobar", Field.Store.YES, Field.Index.ANALYZED));
    doc.add(new Field("source", "marketing info", Field.Store.YES, Field.Index.ANALYZED)); 
    writer.addDocument(doc);
    
    writer.optimize();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(directory);
    
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("source", "marketing"));
    phraseQuery.add(new Term("source", "info"));
    ScoreDoc[] hits = searcher.search(phraseQuery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(phraseQuery,searcher);

    
    TermQuery termQuery = new TermQuery(new Term("contents","foobar"));
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(termQuery,searcher);

    
    searcher.close();
    
    writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, 
                             IndexWriter.MaxFieldLength.LIMITED);
    doc = new Document();
    doc.add(new Field("contents", "map entry woo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("contents", "woo map entry", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("contents", "map foobarword entry woo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.optimize();
    writer.close();
    
    searcher = new IndexSearcher(directory);
    
    termQuery = new TermQuery(new Term("contents","woo"));
    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("contents","map"));
    phraseQuery.add(new Term("contents","entry"));
    
    hits = searcher.search(termQuery, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    hits = searcher.search(phraseQuery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);

    
    booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    
    booleanQuery = new BooleanQuery();
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(booleanQuery,searcher);

    
    searcher.close();
    directory.close();
  }
  
  public void testSlopScoring() throws IOException {
    Directory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, 
                                         IndexWriter.MaxFieldLength.LIMITED);

    Document doc = new Document();
    doc.add(new Field("field", "foo firstname lastname foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    
    Document doc2 = new Document();
    doc2.add(new Field("field", "foo firstname xxx lastname foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc2);
    
    Document doc3 = new Document();
    doc3.add(new Field("field", "foo firstname xxx yyy lastname foo", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc3);
    
    writer.optimize();
    writer.close();

    Searcher searcher = new IndexSearcher(directory);
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "firstname"));
    query.add(new Term("field", "lastname"));
    query.setSlop(Integer.MAX_VALUE);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    // Make sure that those matches where the terms appear closer to
    // each other get a higher score:
    assertEquals(0.71, hits[0].score, 0.01);
    assertEquals(0, hits[0].doc);
    assertEquals(0.44, hits[1].score, 0.01);
    assertEquals(1, hits[1].doc);
    assertEquals(0.31, hits[2].score, 0.01);
    assertEquals(2, hits[2].doc);
    QueryUtils.check(query,searcher);        
  }
  
  public void testToString() throws Exception {
    StopAnalyzer analyzer = new StopAnalyzer();
    StopFilter.setEnablePositionIncrementsDefault(true);
    QueryParser qp = new QueryParser("field", analyzer);
    qp.setEnablePositionIncrements(true);
    PhraseQuery q = (PhraseQuery)qp.parse("\"this hi this is a test is\"");
    assertEquals("field:\"? hi ? ? ? test\"", q.toString());
    q.add(new Term("field", "hello"), 1);
    assertEquals("field:\"? hi|hello ? ? ? test\"", q.toString());
  }

  public void testWrappedPhrase() throws IOException {
    query.add(new Term("repeated", "first"));
    query.add(new Term("repeated", "part"));
    query.add(new Term("repeated", "second"));
    query.add(new Term("repeated", "part"));
    query.setSlop(100);

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("slop of 100 just right", 1, hits.length);
    QueryUtils.check(query,searcher);

    query.setSlop(99);

    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("slop of 99 not enough", 0, hits.length);
    QueryUtils.check(query,searcher);
  }

  // work on two docs like this: "phrase exist notexist exist found"
  public void testNonExistingPhrase() throws IOException {
    // phrase without repetitions that exists in 2 docs
    query.add(new Term("nonexist", "phrase"));
    query.add(new Term("nonexist", "notexist"));
    query.add(new Term("nonexist", "found"));
    query.setSlop(2); // would be found this way

    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("phrase without repetitions exists in 2 docs", 2, hits.length);
    QueryUtils.check(query,searcher);

    // phrase with repetitions that exists in 2 docs
    query = new PhraseQuery();
    query.add(new Term("nonexist", "phrase"));
    query.add(new Term("nonexist", "exist"));
    query.add(new Term("nonexist", "exist"));
    query.setSlop(1); // would be found 

    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("phrase with repetitions exists in two docs", 2, hits.length);
    QueryUtils.check(query,searcher);

    // phrase I with repetitions that does not exist in any doc
    query = new PhraseQuery();
    query.add(new Term("nonexist", "phrase"));
    query.add(new Term("nonexist", "notexist"));
    query.add(new Term("nonexist", "phrase"));
    query.setSlop(1000); // would not be found no matter how high the slop is

    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("nonexisting phrase with repetitions does not exist in any doc", 0, hits.length);
    QueryUtils.check(query,searcher);

    // phrase II with repetitions that does not exist in any doc
    query = new PhraseQuery();
    query.add(new Term("nonexist", "phrase"));
    query.add(new Term("nonexist", "exist"));
    query.add(new Term("nonexist", "exist"));
    query.add(new Term("nonexist", "exist"));
    query.setSlop(1000); // would not be found no matter how high the slop is

    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("nonexisting phrase with repetitions does not exist in any doc", 0, hits.length);
    QueryUtils.check(query,searcher);

  }

  /**
   * Working on a 2 fields like this:
   *    Field("field", "one two three four five")
   *    Field("palindrome", "one two three two one")
   * Phrase of size 2 occuriong twice, once in order and once in reverse, 
   * because doc is a palyndrome, is counted twice. 
   * Also, in this case order in query does not matter. 
   * Also, when an exact match is found, both sloppy scorer and exact scorer scores the same.   
   */
  public void testPalyndrome2() throws Exception {
    
    // search on non palyndrome, find phrase with no slop, using exact phrase scorer
    query.setSlop(0); // to use exact phrase scorer
    query.add(new Term("field", "two"));
    query.add(new Term("field", "three"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("phrase found with exact phrase scorer", 1, hits.length);
    float score0 = hits[0].score;
    //System.out.println("(exact) field: two three: "+score0);
    QueryUtils.check(query,searcher);

    // search on non palyndrome, find phrase with slop 2, though no slop required here.
    query.setSlop(2); // to use sloppy scorer 
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score1 = hits[0].score;
    //System.out.println("(sloppy) field: two three: "+score1);
    assertEquals("exact scorer and sloppy scorer score the same when slop does not matter",score0, score1, SCORE_COMP_THRESH);
    QueryUtils.check(query,searcher);

    // search ordered in palyndrome, find it twice
    query = new PhraseQuery();
    query.setSlop(2); // must be at least two for both ordered and reversed to match
    query.add(new Term("palindrome", "two"));
    query.add(new Term("palindrome", "three"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score2 = hits[0].score;
    //System.out.println("palindrome: two three: "+score2);
    QueryUtils.check(query,searcher);
    
    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("ordered scores higher in palindrome",score1+SCORE_COMP_THRESH<score2);

    // search reveresed in palyndrome, find it twice
    query = new PhraseQuery();
    query.setSlop(2); // must be at least two for both ordered and reversed to match
    query.add(new Term("palindrome", "three"));
    query.add(new Term("palindrome", "two"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score3 = hits[0].score;
    //System.out.println("palindrome: three two: "+score3);
    QueryUtils.check(query,searcher);

    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("reversed scores higher in palindrome",score1+SCORE_COMP_THRESH<score3);
    //assertEquals("ordered or reversed does not matter",score2, score3, SCORE_COMP_THRESH);
  }

  /**
   * Working on a 2 fields like this:
   *    Field("field", "one two three four five")
   *    Field("palindrome", "one two three two one")
   * Phrase of size 3 occuriong twice, once in order and once in reverse, 
   * because doc is a palyndrome, is counted twice. 
   * Also, in this case order in query does not matter. 
   * Also, when an exact match is found, both sloppy scorer and exact scorer scores the same.   
   */
  public void testPalyndrome3() throws Exception {
    
    // search on non palyndrome, find phrase with no slop, using exact phrase scorer
    query.setSlop(0); // to use exact phrase scorer
    query.add(new Term("field", "one"));
    query.add(new Term("field", "two"));
    query.add(new Term("field", "three"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("phrase found with exact phrase scorer", 1, hits.length);
    float score0 = hits[0].score;
    //System.out.println("(exact) field: one two three: "+score0);
    QueryUtils.check(query,searcher);

    // search on non palyndrome, find phrase with slop 3, though no slop required here.
    query.setSlop(4); // to use sloppy scorer 
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score1 = hits[0].score;
    //System.out.println("(sloppy) field: one two three: "+score1);
    assertEquals("exact scorer and sloppy scorer score the same when slop does not matter",score0, score1, SCORE_COMP_THRESH);
    QueryUtils.check(query,searcher);

    // search ordered in palyndrome, find it twice
    query = new PhraseQuery();
    query.setSlop(4); // must be at least four for both ordered and reversed to match
    query.add(new Term("palindrome", "one"));
    query.add(new Term("palindrome", "two"));
    query.add(new Term("palindrome", "three"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score2 = hits[0].score;
    //System.out.println("palindrome: one two three: "+score2);
    QueryUtils.check(query,searcher);
    
    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("ordered scores higher in palindrome",score1+SCORE_COMP_THRESH<score2);

    // search reveresed in palyndrome, find it twice
    query = new PhraseQuery();
    query.setSlop(4); // must be at least four for both ordered and reversed to match
    query.add(new Term("palindrome", "three"));
    query.add(new Term("palindrome", "two"));
    query.add(new Term("palindrome", "one"));
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score3 = hits[0].score;
    //System.out.println("palindrome: three two one: "+score3);
    QueryUtils.check(query,searcher);

    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("reversed scores higher in palindrome",score1+SCORE_COMP_THRESH<score3);
    //assertEquals("ordered or reversed does not matter",score2, score3, SCORE_COMP_THRESH);
  }

  // LUCENE-1280
  public void testEmptyPhraseQuery() throws Throwable {
    final PhraseQuery q1 = new PhraseQuery();
    final BooleanQuery q2 = new BooleanQuery();
    q2.add(new PhraseQuery(), BooleanClause.Occur.MUST);
    q2.toString();
  }
  
}
