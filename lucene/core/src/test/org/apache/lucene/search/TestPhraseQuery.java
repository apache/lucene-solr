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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Impact;
import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests {@link PhraseQuery}.
 *
 * @see TestPositionIncrement
 */
public class TestPhraseQuery extends LuceneTestCase {

  /** threshold for comparing floats */
  public static final float SCORE_COMP_THRESH = 1e-6f;
  
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private PhraseQuery query;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    Analyzer analyzer = new Analyzer() {
      @Override
      public TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new MockTokenizer(MockTokenizer.WHITESPACE, false));
      }

      @Override
      public int getPositionIncrementGap(String fieldName) {
        return 100;
      }
    };
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, analyzer);
    
    Document doc = new Document();
    doc.add(newTextField("field", "one two three four five", Field.Store.YES));
    doc.add(newTextField("repeated", "this is a repeated field - first part", Field.Store.YES));
    Field repeatedField = newTextField("repeated", "second part of a repeated field", Field.Store.YES);
    doc.add(repeatedField);
    doc.add(newTextField("palindrome", "one two three two one", Field.Store.YES));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(newTextField("nonexist", "phrase exist notexist exist found", Field.Store.YES));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(newTextField("nonexist", "phrase exist notexist exist found", Field.Store.YES));
    writer.addDocument(doc);

    reader = writer.getReader();
    writer.close();

    searcher = new IndexSearcher(reader);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    reader.close();
    reader = null;
    directory.close();
    directory = null;
  }

  public void testNotCloseEnough() throws Exception {
    query = new PhraseQuery(2, "field", "one", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    QueryUtils.check(random(), query,searcher);
  }

  public void testBarelyCloseEnough() throws Exception {
    query = new PhraseQuery(3, "field", "one", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query, searcher);
  }

  /**
   * Ensures slop of 0 works for exact matches, but not reversed
   */
  public void testExact() throws Exception {
    // slop is zero by default
    query = new PhraseQuery("field", "four", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("exact match", 1, hits.length);
    QueryUtils.check(random(), query,searcher);


    query = new PhraseQuery("field", "two", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("reverse not exact", 0, hits.length);
    QueryUtils.check(random(), query,searcher);
  }

  public void testSlop1() throws Exception {
    // Ensures slop of 1 works with terms in order.
    query = new PhraseQuery(1, "field", "one", "two");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("in order", 1, hits.length);
    QueryUtils.check(random(), query,searcher);


    // Ensures slop of 1 does not work for phrases out of order;
    // must be at least 2.
    query = new PhraseQuery(1, "field", "two", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("reversed, slop not 2 or more", 0, hits.length);
    QueryUtils.check(random(), query,searcher);
  }

  /**
   * As long as slop is at least 2, terms can be reversed
   */
  public void testOrderDoesntMatter() throws Exception {
    // must be at least two for reverse order match
    query = new PhraseQuery(2, "field", "two", "one");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    QueryUtils.check(random(), query,searcher);


    query = new PhraseQuery(2, "field", "three", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("not sloppy enough", 0, hits.length);
    QueryUtils.check(random(), query,searcher);

  }

  /**
   * slop is the total number of positional moves allowed
   * to line up a phrase
   */
  public void testMultipleTerms() throws Exception {
    query = new PhraseQuery(2, "field", "one", "three", "five");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("two total moves", 1, hits.length);
    QueryUtils.check(random(), query,searcher);

    // it takes six moves to match this phrase
    query = new PhraseQuery(5, "field", "five", "three", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("slop of 5 not close enough", 0, hits.length);
    QueryUtils.check(random(), query,searcher);


    query = new PhraseQuery(6, "field", "five", "three", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("slop of 6 just right", 1, hits.length);
    QueryUtils.check(random(), query,searcher);

  }
  
  public void testPhraseQueryWithStopAnalyzer() throws Exception {
    Directory directory = newDirectory();
    Analyzer stopAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET);
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(stopAnalyzer));
    Document doc = new Document();
    doc.add(newTextField("field", "the stop words are here", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);

    // valid exact phrase query
    PhraseQuery query = new PhraseQuery("field", "stop", "words");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), query,searcher);

    reader.close();
    directory.close();
  }
  
  public void testPhraseQueryInConjunctionScorer() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    
    Document doc = new Document();
    doc.add(newTextField("source", "marketing info", Field.Store.YES));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(newTextField("contents", "foobar", Field.Store.YES));
    doc.add(newTextField("source", "marketing info", Field.Store.YES)); 
    writer.addDocument(doc);
    
    IndexReader reader = writer.getReader();
    writer.close();
    
    IndexSearcher searcher = newSearcher(reader);
    
    PhraseQuery phraseQuery = new PhraseQuery("source", "marketing", "info");
    ScoreDoc[] hits = searcher.search(phraseQuery, 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), phraseQuery,searcher);

    
    TermQuery termQuery = new TermQuery(new Term("contents","foobar"));
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery.build(), 1000).scoreDocs;
    assertEquals(1, hits.length);
    QueryUtils.check(random(), termQuery,searcher);

    
    reader.close();
    
    writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
    doc = new Document();
    doc.add(newTextField("contents", "map entry woo", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("contents", "woo map entry", Field.Store.YES));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(newTextField("contents", "map foobarword entry woo", Field.Store.YES));
    writer.addDocument(doc);

    reader = writer.getReader();
    writer.close();
    
    searcher = newSearcher(reader);
    
    termQuery = new TermQuery(new Term("contents","woo"));
    phraseQuery = new PhraseQuery("contents", "map", "entry");
    
    hits = searcher.search(termQuery, 1000).scoreDocs;
    assertEquals(3, hits.length);
    hits = searcher.search(phraseQuery, 1000).scoreDocs;
    assertEquals(2, hits.length);

    
    booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery.build(), 1000).scoreDocs;
    assertEquals(2, hits.length);
    
    booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery.build(), 1000).scoreDocs;
    assertEquals(2, hits.length);
    QueryUtils.check(random(), booleanQuery.build(),searcher);

    
    reader.close();
    directory.close();
  }
  
  public void testSlopScoring() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(new MockAnalyzer(random()))
          .setMergePolicy(newLogMergePolicy())
          .setSimilarity(new BM25Similarity()));

    Document doc = new Document();
    doc.add(newTextField("field", "foo firstname lastname foo", Field.Store.YES));
    writer.addDocument(doc);
    
    Document doc2 = new Document();
    doc2.add(newTextField("field", "foo firstname zzz lastname foo", Field.Store.YES));
    writer.addDocument(doc2);
    
    Document doc3 = new Document();
    doc3.add(newTextField("field", "foo firstname zzz yyy lastname foo", Field.Store.YES));
    writer.addDocument(doc3);
    
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new ClassicSimilarity());
    PhraseQuery query = new PhraseQuery(Integer.MAX_VALUE, "field", "firstname", "lastname");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    // Make sure that those matches where the terms appear closer to
    // each other get a higher score:
    assertEquals(1.0, hits[0].score, 0.01);
    assertEquals(0, hits[0].doc);
    assertEquals(0.63, hits[1].score, 0.01);
    assertEquals(1, hits[1].doc);
    assertEquals(0.47, hits[2].score, 0.01);
    assertEquals(2, hits[2].doc);
    QueryUtils.check(random(), query,searcher);
    reader.close();
    directory.close();
  }
  
  public void testToString() throws Exception {
    PhraseQuery q = new PhraseQuery("field", new String[0]);
    assertEquals("\"\"", q.toString());

    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "hi"), 1);
    q = builder.build();
    assertEquals("field:\"? hi\"", q.toString());

    
    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "hi"), 1);
    builder.add(new Term("field", "test"), 5);
    q = builder.build(); // Query "this hi this is a test is"

    assertEquals("field:\"? hi ? ? ? test\"", q.toString());

    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "hi"), 1);
    builder.add(new Term("field", "hello"), 1);
    builder.add(new Term("field", "test"), 5);
    q = builder.build();
    assertEquals("field:\"? hi|hello ? ? ? test\"", q.toString());

    builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "hi"), 1);
    builder.add(new Term("field", "hello"), 1);
    builder.add(new Term("field", "test"), 5);
    builder.setSlop(5);
    q = builder.build();
    assertEquals("field:\"? hi|hello ? ? ? test\"~5", q.toString());
  }

  public void testWrappedPhrase() throws IOException {
    query = new PhraseQuery(100, "repeated", "first", "part", "second", "part");

    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("slop of 100 just right", 1, hits.length);
    QueryUtils.check(random(), query,searcher);

    query = new PhraseQuery(99, "repeated", "first", "part", "second", "part");

    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("slop of 99 not enough", 0, hits.length);
    QueryUtils.check(random(), query,searcher);
  }

  // work on two docs like this: "phrase exist notexist exist found"
  public void testNonExistingPhrase() throws IOException {
    // phrase without repetitions that exists in 2 docs
    query = new PhraseQuery(2, "nonexist", "phrase", "notexist", "found");

    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("phrase without repetitions exists in 2 docs", 2, hits.length);
    QueryUtils.check(random(), query,searcher);

    // phrase with repetitions that exists in 2 docs
    query = new PhraseQuery(1, "nonexist", "phrase", "exist", "exist");

    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("phrase with repetitions exists in two docs", 2, hits.length);
    QueryUtils.check(random(), query,searcher);

    // phrase I with repetitions that does not exist in any doc
    query = new PhraseQuery(1000, "nonexist", "phrase", "notexist", "phrase");

    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("nonexisting phrase with repetitions does not exist in any doc", 0, hits.length);
    QueryUtils.check(random(), query,searcher);

    // phrase II with repetitions that does not exist in any doc
    query = new PhraseQuery(1000, "nonexist", "phrase", "exist", "exist", "exist");

    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("nonexisting phrase with repetitions does not exist in any doc", 0, hits.length);
    QueryUtils.check(random(), query,searcher);

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
    query = new PhraseQuery("field", "two", "three"); // to use exact phrase scorer
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("phrase found with exact phrase scorer", 1, hits.length);
    float score0 = hits[0].score;
    //System.out.println("(exact) field: two three: "+score0);
    QueryUtils.check(random(), query,searcher);

    // search on non palyndrome, find phrase with slop 2, though no slop required here.
    query = new PhraseQuery("field", "two", "three"); // to use sloppy scorer 
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score1 = hits[0].score;
    //System.out.println("(sloppy) field: two three: "+score1);
    assertEquals("exact scorer and sloppy scorer score the same when slop does not matter",score0, score1, SCORE_COMP_THRESH);
    QueryUtils.check(random(), query,searcher);

    // search ordered in palyndrome, find it twice
    query = new PhraseQuery(2, "palindrome", "two", "three"); // must be at least two for both ordered and reversed to match
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    //float score2 = hits[0].score;
    //System.out.println("palindrome: two three: "+score2);
    QueryUtils.check(random(), query,searcher);
    
    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("ordered scores higher in palindrome",score1+SCORE_COMP_THRESH<score2);

    // search reveresed in palyndrome, find it twice
    query = new PhraseQuery(2, "palindrome", "three", "two"); // must be at least two for both ordered and reversed to match
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    //float score3 = hits[0].score;
    //System.out.println("palindrome: three two: "+score3);
    QueryUtils.check(random(), query,searcher);

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
    // slop=0 to use exact phrase scorer
    query = new PhraseQuery(0, "field", "one", "two", "three");
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("phrase found with exact phrase scorer", 1, hits.length);
    float score0 = hits[0].score;
    //System.out.println("(exact) field: one two three: "+score0);
    QueryUtils.check(random(), query,searcher);

    // just make sure no exc:
    searcher.explain(query, 0);

    // search on non palyndrome, find phrase with slop 3, though no slop required here.
    // slop=4 to use sloppy scorer
    query = new PhraseQuery(4, "field", "one", "two", "three");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    float score1 = hits[0].score;
    //System.out.println("(sloppy) field: one two three: "+score1);
    assertEquals("exact scorer and sloppy scorer score the same when slop does not matter",score0, score1, SCORE_COMP_THRESH);
    QueryUtils.check(random(), query,searcher);

    // search ordered in palyndrome, find it twice
    // slop must be at least four for both ordered and reversed to match
    query = new PhraseQuery(4, "palindrome", "one", "two", "three");
    hits = searcher.search(query, 1000).scoreDocs;

    // just make sure no exc:
    searcher.explain(query, 0);

    assertEquals("just sloppy enough", 1, hits.length);
    //float score2 = hits[0].score;
    //System.out.println("palindrome: one two three: "+score2);
    QueryUtils.check(random(), query,searcher);
    
    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("ordered scores higher in palindrome",score1+SCORE_COMP_THRESH<score2);

    // search reveresed in palyndrome, find it twice
    // must be at least four for both ordered and reversed to match
    query = new PhraseQuery(4, "palindrome", "three", "two", "one");
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("just sloppy enough", 1, hits.length);
    //float score3 = hits[0].score;
    //System.out.println("palindrome: three two one: "+score3);
    QueryUtils.check(random(), query,searcher);

    //commented out for sloppy-phrase efficiency (issue 736) - see SloppyPhraseScorer.phraseFreq(). 
    //assertTrue("reversed scores higher in palindrome",score1+SCORE_COMP_THRESH<score3);
    //assertEquals("ordered or reversed does not matter",score2, score3, SCORE_COMP_THRESH);
  }

  // LUCENE-1280
  public void testEmptyPhraseQuery() throws Throwable {
    final BooleanQuery.Builder q2 = new BooleanQuery.Builder();
    q2.add(new PhraseQuery("field", new String[0]), BooleanClause.Occur.MUST);
    q2.build().toString();
  }
  
  /* test that a single term is rewritten to a term query */
  public void testRewrite() throws IOException {
    PhraseQuery pq = new PhraseQuery("foo", "bar");
    Query rewritten = pq.rewrite(searcher.getIndexReader());
    assertTrue(rewritten instanceof TermQuery);
  }

  /** Tests PhraseQuery with terms at the same position in the query. */
  public void testZeroPosIncr() throws IOException {
    Directory dir = newDirectory();
    final Token[] tokens = new Token[3];
    tokens[0] = new Token();
    tokens[0].append("a");
    tokens[0].setPositionIncrement(1);
    tokens[1] = new Token();
    tokens[1].append("aa");
    tokens[1].setPositionIncrement(0);
    tokens[2] = new Token();
    tokens[2].append("b");
    tokens[2].setPositionIncrement(1);

    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new TextField("field", new CannedTokenStream(tokens)));
    writer.addDocument(doc);
    IndexReader r = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(r);

    // Sanity check; simple "a b" phrase:
    PhraseQuery.Builder pqBuilder = new PhraseQuery.Builder();
    pqBuilder.add(new Term("field", "a"), 0);
    pqBuilder.add(new Term("field", "b"), 1);
    assertEquals(1, searcher.count(pqBuilder.build()));

    // Now with "a|aa b"
    pqBuilder = new PhraseQuery.Builder();
    pqBuilder.add(new Term("field", "a"), 0);
    pqBuilder.add(new Term("field", "aa"), 0);
    pqBuilder.add(new Term("field", "b"), 1);
    assertEquals(1, searcher.count(pqBuilder.build()));

    // Now with "a|z b" which should not match; this isn't a MultiPhraseQuery
    pqBuilder = new PhraseQuery.Builder();
    pqBuilder.add(new Term("field", "a"), 0);
    pqBuilder.add(new Term("field", "z"), 0);
    pqBuilder.add(new Term("field", "b"), 1);
    assertEquals(0, searcher.count(pqBuilder.build()));

    r.close();
    dir.close();
  }

  public void testRandomPhrases() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random());

    RandomIndexWriter w  = new RandomIndexWriter(random(), dir, newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));
    List<List<String>> docs = new ArrayList<>();
    Document d = new Document();
    Field f = newTextField("f", "", Field.Store.NO);
    d.add(f);

    Random r = random();

    int NUM_DOCS = atLeast(10);
    for (int i = 0; i < NUM_DOCS; i++) {
      // at night, must be > 4096 so it spans multiple chunks
      int termCount = TEST_NIGHTLY ? atLeast(4097) : atLeast(200);

      List<String> doc = new ArrayList<>();

      StringBuilder sb = new StringBuilder();
      while(doc.size() < termCount) {
        if (r.nextInt(5) == 1 || docs.size() == 0) {
          // make new non-empty-string term
          String term;
          while(true) {
            term = TestUtil.randomUnicodeString(r);
            if (term.length() > 0) {
              break;
            }
          }
          try (TokenStream ts = analyzer.tokenStream("ignore", term)) {
            CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while(ts.incrementToken()) {
              String text = termAttr.toString();
              doc.add(text);
              sb.append(text).append(' ');
            }
            ts.end();
          }
        } else {
          // pick existing sub-phrase
          List<String> lastDoc = docs.get(r.nextInt(docs.size()));
          int len = TestUtil.nextInt(r, 1, 10);
          int start = r.nextInt(lastDoc.size()-len);
          for(int k=start;k<start+len;k++) {
            String t = lastDoc.get(k);
            doc.add(t);
            sb.append(t).append(' ');
          }
        }
      }
      docs.add(doc);
      f.setStringValue(sb.toString());
      w.addDocument(d);
    }

    IndexReader reader = w.getReader();
    IndexSearcher s = newSearcher(reader);
    w.close();

    // now search
    int num = atLeast(3);
    for(int i=0;i<num;i++) {
      int docID = r.nextInt(docs.size());
      List<String> doc = docs.get(docID);
      
      final int numTerm = TestUtil.nextInt(r, 2, 20);
      final int start = r.nextInt(doc.size()-numTerm);
      PhraseQuery.Builder builder = new PhraseQuery.Builder();
      StringBuilder sb = new StringBuilder();
      for(int t=start;t<start+numTerm;t++) {
        builder.add(new Term("f", doc.get(t)), t);
        sb.append(doc.get(t)).append(' ');
      }
      PhraseQuery pq = builder.build();

      TopDocs hits = s.search(pq, NUM_DOCS);
      boolean found = false;
      for(int j=0;j<hits.scoreDocs.length;j++) {
        if (hits.scoreDocs[j].doc == docID) {
          found = true;
          break;
        }
      }

      assertTrue("phrase '" + sb + "' not found; start=" + start + ", it=" + i + ", expected doc " + docID, found);
    }

    reader.close();
    dir.close();
  }
  
  public void testNegativeSlop() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {
      new PhraseQuery(-2, "field", "two", "one");
    });
  }

  public void testNegativePosition() throws Exception {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    expectThrows(IllegalArgumentException.class, () -> {
      builder.add(new Term("field", "two"), -42);
    });
  }

  public void testBackwardPositions() throws Exception {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term("field", "one"), 1);
    builder.add(new Term("field", "two"), 5);
    expectThrows(IllegalArgumentException.class, () -> {
      builder.add(new Term("field", "three"), 4);
    });
  }

  static String[] DOCS = new String[] {
      "a b c d e f g h",
      "b c b",
      "c d d d e f g b",
      "c b a b c",
      "a a b b c c d d",
      "a b c d a b c d a b c d"
  };

  public void testTopPhrases() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    String[] docs = ArrayUtil.copyOfSubArray(DOCS, 0, DOCS.length);
    Collections.shuffle(Arrays.asList(docs), random());
    for (String value : DOCS) {
      Document doc = new Document();
      doc.add(new TextField("f", value, Store.NO));
      w.addDocument(doc);
    }
    IndexReader r = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(r);
    for (Query query : Arrays.asList(
        new PhraseQuery("f", "b", "c"), // common phrase
        new PhraseQuery("f", "e", "f"), // always appear next to each other
        new PhraseQuery("f", "d", "d")  // repeated term
        )) {
      for (int topN = 1; topN <= 2; ++topN) {
        TopScoreDocCollector collector1 = TopScoreDocCollector.create(topN, null, Integer.MAX_VALUE);
        searcher.search(query, collector1);
        ScoreDoc[] hits1 = collector1.topDocs().scoreDocs;
        TopScoreDocCollector collector2 = TopScoreDocCollector.create(topN, null, 1);
        searcher.search(query, collector2);
        ScoreDoc[] hits2 = collector2.topDocs().scoreDocs;
        assertTrue("" + query, hits1.length > 0);
        CheckHits.checkEqual(query, hits1, hits2);
      }
    }
    r.close();
    dir.close();
  }

  public void testMergeImpacts() throws IOException {
    DummyImpactsEnum impacts1 = new DummyImpactsEnum(1000);
    DummyImpactsEnum impacts2 = new DummyImpactsEnum(2000);
    ImpactsSource mergedImpacts = ExactPhraseMatcher.mergeImpacts(new ImpactsEnum[] { impacts1, impacts2 });

    impacts1.reset(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        });

    // Merge with empty impacts
    impacts2.reset(
        new Impact[0][],
        new int[0]);
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    // Merge with dummy impacts
    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(Integer.MAX_VALUE, 1) }
        },
        new int[] {
            5000
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    // Merge with dummy impacts that we don't special case
    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(Integer.MAX_VALUE, 2) }
        },
        new int[] {
            5000
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(8, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    // First level of impacts2 doesn't cover the first level of impacts1
    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(6, 13) },
          new Impact[] { new Impact(3, 9), new Impact(5, 11), new Impact(7, 13) }
        },
        new int[] {
            90,
            1000
        }); 
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, 12), new Impact(7, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(7, 13) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    // Second level of impacts2 doesn't cover the first level of impacts1
    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(6, 11) },
          new Impact[] { new Impact(3, 9), new Impact(5, 11), new Impact(7, 13) }
        },
        new int[] {
            150,
            900
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(3, 11), new Impact(5, 12), new Impact(6, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13),  new Impact(12, 14) } // same as impacts1
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(4, 10), new Impact(9, 13) },
          new Impact[] { new Impact(1, 1), new Impact(4, 10), new Impact(5, 11), new Impact(8, 13), new Impact(12, 14), new Impact(13, 15) }
        },
        new int[] {
            113,
            950
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(4, 12), new Impact(8, 13) },
          new Impact[] { new Impact(3, 10), new Impact(5, 11), new Impact(8, 13), new Impact(12, 14) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());

    // Make sure negative norms are treated as unsigned
    impacts1.reset(
        new Impact[][] {
          new Impact[] { new Impact(3, 10), new Impact(5, -10), new Impact(8, -5) },
          new Impact[] { new Impact(3, 10), new Impact(5, -15), new Impact(8, -5),  new Impact(12, -3) }
        },
        new int[] {
            110,
            945
        });
    impacts2.reset(
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(12, -4) },
          new Impact[] { new Impact(3, 9), new Impact(12, -4), new Impact(20, -1) }
        },
        new int[] {
            150,
            960
        });
    assertEquals(
        new Impact[][] {
          new Impact[] { new Impact(2, 10), new Impact(8, -4) },
          new Impact[] { new Impact(3, 10), new Impact(8, -4), new Impact(12, -3) }
        },
        new int[] {
            110,
            945
        },
        mergedImpacts.getImpacts());
  }

  private static void assertEquals(Impact[][] impacts, int[] docIdUpTo, Impacts actual) {
    assertEquals(impacts.length, actual.numLevels());
    for (int i = 0; i < impacts.length; ++i) {
      assertEquals(docIdUpTo[i], actual.getDocIdUpTo(i));
      assertEquals(Arrays.asList(impacts[i]), actual.getImpacts(i));
    }
  }

  private static class DummyImpactsEnum extends ImpactsEnum {

    private final long cost;
    private Impact[][] impacts;
    private int[] docIdUpTo;

    DummyImpactsEnum(long cost) {
      this.cost = cost;
    }

    void reset(Impact[][] impacts, int[] docIdUpTo) {
      this.impacts = impacts;
      this.docIdUpTo = docIdUpTo;
    }

    @Override
    public void advanceShallow(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Impacts getImpacts() throws IOException {
      return new Impacts() {

        @Override
        public int numLevels() {
          return impacts.length;
        }

        @Override
        public int getDocIdUpTo(int level) {
          return docIdUpTo[level];
        }

        @Override
        public List<Impact> getImpacts(int level) {
          return Arrays.asList(impacts[level]);
        }

      };
    }

    @Override
    public int freq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef getPayload() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return cost;
    }

  }

  public void testRandomTopDocs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    int numDocs = TEST_NIGHTLY ? atLeast(128 * 8 * 8 * 3) : atLeast(100); // at night, make sure some terms have skip data
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int numTerms = random().nextInt(1 << random().nextInt(5));
      String text = IntStream.range(0, numTerms)
          .mapToObj(index -> random().nextBoolean() ? "a" : random().nextBoolean() ? "b" : "c")
          .collect(Collectors.joining(" "));
      doc.add(new TextField("foo", text, Store.NO));
      w.addDocument(doc);
    }
    IndexReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    for (String firstTerm : new String[] {"a", "b", "c"}) {
      for (String secondTerm : new String[] {"a", "b", "c"}) {
        Query query = new PhraseQuery("foo", new BytesRef(firstTerm), new BytesRef(secondTerm));

        TopScoreDocCollector collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
        TopScoreDocCollector collector2 = TopScoreDocCollector.create(10, null, 10); // TOP_SCORES

        searcher.search(query, collector1);
        searcher.search(query, collector2);
        CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);

        Query filteredQuery = new BooleanQuery.Builder()
            .add(query, Occur.MUST)
            .add(new TermQuery(new Term("foo", "b")), Occur.FILTER)
            .build();

        collector1 = TopScoreDocCollector.create(10, null, Integer.MAX_VALUE); // COMPLETE
        collector2 = TopScoreDocCollector.create(10, null, 10); // TOP_SCORES
        searcher.search(filteredQuery, collector1);
        searcher.search(filteredQuery, collector2);
        CheckHits.checkEqual(query, collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
      }
    }
    reader.close();
    dir.close();
  }

  public void testNullTerm() {
    NullPointerException e = expectThrows(NullPointerException.class, () -> new PhraseQuery.Builder().add(null));
    assertEquals("Cannot add a null term to PhraseQuery", e.getMessage());

    e = expectThrows(NullPointerException.class, () -> new PhraseQuery("field", (BytesRef)null));
    assertEquals("Cannot add a null term to PhraseQuery", e.getMessage());

    e = expectThrows(NullPointerException.class, () -> new PhraseQuery("field", (String)null));
    assertEquals("Cannot add a null term to PhraseQuery", e.getMessage());
  }
}
