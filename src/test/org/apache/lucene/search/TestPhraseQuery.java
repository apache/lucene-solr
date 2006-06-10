package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.io.Reader;

/**
 * Tests {@link PhraseQuery}.
 *
 * @see TestPositionIncrement
 * @author Erik Hatcher
 */
public class TestPhraseQuery extends TestCase {
  private IndexSearcher searcher;
  private PhraseQuery query;
  private RAMDirectory directory;

  public void setUp() throws Exception {
    directory = new RAMDirectory();
    Analyzer analyzer = new Analyzer() {
      public TokenStream tokenStream(String fieldName, Reader reader) {
        return new WhitespaceTokenizer(reader);
      }

      public int getPositionIncrementGap(String fieldName) {
        return 100;
      }
    };
    IndexWriter writer = new IndexWriter(directory, analyzer, true);
    
    Document doc = new Document();
    doc.add(new Field("field", "one two three four five", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add(new Field("repeated", "this is a repeated field - first part", Field.Store.YES, Field.Index.TOKENIZED));
    Fieldable repeatedField = new Field("repeated", "second part of a repeated field", Field.Store.YES, Field.Index.TOKENIZED);
    doc.add(repeatedField);
    writer.addDocument(doc);
    
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(directory);
    query = new PhraseQuery();
  }

  public void tearDown() throws Exception {
    searcher.close();
    directory.close();
  }

  public void testNotCloseEnough() throws Exception {
    query.setSlop(2);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "five"));
    Hits hits = searcher.search(query);
    assertEquals(0, hits.length());
  }

  public void testBarelyCloseEnough() throws Exception {
    query.setSlop(3);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "five"));
    Hits hits = searcher.search(query);
    assertEquals(1, hits.length());
  }

  /**
   * Ensures slop of 0 works for exact matches, but not reversed
   */
  public void testExact() throws Exception {
    // slop is zero by default
    query.add(new Term("field", "four"));
    query.add(new Term("field", "five"));
    Hits hits = searcher.search(query);
    assertEquals("exact match", 1, hits.length());

    query = new PhraseQuery();
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query);
    assertEquals("reverse not exact", 0, hits.length());
  }

  public void testSlop1() throws Exception {
    // Ensures slop of 1 works with terms in order.
    query.setSlop(1);
    query.add(new Term("field", "one"));
    query.add(new Term("field", "two"));
    Hits hits = searcher.search(query);
    assertEquals("in order", 1, hits.length());

    // Ensures slop of 1 does not work for phrases out of order;
    // must be at least 2.
    query = new PhraseQuery();
    query.setSlop(1);
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query);
    assertEquals("reversed, slop not 2 or more", 0, hits.length());
  }

  /**
   * As long as slop is at least 2, terms can be reversed
   */
  public void testOrderDoesntMatter() throws Exception {
    query.setSlop(2); // must be at least two for reverse order match
    query.add(new Term("field", "two"));
    query.add(new Term("field", "one"));
    Hits hits = searcher.search(query);
    assertEquals("just sloppy enough", 1, hits.length());

    query = new PhraseQuery();
    query.setSlop(2);
    query.add(new Term("field", "three"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query);
    assertEquals("not sloppy enough", 0, hits.length());
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
    Hits hits = searcher.search(query);
    assertEquals("two total moves", 1, hits.length());

    query = new PhraseQuery();
    query.setSlop(5); // it takes six moves to match this phrase
    query.add(new Term("field", "five"));
    query.add(new Term("field", "three"));
    query.add(new Term("field", "one"));
    hits = searcher.search(query);
    assertEquals("slop of 5 not close enough", 0, hits.length());

    query.setSlop(6);
    hits = searcher.search(query);
    assertEquals("slop of 6 just right", 1, hits.length());
  }
  
  public void testPhraseQueryWithStopAnalyzer() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    StopAnalyzer stopAnalyzer = new StopAnalyzer();
    IndexWriter writer = new IndexWriter(directory, stopAnalyzer, true);
    Document doc = new Document();
    doc.add(new Field("field", "the stop words are here", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);

    // valid exact phrase query
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field","stop"));
    query.add(new Term("field","words"));
    Hits hits = searcher.search(query);
    assertEquals(1, hits.length());

    // currently StopAnalyzer does not leave "holes", so this matches.
    query = new PhraseQuery();
    query.add(new Term("field", "words"));
    query.add(new Term("field", "here"));
    hits = searcher.search(query);
    assertEquals(1, hits.length());

    searcher.close();
  }
  
  public void testPhraseQueryInConjunctionScorer() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    
    Document doc = new Document();
    doc.add(new Field("source", "marketing info", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new Field("contents", "foobar", Field.Store.YES, Field.Index.TOKENIZED));
    doc.add(new Field("source", "marketing info", Field.Store.YES, Field.Index.TOKENIZED)); 
    writer.addDocument(doc);
    
    writer.optimize();
    writer.close();
    
    IndexSearcher searcher = new IndexSearcher(directory);
    
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("source", "marketing"));
    phraseQuery.add(new Term("source", "info"));
    Hits hits = searcher.search(phraseQuery);
    assertEquals(2, hits.length());
    
    TermQuery termQuery = new TermQuery(new Term("contents","foobar"));
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery);
    assertEquals(1, hits.length());
    
    searcher.close();
    
    writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    doc = new Document();
    doc.add(new Field("contents", "map entry woo", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("contents", "woo map entry", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("contents", "map foobarword entry woo", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);

    writer.optimize();
    writer.close();
    
    searcher = new IndexSearcher(directory);
    
    termQuery = new TermQuery(new Term("contents","woo"));
    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("contents","map"));
    phraseQuery.add(new Term("contents","entry"));
    
    hits = searcher.search(termQuery);
    assertEquals(3, hits.length());
    hits = searcher.search(phraseQuery);
    assertEquals(2, hits.length());
    
    booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery);
    assertEquals(2, hits.length());
    
    booleanQuery = new BooleanQuery();
    booleanQuery.add(phraseQuery, BooleanClause.Occur.MUST);
    booleanQuery.add(termQuery, BooleanClause.Occur.MUST);
    hits = searcher.search(booleanQuery);
    assertEquals(2, hits.length());
    
    searcher.close();
    directory.close();
  }
  
  public void testSlopScoring() throws IOException {
    Directory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);

    Document doc = new Document();
    doc.add(new Field("field", "foo firstname lastname foo", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    
    Document doc2 = new Document();
    doc2.add(new Field("field", "foo firstname xxx lastname foo", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc2);
    
    Document doc3 = new Document();
    doc3.add(new Field("field", "foo firstname xxx yyy lastname foo", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc3);
    
    writer.optimize();
    writer.close();

    Searcher searcher = new IndexSearcher(directory);
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "firstname"));
    query.add(new Term("field", "lastname"));
    query.setSlop(Integer.MAX_VALUE);
    Hits hits = searcher.search(query);
    assertEquals(3, hits.length());
    // Make sure that those matches where the terms appear closer to
    // each other get a higher score:
    assertEquals(0.71, hits.score(0), 0.01);
    assertEquals(0, hits.id(0));
    assertEquals(0.44, hits.score(1), 0.01);
    assertEquals(1, hits.id(1));
    assertEquals(0.31, hits.score(2), 0.01);
    assertEquals(2, hits.id(2));
  }

  public void testWrappedPhrase() throws IOException {
    query.add(new Term("repeated", "first"));
    query.add(new Term("repeated", "part"));
    query.add(new Term("repeated", "second"));
    query.add(new Term("repeated", "part"));
    query.setSlop(99);

    Hits hits = searcher.search(query);
    assertEquals(0, hits.length());
  }

}
