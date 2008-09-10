package org.apache.lucene.analysis.shingle;

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

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;

/**
 * A test class for ShingleAnalyzerWrapper as regards queries and scoring.
 */
public class ShingleAnalyzerWrapperTest extends TestCase {

  public IndexSearcher searcher;

  public static void main(String[] args) {
    junit.textui.TestRunner.run(ShingleAnalyzerWrapperTest.class);
  }

  /**
   * Set up a new index in RAM with three test phrases and the supplied Analyzer.
   *
   * @param analyzer the analyzer to use
   * @return an indexSearcher on the test index.
   * @throws Exception if an error occurs with index writer or searcher
   */
  public IndexSearcher setUpSearcher(Analyzer analyzer) throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, analyzer, true);

    Document doc;
    doc = new Document();
    doc.add(new Field("content", "please divide this sentence into shingles",
            Field.Store.YES,Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("content", "just another test sentence",
                      Field.Store.YES,Field.Index.ANALYZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("content", "a sentence which contains no test",
                      Field.Store.YES,Field.Index.ANALYZED));
    writer.addDocument(doc);

    writer.close();

    return new IndexSearcher(dir);
  }

  protected Hits queryParsingTest(Analyzer analyzer, String qs) throws Exception {
    searcher = setUpSearcher(analyzer);

    QueryParser qp = new QueryParser("content", analyzer);

    Query q = qp.parse(qs);

    return searcher.search(q);
  }

  protected void compareRanks(Hits hits, int[] ranks) throws Exception {
    assertEquals(ranks.length, hits.length());
    for (int i = 0; i < ranks.length; i++) {
      assertEquals(ranks[i], hits.id(i));
    }
  }

  /*
   * Will not work on an index without unigrams, since QueryParser automatically
   * tokenizes on whitespace.
   */
  public void testShingleAnalyzerWrapperQueryParsing() throws Exception {
    Hits hits = queryParsingTest(new ShingleAnalyzerWrapper
                                     (new WhitespaceAnalyzer(), 2),
                                 "test sentence");
    int[] ranks = new int[] { 1, 2, 0 };
    compareRanks(hits, ranks);
  }

  /*
   * This one fails with an exception.
   */
  public void testShingleAnalyzerWrapperPhraseQueryParsingFails() throws Exception {
    Hits hits = queryParsingTest(new ShingleAnalyzerWrapper
                                     (new WhitespaceAnalyzer(), 2),
                                 "\"this sentence\"");
    int[] ranks = new int[] { 0 };
    compareRanks(hits, ranks);
  }

  /*
   * This one works, actually.
   */
  public void testShingleAnalyzerWrapperPhraseQueryParsing() throws Exception {
    Hits hits = queryParsingTest(new ShingleAnalyzerWrapper
                                     (new WhitespaceAnalyzer(), 2),
                                 "\"test sentence\"");
    int[] ranks = new int[] { 1 };
    compareRanks(hits, ranks);
  }

  /*
   * Same as above, is tokenized without using the analyzer.
   */
  public void testShingleAnalyzerWrapperRequiredQueryParsing() throws Exception {
    Hits hits = queryParsingTest(new ShingleAnalyzerWrapper
                                     (new WhitespaceAnalyzer(), 2),
                                 "+test +sentence");
    int[] ranks = new int[] { 1, 2 };
    compareRanks(hits, ranks);
  }

  /*
   * This shows how to construct a phrase query containing shingles.
   */
  public void testShingleAnalyzerWrapperPhraseQuery() throws Exception {
    Analyzer analyzer = new ShingleAnalyzerWrapper(new WhitespaceAnalyzer(), 2);
    searcher = setUpSearcher(analyzer);

    PhraseQuery q = new PhraseQuery();

    TokenStream ts = analyzer.tokenStream("content",
                                          new StringReader("this sentence"));
    int j = -1;
    final Token reusableToken = new Token();
    for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
      j += nextToken.getPositionIncrement();
      String termText = nextToken.term();
      q.add(new Term("content", termText), j);
    }

    Hits hits = searcher.search(q);
    int[] ranks = new int[] { 0 };
    compareRanks(hits, ranks);
  }

  /*
   * How to construct a boolean query with shingles. A query like this will
   * implicitly score those documents higher that contain the words in the query
   * in the right order and adjacent to each other.
   */
  public void testShingleAnalyzerWrapperBooleanQuery() throws Exception {
    Analyzer analyzer = new ShingleAnalyzerWrapper(new WhitespaceAnalyzer(), 2);
    searcher = setUpSearcher(analyzer);

    BooleanQuery q = new BooleanQuery();

    TokenStream ts = analyzer.tokenStream("content",
                                          new StringReader("test sentence"));
    final Token reusableToken = new Token();
    for (Token nextToken = ts.next(reusableToken); nextToken != null; nextToken = ts.next(reusableToken)) {
      String termText =  nextToken.term();
      q.add(new TermQuery(new Term("content", termText)),
            BooleanClause.Occur.SHOULD);
    }

    Hits hits = searcher.search(q);
    int[] ranks = new int[] { 1, 2, 0 };
    compareRanks(hits, ranks);
  }
}
