package org.apache.lucene.search.highlight;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.highlight.SynonymTokenizer.TestHighlightRunner;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * JUnit Test for Highlighter class.
 *
 */
public class HighlighterTest extends BaseTokenStreamTestCase implements Formatter {

  private IndexReader reader;
  static final String FIELD_NAME = "contents";
  private static final String NUMERIC_FIELD_NAME = "nfield";
  private Query query;
  Directory ramDir;
  public IndexSearcher searcher = null;
  int numHighlights = 0;
  Analyzer analyzer;
  TopDocs hits;

  String[] texts = {
      "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
      "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
      "JFK has been shot", "John Kennedy has been shot",
      "This text has a typo in referring to Keneddy",
      "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b", "lets is a the lets is a the lets is a the lets" };
  
  public void testQueryScorerHits() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);

    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "very"));
    phraseQuery.add(new Term(FIELD_NAME, "long"));

    query = phraseQuery;
    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);


    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document doc = searcher.doc(hits.scoreDocs[i].doc);
      String storedField = doc.get(FIELD_NAME);

      TokenStream stream = TokenSources.getAnyTokenStream(searcher
          .getIndexReader(), hits.scoreDocs[i].doc, FIELD_NAME, doc, analyzer);

      Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);

      highlighter.setTextFragmenter(fragmenter);

      String fragment = highlighter.getBestFragment(stream, storedField);

      if (VERBOSE) System.out.println(fragment);
    }
  }
  
  public void testHighlightingCommonTermsQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 3);
    query.add(new Term(FIELD_NAME, "this"));
    query.add(new Term(FIELD_NAME, "long"));
    query.add(new Term(FIELD_NAME, "very"));

    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    Document doc = searcher.doc(hits.scoreDocs[0].doc);
    String storedField = doc.get(FIELD_NAME);

    TokenStream stream = TokenSources.getAnyTokenStream(searcher
        .getIndexReader(), hits.scoreDocs[0].doc, FIELD_NAME, doc, analyzer);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("Hello <B>this</B> is a piece of text that is <B>very</B> <B>long</B> and contains too much preamble and the meat is really here which says kennedy has been shot", fragment);
    
    doc = searcher.doc(hits.scoreDocs[1].doc);
    storedField = doc.get(FIELD_NAME);

    stream = TokenSources.getAnyTokenStream(searcher
        .getIndexReader(), hits.scoreDocs[1].doc, FIELD_NAME, doc, analyzer);
    highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer));
    fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("<B>This</B> piece of text refers to Kennedy at the beginning then has a longer piece of text that is <B>very</B>", fragment);
  }
  
  public void testHighlightUnknowQueryAfterRewrite() throws IOException, InvalidTokenOffsetsException {
    Query query = new Query() {
      
      @Override
      public Query rewrite(IndexReader reader) throws IOException {
        CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 3);
        query.add(new Term(FIELD_NAME, "this"));
        query.add(new Term(FIELD_NAME, "long"));
        query.add(new Term(FIELD_NAME, "very"));
        return query;
      }

      @Override
      public String toString(String field) {
        return null;
      }

      @Override
      public int hashCode() {
        return 31 * super.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        return super.equals(obj);
      }
    };
    
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    
    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    Document doc = searcher.doc(hits.scoreDocs[0].doc);
    String storedField = doc.get(FIELD_NAME);

    TokenStream stream = TokenSources.getAnyTokenStream(searcher
        .getIndexReader(), hits.scoreDocs[0].doc, FIELD_NAME, doc, analyzer);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("Hello <B>this</B> is a piece of text that is <B>very</B> <B>long</B> and contains too much preamble and the meat is really here which says kennedy has been shot", fragment);
    
    doc = searcher.doc(hits.scoreDocs[1].doc);
    storedField = doc.get(FIELD_NAME);

    stream = TokenSources.getAnyTokenStream(searcher
        .getIndexReader(), hits.scoreDocs[1].doc, FIELD_NAME, doc, analyzer);
    highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer));
    fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("<B>This</B> piece of text refers to Kennedy at the beginning then has a longer piece of text that is <B>very</B>", fragment);
    
  }
  
  public void testHighlightingWithDefaultField() throws Exception {

    String s1 = "I call our world Flatland, not because we call it so,";

    // Verify that a query against the default field results in text being
    // highlighted
    // regardless of the field name.

    PhraseQuery q = new PhraseQuery();
    q.setSlop(3);
    q.add(new Term(FIELD_NAME, "world"));
    q.add(new Term(FIELD_NAME, "flatland"));

    String expected = "I call our <B>world</B> <B>Flatland</B>, not because we call it so,";
    String observed = highlightField(q, "SOME_FIELD_NAME", s1);
    if (VERBOSE) System.out.println("Expected: \"" + expected + "\n" + "Observed: \"" + observed);
    assertEquals("Query in the default field results in text for *ANY* field being highlighted",
        expected, observed);

    // Verify that a query against a named field does not result in any
    // highlighting
    // when the query field name differs from the name of the field being
    // highlighted,
    // which in this example happens to be the default field name.
    q = new PhraseQuery();
    q.setSlop(3);
    q.add(new Term("text", "world"));
    q.add(new Term("text", "flatland"));

    expected = s1;
    observed = highlightField(q, FIELD_NAME, s1);
    if (VERBOSE) System.out.println("Expected: \"" + expected + "\n" + "Observed: \"" + observed);
    assertEquals(
        "Query in a named field does not result in highlighting when that field isn't in the query",
        s1, highlightField(q, FIELD_NAME, s1));
  }
  
  /**
   * This method intended for use with <tt>testHighlightingWithDefaultField()</tt>
   */
  private String highlightField(Query query, String fieldName, String text)
      throws IOException, InvalidTokenOffsetsException {
    TokenStream tokenStream = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true)
        .tokenStream(fieldName, new StringReader(text));
    // Assuming "<B>", "</B>" used to highlight
    SimpleHTMLFormatter formatter = new SimpleHTMLFormatter();
    QueryScorer scorer = new QueryScorer(query, fieldName, FIELD_NAME);
    Highlighter highlighter = new Highlighter(formatter, scorer);
    highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));

    String rv = highlighter.getBestFragments(tokenStream, text, 1, "(FIELD TEXT TRUNCATED)");
    return rv.length() == 0 ? text : rv;
  }

  public void testSimpleSpanHighlighter() throws Exception {
    doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME,
          new StringReader(text));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    // Not sure we can assert anything here - just running to check we dont
    // throw any exceptions
  }

  // LUCENE-1752
  public void testRepeatingTermsInMultBooleans() throws Exception {
    String content = "x y z a b c d e f g b c g";
    String f1 = "f1";
    String f2 = "f2";

    PhraseQuery f1ph1 = new PhraseQuery();
    f1ph1.add(new Term(f1, "a"));
    f1ph1.add(new Term(f1, "b"));
    f1ph1.add(new Term(f1, "c"));
    f1ph1.add(new Term(f1, "d"));

    PhraseQuery f2ph1 = new PhraseQuery();
    f2ph1.add(new Term(f2, "a"));
    f2ph1.add(new Term(f2, "b"));
    f2ph1.add(new Term(f2, "c"));
    f2ph1.add(new Term(f2, "d"));

    PhraseQuery f1ph2 = new PhraseQuery();
    f1ph2.add(new Term(f1, "b"));
    f1ph2.add(new Term(f1, "c"));
    f1ph2.add(new Term(f1, "g"));

    PhraseQuery f2ph2 = new PhraseQuery();
    f2ph2.add(new Term(f2, "b"));
    f2ph2.add(new Term(f2, "c"));
    f2ph2.add(new Term(f2, "g"));

    BooleanQuery booleanQuery = new BooleanQuery();
    BooleanQuery leftChild = new BooleanQuery();
    leftChild.add(f1ph1, Occur.SHOULD);
    leftChild.add(f2ph1, Occur.SHOULD);
    booleanQuery.add(leftChild, Occur.MUST);

    BooleanQuery rightChild = new BooleanQuery();
    rightChild.add(f1ph2, Occur.SHOULD);
    rightChild.add(f2ph2, Occur.SHOULD);
    booleanQuery.add(rightChild, Occur.MUST);

    QueryScorer scorer = new QueryScorer(booleanQuery, f1);
    scorer.setExpandMultiTermQuery(false);

    Highlighter h = new Highlighter(this, scorer);

    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    h.getBestFragment(analyzer, f1, content);

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 7);
  }

  public void testSimpleQueryScorerPhraseHighlighting() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "very"));
    phraseQuery.add(new Term(FIELD_NAME, "long"));
    phraseQuery.add(new Term(FIELD_NAME, "contains"), 3);
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 3);
    
    numHighlights = 0;

    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "piece"), 1);
    phraseQuery.add(new Term(FIELD_NAME, "text"), 3);
    phraseQuery.add(new Term(FIELD_NAME, "refers"), 4);
    phraseQuery.add(new Term(FIELD_NAME, "kennedy"), 6);

    doSearching(phraseQuery);

    maxNumFragmentsRequired = 2;

    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
    
    numHighlights = 0;

    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "lets"));
    phraseQuery.add(new Term(FIELD_NAME, "lets"), 4);
    phraseQuery.add(new Term(FIELD_NAME, "lets"), 8);
    phraseQuery.add(new Term(FIELD_NAME, "lets"), 12);

    doSearching(phraseQuery);

    maxNumFragmentsRequired = 2;

    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
    
  }
  
  public void testSpanRegexQuery() throws Exception {
    query = new SpanOrQuery(new SpanMultiTermQueryWrapper<RegexpQuery>(new RegexpQuery(new Term(FIELD_NAME, "ken.*"))));
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }
    
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }
  
  public void testRegexQuery() throws Exception {
    query = new RegexpQuery(new Term(FIELD_NAME, "ken.*"));
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }
    
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }
  
  public void testExternalReader() throws Exception {
    query = new RegexpQuery(new Term(FIELD_NAME, "ken.*"));
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, reader, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }
    
    assertTrue(reader.docFreq(new Term(FIELD_NAME, "hello")) > 0);
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }
  
  public void testNumericRangeQuery() throws Exception {
    // doesn't currently highlight, but make sure it doesn't cause exception either
    query = NumericRangeQuery.newIntRange(NUMERIC_FIELD_NAME, 2, 6, true, true);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).getField(NUMERIC_FIELD_NAME).numericValue().toString();
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

//      String result = 
        highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,"...");
      //if (VERBOSE) System.out.println("\t" + result);
    }


  }

  public void testSimpleQueryScorerPhraseHighlighting2() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.setSlop(5);
    phraseQuery.add(new Term(FIELD_NAME, "text"));
    phraseQuery.add(new Term(FIELD_NAME, "piece"));
    phraseQuery.add(new Term(FIELD_NAME, "long"));
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer =  new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this,scorer);
    highlighter.setTextFragmenter(new SimpleFragmenter(40));
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 6);
  }

  public void testSimpleQueryScorerPhraseHighlighting3() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "x"));
    phraseQuery.add(new Term(FIELD_NAME, "y"));
    phraseQuery.add(new Term(FIELD_NAME, "z"));
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
      QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == 3);
    }
  }
  
  public void testSimpleSpanFragmenter() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "piece"));
    phraseQuery.add(new Term(FIELD_NAME, "text"), 2);
    phraseQuery.add(new Term(FIELD_NAME, "very"), 5);
    phraseQuery.add(new Term(FIELD_NAME, "long"), 6);
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
  
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 5));

      String result = highlighter.getBestFragments(tokenStream, text,
          maxNumFragmentsRequired, "...");
      if (VERBOSE) System.out.println("\t" + result);

    }

    phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "been"));
    phraseQuery.add(new Term(FIELD_NAME, "shot"));

    doSearching(query);

    maxNumFragmentsRequired = 2;
    
    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);

    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 20));

      String result = highlighter.getBestFragments(tokenStream, text,
          maxNumFragmentsRequired, "...");
      if (VERBOSE) System.out.println("\t" + result);

    }
  }
  
  // position sensitive query added after position insensitive query
  public void testPosTermStdTerm() throws Exception {
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(new TermQuery(new Term(FIELD_NAME, "y")), Occur.SHOULD);

    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term(FIELD_NAME, "x"));
    phraseQuery.add(new Term(FIELD_NAME, "y"));
    phraseQuery.add(new Term(FIELD_NAME, "z"));
    booleanQuery.add(phraseQuery, Occur.SHOULD);

    doSearching(booleanQuery);

    int maxNumFragmentsRequired = 2;
    
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this,scorer);
    
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == 4);
    }
  }

  public void testQueryScorerMultiPhraseQueryHighlighting() throws Exception {
    MultiPhraseQuery mpq = new MultiPhraseQuery();

    mpq.add(new Term[] { new Term(FIELD_NAME, "wordx"), new Term(FIELD_NAME, "wordb") });
    mpq.add(new Term(FIELD_NAME, "wordy"));

    doSearching(mpq);

    final int maxNumFragmentsRequired = 2;
    assertExpectedHighlightCount(maxNumFragmentsRequired, 6);
  }

  public void testQueryScorerMultiPhraseQueryHighlightingWithGap() throws Exception {
    MultiPhraseQuery mpq = new MultiPhraseQuery();

    /*
     * The toString of MultiPhraseQuery doesn't work so well with these
     * out-of-order additions, but the Query itself seems to match accurately.
     */

    mpq.add(new Term[] { new Term(FIELD_NAME, "wordz") }, 2);
    mpq.add(new Term[] { new Term(FIELD_NAME, "wordx") }, 0);

    doSearching(mpq);

    final int maxNumFragmentsRequired = 1;
    final int expectedHighlights = 2;

    assertExpectedHighlightCount(maxNumFragmentsRequired, expectedHighlights);
  }

  public void testNearSpanSimpleQuery() throws Exception {
    doSearching(new SpanNearQuery(new SpanQuery[] {
        new SpanTermQuery(new Term(FIELD_NAME, "beginning")),
        new SpanTermQuery(new Term(FIELD_NAME, "kennedy")) }, 3, false));

    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        mode = QUERY;
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
      }
    };

    helper.run();

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 2);
  }

  public void testSimpleQueryTermScorerHighlighter() throws Exception {
    doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));
    Highlighter highlighter = new Highlighter(new QueryTermScorer(query));
    highlighter.setTextFragmenter(new SimpleFragmenter(40));
    int maxNumFragmentsRequired = 2;
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }
    // Not sure we can assert anything here - just running to check we dont
    // throw any exceptions
  }

  public void testSpanHighlighting() throws Exception {
    Query query1 = new SpanNearQuery(new SpanQuery[] {
        new SpanTermQuery(new Term(FIELD_NAME, "wordx")),
        new SpanTermQuery(new Term(FIELD_NAME, "wordy")) }, 1, false);
    Query query2 = new SpanNearQuery(new SpanQuery[] {
        new SpanTermQuery(new Term(FIELD_NAME, "wordy")),
        new SpanTermQuery(new Term(FIELD_NAME, "wordc")) }, 1, false);
    BooleanQuery bquery = new BooleanQuery();
    bquery.add(query1, Occur.SHOULD);
    bquery.add(query2, Occur.SHOULD);
    doSearching(bquery);
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        mode = QUERY;
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
      }
    };

    helper.run();
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 7);
  }

  public void testNotSpanSimpleQuery() throws Exception {
    doSearching(new SpanNotQuery(new SpanNearQuery(new SpanQuery[] {
        new SpanTermQuery(new Term(FIELD_NAME, "shot")),
        new SpanTermQuery(new Term(FIELD_NAME, "kennedy")) }, 3, false), new SpanTermQuery(
        new Term(FIELD_NAME, "john"))));
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        mode = QUERY;
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
      }
    };

    helper.run();
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
  }

  public void testGetBestFragmentsSimpleQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }
  
  public void testGetBestFragmentsConstantScore() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        if (random().nextBoolean()) {
          BooleanQuery bq = new BooleanQuery();
          bq.add(new ConstantScoreQuery(new QueryWrapperFilter(new TermQuery(
              new Term(FIELD_NAME, "kennedy")))), Occur.MUST);
          bq.add(new ConstantScoreQuery(new TermQuery(new Term(FIELD_NAME, "kennedy"))), Occur.MUST);
          doSearching(bq);
        } else {
          doSearching(new ConstantScoreQuery(new TermQuery(new Term(FIELD_NAME,
              "kennedy"))));
        }
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }

  public void testGetFuzzyFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        FuzzyQuery fuzzyQuery = new FuzzyQuery(new Term(FIELD_NAME, "kinnedy"), 2);
        fuzzyQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        doSearching(fuzzyQuery);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this, true);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }

  public void testGetWildCardFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        WildcardQuery wildcardQuery = new WildcardQuery(new Term(FIELD_NAME, "k?nnedy"));
        wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        doSearching(wildcardQuery);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }

  public void testGetMidWildCardFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        WildcardQuery wildcardQuery = new WildcardQuery(new Term(FIELD_NAME, "k*dy"));
        wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        doSearching(wildcardQuery);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetRangeFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;

        // Need to explicitly set the QueryParser property to use TermRangeQuery
        // rather
        // than RangeFilters

        TermRangeQuery rangeQuery = new TermRangeQuery(
            FIELD_NAME,
            new BytesRef("kannedy"),
            new BytesRef("kznnedy"),
            true, true);
        rangeQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);

        query = rangeQuery;
        doSearching(query);

        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testConstantScoreMultiTermQuery() throws Exception {

    numHighlights = 0;

    query = new WildcardQuery(new Term(FIELD_NAME, "ken*"));
    ((WildcardQuery)query).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
    searcher = newSearcher(reader);
    // can't rewrite ConstantScore if you want to highlight it -
    // it rewrites to ConstantScoreQuery which cannot be highlighted
    // query = unReWrittenQuery.rewrite(reader);
    if (VERBOSE) System.out.println("Searching for: " + query.toString(FIELD_NAME));
    hits = searcher.search(query, null, 1000);

    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(HighlighterTest.FIELD_NAME);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, HighlighterTest.FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(HighlighterTest.FIELD_NAME, new StringReader(text));

      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      if (VERBOSE) System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
    
    // try null field
    
    hits = searcher.search(query, null, 1000);
    
    numHighlights = 0;

    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(HighlighterTest.FIELD_NAME);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, null);
      TokenStream tokenStream = analyzer.tokenStream(HighlighterTest.FIELD_NAME, new StringReader(text));

      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      if (VERBOSE) System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
    
    // try default field
    
    hits = searcher.search(query, null, 1000);
    
    numHighlights = 0;

    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(HighlighterTest.FIELD_NAME);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, "random_field", HighlighterTest.FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(HighlighterTest.FIELD_NAME, new StringReader(text));

      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      if (VERBOSE) System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }

  public void testGetBestFragmentsPhrase() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        PhraseQuery phraseQuery = new PhraseQuery();
        phraseQuery.add(new Term(FIELD_NAME, "john"));
        phraseQuery.add(new Term(FIELD_NAME, "kennedy"));
        doSearching(phraseQuery);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsQueryScorer() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        SpanQuery clauses[] = { new SpanTermQuery(new Term("contents", "john")),
            new SpanTermQuery(new Term("contents", "kennedy")), };

        SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
        doSearching(snq);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testOffByOne() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        TermQuery query = new TermQuery(new Term("data", "help"));
        Highlighter hg = new Highlighter(new SimpleHTMLFormatter(), new QueryTermScorer(query));
        hg.setTextFragmenter(new NullFragmenter());

        String match = hg.getBestFragment(analyzer, "data", "help me [54-65]");
        assertEquals("<B>help</B> me [54-65]", match);

      }
    };

    helper.start();
  }

  public void testGetBestFragmentsFilteredQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        TermRangeFilter rf = TermRangeFilter.newStringRange("contents", "john", "john", true, true);
        SpanQuery clauses[] = { new SpanTermQuery(new Term("contents", "john")),
            new SpanTermQuery(new Term("contents", "kennedy")), };
        SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
        FilteredQuery fq = new FilteredQuery(snq, rf);

        doSearching(fq);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsFilteredPhraseQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        TermRangeFilter rf = TermRangeFilter.newStringRange("contents", "john", "john", true, true);
        PhraseQuery pq = new PhraseQuery();
        pq.add(new Term("contents", "john"));
        pq.add(new Term("contents", "kennedy"));
        FilteredQuery fq = new FilteredQuery(pq, rf);

        doSearching(fq);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsMultiTerm() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        BooleanQuery booleanQuery = new BooleanQuery();
        booleanQuery.add(new TermQuery(new Term(FIELD_NAME, "john")), Occur.SHOULD);
        PrefixQuery prefixQuery = new PrefixQuery(new Term(FIELD_NAME, "kenn"));
        prefixQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
        booleanQuery.add(prefixQuery, Occur.SHOULD);

        doSearching(booleanQuery);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsWithOr() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;

        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term(FIELD_NAME, "jfk")), Occur.SHOULD);
        query.add(new TermQuery(new Term(FIELD_NAME, "kennedy")), Occur.SHOULD);

        doSearching(query);
        doStandardHighlights(analyzer, searcher, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };
    helper.start();
  }

  public void testGetBestSingleFragment() throws Exception {

    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));
        numHighlights = 0;
        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.setTextFragmenter(new SimpleFragmenter(40));
          String result = highlighter.getBestFragment(tokenStream, text);
          if (VERBOSE) System.out.println("\t" + result);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.getBestFragment(analyzer, FIELD_NAME, text);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.getBestFragments(analyzer, FIELD_NAME, text, 10);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

      }

    };

    helper.start();

  }

  public void testGetBestSingleFragmentWithWeights() throws Exception {

    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        WeightedSpanTerm[] wTerms = new WeightedSpanTerm[2];
        wTerms[0] = new WeightedSpanTerm(10f, "hello");

        List<PositionSpan> positionSpans = new ArrayList<PositionSpan>();
        positionSpans.add(new PositionSpan(0, 0));
        wTerms[0].addPositionSpans(positionSpans);

        wTerms[1] = new WeightedSpanTerm(1f, "kennedy");
        positionSpans = new ArrayList<PositionSpan>();
        positionSpans.add(new PositionSpan(14, 14));
        wTerms[1].addPositionSpans(positionSpans);

        Highlighter highlighter = getHighlighter(wTerms, HighlighterTest.this);// new
        // Highlighter(new
        // QueryTermScorer(wTerms));
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(texts[0]));
        highlighter.setTextFragmenter(new SimpleFragmenter(2));

        String result = highlighter.getBestFragment(tokenStream, texts[0]).trim();
        assertTrue("Failed to find best section using weighted terms. Found: [" + result + "]",
            "<B>Hello</B>".equals(result));

        // readjust weights
        wTerms[1].setWeight(50f);
        tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(texts[0]));
        highlighter = getHighlighter(wTerms, HighlighterTest.this);
        highlighter.setTextFragmenter(new SimpleFragmenter(2));

        result = highlighter.getBestFragment(tokenStream, texts[0]).trim();
        assertTrue("Failed to find best section using weighted terms. Found: " + result,
            "<B>kennedy</B>".equals(result));
      }

    };

    helper.start();

  }

  // tests a "complex" analyzer that produces multiple
  // overlapping tokens
  public void testOverlapAnalyzer() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        HashMap<String,String> synonyms = new HashMap<String,String>();
        synonyms.put("football", "soccer,footie");
        Analyzer analyzer = new SynonymAnalyzer(synonyms);

        String s = "football-soccer in the euro 2004 footie competition";

        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term("bookid", "football")), Occur.SHOULD);
        query.add(new TermQuery(new Term("bookid", "soccer")), Occur.SHOULD);
        query.add(new TermQuery(new Term("bookid", "footie")), Occur.SHOULD);

        Highlighter highlighter = getHighlighter(query, null, HighlighterTest.this);

        // Get 3 best fragments and separate with a "..."
        TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));

        String result = highlighter.getBestFragments(tokenStream, s, 3, "...");
        String expectedResult = "<B>football</B>-<B>soccer</B> in the euro 2004 <B>footie</B> competition";
        assertTrue("overlapping analyzer should handle highlights OK, expected:" + expectedResult
            + " actual:" + result, expectedResult.equals(result));
      }

    };

    helper.start();

  }

  public void testGetSimpleHighlight() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));
        // new Highlighter(HighlighterTest.this, new QueryTermScorer(query));

        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          String result = highlighter.getBestFragment(tokenStream, text);
          if (VERBOSE) System.out.println("\t" + result);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };
    helper.start();
  }

  public void testGetTextFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {

        doSearching(new TermQuery(new Term(FIELD_NAME, "kennedy")));

        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);// new Highlighter(this, new
          // QueryTermScorer(query));
          highlighter.setTextFragmenter(new SimpleFragmenter(20));
          String stringResults[] = highlighter.getBestFragments(tokenStream, text, 10);

          tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          TextFragment fragmentResults[] = highlighter.getBestTextFragments(tokenStream, text,
              true, 10);

          assertTrue("Failed to find correct number of text Fragments: " + fragmentResults.length
              + " vs " + stringResults.length, fragmentResults.length == stringResults.length);
          for (int j = 0; j < stringResults.length; j++) {
            if (VERBOSE) System.out.println(fragmentResults[j]);
            assertTrue("Failed to find same text Fragments: " + fragmentResults[j] + " found",
                fragmentResults[j].toString().equals(stringResults[j]));

          }

        }
      }
    };
    helper.start();
  }

  public void testMaxSizeHighlight() throws Exception {
    final MockAnalyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
    // we disable MockTokenizer checks because we will forcefully limit the 
    // tokenstream and call end() before incrementToken() returns false.
    analyzer.setEnableChecks(false);
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        doSearching(new TermQuery(new Term(FIELD_NAME, "meat")));
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(texts[0]));
        Highlighter highlighter = getHighlighter(query, FIELD_NAME,
            HighlighterTest.this);// new Highlighter(this, new
        // QueryTermScorer(query));
        highlighter.setMaxDocCharsToAnalyze(30);

        highlighter.getBestFragment(tokenStream, texts[0]);
        assertTrue("Setting MaxDocBytesToAnalyze should have prevented "
            + "us from finding matches for this record: " + numHighlights + " found",
            numHighlights == 0);
      }
    };

    helper.start();
  }

  public void testMaxSizeHighlightTruncates() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        String goodWord = "goodtoken";
        CharacterRunAutomaton stopWords = new CharacterRunAutomaton(BasicAutomata.makeString("stoppedtoken"));
        // we disable MockTokenizer checks because we will forcefully limit the 
        // tokenstream and call end() before incrementToken() returns false.
        final MockAnalyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords, true);
        analyzer.setEnableChecks(false);
        TermQuery query = new TermQuery(new Term("data", goodWord));

        String match;
        StringBuilder sb = new StringBuilder();
        sb.append(goodWord);
        for (int i = 0; i < 10000; i++) {
          sb.append(" ");
          // only one stopword
          sb.append("stoppedtoken");
        }
        SimpleHTMLFormatter fm = new SimpleHTMLFormatter();
        Highlighter hg = getHighlighter(query, "data", fm);// new Highlighter(fm,
        // new
        // QueryTermScorer(query));
        hg.setTextFragmenter(new NullFragmenter());
        hg.setMaxDocCharsToAnalyze(100);
        match = hg.getBestFragment(analyzer, "data", sb.toString());
        assertTrue("Matched text should be no more than 100 chars in length ", match.length() < hg
            .getMaxDocCharsToAnalyze());

        // add another tokenized word to the overrall length - but set way
        // beyond
        // the length of text under consideration (after a large slug of stop
        // words
        // + whitespace)
        sb.append(" ");
        sb.append(goodWord);
        match = hg.getBestFragment(analyzer, "data", sb.toString());
        assertTrue("Matched text should be no more than 100 chars in length ", match.length() < hg
            .getMaxDocCharsToAnalyze());
      }
    };

    helper.start();

  }
  
  public void testMaxSizeEndHighlight() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {
      @Override
      public void run() throws Exception {
        CharacterRunAutomaton stopWords = new CharacterRunAutomaton(new RegExp("i[nt]").toAutomaton());
        TermQuery query = new TermQuery(new Term("text", "searchterm"));

        String text = "this is a text with searchterm in it";
        SimpleHTMLFormatter fm = new SimpleHTMLFormatter();
        Highlighter hg = getHighlighter(query, "text", fm);
        hg.setTextFragmenter(new NullFragmenter());
        hg.setMaxDocCharsToAnalyze(36);
        String match = hg.getBestFragment(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords, true), "text", text);
        assertTrue(
            "Matched text should contain remainder of text after highlighted query ",
            match.endsWith("in it"));
      }
    };
    helper.start();
  }

  public void testUnRewrittenQuery() throws Exception {
    final TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        // test to show how rewritten query can still be used
        searcher = newSearcher(reader);
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
        
        BooleanQuery query = new BooleanQuery();
        query.add(new WildcardQuery(new Term(FIELD_NAME, "jf?")), Occur.SHOULD);
        query.add(new WildcardQuery(new Term(FIELD_NAME, "kenned*")), Occur.SHOULD);

        if (VERBOSE) System.out.println("Searching with primitive query");
        // forget to set this and...
        // query=query.rewrite(reader);
        TopDocs hits = searcher.search(query, null, 1000);

        // create an instance of the highlighter with the tags used to surround
        // highlighted text
        // QueryHighlightExtractor highlighter = new
        // QueryHighlightExtractor(this,
        // query, new StandardAnalyzer(TEST_VERSION));

        int maxNumFragmentsRequired = 3;

        for (int i = 0; i < hits.totalHits; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, HighlighterTest.this, false);

          highlighter.setTextFragmenter(new SimpleFragmenter(40));

          String highlightedText = highlighter.getBestFragments(tokenStream, text,
              maxNumFragmentsRequired, "...");

          if (VERBOSE) System.out.println(highlightedText);
        }
        // We expect to have zero highlights if the query is multi-terms and is
        // not
        // rewritten!
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 0);
      }
    };

    helper.start();
  }

  public void testNoFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        doSearching(new TermQuery(new Term(FIELD_NAME, "aninvalidquerywhichshouldyieldnoresults")));

        for (String text : texts) {
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          String result = highlighter.getBestFragment(tokenStream, text);
          assertNull("The highlight result should be null for text with no query terms", result);
        }
      }
    };

    helper.start();
  }

  /**
   * Demonstrates creation of an XHTML compliant doc using new encoding facilities.
   */
  public void testEncoding() throws Exception {

    String rawDocContent = "\"Smith & sons' prices < 3 and >4\" claims article";
    // run the highlighter on the raw content (scorer does not score any tokens
    // for
    // highlighting but scores a single fragment for selection
    Highlighter highlighter = new Highlighter(this, new SimpleHTMLEncoder(), new Scorer() {
      @Override
      public void startFragment(TextFragment newFragment) {
      }

      @Override
      public float getTokenScore() {
        return 0;
      }

      @Override
      public float getFragmentScore() {
        return 1;
      }

      @Override
      public TokenStream init(TokenStream tokenStream) {
        return null;
      }
    });
    highlighter.setTextFragmenter(new SimpleFragmenter(2000));
    TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(rawDocContent));

    String encodedSnippet = highlighter.getBestFragments(tokenStream, rawDocContent, 1, "");
    // An ugly bit of XML creation:
    String xhtml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n"
        + "<head>\n" + "<title>My Test HTML Document</title>\n" + "</head>\n" + "<body>\n" + "<h2>"
        + encodedSnippet + "</h2>\n" + "</body>\n" + "</html>";
    // now an ugly built of XML parsing to test the snippet is encoded OK
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    org.w3c.dom.Document doc = db.parse(new ByteArrayInputStream(xhtml.getBytes("UTF-8")));
    Element root = doc.getDocumentElement();
    NodeList nodes = root.getElementsByTagName("body");
    Element body = (Element) nodes.item(0);
    nodes = body.getElementsByTagName("h2");
    Element h2 = (Element) nodes.item(0);
    String decodedSnippet = h2.getFirstChild().getNodeValue();
    assertEquals("XHTML Encoding should have worked:", rawDocContent, decodedSnippet);
  }

  public void testFieldSpecificHighlighting() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        String docMainText = "fred is one of the people";

        BooleanQuery query = new BooleanQuery();
        query.add(new TermQuery(new Term(FIELD_NAME, "fred")), Occur.SHOULD);
        query.add(new TermQuery(new Term("category", "people")), Occur.SHOULD);

        // highlighting respects fieldnames used in query

        Scorer fieldSpecificScorer = null;
        if (mode == TestHighlightRunner.QUERY) {
          fieldSpecificScorer = new QueryScorer(query, FIELD_NAME);
        } else if (mode == TestHighlightRunner.QUERY_TERM) {
          fieldSpecificScorer = new QueryTermScorer(query, "contents");
        }
        Highlighter fieldSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(),
            fieldSpecificScorer);
        fieldSpecificHighlighter.setTextFragmenter(new NullFragmenter());
        String result = fieldSpecificHighlighter.getBestFragment(analyzer, FIELD_NAME, docMainText);
        assertEquals("Should match", result, "<B>fred</B> is one of the people");

        // highlighting does not respect fieldnames used in query
        Scorer fieldInSpecificScorer = null;
        if (mode == TestHighlightRunner.QUERY) {
          fieldInSpecificScorer = new QueryScorer(query, null);
        } else if (mode == TestHighlightRunner.QUERY_TERM) {
          fieldInSpecificScorer = new QueryTermScorer(query);
        }

        Highlighter fieldInSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(),
            fieldInSpecificScorer);
        fieldInSpecificHighlighter.setTextFragmenter(new NullFragmenter());
        result = fieldInSpecificHighlighter.getBestFragment(analyzer, FIELD_NAME, docMainText);
        assertEquals("Should match", result, "<B>fred</B> is one of the <B>people</B>");

        reader.close();
      }
    };

    helper.start();

  }

  protected TokenStream getTS2() {
    // String s = "Hi-Speed10 foo";
    return new TokenStream() {
      Iterator<Token> iter;
      List<Token> lst;
      private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
      {
        lst = new ArrayList<Token>();
        Token t;
        t = createToken("hi", 0, 2);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("hispeed", 0, 8);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("speed", 3, 8);
        t.setPositionIncrement(0);
        lst.add(t);
        t = createToken("10", 8, 10);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("foo", 11, 14);
        t.setPositionIncrement(1);
        lst.add(t);
        iter = lst.iterator();
      }

      @Override
      public boolean incrementToken() {
        if(iter.hasNext()) {
          Token token = iter.next();
          clearAttributes();
          termAtt.setEmpty().append(token);
          posIncrAtt.setPositionIncrement(token.getPositionIncrement());
          offsetAtt.setOffset(token.startOffset(), token.endOffset());
          return true;
        }
        return false;
      }

      @Override
      public void reset() throws IOException {
        super.reset();
        iter = lst.iterator();
      }
    };
  }

  // same token-stream as above, but the bigger token comes first this time
  protected TokenStream getTS2a() {
    // String s = "Hi-Speed10 foo";
    return new TokenStream() {
      Iterator<Token> iter;
      List<Token> lst;
      private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
      private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
      {
        lst = new ArrayList<Token>();
        Token t;
        t = createToken("hispeed", 0, 8);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("hi", 0, 2);
        t.setPositionIncrement(0);
        lst.add(t);
        t = createToken("speed", 3, 8);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("10", 8, 10);
        t.setPositionIncrement(1);
        lst.add(t);
        t = createToken("foo", 11, 14);
        t.setPositionIncrement(1);
        lst.add(t);
        iter = lst.iterator();
      }

      @Override
      public boolean incrementToken() {
        if(iter.hasNext()) {
          Token token = iter.next();
          clearAttributes();
          termAtt.setEmpty().append(token);
          posIncrAtt.setPositionIncrement(token.getPositionIncrement());
          offsetAtt.setOffset(token.startOffset(), token.endOffset());
          return true;
        }
        return false;
      }

      @Override
      public void reset() throws IOException {
        super.reset();
        iter = lst.iterator();
      }
    };
  }

  public void testOverlapAnalyzer2() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        String s = "Hi-Speed10 foo";

        Query query;
        Highlighter highlighter;
        String result;

        query = new TermQuery(new Term("text", "foo"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-Speed10 <B>foo</B>", result);

        query = new TermQuery(new Term("text", "10"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-Speed<B>10</B> foo", result);

        query = new TermQuery(new Term("text", "hi"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi</B>-Speed10 foo", result);

        query = new TermQuery(new Term("text", "speed"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-<B>Speed</B>10 foo", result);

        query = new TermQuery(new Term("text", "hispeed"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        BooleanQuery booleanQuery = new BooleanQuery();
        booleanQuery.add(new TermQuery(new Term("text", "hi")), Occur.SHOULD);
        booleanQuery.add(new TermQuery(new Term("text", "speed")), Occur.SHOULD);

        query = booleanQuery;
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        // ///////////////// same tests, just put the bigger overlapping token
        // first
        query = new TermQuery(new Term("text", "foo"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-Speed10 <B>foo</B>", result);

        query = new TermQuery(new Term("text", "10"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-Speed<B>10</B> foo", result);

        query = new TermQuery(new Term("text", "hi"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi</B>-Speed10 foo", result);

        query = new TermQuery(new Term("text", "speed"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-<B>Speed</B>10 foo", result);

        query = new TermQuery(new Term("text", "hispeed"));
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        query = booleanQuery;
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);
      }
    };

    helper.start();
  }
  
  private Directory dir;
  private Analyzer a;
  
  public void testWeightedTermsWithDeletes() throws IOException, InvalidTokenOffsetsException {
    makeIndex();
    deleteDocument();
    searchIndex();
  }
  
  private Document doc( String f, String v ){
    Document doc = new Document();
    doc.add( new TextField( f, v, Field.Store.YES));
    return doc;
  }
  
  private void makeIndex() throws IOException {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field del" ) );
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field" ) );
    writer.forceMerge(1);
    writer.close();
  }
  
  private void deleteDocument() throws IOException {
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setOpenMode(OpenMode.APPEND));
    writer.deleteDocuments( new Term( "t_text1", "del" ) );
    // To see negative idf, keep comment the following line
    //writer.forceMerge(1);
    writer.close();
  }
  
  private void searchIndex() throws IOException, InvalidTokenOffsetsException {
    Query query = new TermQuery(new Term("t_text1", "random"));
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    // This scorer can return negative idf -> null fragment
    Scorer scorer = new QueryTermScorer( query, searcher.getIndexReader(), "t_text1" );
    // This scorer doesn't use idf (patch version)
    //Scorer scorer = new QueryTermScorer( query, "t_text1" );
    Highlighter h = new Highlighter( scorer );

    TopDocs hits = searcher.search(query, null, 10);
    for( int i = 0; i < hits.totalHits; i++ ){
      Document doc = searcher.doc( hits.scoreDocs[i].doc );
      String result = h.getBestFragment( a, "t_text1", doc.get( "t_text1" ));
      if (VERBOSE) System.out.println("result:" +  result);
      assertEquals("more <B>random</B> words for second field", result);
    }
    reader.close();
  }

  /*
   * 
   * public void testBigramAnalyzer() throws IOException, ParseException {
   * //test to ensure analyzers with none-consecutive start/end offsets //dont
   * double-highlight text //setup index 1 RAMDirectory ramDir = new
   * RAMDirectory(); Analyzer bigramAnalyzer=new CJKAnalyzer(); IndexWriter
   * writer = new IndexWriter(ramDir,bigramAnalyzer , true); Document d = new
   * Document(); Field f = new Field(FIELD_NAME, "java abc def", true, true,
   * true); d.add(f); writer.addDocument(d); writer.close(); IndexReader reader =
   * DirectoryReader.open(ramDir);
   * 
   * IndexSearcher searcher=new IndexSearcher(reader); query =
   * QueryParser.parse("abc", FIELD_NAME, bigramAnalyzer);
   * System.out.println("Searching for: " + query.toString(FIELD_NAME)); hits =
   * searcher.search(query);
   * 
   * Highlighter highlighter = new Highlighter(this,new
   * QueryFragmentScorer(query));
   * 
   * for (int i = 0; i < hits.totalHits; i++) { String text =
   * searcher.doc2(hits.scoreDocs[i].doc).get(FIELD_NAME); TokenStream
   * tokenStream=bigramAnalyzer.tokenStream(FIELD_NAME,new StringReader(text));
   * String highlightedText = highlighter.getBestFragment(tokenStream,text);
   * System.out.println(highlightedText); } }
   */

  @Override
  public String highlightTerm(String originalText, TokenGroup group) {
    if (group.getTotalScore() <= 0) {
      return originalText;
    }
    numHighlights++; // update stats used in assertions
    return "<B>" + originalText + "</B>";
  }

  public void doSearching(Query unReWrittenQuery) throws Exception {
    searcher = newSearcher(reader);
    // for any multi-term queries to work (prefix, wildcard, range,fuzzy etc)
    // you must use a rewritten query!
    query = unReWrittenQuery.rewrite(reader);
    if (VERBOSE) System.out.println("Searching for: " + query.toString(FIELD_NAME));
    hits = searcher.search(query, null, 1000);
  }

  public void assertExpectedHighlightCount(final int maxNumFragmentsRequired,
      final int expectedHighlights) throws Exception {
    for (int i = 0; i < hits.totalHits; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
      QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == expectedHighlights);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true);
    dir = newDirectory();
    ramDir = newDirectory();
    IndexWriter writer = new IndexWriter(ramDir, newIndexWriterConfig(
        TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET, true)));
    for (String text : texts) {
      addDoc(writer, text);
    }
    Document doc = new Document();
    doc.add(new IntField(NUMERIC_FIELD_NAME, 1, Field.Store.NO));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 1));
    writer.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new IntField(NUMERIC_FIELD_NAME, 3, Field.Store.NO));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 3));
    writer.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new IntField(NUMERIC_FIELD_NAME, 5, Field.Store.NO));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 5));
    writer.addDocument(doc, analyzer);

    doc = new Document();
    doc.add(new IntField(NUMERIC_FIELD_NAME, 7, Field.Store.NO));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 7));
    writer.addDocument(doc, analyzer);

    writer.forceMerge(1);
    writer.close();
    reader = DirectoryReader.open(ramDir);
    numHighlights = 0;
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    ramDir.close();
    super.tearDown();
  }
  private void addDoc(IndexWriter writer, String text) throws IOException {
    Document d = new Document();

    Field f = new TextField(FIELD_NAME, text, Field.Store.YES);
    d.add(f);
    writer.addDocument(d);

  }

  private static Token createToken(String term, int start, int offset)
  {
    return new Token(term, start, offset);
  }

}

// ===================================================================
// ========== BEGIN TEST SUPPORTING CLASSES
// ========== THESE LOOK LIKE, WITH SOME MORE EFFORT THESE COULD BE
// ========== MADE MORE GENERALLY USEFUL.
// TODO - make synonyms all interchangeable with each other and produce
// a version that does hyponyms - the "is a specialised type of ...."
// so that car = audi, bmw and volkswagen but bmw != audi so different
// behaviour to synonyms
// ===================================================================

final class SynonymAnalyzer extends Analyzer {
  private Map<String,String> synonyms;

  public SynonymAnalyzer(Map<String,String> synonyms) {
    this.synonyms = synonyms;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.Analyzer#tokenStream(java.lang.String,
   *      java.io.Reader)
   */
  @Override
  public TokenStreamComponents createComponents(String arg0, Reader arg1) {
    Tokenizer stream = new MockTokenizer(arg1, MockTokenizer.SIMPLE, true);
    stream.addAttribute(CharTermAttribute.class);
    stream.addAttribute(PositionIncrementAttribute.class);
    stream.addAttribute(OffsetAttribute.class);
    return new TokenStreamComponents(stream, new SynonymTokenizer(stream, synonyms));
  }
}

/**
 * Expands a token stream with synonyms (TODO - make the synonyms analyzed by choice of analyzer)
 *
 */
final class SynonymTokenizer extends TokenStream {
  private final TokenStream realStream;
  private Token currentRealToken = null;
  private final Map<String, String> synonyms;
  private StringTokenizer st = null;
  private final CharTermAttribute realTermAtt;
  private final PositionIncrementAttribute realPosIncrAtt;
  private final OffsetAttribute realOffsetAtt;
  private final CharTermAttribute termAtt;
  private final PositionIncrementAttribute posIncrAtt;
  private final OffsetAttribute offsetAtt;

  public SynonymTokenizer(TokenStream realStream, Map<String, String> synonyms) {
    this.realStream = realStream;
    this.synonyms = synonyms;
    realTermAtt = realStream.addAttribute(CharTermAttribute.class);
    realPosIncrAtt = realStream.addAttribute(PositionIncrementAttribute.class);
    realOffsetAtt = realStream.addAttribute(OffsetAttribute.class);

    termAtt = addAttribute(CharTermAttribute.class);
    posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    offsetAtt = addAttribute(OffsetAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {

    if (currentRealToken == null) {
      boolean next = realStream.incrementToken();
      if (!next) {
        return false;
      }
      //Token nextRealToken = new Token(, offsetAtt.startOffset(), offsetAtt.endOffset());
      clearAttributes();
      termAtt.copyBuffer(realTermAtt.buffer(), 0, realTermAtt.length());
      offsetAtt.setOffset(realOffsetAtt.startOffset(), realOffsetAtt.endOffset());
      posIncrAtt.setPositionIncrement(realPosIncrAtt.getPositionIncrement());

      String expansions =  synonyms.get(realTermAtt.toString());
      if (expansions == null) {
        return true;
      }
      st = new StringTokenizer(expansions, ",");
      if (st.hasMoreTokens()) {
        currentRealToken = new Token(realOffsetAtt.startOffset(), realOffsetAtt.endOffset());
        currentRealToken.copyBuffer(realTermAtt.buffer(), 0, realTermAtt.length());
      }
      
      return true;
    } else {
      String tok = st.nextToken();
      clearAttributes();
      termAtt.setEmpty().append(tok);
      offsetAtt.setOffset(currentRealToken.startOffset(), currentRealToken.endOffset());
      posIncrAtt.setPositionIncrement(0);
      if (!st.hasMoreTokens()) {
        currentRealToken = null;
        st = null;
      }
      return true;
    }
    
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.realStream.reset();
    this.currentRealToken = null;
    this.st = null;
  }

  @Override
  public void end() throws IOException {
    super.end();
    this.realStream.end();
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.realStream.close();
  }

  static abstract class TestHighlightRunner {
    static final int QUERY = 0;
    static final int QUERY_TERM = 1;

    int mode = QUERY;
    Fragmenter frag = new SimpleFragmenter(20);
    
    public Highlighter getHighlighter(Query query, String fieldName, Formatter formatter) {
      return getHighlighter(query, fieldName, formatter, true);
    }
    
    public Highlighter getHighlighter(Query query, String fieldName, Formatter formatter, boolean expanMultiTerm) {
      Scorer scorer;
      if (mode == QUERY) {
        scorer = new QueryScorer(query, fieldName);
        if(!expanMultiTerm) {
          ((QueryScorer)scorer).setExpandMultiTermQuery(false);
        }
      } else if (mode == QUERY_TERM) {
        scorer = new QueryTermScorer(query);
      } else {
        throw new RuntimeException("Unknown highlight mode");
      }
      
      return new Highlighter(formatter, scorer);
    }

    Highlighter getHighlighter(WeightedTerm[] weightedTerms, Formatter formatter) {
      if (mode == QUERY) {
        return  new Highlighter(formatter, new QueryScorer((WeightedSpanTerm[]) weightedTerms));
      } else if (mode == QUERY_TERM) {
        return new Highlighter(formatter, new QueryTermScorer(weightedTerms));

      } else {
        throw new RuntimeException("Unknown highlight mode");
      }
    }

    void doStandardHighlights(Analyzer analyzer, IndexSearcher searcher, TopDocs hits, Query query, Formatter formatter)
    throws Exception {
      doStandardHighlights(analyzer, searcher, hits, query, formatter, false);
    }
    
    void doStandardHighlights(Analyzer analyzer, IndexSearcher searcher, TopDocs hits, Query query, Formatter formatter, boolean expandMT)
        throws Exception {

      for (int i = 0; i < hits.totalHits; i++) {
        String text = searcher.doc(hits.scoreDocs[i].doc).get(HighlighterTest.FIELD_NAME);
        int maxNumFragmentsRequired = 2;
        String fragmentSeparator = "...";
        Scorer scorer = null;
        TokenStream tokenStream = analyzer.tokenStream(HighlighterTest.FIELD_NAME, new StringReader(text));
        if (mode == QUERY) {
          scorer = new QueryScorer(query);
        } else if (mode == QUERY_TERM) {
          scorer = new QueryTermScorer(query);
        }
        Highlighter highlighter = new Highlighter(formatter, scorer);
        highlighter.setTextFragmenter(frag);

        String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
            fragmentSeparator);
        if (LuceneTestCase.VERBOSE) System.out.println("\t" + result);
      }
    }

    abstract void run() throws Exception;

    void start() throws Exception {
      if (LuceneTestCase.VERBOSE) System.out.println("Run QueryScorer");
      run();
      if (LuceneTestCase.VERBOSE) System.out.println("Run QueryTermScorer");
      mode = QUERY_TERM;
      run();
    }
  }
}
