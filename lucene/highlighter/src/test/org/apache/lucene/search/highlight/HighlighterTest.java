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
package org.apache.lucene.search.highlight;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.CannedTokenStream;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockPayloadAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PhraseQuery.Builder;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.SynonymTokenizer.TestHighlightRunner;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.Automata;
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
  Directory dir1;
  public IndexSearcher searcher = null;
  int numHighlights = 0;
  MockAnalyzer analyzer;
  TopDocs hits;
  FieldType fieldType;//see doc()

  final FieldType FIELD_TYPE_TV;
  {
    FieldType fieldType = new FieldType(TextField.TYPE_STORED);
    fieldType.setStoreTermVectors(true);
    fieldType.setStoreTermVectorPositions(true);
    fieldType.setStoreTermVectorPayloads(true);
    fieldType.setStoreTermVectorOffsets(true);
    fieldType.freeze();
    FIELD_TYPE_TV = fieldType;
  }

  String[] texts = {
      "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
      "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
      "JFK has been shot", "John Kennedy has been shot",
      "This text has a typo in referring to Keneddy",
      "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b", "lets is a the lets is a the lets is a the lets",
      "Attribute instances are reused for all tokens of a document. Thus, a TokenStream/-Filter needs to update the appropriate Attribute(s) in incrementToken(). The consumer, commonly the Lucene indexer, consumes the data in the Attributes and then calls incrementToken() again until it retuns false, which indicates that the end of the stream was reached. This means that in each call of incrementToken() a TokenStream/-Filter can safely overwrite the data in the Attribute instances. "
  };

  // Convenience method for succinct tests; doesn't represent "best practice"
  private TokenStream getAnyTokenStream(String fieldName, int docId)
      throws IOException {
    return TokenSources.getTokenStream(fieldName, searcher.getIndexReader().getTermVectors(docId),
        searcher.doc(docId).get(fieldName), analyzer, -1);
  }

  public void testFunctionScoreQuery() throws Exception {
    TermQuery termQuery = new TermQuery(new Term(FIELD_NAME, "very"));
    FunctionScoreQuery query = new FunctionScoreQuery(termQuery, DoubleValuesSource.constant(1));

    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10, new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE));
    assertEquals(2, hits.totalHits.value);
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    final int docId0 = hits.scoreDocs[0].doc;
    Document doc = searcher.doc(docId0);
    String storedField = doc.get(FIELD_NAME);

    TokenStream stream = getAnyTokenStream(FIELD_NAME, docId0);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("Hello this is a piece of text that is <B>very</B> long and contains too much preamble and the meat is really here which says kennedy has been shot", fragment);

  }

  public void testQueryScorerHits() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(FIELD_NAME, "very", "long");

    query = phraseQuery;
    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);


    for (int i = 0; i < hits.scoreDocs.length; i++) {
      final int docId = hits.scoreDocs[i].doc;
      Document doc = searcher.doc(docId);
      String storedField = doc.get(FIELD_NAME);

      TokenStream stream = getAnyTokenStream(FIELD_NAME, docId);

      Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);

      highlighter.setTextFragmenter(fragmenter);

      String fragment = highlighter.getBestFragment(stream, storedField);

      if (VERBOSE) System.out.println(fragment);
    }
  }
  
  public void testHighlightingCommonTermsQuery() throws Exception {
    CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 3);
    query.add(new Term(FIELD_NAME, "this"));//stop-word
    query.add(new Term(FIELD_NAME, "long"));
    query.add(new Term(FIELD_NAME, "very"));

    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10, new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE));
    assertEquals(2, hits.totalHits.value);
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    final int docId0 = hits.scoreDocs[0].doc;
    Document doc = searcher.doc(docId0);
    String storedField = doc.get(FIELD_NAME);

    TokenStream stream = getAnyTokenStream(FIELD_NAME, docId0);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("Hello this is a piece of text that is <B>very</B> <B>long</B> and contains too much preamble and the meat is really here which says kennedy has been shot", fragment);

    final int docId1 = hits.scoreDocs[1].doc;
    doc = searcher.doc(docId1);
    storedField = doc.get(FIELD_NAME);

    stream = getAnyTokenStream(FIELD_NAME, docId1);
    highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer));
    fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("This piece of text refers to Kennedy at the beginning then has a longer piece of text that is <B>very</B>", fragment);
  }

  public void testHighlightingSynonymQuery() throws Exception {
    searcher = newSearcher(reader);
    Query query = new SynonymQuery.Builder(FIELD_NAME)
        .addTerm(new Term(FIELD_NAME, "jfk"))
        .addTerm(new Term(FIELD_NAME, "kennedy"))
        .build();
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);
    TokenStream stream = getAnyTokenStream(FIELD_NAME, 2);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String storedField = searcher.doc(2).get(FIELD_NAME);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("<B>JFK</B> has been shot", fragment);

    stream = getAnyTokenStream(FIELD_NAME, 3);
    storedField = searcher.doc(3).get(FIELD_NAME);
    fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("John <B>Kennedy</B> has been shot", fragment);
  }

  public void testHighlightUnknownQueryAfterRewrite() throws IOException, InvalidTokenOffsetsException {
    Query query = new Query() {
      
      @Override
      public Query rewrite(IndexReader reader) throws IOException {
        CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 3);
        query.add(new Term(FIELD_NAME, "this"));//stop-word
        query.add(new Term(FIELD_NAME, "long"));
        query.add(new Term(FIELD_NAME, "very"));
        return query;
      }

      @Override
      public void visit(QueryVisitor visitor) {

      }

      @Override
      public String toString(String field) {
        return null;
      }

      @Override
      public int hashCode() {
        return System.identityHashCode(this);
      }

      @Override
      public boolean equals(Object obj) {
        return obj == this;
      }
    };

    searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10, new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE));
    assertEquals(2, hits.totalHits.value);
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    final int docId0 = hits.scoreDocs[0].doc;
    Document doc = searcher.doc(docId0);
    String storedField = doc.get(FIELD_NAME);

    TokenStream stream = getAnyTokenStream(FIELD_NAME, docId0);
    Fragmenter fragmenter = new SimpleSpanFragmenter(scorer);
    highlighter.setTextFragmenter(fragmenter);
    String fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("Hello this is a piece of text that is <B>very</B> <B>long</B> and contains too much preamble and the meat is really here which says kennedy has been shot", fragment);

    final int docId1 = hits.scoreDocs[1].doc;
    doc = searcher.doc(docId1);
    storedField = doc.get(FIELD_NAME);

    stream = getAnyTokenStream(FIELD_NAME, docId1);
    highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer));
    fragment = highlighter.getBestFragment(stream, storedField);
    assertEquals("This piece of text refers to Kennedy at the beginning then has a longer piece of text that is <B>very</B>", fragment);
    
  }
  
  public void testHighlightingWithDefaultField() throws Exception {

    String s1 = "I call our world Flatland, not because we call it so,";

    // Verify that a query against the default field results in text being
    // highlighted
    // regardless of the field name.

    PhraseQuery q = new PhraseQuery(3, FIELD_NAME, "world", "flatland");

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
    q = new PhraseQuery(3, "text", "world", "flatland");

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
    TokenStream tokenStream = analyzer.tokenStream(fieldName, text);
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
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);
      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    // Not sure we can assert anything here - just running to check we dont
    // throw any exceptions
  }

  // LUCENE-2229
  public void testSimpleSpanHighlighterWithStopWordsStraddlingFragmentBoundaries() throws Exception {
    doSearching(new PhraseQuery(FIELD_NAME, "all", "tokens"));

    int maxNumFragmentsRequired = 1;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(scorer);

    assertEquals("Must have one hit", 1, hits.totalHits.value);
    for (int i = 0; i < hits.totalHits.value; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);

      highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 36));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired, "...");
      if (VERBOSE) System.out.println("\t" + result);

      assertTrue("Fragment must be less than 60 characters long", result.length() < 60);
    }
  }

  // LUCENE-1752
  public void testRepeatingTermsInMultBooleans() throws Exception {
    String content = "x y z a b c d e f g b c g";
    String f1 = "f1";
    String f2 = "f2";

    PhraseQuery f1ph1 = new PhraseQuery(f1, "a", "b", "c", "d");

    PhraseQuery f2ph1 = new PhraseQuery(f2, "a", "b", "c", "d");

    PhraseQuery f1ph2 = new PhraseQuery(f1, "b", "c", "g");

    PhraseQuery f2ph2 = new PhraseQuery(f2, "b", "c", "g");

    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    BooleanQuery.Builder leftChild = new BooleanQuery.Builder();
    leftChild.add(f1ph1, Occur.SHOULD);
    leftChild.add(f2ph1, Occur.SHOULD);
    booleanQuery.add(leftChild.build(), Occur.MUST);

    BooleanQuery.Builder rightChild = new BooleanQuery.Builder();
    rightChild.add(f1ph2, Occur.SHOULD);
    rightChild.add(f2ph2, Occur.SHOULD);
    booleanQuery.add(rightChild.build(), Occur.MUST);

    QueryScorer scorer = new QueryScorer(booleanQuery.build(), f1);
    scorer.setExpandMultiTermQuery(false);

    Highlighter h = new Highlighter(this, scorer);

    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);

    h.getBestFragment(analyzer, f1, content);

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 7);
  }

  public void testSimpleQueryScorerPhraseHighlighting() throws Exception {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    builder.add(new Term(FIELD_NAME, "very"), 0);
    builder.add(new Term(FIELD_NAME, "long"), 1);
    builder.add(new Term(FIELD_NAME, "contains"), 3);
    PhraseQuery phraseQuery = builder.build();
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 3);
    
    numHighlights = 0;

    builder = new PhraseQuery.Builder();
    builder.add(new Term(FIELD_NAME, "piece"), 1);
    builder.add(new Term(FIELD_NAME, "text"), 3);
    builder.add(new Term(FIELD_NAME, "refers"), 4);
    builder.add(new Term(FIELD_NAME, "kennedy"), 6);
    phraseQuery = builder.build();

    doSearching(phraseQuery);

    maxNumFragmentsRequired = 2;

    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
    
    numHighlights = 0;

    builder = new PhraseQuery.Builder();
    builder.add(new Term(FIELD_NAME, "lets"), 0);
    builder.add(new Term(FIELD_NAME, "lets"), 4);
    builder.add(new Term(FIELD_NAME, "lets"), 8);
    builder.add(new Term(FIELD_NAME, "lets"), 12);
    phraseQuery = builder.build();

    doSearching(phraseQuery);

    maxNumFragmentsRequired = 2;

    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
    
  }
  
  public void testSpanRegexQuery() throws Exception {
    query = new SpanOrQuery(new SpanMultiTermQueryWrapper<>(new RegexpQuery(new Term(FIELD_NAME, "ken.*"))));
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

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
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

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
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }
    
    assertTrue(reader.docFreq(new Term(FIELD_NAME, "hello")) > 0);
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }
  
  public void testDimensionalRangeQuery() throws Exception {
    // doesn't currently highlight, but make sure it doesn't cause exception either
    query = IntPoint.newRangeQuery(NUMERIC_FIELD_NAME, 2, 6);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 100);
    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).getField(NUMERIC_FIELD_NAME).numericValue().toString();
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

//      String result = 
        highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,"...");
      //if (VERBOSE) System.out.println("\t" + result);
    }


  }
  
  public void testSimpleQueryScorerPhraseHighlighting2() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(5, FIELD_NAME, "text", "piece", "long");
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer =  new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this,scorer);
    highlighter.setTextFragmenter(new SimpleFragmenter(40));
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 6);
  }

  public void testSimpleQueryScorerPhraseHighlighting3() throws Exception {
    PhraseQuery phraseQuery = new PhraseQuery(FIELD_NAME, "x", "y", "z");
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
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
    Builder builder = new PhraseQuery.Builder();
    builder.add(new Term(FIELD_NAME, "piece"), 0);
    builder.add(new Term(FIELD_NAME, "text"), 2);
    builder.add(new Term(FIELD_NAME, "very"), 5);
    builder.add(new Term(FIELD_NAME, "long"), 6);
    PhraseQuery phraseQuery = builder.build();
    doSearching(phraseQuery);

    int maxNumFragmentsRequired = 2;

    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this, scorer);
  
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 5));

      String result = highlighter.getBestFragments(tokenStream, text,
          maxNumFragmentsRequired, "...");
      if (VERBOSE) System.out.println("\t" + result);

    }

    phraseQuery = new PhraseQuery(FIELD_NAME, "been", "shot");

    doSearching(query);

    maxNumFragmentsRequired = 2;
    
    scorer = new QueryScorer(query, FIELD_NAME);
    highlighter = new Highlighter(this, scorer);

    for (int i = 0; i < hits.totalHits.value; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);

      highlighter.setTextFragmenter(new SimpleSpanFragmenter(scorer, 20));

      String result = highlighter.getBestFragments(tokenStream, text,
          maxNumFragmentsRequired, "...");
      if (VERBOSE) System.out.println("\t" + result);

    }
  }
  
  // position sensitive query added after position insensitive query
  public void testPosTermStdTerm() throws Exception {
    BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
    booleanQuery.add(new TermQuery(new Term(FIELD_NAME, "y")), Occur.SHOULD);

    PhraseQuery phraseQuery = new PhraseQuery(FIELD_NAME, "x", "y", "z");
    booleanQuery.add(phraseQuery, Occur.SHOULD);

    doSearching(booleanQuery.build());

    int maxNumFragmentsRequired = 2;
    
    QueryScorer scorer = new QueryScorer(query, FIELD_NAME);
    Highlighter highlighter = new Highlighter(this,scorer);
    
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

      highlighter.setTextFragmenter(new SimpleFragmenter(40));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      if (VERBOSE) System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == 4);
    }
  }

  public void testQueryScorerMultiPhraseQueryHighlighting() throws Exception {
    MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();

    mpqb.add(new Term[] { new Term(FIELD_NAME, "wordx"), new Term(FIELD_NAME, "wordb") });
    mpqb.add(new Term(FIELD_NAME, "wordy"));

    doSearching(mpqb.build());

    final int maxNumFragmentsRequired = 2;
    assertExpectedHighlightCount(maxNumFragmentsRequired, 6);
  }

  public void testQueryScorerMultiPhraseQueryHighlightingWithGap() throws Exception {
    MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();

    /*
     * The toString of MultiPhraseQuery doesn't work so well with these
     * out-of-order additions, but the Query itself seems to match accurately.
     */

    mpqb.add(new Term[] { new Term(FIELD_NAME, "wordz") }, 2);
    mpqb.add(new Term[] { new Term(FIELD_NAME, "wordx") }, 0);

    doSearching(mpqb.build());

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
    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

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
    BooleanQuery.Builder bquery = new BooleanQuery.Builder();
    bquery.add(query1, Occur.SHOULD);
    bquery.add(query2, Occur.SHOULD);
    doSearching(bquery.build());
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
          BooleanQuery.Builder bq = new BooleanQuery.Builder();
          bq.add(new ConstantScoreQuery(new TermQuery(
              new Term(FIELD_NAME, "kennedy"))), Occur.MUST);
          bq.add(new ConstantScoreQuery(new TermQuery(new Term(FIELD_NAME, "kennedy"))), Occur.MUST);
          doSearching(bq.build());
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
        fuzzyQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
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
        wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
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
        wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
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
        rangeQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);

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
    ((WildcardQuery)query).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
    searcher = newSearcher(reader);
    // can't rewrite ConstantScore if you want to highlight it -
    // it rewrites to ConstantScoreQuery which cannot be highlighted
    // query = unReWrittenQuery.rewrite(reader);
    if (VERBOSE) System.out.println("Searching for: " + query.toString(FIELD_NAME));
    hits = searcher.search(query, 1000);

    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, HighlighterTest.FIELD_NAME);

      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      if (VERBOSE) System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
    
    // try null field
    
    hits = searcher.search(query, 1000);
    
    numHighlights = 0;

    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, null);

      Highlighter highlighter = new Highlighter(this, scorer);

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      if (VERBOSE) System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
    
    // try default field
    
    hits = searcher.search(query, 1000);
    
    numHighlights = 0;

    for (int i = 0; i < hits.totalHits.value; i++) {
      final int docId = hits.scoreDocs[i].doc;
      final Document doc = searcher.doc(docId);
      String text = doc.get(FIELD_NAME);
      TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      QueryScorer scorer = new QueryScorer(query, "random_field", HighlighterTest.FIELD_NAME);

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
        PhraseQuery phraseQuery = new PhraseQuery(FIELD_NAME, "john", "kennedy");
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
        SpanQuery clauses[] = { new SpanTermQuery(new Term("contents", "john")),
            new SpanTermQuery(new Term("contents", "kennedy")), };
        SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(snq, Occur.MUST);
        bq.add(TermRangeQuery.newStringRange("contents", "john", "john", true, true), Occur.FILTER);

        doSearching(bq.build());
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
        PhraseQuery pq = new PhraseQuery("contents", "john", "kennedy");
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(pq, Occur.MUST);
        bq.add(TermRangeQuery.newStringRange("contents", "john", "john", true, true), Occur.FILTER);

        doSearching(bq.build());
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
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.add(new TermQuery(new Term(FIELD_NAME, "john")), Occur.SHOULD);
        PrefixQuery prefixQuery = new PrefixQuery(new Term(FIELD_NAME, "kenn"));
        prefixQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
        booleanQuery.add(prefixQuery, Occur.SHOULD);

        doSearching(booleanQuery.build());
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

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new TermQuery(new Term(FIELD_NAME, "jfk")), Occur.SHOULD);
        query.add(new TermQuery(new Term(FIELD_NAME, "kennedy")), Occur.SHOULD);

        doSearching(query.build());
        doStandardHighlights(analyzer, searcher, hits, query.build(), HighlighterTest.this);
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
        for (int i = 0; i < hits.totalHits.value; i++) {
          final int docId = hits.scoreDocs[i].doc;
          final Document doc = searcher.doc(docId);
          String text = doc.get(FIELD_NAME);
          TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.setTextFragmenter(new SimpleFragmenter(40));
          String result = highlighter.getBestFragment(tokenStream, text);
          if (VERBOSE) System.out.println("\t" + result);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.totalHits.value; i++) {
          final int docId = hits.scoreDocs[i].doc;
          final Document doc = searcher.doc(docId);
          String text = doc.get(FIELD_NAME);
          TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.getBestFragment(tokenStream, text);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.totalHits.value; i++) {
          final int docId = hits.scoreDocs[i].doc;
          final Document doc = searcher.doc(docId);
          String text = doc.get(FIELD_NAME);
          TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);
          highlighter.getBestFragments(tokenStream, text, 10);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

      }

    };

    helper.start();

  }

  public void testNotRewriteMultiTermQuery() throws IOException {
    // field "bar": (not the field we ultimately want to extract)
    MultiTermQuery mtq = new TermRangeQuery("bar", new BytesRef("aa"), new BytesRef("zz"), true, true) ;
    WeightedSpanTermExtractor extractor = new WeightedSpanTermExtractor() {
      @Override
      protected void extract(Query query, float boost, Map<String, WeightedSpanTerm> terms) throws IOException {
        assertEquals(mtq, query);
        super.extract(query, boost, terms);
      }
    };
    extractor.setExpandMultiTermQuery(true);
    extractor.setMaxDocCharsToAnalyze(51200);
    extractor.getWeightedSpanTerms(
        mtq, 3, new CannedTokenStream(new Token("aa",0,2), new Token("bb", 2,4)), "foo"); // field "foo"
  }

  public void testGetBestSingleFragmentWithWeights() throws Exception {

    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        WeightedSpanTerm[] wTerms = new WeightedSpanTerm[2];
        wTerms[0] = new WeightedSpanTerm(10f, "hello");

        List<PositionSpan> positionSpans = new ArrayList<>();
        positionSpans.add(new PositionSpan(0, 0));
        wTerms[0].addPositionSpans(positionSpans);

        wTerms[1] = new WeightedSpanTerm(1f, "kennedy");
        positionSpans = new ArrayList<>();
        positionSpans.add(new PositionSpan(14, 14));
        wTerms[1].addPositionSpans(positionSpans);

        Highlighter highlighter = getHighlighter(wTerms, HighlighterTest.this);// new
        // Highlighter(new
        // QueryTermScorer(wTerms));
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, texts[0]);
        highlighter.setTextFragmenter(new SimpleFragmenter(2));

        String result = highlighter.getBestFragment(tokenStream, texts[0]).trim();
        assertTrue("Failed to find best section using weighted terms. Found: [" + result + "]",
            "<B>Hello</B>".equals(result));

        // readjust weights
        wTerms[1].setWeight(50f);
        tokenStream = analyzer.tokenStream(FIELD_NAME, texts[0]);
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
        HashMap<String,String> synonyms = new HashMap<>();
        synonyms.put("football", "soccer,footie");
        Analyzer analyzer = new SynonymAnalyzer(synonyms);

        String s = "football-soccer in the euro 2004 footie competition";

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new TermQuery(new Term("bookid", "football")), Occur.SHOULD);
        query.add(new TermQuery(new Term("bookid", "soccer")), Occur.SHOULD);
        query.add(new TermQuery(new Term("bookid", "footie")), Occur.SHOULD);

        Highlighter highlighter = getHighlighter(query.build(), null, HighlighterTest.this);

        // Get 3 best fragments and separate with a "..."
        TokenStream tokenStream = analyzer.tokenStream(null, s);

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

        for (int i = 0; i < hits.totalHits.value; i++) {
          String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);
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

        for (int i = 0; i < hits.totalHits.value; i++) {
          final int docId = hits.scoreDocs[i].doc;
          final Document doc = searcher.doc(docId);
          String text = doc.get(FIELD_NAME);
          TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);

          Highlighter highlighter = getHighlighter(query, FIELD_NAME,
              HighlighterTest.this);// new Highlighter(this, new
          // QueryTermScorer(query));
          highlighter.setTextFragmenter(new SimpleFragmenter(20));
          String stringResults[] = highlighter.getBestFragments(tokenStream, text, 10);

          tokenStream = analyzer.tokenStream(FIELD_NAME, text);
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
    // we disable MockTokenizer checks because we will forcefully limit the
    // tokenstream and call end() before incrementToken() returns false.
    // But we first need to clear the re-used tokenstream components that have enableChecks.
    analyzer.getReuseStrategy().setReusableComponents(analyzer, FIELD_NAME, null);
    analyzer.setEnableChecks(false);
    TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        doSearching(new TermQuery(new Term(FIELD_NAME, "meat")));
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, texts[0]);
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
        CharacterRunAutomaton stopWords = new CharacterRunAutomaton(Automata.makeString("stoppedtoken"));
        // we disable MockTokenizer checks because we will forcefully limit the 
        // tokenstream and call end() before incrementToken() returns false.
        final MockAnalyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords);
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
        String match = hg.getBestFragment(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, stopWords), "text", text);
        assertTrue(
            "Matched text should contain remainder of text after highlighted query ",
            match.endsWith("in it"));
      }
    };
    helper.start();
  }

  public void testHighlighterWithPhraseQuery() throws IOException, InvalidTokenOffsetsException {
    final String fieldName = "substring";

    final PhraseQuery query = new PhraseQuery(fieldName, new BytesRef[] { new BytesRef("uchu") });

    assertHighlighting(query, new SimpleHTMLFormatter("<b>", "</b>"), "Buchung", "B<b>uchu</b>ng", fieldName);
  }

  public void testHighlighterWithMultiPhraseQuery() throws IOException, InvalidTokenOffsetsException {
    final String fieldName = "substring";

    final MultiPhraseQuery mpq = new MultiPhraseQuery.Builder()
        .add(new Term(fieldName, "uchu")).build();

    assertHighlighting(mpq, new SimpleHTMLFormatter("<b>", "</b>"), "Buchung", "B<b>uchu</b>ng", fieldName);
  }

  private void assertHighlighting(Query query, Formatter formatter, String text, String expected, String fieldName)
      throws IOException, InvalidTokenOffsetsException {
    final Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        return new TokenStreamComponents(new NGramTokenizer(4, 4));
      }
    };

    final QueryScorer fragmentScorer = new QueryScorer(query, fieldName);

    final Highlighter highlighter = new Highlighter(formatter, fragmentScorer);
    highlighter.setTextFragmenter(new SimpleFragmenter(100));
    final String fragment = highlighter.getBestFragment(analyzer, fieldName, text);

    assertEquals(expected, fragment);
  }

  public void testUnRewrittenQuery() throws Exception {
    final TestHighlightRunner helper = new TestHighlightRunner() {

      @Override
      public void run() throws Exception {
        numHighlights = 0;
        // test to show how rewritten query can still be used
        searcher = newSearcher(reader);

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new WildcardQuery(new Term(FIELD_NAME, "jf?")), Occur.SHOULD);
        query.add(new WildcardQuery(new Term(FIELD_NAME, "kenned*")), Occur.SHOULD);

        if (VERBOSE) System.out.println("Searching with primitive query");
        // forget to set this and...
        // query=query.rewrite(reader);
        TopDocs hits = searcher.search(query.build(), 1000);

        // create an instance of the highlighter with the tags used to surround
        // highlighted text
        // QueryHighlightExtractor highlighter = new
        // QueryHighlightExtractor(this,
        // query, new StandardAnalyzer(TEST_VERSION));

        int maxNumFragmentsRequired = 3;

        for (int i = 0; i < hits.totalHits.value; i++) {
          final int docId = hits.scoreDocs[i].doc;
          final Document doc = searcher.doc(docId);
          String text = doc.get(FIELD_NAME);
          TokenStream tokenStream = getAnyTokenStream(FIELD_NAME, docId);
          Highlighter highlighter = getHighlighter(query.build(), FIELD_NAME, HighlighterTest.this, false);

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
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);
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
    TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, rawDocContent);

    String encodedSnippet = highlighter.getBestFragments(tokenStream, rawDocContent, 1, "");
    // An ugly bit of XML creation:
    String xhtml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n"
        + "<head>\n" + "<title>My Test HTML Document</title>\n" + "</head>\n" + "<body>\n" + "<h2>"
        + encodedSnippet + "</h2>\n" + "</body>\n" + "</html>";
    // now an ugly built of XML parsing to test the snippet is encoded OK
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    org.w3c.dom.Document doc = db.parse(new ByteArrayInputStream(xhtml.getBytes(StandardCharsets.UTF_8)));
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

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        query.add(new TermQuery(new Term(FIELD_NAME, "fred")), Occur.SHOULD);
        query.add(new TermQuery(new Term("category", "people")), Occur.SHOULD);

        // highlighting respects fieldnames used in query

        Scorer fieldSpecificScorer = null;
        if (mode == TestHighlightRunner.QUERY) {
          fieldSpecificScorer = new QueryScorer(query.build(), FIELD_NAME);
        } else if (mode == TestHighlightRunner.QUERY_TERM) {
          fieldSpecificScorer = new QueryTermScorer(query.build(), "contents");
        }
        Highlighter fieldSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(),
            fieldSpecificScorer);
        fieldSpecificHighlighter.setTextFragmenter(new NullFragmenter());
        String result = fieldSpecificHighlighter.getBestFragment(analyzer, FIELD_NAME, docMainText);
        assertEquals("Should match", result, "<B>fred</B> is one of the people");

        // highlighting does not respect fieldnames used in query
        Scorer fieldInSpecificScorer = null;
        if (mode == TestHighlightRunner.QUERY) {
          fieldInSpecificScorer = new QueryScorer(query.build(), null);
        } else if (mode == TestHighlightRunner.QUERY_TERM) {
          fieldInSpecificScorer = new QueryTermScorer(query.build());
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
        lst = new ArrayList<>();
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
        lst = new ArrayList<>();
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

        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        booleanQuery.add(new TermQuery(new Term("text", "hi")), Occur.SHOULD);
        booleanQuery.add(new TermQuery(new Term("text", "speed")), Occur.SHOULD);

        query = booleanQuery.build();
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

        query = booleanQuery.build();
        highlighter = getHighlighter(query, "text", HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);
      }
    };

    helper.start();
  }
  
  private Directory dir2;
  private Analyzer a;
  
  public void testWeightedTermsWithDeletes() throws IOException, InvalidTokenOffsetsException {
    makeIndex();
    deleteDocument();
    searchIndex();
  }
  
  private void makeIndex() throws IOException {
    IndexWriter writer = new IndexWriter(dir1, new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)));
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field del" ) );
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field" ) );
    writer.forceMerge(1);
    writer.close();
  }
  
  private void deleteDocument() throws IOException {
    IndexWriter writer = new IndexWriter(dir1, new IndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setOpenMode(OpenMode.APPEND));
    writer.deleteDocuments( new Term( "t_text1", "del" ) );
    // To see negative idf, keep comment the following line
    //writer.forceMerge(1);
    writer.close();
  }
  
  private void searchIndex() throws IOException, InvalidTokenOffsetsException {
    Query query = new TermQuery(new Term("t_text1", "random"));
    IndexReader reader = DirectoryReader.open(dir1);
    IndexSearcher searcher = newSearcher(reader);
    // This scorer can return negative idf -> null fragment
    Scorer scorer = new QueryTermScorer( query, searcher.getIndexReader(), "t_text1" );
    // This scorer doesn't use idf (patch version)
    //Scorer scorer = new QueryTermScorer( query, "t_text1" );
    Highlighter h = new Highlighter( scorer );

    TopDocs hits = searcher.search(query, 10);
    for( int i = 0; i < hits.totalHits.value; i++ ){
      Document doc = searcher.doc( hits.scoreDocs[i].doc );
      String result = h.getBestFragment( a, "t_text1", doc.get( "t_text1" ));
      if (VERBOSE) System.out.println("result:" +  result);
      assertEquals("more <B>random</B> words for second field", result);
    }
    reader.close();
  }

  /** We can highlight based on payloads. It's supported both via term vectors and MemoryIndex since Lucene 5. */
  public void testPayloadQuery() throws IOException, InvalidTokenOffsetsException {
    final String text = "random words and words";//"words" at positions 1 & 4

    Analyzer analyzer = new MockPayloadAnalyzer();//sets payload to "pos: X" (where X is position #)
    try (IndexWriter writer = new IndexWriter(dir1, new IndexWriterConfig(analyzer))) {
      writer.deleteAll();
      Document doc = new Document();

      doc.add(new Field(FIELD_NAME, text, fieldType));
      writer.addDocument(doc);
      writer.commit();
    }
    try (IndexReader reader = DirectoryReader.open(dir1)) {
      Query query = new SpanPayloadCheckQuery(new SpanTermQuery(new Term(FIELD_NAME, "words")),
          Collections.singletonList(new BytesRef("pos: 1")));//just match the first "word" occurrence
      IndexSearcher searcher = newSearcher(reader);
      QueryScorer scorer = new QueryScorer(query, searcher.getIndexReader(), FIELD_NAME);
      scorer.setUsePayloads(true);
      Highlighter h = new Highlighter(scorer);

      TopDocs hits = searcher.search(query, 10);
      assertEquals(1, hits.scoreDocs.length);
      @SuppressWarnings("deprecation")
      TokenStream stream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), 0, FIELD_NAME, analyzer);
      if (random().nextBoolean()) {
        stream = new CachingTokenFilter(stream);//conceals detection of TokenStreamFromTermVector
      }
      String result = h.getBestFragment(stream, text);
      assertEquals("random <B>words</B> and words", result);//only highlight first "word"
    }
  }

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
    hits = searcher.search(query, 1000);
  }

  public void assertExpectedHighlightCount(final int maxNumFragmentsRequired,
      final int expectedHighlights) throws Exception {
    for (int i = 0; i < hits.totalHits.value; i++) {
      String text = searcher.doc(hits.scoreDocs[i].doc).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, text);
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

    //Not many use this setup:
    a = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    dir1 = newDirectory();

    //Most tests use this setup:
    analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET);
    dir2 = newDirectory();
    fieldType = random().nextBoolean() ? FIELD_TYPE_TV : TextField.TYPE_STORED;
    IndexWriter writer = new IndexWriter(dir2, newIndexWriterConfig(analyzer).setMergePolicy(newLogMergePolicy()));

    for (String text : texts) {
      writer.addDocument(doc(FIELD_NAME, text));
    }

    // a few tests need other docs...:
    Document doc = new Document();
    doc.add(new IntPoint(NUMERIC_FIELD_NAME, 1));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 1));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint(NUMERIC_FIELD_NAME, 3));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 3));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint(NUMERIC_FIELD_NAME, 5));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 5));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint(NUMERIC_FIELD_NAME, 7));
    doc.add(new StoredField(NUMERIC_FIELD_NAME, 7));
    writer.addDocument(doc);

    Document childDoc = doc(FIELD_NAME, "child document");
    Document parentDoc = doc(FIELD_NAME, "parent document");
    writer.addDocuments(Arrays.asList(childDoc, parentDoc));
    
    writer.forceMerge(1);
    writer.close();
    reader = DirectoryReader.open(dir2);

    //Misc:
    numHighlights = 0;
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir1.close();
    dir2.close();
    super.tearDown();
  }

  private Document doc(String name, String value) {
    Document d = new Document();
    d.add(new Field(name, value, fieldType));//fieldType is randomly chosen for term vectors in setUp
    return d;
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
  public TokenStreamComponents createComponents(String arg0) {
    Tokenizer stream = new MockTokenizer(MockTokenizer.SIMPLE, true);
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
        currentRealToken = new Token();
        currentRealToken.setOffset(realOffsetAtt.startOffset(), realOffsetAtt.endOffset());
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

      for (int i = 0; i < hits.totalHits.value; i++) {
        final int docId = hits.scoreDocs[i].doc;
        final Document doc = searcher.doc(docId);
        String text = doc.get(HighlighterTest.FIELD_NAME);
        int maxNumFragmentsRequired = 2;
        String fragmentSeparator = "...";
        Scorer scorer = null;
        TokenStream tokenStream =
            TokenSources.getTokenStream(HighlighterTest.FIELD_NAME,
                searcher.getIndexReader().getTermVectors(docId), text, analyzer, -1);
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
