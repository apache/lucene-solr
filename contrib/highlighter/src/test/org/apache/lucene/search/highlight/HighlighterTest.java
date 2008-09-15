package org.apache.lucene.search.highlight;

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

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeFilter;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.highlight.SynonymTokenizer.TestHighlightRunner;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * JUnit Test for Highlighter class.
 *
 */
public class HighlighterTest extends TestCase implements Formatter {
  private IndexReader reader;
  static final String FIELD_NAME = "contents";
  private Query query;
  RAMDirectory ramDir;
  public Searcher searcher = null;
  public Hits hits = null;
  int numHighlights = 0;
  Analyzer analyzer = new StandardAnalyzer();

  String[] texts = {
      "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
      "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
      "JFK has been shot", "John Kennedy has been shot",
      "This text has a typo in referring to Keneddy",
      "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b" };

  /**
   * Constructor for HighlightExtractorTest.
   * 
   * @param arg0
   */
  public HighlighterTest(String arg0) {
    super(arg0);
  }

  public void testHighlightingWithDefaultField() throws Exception {

    String s1 = "I call our world Flatland, not because we call it so,";

    QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());

    // Verify that a query against the default field results in text being
    // highlighted
    // regardless of the field name.
    Query q = parser.parse("\"world Flatland\"~3");
    String expected = "I call our <B>world</B> <B>Flatland</B>, not because we call it so,";
    String observed = highlightField(q, "SOME_FIELD_NAME", s1);
    System.out.println("Expected: \"" + expected + "\n" + "Observed: \"" + observed);
    assertEquals("Query in the default field results in text for *ANY* field being highlighted",
        expected, observed);

    // Verify that a query against a named field does not result in any
    // highlighting
    // when the query field name differs from the name of the field being
    // highlighted,
    // which in this example happens to be the default field name.
    q = parser.parse("text:\"world Flatland\"~3");
    expected = s1;
    observed = highlightField(q, FIELD_NAME, s1);
    System.out.println("Expected: \"" + expected + "\n" + "Observed: \"" + observed);
    assertEquals(
        "Query in a named field does not result in highlighting when that field isn't in the query",
        s1, highlightField(q, FIELD_NAME, s1));
  }

  /**
   * This method intended for use with <tt>testHighlightingWithDefaultField()</tt>
   */
  private static String highlightField(Query query, String fieldName, String text)
      throws IOException {
    CachingTokenFilter tokenStream = new CachingTokenFilter(new StandardAnalyzer().tokenStream(
        fieldName, new StringReader(text)));
    // Assuming "<B>", "</B>" used to highlight
    SimpleHTMLFormatter formatter = new SimpleHTMLFormatter();
    Highlighter highlighter = new Highlighter(formatter, new SpanScorer(query, fieldName,
        tokenStream, FIELD_NAME));
    highlighter.setTextFragmenter(new SimpleFragmenter(Integer.MAX_VALUE));
    tokenStream.reset();
    String rv = highlighter.getBestFragments(tokenStream, text, 1, "(FIELD TEXT TRUNCATED)");
    return rv.length() == 0 ? text : rv;
  }

  public void testSimpleSpanHighlighter() throws Exception {
    doSearching("Kennedy");

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);
    }

    // Not sure we can assert anything here - just running to check we dont
    // throw any exceptions
  }

  public void testSimpleSpanPhraseHighlighting() throws Exception {
    doSearching("\"very long and contains\"");

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(this,
          new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 3);
  }

  public void testSimpleSpanPhraseHighlighting2() throws Exception {
    doSearching("\"text piece long\"~5");

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(this,
          new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);
    }

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 6);
  }

  public void testSimpleSpanPhraseHighlighting3() throws Exception {
    doSearching("\"x y z\"");

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(this,
          new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == 3);
    }
  }
  
  // position sensitive query added after position insensitive query
  public void testPosTermStdTerm() throws Exception {
    doSearching("y \"x y z\"");

    int maxNumFragmentsRequired = 2;

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(this,
          new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == 4);
    }
  }

  public void testSpanMultiPhraseQueryHighlighting() throws Exception {
    MultiPhraseQuery mpq = new MultiPhraseQuery();

    mpq.add(new Term[] { new Term(FIELD_NAME, "wordx"), new Term(FIELD_NAME, "wordb") });
    mpq.add(new Term(FIELD_NAME, "wordy"));

    doSearching(mpq);

    final int maxNumFragmentsRequired = 2;
    assertExpectedHighlightCount(maxNumFragmentsRequired, 6);
  }

  public void testSpanMultiPhraseQueryHighlightingWithGap() throws Exception {
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

      public void run() throws Exception {
        mode = SPAN;
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
      }
    };

    helper.run();

    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 2);
  }

  public void testSimpleHighlighter() throws Exception {
    doSearching("Kennedy");
    Highlighter highlighter = new Highlighter(new QueryScorer(query));
    highlighter.setTextFragmenter(new SimpleFragmenter(40));
    int maxNumFragmentsRequired = 2;
    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);
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

      public void run() throws Exception {
        mode = SPAN;
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
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

      public void run() throws Exception {
        mode = SPAN;
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
      }
    };

    helper.run();
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 4);
  }

  public void testGetBestFragmentsSimpleQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("Kennedy");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }

  public void testGetFuzzyFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("Kinnedy~");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetWildCardFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("K?nnedy");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };

    helper.start();
  }

  public void testGetMidWildCardFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("K*dy");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetRangeFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        String queryString = FIELD_NAME + ":[kannedy TO kznnedy]";

        // Need to explicitly set the QueryParser property to use RangeQuery
        // rather
        // than RangeFilters
        QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
        parser.setUseOldRangeQuery(true);
        query = parser.parse(queryString);
        doSearching(query);

        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetConstantScoreRangeFragments() throws Exception {

    numHighlights = 0;
    String queryString = FIELD_NAME + ":[kannedy TO kznnedy]";

    // Need to explicitly set the QueryParser property to use RangeQuery
    // rather
    // than RangeFilters
    QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
    // parser.setUseOldRangeQuery(true);
    query = parser.parse(queryString);

    searcher = new IndexSearcher(ramDir);
    // can't rewrite ConstantScoreRangeQuery if you want to highlight it -
    // it rewrites to ConstantScoreQuery which cannot be highlighted
    // query = unReWrittenQuery.rewrite(reader);
    System.out.println("Searching for: " + query.toString(FIELD_NAME));
    hits = searcher.search(query);

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(HighlighterTest.FIELD_NAME);
      int maxNumFragmentsRequired = 2;
      String fragmentSeparator = "...";
      SpanScorer scorer = null;
      TokenStream tokenStream = null;

      tokenStream = new CachingTokenFilter(analyzer.tokenStream(HighlighterTest.FIELD_NAME,
          new StringReader(text)));
      
      SpanScorer.setHighlightCnstScrRngQuery(true);
      scorer = new SpanScorer(query, HighlighterTest.FIELD_NAME, (CachingTokenFilter) tokenStream);
      
      Highlighter highlighter = new Highlighter(this, scorer);

      ((CachingTokenFilter) tokenStream).reset();

      highlighter.setTextFragmenter(new SimpleFragmenter(20));

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          fragmentSeparator);
      System.out.println("\t" + result);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 5);
  }

  public void testGetBestFragmentsPhrase() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("\"John Kennedy\"");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsSpan() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        SpanQuery clauses[] = { new SpanTermQuery(new Term("contents", "john")),
            new SpanTermQuery(new Term("contents", "kennedy")), };

        SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
        doSearching(snq);
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testOffByOne() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        TermQuery query = new TermQuery(new Term("data", "help"));
        Highlighter hg = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(query));
        hg.setTextFragmenter(new NullFragmenter());

        String match = null;
        match = hg.getBestFragment(new StandardAnalyzer(), "data", "help me [54-65]");
        assertEquals("<B>help</B> me [54-65]", match);

      }
    };

    helper.start();
  }

  public void testGetBestFragmentsFilteredQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        RangeFilter rf = new RangeFilter("contents", "john", "john", true, true);
        SpanQuery clauses[] = { new SpanTermQuery(new Term("contents", "john")),
            new SpanTermQuery(new Term("contents", "kennedy")), };
        SpanNearQuery snq = new SpanNearQuery(clauses, 1, true);
        FilteredQuery fq = new FilteredQuery(snq, rf);

        doSearching(fq);
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsFilteredPhraseQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        RangeFilter rf = new RangeFilter("contents", "john", "john", true, true);
        PhraseQuery pq = new PhraseQuery();
        pq.add(new Term("contents", "john"));
        pq.add(new Term("contents", "kennedy"));
        FilteredQuery fq = new FilteredQuery(pq, rf);

        doSearching(fq);
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        // Currently highlights "John" and "Kennedy" separately
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 2);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsMultiTerm() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("John Kenn*");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };

    helper.start();
  }

  public void testGetBestFragmentsWithOr() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("JFK OR Kennedy");
        doStandardHighlights(analyzer, hits, query, HighlighterTest.this);
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 5);
      }
    };
    helper.start();
  }

  public void testGetBestSingleFragment() throws Exception {

    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        doSearching("Kennedy");
        numHighlights = 0;
        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
              HighlighterTest.this);
          highlighter.setTextFragmenter(new SimpleFragmenter(40));
          String result = highlighter.getBestFragment(tokenStream, text);
          System.out.println("\t" + result);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
              HighlighterTest.this);
          highlighter.getBestFragment(analyzer, FIELD_NAME, text);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);

        numHighlights = 0;
        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);

          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
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

      public void run() throws Exception {
        WeightedSpanTerm[] wTerms = new WeightedSpanTerm[2];
        wTerms[0] = new WeightedSpanTerm(10f, "hello");

        List positionSpans = new ArrayList();
        positionSpans.add(new PositionSpan(0, 0));
        wTerms[0].addPositionSpans(positionSpans);

        wTerms[1] = new WeightedSpanTerm(1f, "kennedy");
        positionSpans = new ArrayList();
        positionSpans.add(new PositionSpan(14, 14));
        wTerms[1].addPositionSpans(positionSpans);

        Highlighter highlighter = getHighlighter(wTerms, HighlighterTest.this);// new
        // Highlighter(new
        // QueryScorer(wTerms));
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

      public void run() throws Exception {
        HashMap synonyms = new HashMap();
        synonyms.put("football", "soccer,footie");
        Analyzer analyzer = new SynonymAnalyzer(synonyms);
        String srchkey = "football";

        String s = "football-soccer in the euro 2004 footie competition";
        QueryParser parser = new QueryParser("bookid", analyzer);
        Query query = parser.parse(srchkey);

        TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));
        Highlighter highlighter = getHighlighter(query, null, tokenStream, HighlighterTest.this);

        // Get 3 best fragments and seperate with a "..."
        tokenStream = analyzer.tokenStream(null, new StringReader(s));
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

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("Kennedy");
        // new Highlighter(HighlighterTest.this, new QueryScorer(query));

        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
              HighlighterTest.this);
          String result = highlighter.getBestFragment(tokenStream, text);
          System.out.println("\t" + result);
        }
        assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
            numHighlights == 4);
      }
    };
    helper.start();
  }

  public void testGetTextFragments() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {

        doSearching("Kennedy");

        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));

          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
              HighlighterTest.this);// new Highlighter(this, new
          // QueryScorer(query));
          highlighter.setTextFragmenter(new SimpleFragmenter(20));
          String stringResults[] = highlighter.getBestFragments(tokenStream, text, 10);

          tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          TextFragment fragmentResults[] = highlighter.getBestTextFragments(tokenStream, text,
              true, 10);

          assertTrue("Failed to find correct number of text Fragments: " + fragmentResults.length
              + " vs " + stringResults.length, fragmentResults.length == stringResults.length);
          for (int j = 0; j < stringResults.length; j++) {
            System.out.println(fragmentResults[j]);
            assertTrue("Failed to find same text Fragments: " + fragmentResults[j] + " found",
                fragmentResults[j].toString().equals(stringResults[j]));

          }

        }
      }
    };
    helper.start();
  }

  public void testMaxSizeHighlight() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        doSearching("meat");
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(texts[0]));
        Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
            HighlighterTest.this);// new Highlighter(this, new
        // QueryScorer(query));
        highlighter.setMaxDocBytesToAnalyze(30);

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

      public void run() throws Exception {
        String goodWord = "goodtoken";
        String stopWords[] = { "stoppedtoken" };

        TermQuery query = new TermQuery(new Term("data", goodWord));

        String match = null;
        StringBuffer sb = new StringBuffer();
        sb.append(goodWord);
        for (int i = 0; i < 10000; i++) {
          sb.append(" ");
          sb.append(stopWords[0]);
        }
        SimpleHTMLFormatter fm = new SimpleHTMLFormatter();
        Highlighter hg = getHighlighter(query, "data", new StandardAnalyzer(stopWords).tokenStream(
            "data", new StringReader(sb.toString())), fm);// new Highlighter(fm,
        // new
        // QueryScorer(query));
        hg.setTextFragmenter(new NullFragmenter());
        hg.setMaxDocBytesToAnalyze(100);
        match = hg.getBestFragment(new StandardAnalyzer(stopWords), "data", sb.toString());
        assertTrue("Matched text should be no more than 100 chars in length ", match.length() < hg
            .getMaxDocBytesToAnalyze());

        // add another tokenized word to the overrall length - but set way
        // beyond
        // the length of text under consideration (after a large slug of stop
        // words
        // + whitespace)
        sb.append(" ");
        sb.append(goodWord);
        match = hg.getBestFragment(new StandardAnalyzer(stopWords), "data", sb.toString());
        assertTrue("Matched text should be no more than 100 chars in length ", match.length() < hg
            .getMaxDocBytesToAnalyze());
      }
    };

    helper.start();

  }
  
  public void testMaxSizeEndHighlight() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {
      public void run() throws Exception {
        String stopWords[] = { "in", "it" };
        TermQuery query = new TermQuery(new Term("text", "searchterm"));

        String text = "this is a text with searchterm in it";
        SimpleHTMLFormatter fm = new SimpleHTMLFormatter();
        Highlighter hg = getHighlighter(query, "text", new StandardAnalyzer(
            stopWords).tokenStream("text", new StringReader(text)), fm);
        hg.setTextFragmenter(new NullFragmenter());
        hg.setMaxDocCharsToAnalyze(36);
        String match = hg.getBestFragment(new StandardAnalyzer(stopWords), "text", text);
        assertTrue(
            "Matched text should contain remainder of text after highlighted query ",
            match.endsWith("in it"));
      }
    };
    helper.start();
  }

  public void testUnRewrittenQuery() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        numHighlights = 0;
        // test to show how rewritten query can still be used
        searcher = new IndexSearcher(ramDir);
        Analyzer analyzer = new StandardAnalyzer();

        QueryParser parser = new QueryParser(FIELD_NAME, analyzer);
        Query query = parser.parse("JF? or Kenned*");
        System.out.println("Searching with primitive query");
        // forget to set this and...
        // query=query.rewrite(reader);
        Hits hits = searcher.search(query);

        // create an instance of the highlighter with the tags used to surround
        // highlighted text
        // QueryHighlightExtractor highlighter = new
        // QueryHighlightExtractor(this,
        // query, new StandardAnalyzer());

        int maxNumFragmentsRequired = 3;

        for (int i = 0; i < hits.length(); i++) {
          String text = hits.doc(i).get(FIELD_NAME);
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
              HighlighterTest.this);
          highlighter.setTextFragmenter(new SimpleFragmenter(40));
          String highlightedText = highlighter.getBestFragments(tokenStream, text,
              maxNumFragmentsRequired, "...");
          System.out.println(highlightedText);
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

      public void run() throws Exception {
        doSearching("AnInvalidQueryWhichShouldYieldNoResults");

        for (int i = 0; i < texts.length; i++) {
          String text = texts[i];
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
          Highlighter highlighter = getHighlighter(query, FIELD_NAME, tokenStream,
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
   * 
   * @throws Exception
   */
  public void testEncoding() throws Exception {

    String rawDocContent = "\"Smith & sons' prices < 3 and >4\" claims article";
    // run the highlighter on the raw content (scorer does not score any tokens
    // for
    // highlighting but scores a single fragment for selection
    Highlighter highlighter = new Highlighter(this, new SimpleHTMLEncoder(), new Scorer() {
      public void startFragment(TextFragment newFragment) {
      }

      public float getTokenScore(Token token) {
        return 0;
      }

      public float getFragmentScore() {
        return 1;
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
    org.w3c.dom.Document doc = db.parse(new ByteArrayInputStream(xhtml.getBytes()));
    Element root = doc.getDocumentElement();
    NodeList nodes = root.getElementsByTagName("body");
    Element body = (Element) nodes.item(0);
    nodes = body.getElementsByTagName("h2");
    Element h2 = (Element) nodes.item(0);
    String decodedSnippet = h2.getFirstChild().getNodeValue();
    assertEquals("XHTML Encoding should have worked:", rawDocContent, decodedSnippet);
  }

  public void testMultiSearcher() throws Exception {
    // setup index 1
    RAMDirectory ramDir1 = new RAMDirectory();
    IndexWriter writer1 = new IndexWriter(ramDir1, new StandardAnalyzer(), true);
    Document d = new Document();
    Field f = new Field(FIELD_NAME, "multiOne", Field.Store.YES, Field.Index.ANALYZED);
    d.add(f);
    writer1.addDocument(d);
    writer1.optimize();
    writer1.close();
    IndexReader reader1 = IndexReader.open(ramDir1);

    // setup index 2
    RAMDirectory ramDir2 = new RAMDirectory();
    IndexWriter writer2 = new IndexWriter(ramDir2, new StandardAnalyzer(), true);
    d = new Document();
    f = new Field(FIELD_NAME, "multiTwo", Field.Store.YES, Field.Index.ANALYZED);
    d.add(f);
    writer2.addDocument(d);
    writer2.optimize();
    writer2.close();
    IndexReader reader2 = IndexReader.open(ramDir2);

    IndexSearcher searchers[] = new IndexSearcher[2];
    searchers[0] = new IndexSearcher(ramDir1);
    searchers[1] = new IndexSearcher(ramDir2);
    MultiSearcher multiSearcher = new MultiSearcher(searchers);
    QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
    query = parser.parse("multi*");
    System.out.println("Searching for: " + query.toString(FIELD_NAME));
    // at this point the multisearcher calls combine(query[])
    hits = multiSearcher.search(query);

    // query = QueryParser.parse("multi*", FIELD_NAME, new StandardAnalyzer());
    Query expandedQueries[] = new Query[2];
    expandedQueries[0] = query.rewrite(reader1);
    expandedQueries[1] = query.rewrite(reader2);
    query = query.combine(expandedQueries);

    // create an instance of the highlighter with the tags used to surround
    // highlighted text
    Highlighter highlighter = new Highlighter(this, new QueryScorer(query));

    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(text));
      String highlightedText = highlighter.getBestFragment(tokenStream, text);
      System.out.println(highlightedText);
    }
    assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
        numHighlights == 2);

  }

  public void testFieldSpecificHighlighting() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        String docMainText = "fred is one of the people";
        QueryParser parser = new QueryParser(FIELD_NAME, analyzer);
        Query query = parser.parse("fred category:people");

        // highlighting respects fieldnames used in query

        Scorer fieldSpecificScorer = null;
        if (mode == this.SPAN) {
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(docMainText));
          CachingTokenFilter ctf = new CachingTokenFilter(tokenStream);
          fieldSpecificScorer = new SpanScorer(query, FIELD_NAME, ctf);
        } else if (mode == this.STANDARD) {
          fieldSpecificScorer = new QueryScorer(query, "contents");
        }
        Highlighter fieldSpecificHighlighter = new Highlighter(new SimpleHTMLFormatter(),
            fieldSpecificScorer);
        fieldSpecificHighlighter.setTextFragmenter(new NullFragmenter());
        String result = fieldSpecificHighlighter.getBestFragment(analyzer, FIELD_NAME, docMainText);
        assertEquals("Should match", result, "<B>fred</B> is one of the people");

        // highlighting does not respect fieldnames used in query
        Scorer fieldInSpecificScorer = null;
        if (mode == this.SPAN) {
          TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME, new StringReader(docMainText));
          CachingTokenFilter ctf = new CachingTokenFilter(tokenStream);
          fieldInSpecificScorer = new SpanScorer(query, null, ctf);
        } else if (mode == this.STANDARD) {
          fieldInSpecificScorer = new QueryScorer(query);
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
      Iterator iter;
      List lst;
      {
        lst = new ArrayList();
        Token t;
        t = createToken("hi", 0, 2);
        lst.add(t);
        t = createToken("hispeed", 0, 8);
        lst.add(t);
        t = createToken("speed", 3, 8);
        t.setPositionIncrement(0);
        lst.add(t);
        t = createToken("10", 8, 10);
        lst.add(t);
        t = createToken("foo", 11, 14);
        lst.add(t);
        iter = lst.iterator();
      }

      public Token next(final Token reusableToken) throws IOException {
        assert reusableToken != null;
        return iter.hasNext() ? (Token) iter.next() : null;
      }
    };
  }

  // same token-stream as above, but the bigger token comes first this time
  protected TokenStream getTS2a() {
    // String s = "Hi-Speed10 foo";
    return new TokenStream() {
      Iterator iter;
      List lst;
      {
        lst = new ArrayList();
        Token t;
        t = createToken("hispeed", 0, 8);
        lst.add(t);
        t = createToken("hi", 0, 2);
        t.setPositionIncrement(0);
        lst.add(t);
        t = createToken("speed", 3, 8);
        lst.add(t);
        t = createToken("10", 8, 10);
        lst.add(t);
        t = createToken("foo", 11, 14);
        lst.add(t);
        iter = lst.iterator();
      }

      public Token next(final Token reusableToken) throws IOException {
        assert reusableToken != null;
        return iter.hasNext() ? (Token) iter.next() : null;
      }
    };
  }

  public void testOverlapAnalyzer2() throws Exception {
    TestHighlightRunner helper = new TestHighlightRunner() {

      public void run() throws Exception {
        String s = "Hi-Speed10 foo";

        Query query;
        Highlighter highlighter;
        String result;

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("foo");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-Speed10 <B>foo</B>", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("10");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-Speed<B>10</B> foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hi");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi</B>-Speed10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("speed");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("Hi-<B>Speed</B>10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hispeed");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hi speed");
        highlighter = getHighlighter(query, "text", getTS2(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        // ///////////////// same tests, just put the bigger overlapping token
        // first
        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("foo");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-Speed10 <B>foo</B>", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("10");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-Speed<B>10</B> foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hi");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi</B>-Speed10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("speed");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("Hi-<B>Speed</B>10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hispeed");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);

        query = new QueryParser("text", new WhitespaceAnalyzer()).parse("hi speed");
        highlighter = getHighlighter(query, "text", getTS2a(), HighlighterTest.this);
        result = highlighter.getBestFragments(getTS2a(), s, 3, "...");
        assertEquals("<B>Hi-Speed</B>10 foo", result);
      }
    };

    helper.start();
  }
  
  private Directory dir = new RAMDirectory();
  private Analyzer a = new WhitespaceAnalyzer();
  
  public void testWeightedTermsWithDeletes() throws IOException, ParseException {
    makeIndex();
    deleteDocument();
    searchIndex();
  }
  
  private Document doc( String f, String v ){
    Document doc = new Document();
    doc.add( new Field( f, v, Store.YES, Index.ANALYZED ) );
    return doc;
  }
  
  private void makeIndex() throws IOException {
    IndexWriter writer = new IndexWriter( dir, a, MaxFieldLength.LIMITED );
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field del" ) );
    writer.addDocument( doc( "t_text1", "random words for highlighting tests del" ) );
    writer.addDocument( doc( "t_text1", "more random words for second field" ) );
    writer.optimize();
    writer.close();
  }
  
  private void deleteDocument() throws IOException {
    IndexWriter writer = new IndexWriter( dir, a, false, MaxFieldLength.LIMITED );
    writer.deleteDocuments( new Term( "t_text1", "del" ) );
    // To see negative idf, keep comment the following line
    //writer.optimize();
    writer.close();
  }
  
  private void searchIndex() throws IOException, ParseException {
    String q = "t_text1:random";
    QueryParser parser = new QueryParser( "t_text1", a );
    Query query = parser.parse( q );
    IndexSearcher searcher = new IndexSearcher( dir );
    // This scorer can return negative idf -> null fragment
    Scorer scorer = new QueryScorer( query, searcher.getIndexReader(), "t_text1" );
    // This scorer doesn't use idf (patch version)
    //Scorer scorer = new QueryScorer( query, "t_text1" );
    Highlighter h = new Highlighter( scorer );

    TopDocs hits = searcher.search(query, null, 10);
    for( int i = 0; i < hits.totalHits; i++ ){
      Document doc = searcher.doc( hits.scoreDocs[i].doc );
      String result = h.getBestFragment( a, "t_text1", doc.get( "t_text1" ));
      System.out.println("result:" +  result);
      assertEquals("more <B>random</B> words for second field", result);
    }
    searcher.close();
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
   * IndexReader.open(ramDir);
   * 
   * IndexSearcher searcher=new IndexSearcher(reader); query =
   * QueryParser.parse("abc", FIELD_NAME, bigramAnalyzer);
   * System.out.println("Searching for: " + query.toString(FIELD_NAME)); hits =
   * searcher.search(query);
   * 
   * Highlighter highlighter = new Highlighter(this,new
   * QueryFragmentScorer(query));
   * 
   * for (int i = 0; i < hits.length(); i++) { String text =
   * hits.doc(i).get(FIELD_NAME); TokenStream
   * tokenStream=bigramAnalyzer.tokenStream(FIELD_NAME,new StringReader(text));
   * String highlightedText = highlighter.getBestFragment(tokenStream,text);
   * System.out.println(highlightedText); } }
   */

  public String highlightTerm(String originalText, TokenGroup group) {
    if (group.getTotalScore() <= 0) {
      return originalText;
    }
    numHighlights++; // update stats used in assertions
    return "<B>" + originalText + "</B>";
  }

  public void doSearching(String queryString) throws Exception {
    QueryParser parser = new QueryParser(FIELD_NAME, new StandardAnalyzer());
    query = parser.parse(queryString);
    doSearching(query);
  }

  public void doSearching(Query unReWrittenQuery) throws Exception {
    searcher = new IndexSearcher(ramDir);
    // for any multi-term queries to work (prefix, wildcard, range,fuzzy etc)
    // you must use a rewritten query!
    query = unReWrittenQuery.rewrite(reader);
    System.out.println("Searching for: " + query.toString(FIELD_NAME));
    hits = searcher.search(query);
  }

  public void assertExpectedHighlightCount(final int maxNumFragmentsRequired,
      final int expectedHighlights) throws Exception {
    for (int i = 0; i < hits.length(); i++) {
      String text = hits.doc(i).get(FIELD_NAME);
      CachingTokenFilter tokenStream = new CachingTokenFilter(analyzer.tokenStream(FIELD_NAME,
          new StringReader(text)));
      Highlighter highlighter = new Highlighter(this,
          new SpanScorer(query, FIELD_NAME, tokenStream));
      highlighter.setTextFragmenter(new SimpleFragmenter(40));
      tokenStream.reset();

      String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
          "...");
      System.out.println("\t" + result);

      assertTrue("Failed to find correct number of highlights " + numHighlights + " found",
          numHighlights == expectedHighlights);
    }
  }

  /*
   * @see TestCase#setUp()
   */
  protected void setUp() throws Exception {
    ramDir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(ramDir, new StandardAnalyzer(), true);
    for (int i = 0; i < texts.length; i++) {
      addDoc(writer, texts[i]);
    }

    writer.optimize();
    writer.close();
    reader = IndexReader.open(ramDir);
    numHighlights = 0;
  }

  private void addDoc(IndexWriter writer, String text) throws IOException {
    Document d = new Document();
    Field f = new Field(FIELD_NAME, text, Field.Store.YES, Field.Index.ANALYZED);
    d.add(f);
    writer.addDocument(d);

  }

  /*
   * @see TestCase#tearDown()
   */
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  private static Token createToken(String term, int start, int offset)
  {
    Token token = new Token(start, offset);
    token.setTermBuffer(term);
    return token;
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

class SynonymAnalyzer extends Analyzer {
  private Map synonyms;

  public SynonymAnalyzer(Map synonyms) {
    this.synonyms = synonyms;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.lucene.analysis.Analyzer#tokenStream(java.lang.String,
   *      java.io.Reader)
   */
  public TokenStream tokenStream(String arg0, Reader arg1) {
    return new SynonymTokenizer(new LowerCaseTokenizer(arg1), synonyms);
  }
}

/**
 * Expands a token stream with synonyms (TODO - make the synonyms analyzed by choice of analyzer)
 *
 */
class SynonymTokenizer extends TokenStream {
  private TokenStream realStream;
  private Token currentRealToken = null;
  private Map synonyms;
  StringTokenizer st = null;

  public SynonymTokenizer(TokenStream realStream, Map synonyms) {
    this.realStream = realStream;
    this.synonyms = synonyms;
  }

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    if (currentRealToken == null) {
      Token nextRealToken = realStream.next(reusableToken);
      if (nextRealToken == null) {
        return null;
      }
      String expansions = (String) synonyms.get(nextRealToken.term());
      if (expansions == null) {
        return nextRealToken;
      }
      st = new StringTokenizer(expansions, ",");
      if (st.hasMoreTokens()) {
        currentRealToken = (Token) nextRealToken.clone();
      }
      return currentRealToken;
    } else {
      reusableToken.reinit(st.nextToken(),
                           currentRealToken.startOffset(),
                           currentRealToken.endOffset());
      reusableToken.setPositionIncrement(0);
      if (!st.hasMoreTokens()) {
        currentRealToken = null;
        st = null;
      }
      return reusableToken;
    }
  }

  static abstract class TestHighlightRunner {
    static final int STANDARD = 0;
    static final int SPAN = 1;
    int mode = STANDARD;

    public Highlighter getHighlighter(Query query, String fieldName, TokenStream stream,
        Formatter formatter) {
      if (mode == STANDARD) {
        return new Highlighter(formatter, new QueryScorer(query));
      } else if (mode == SPAN) {
        CachingTokenFilter tokenStream = new CachingTokenFilter(stream);
        Highlighter highlighter;
        try {
          highlighter = new Highlighter(formatter, new SpanScorer(query, fieldName, tokenStream));
          tokenStream.reset();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        return highlighter;
      } else {
        throw new RuntimeException("Unknown highlight mode");
      }
    }

    Highlighter getHighlighter(WeightedTerm[] weightedTerms, Formatter formatter) {
      if (mode == STANDARD) {
        return new Highlighter(formatter, new QueryScorer(weightedTerms));
      } else if (mode == SPAN) {
        Highlighter highlighter;

        highlighter = new Highlighter(formatter, new SpanScorer((WeightedSpanTerm[]) weightedTerms));

        return highlighter;
      } else {
        throw new RuntimeException("Unknown highlight mode");
      }
    }

    void doStandardHighlights(Analyzer analyzer, Hits hits, Query query, Formatter formatter)
        throws Exception {

      for (int i = 0; i < hits.length(); i++) {
        String text = hits.doc(i).get(HighlighterTest.FIELD_NAME);
        int maxNumFragmentsRequired = 2;
        String fragmentSeparator = "...";
        Scorer scorer = null;
        TokenStream tokenStream = null;
        if (mode == SPAN) {
          tokenStream = new CachingTokenFilter(analyzer.tokenStream(HighlighterTest.FIELD_NAME,
              new StringReader(text)));
          scorer = new SpanScorer(query, HighlighterTest.FIELD_NAME,
              (CachingTokenFilter) tokenStream);
        } else if (mode == STANDARD) {
          scorer = new QueryScorer(query);
          tokenStream = analyzer.tokenStream(HighlighterTest.FIELD_NAME, new StringReader(text));
        }
        Highlighter highlighter = new Highlighter(formatter, scorer);
        if (mode == SPAN) {
          ((CachingTokenFilter) tokenStream).reset();
        }
        highlighter.setTextFragmenter(new SimpleFragmenter(20));

        String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired,
            fragmentSeparator);
        System.out.println("\t" + result);
      }
    }

    abstract void run() throws Exception;

    void start() throws Exception {
      System.out.println("Run standard");
      run();
      System.out.println("Run span");
      mode = SPAN;
      run();
    }
  }
}
