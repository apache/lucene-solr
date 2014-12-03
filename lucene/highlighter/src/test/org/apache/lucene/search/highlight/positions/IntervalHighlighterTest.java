package org.apache.lucene.search.highlight.positions;
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

import java.io.IOException;
import java.io.StringReader;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.posfilter.NonOverlappingQuery;
import org.apache.lucene.search.posfilter.OrderedNearQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

@Seed("2C0AB6BC65255FAA")
public class IntervalHighlighterTest extends LuceneTestCase {
  
  protected final static String F = "f";
  protected Directory dir;
  protected IndexSearcher searcher;

  private static final String PORRIDGE_VERSE = "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
      + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!";
  
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }
  
  public void close() throws IOException {
    if (searcher != null) {
      searcher.getIndexReader().close();
      searcher = null;
    }
    dir.close();
  }
  
  // make several docs
  protected void insertDocs(String... values)
      throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setOpenMode(OpenMode.CREATE);
    IndexWriter writer = new IndexWriter(dir, iwc);
    FieldType type = new FieldType();
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    type.setStored(true);
    for (String value : values) {
      Document doc = new Document();
      Field f = newField(F, value, type);
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.close();
    if (searcher != null) {
      searcher.getIndexReader().close();
    }
    searcher = new IndexSearcher(DirectoryReader.open(dir));
  }

  protected static TermQuery termQuery(String term) {
    return new TermQuery(new Term(F, term));
  }
  
  private String[] doSearch(Query q) throws IOException,
      InvalidTokenOffsetsException {
    return doSearch(q, 100);
  }
  
  private class ConstantScorer implements
      org.apache.lucene.search.highlight.Scorer {
    
    @Override
    public TokenStream init(TokenStream tokenStream) throws IOException {
      return tokenStream;
    }
    
    @Override
    public void startFragment(TextFragment newFragment) {}
    
    @Override
    public float getTokenScore() {
      return 1;
    }
    
    @Override
    public float getFragmentScore() {
      return 1;
    }
  }

  private String getHighlight(Query q) throws IOException, InvalidTokenOffsetsException {
    return doSearch(q, Integer.MAX_VALUE)[0];
  }
  
  private String[] doSearch(Query q, int maxFragSize) throws IOException,
      InvalidTokenOffsetsException {
    return doSearch(q, maxFragSize, 0);
  }
  private String[] doSearch(Query q, int maxFragSize, int docIndex) throws IOException, InvalidTokenOffsetsException {
    return doSearch(q, maxFragSize, docIndex, false);
  }
  private String[] doSearch(Query q, int maxFragSize, int docIndex, boolean analyze)
      throws IOException, InvalidTokenOffsetsException {
    // ConstantScorer is a fragment Scorer, not a search result (document)
    // Scorer
    Highlighter highlighter = new Highlighter(new ConstantScorer());
    highlighter.setTextFragmenter(new SimpleFragmenter(maxFragSize));
    HighlightingIntervalCollector collector = new HighlightingIntervalCollector(10);
    if (q instanceof MultiTermQuery) {
      ((MultiTermQuery) q)
          .setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    }
    searcher.search(q, collector);
    DocAndPositions doc = collector.docs[docIndex];
    if (doc == null) return null;
    String text = searcher.getIndexReader().document(doc.doc).get(F);
    // FIXME: test error cases: for non-stored fields, and fields w/no term
    // vectors
    // searcher.getIndexReader().getTermFreqVector(doc.doc, F, pom);
    final TokenStream stream;
    if (analyze) {
      stream = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true,
          MockTokenFilter.EMPTY_STOPSET).tokenStream(F,
          new StringReader(text));
    } else {
      stream = new IntervalTokenStream(text, doc.sortedPositions());
    }
    //
    TextFragment[] fragTexts = highlighter.getBestTextFragments(
         stream , text, false, 10);
    String[] frags = new String[fragTexts.length];
    for (int i = 0; i < frags.length; i++)
      frags[i] = fragTexts[i].toString();
    return frags;
  }
  
  public void testTerm() throws Exception {
    insertDocs("This is a test test");
    String frags[] = doSearch(termQuery("test"));
    assertEquals("This is a <B>test</B> <B>test</B>", frags[0]);
    close();
  }
  
  public void testSeveralSnippets() throws Exception {
    String input = "this is some long text.  It has the word long in many places.  In fact, it has long on some different fragments.  "
        + "Let us see what happens to long in this case.";
    String gold = "this is some <B>long</B> text.  It has the word <B>long</B> in many places.  In fact, it has <B>long</B> on some different fragments.  "
        + "Let us see what happens to <B>long</B> in this case.";
    insertDocs(input);
    String frags[] = doSearch(termQuery("long"), input.length());
    assertEquals(gold, frags[0]);
    close();
  }
  
  public void testBooleanAnd() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(termQuery("This"), Occur.MUST));
    bq.add(new BooleanClause(termQuery("test"), Occur.MUST));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testConstantScore() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(termQuery("This"), Occur.MUST));
    bq.add(new BooleanClause(termQuery("test"), Occur.MUST));
    String frags[] = doSearch(new ConstantScoreQuery(bq));
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanAndOtherOrder() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.MUST));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanOr() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testSingleMatchScorer() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "notoccurringterm")),
        Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("This is a <B>test</B>", frags[0]);
    close();
  }
  
  public void testBooleanNrShouldMatch() throws Exception {
    insertDocs("a b c d e f g h i");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "a")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "b")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "no")), Occur.SHOULD));
    
    // This generates a ConjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    String frags[] = doSearch(bq);
    assertEquals("<B>a</B> <B>b</B> c d e f g h i", frags[0]);
    
    // This generates no scorer
    bq.setMinimumNumberShouldMatch(3);
    frags = doSearch(bq);
    assertNull(frags);
    
    // This generates a DisjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    bq.add(new BooleanClause(new TermQuery(new Term(F, "c")), Occur.SHOULD));
    frags = doSearch(bq);
    assertEquals("<B>a</B> <B>b</B> <B>c</B> d e f g h i", frags[0]);
    close();
  }
  
  public void testPhrase() throws Exception {
    insertDocs("is it that this is a test, is it");
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(F, "is"));
    pq.add(new Term(F, "a"));
    String frags[] = doSearch(pq);
    // make sure we highlight the phrase, and not the terms outside the phrase
    assertEquals("is it that this <B>is</B> <B>a</B> test, is it", frags[0]);
    close();
  }
  
  /*
   * Failing ... PhraseQuery scorer needs positions()?
   */
  //@Ignore
  public void testPhraseOriginal() throws Exception {
    insertDocs("This is a test");
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(F, "a"));
    pq.add(new Term(F, "test"));
    String frags[] = doSearch(pq);
    assertEquals("This is <B>a</B> <B>test</B>", frags[0]);
    close();
  }
  
  public void testNestedBoolean() throws Exception {
    insertDocs("This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new BooleanClause(new TermQuery(new Term(F, "This")), Occur.SHOULD));
    bq2.add(new BooleanClause(new TermQuery(new Term(F, "is")), Occur.SHOULD));
    bq.add(new BooleanClause(bq2, Occur.SHOULD));
    String frags[] = doSearch(bq);
    assertEquals("<B>This</B> <B>is</B> a <B>test</B>", frags[0]);
    close();
  }
  
  public void testWildcard() throws Exception {
    insertDocs("This is a test");
    String frags[] = doSearch(new WildcardQuery(new Term(F, "t*t")));
    assertEquals("This is a <B>test</B>", frags[0]);
    close();
  }

  public void testMixedBooleanNot() throws Exception {
    insertDocs("this is a test", "that is an elephant");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "that")), Occur.MUST_NOT));
    String frags[] = doSearch(bq);
    assertEquals("this is a <B>test</B>", frags[0]);
    close();
  }

  public void testMixedBooleanShould() throws Exception {
    insertDocs("this is a test", "that is an elephant", "the other was a rhinoceros");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "is")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "test")), Occur.SHOULD));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals("this <B>is</B> a <B>test</B>", frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("that <B>is</B> an elephant", frags[0]);

    bq.add(new BooleanClause(new TermQuery(new Term(F, "rhinoceros")), Occur.SHOULD));
    frags = doSearch(bq, 50, 0);
    assertEquals("this <B>is</B> a <B>test</B>", frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("that <B>is</B> an elephant", frags[0]);
    close();
  }
  
  public void testMultipleDocumentsAnd() throws Exception {
    insertDocs("This document has no matches", PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "Pease")), Occur.MUST));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "porridge")), Occur.MUST));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals(
        "<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>",
        frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("This document has some <B>Pease</B> <B>porridge</B> in it",
        frags[0]);
    close();
  }
  

  public void testMultipleDocumentsOr() throws Exception {
    insertDocs("This document has no matches", PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause(new TermQuery(new Term(F, "Pease")), Occur.SHOULD));
    bq.add(new BooleanClause(new TermQuery(new Term(F, "porridge")),
        Occur.SHOULD));
    String frags[] = doSearch(bq, 50, 0);
    assertEquals(
        "<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>",
        frags[0]);
    frags = doSearch(bq, 50, 1);
    assertEquals("This document has some <B>Pease</B> <B>porridge</B> in it",
        frags[0]);
    close();
  }
  
  public void testBrouwerianQuery() throws Exception {

    insertDocs("the quick brown fox jumps over the lazy dog with the quick orange fox");

    OrderedNearQuery query = new OrderedNearQuery(1,
        new TermQuery(new Term(F, "the")), new TermQuery(new Term(F, "quick")), new TermQuery(new Term(F, "fox")));

    assertEquals(getHighlight(query),
                 "<B>the quick brown fox</B> jumps over the lazy dog with <B>the quick orange fox</B>");

    NonOverlappingQuery bq = new NonOverlappingQuery(query, new TermQuery(new Term(F, "orange")));

    assertEquals(getHighlight(bq),
                 "<B>the quick brown fox<B> jumps over the lazy dog with the quick orange fox");

    close();
  }
  
  //@Ignore("not implemented yet - unsupported")
  public void testMultiPhraseQuery() throws Exception {
    MultiPhraseQuery query = new MultiPhraseQuery();
    insertDocs("pease porridge hot but not too hot or otherwise pease porridge cold");

    query.add(terms(F, "pease"), 0);
    query.add(terms(F, "porridge"), 1);
    query.add(terms(F, "hot", "cold"), 2);
    query.setSlop(1);
    
    String[] frags = doSearch(query, Integer.MAX_VALUE);
    assertEquals("<B>pease</B> <B>porridge</B> <B>hot</B> but not too hot or otherwise <B>pease</B> <B>porridge</B> <B>cold</B>", frags[0]);

    close();
  }
  
  //@Ignore("not implemented yet - unsupported")
  public void testMultiPhraseQueryCollisions() throws Exception {
    MultiPhraseQuery query = new MultiPhraseQuery();
    insertDocs("pease porridge hot not too hot or otherwise pease porridge porridge");

    query.add(terms(F, "pease"), 0);
    query.add(terms(F, "porridge"), 1);
    query.add(terms(F, "coldasice", "porridge" ), 2);
    query.setSlop(1);
    
    String[] frags = doSearch(query, Integer.MAX_VALUE);
    assertEquals("pease porridge hot but not too hot or otherwise <B>pease</B> <B>porridge</B> <B>porridge</B>", frags[0]);

    close();
  }

  public void testNearPhraseQuery() throws Exception {

    insertDocs("pease porridge rather hot and pease porridge fairly cold");

    Query firstQ = new OrderedNearQuery(4, termQuery("pease"), termQuery("porridge"), termQuery("hot"));
    {
      String frags[] = doSearch(firstQ, Integer.MAX_VALUE);
      assertEquals("<B>pease porridge rather hot</B> and pease porridge fairly cold", frags[0]);
    }

    // near.3(near.4(pease, porridge, hot), near.4(pease, porridge, cold))
    Query q = new OrderedNearQuery(3,
                firstQ,
                new OrderedNearQuery(4, termQuery("pease"), termQuery("porridge"), termQuery("cold")));

    String frags[] = doSearch(q, Integer.MAX_VALUE);
    assertEquals("<B>pease porridge rather hot and pease porridge fairly cold</B>",
                 frags[0]);

    close();
  }

  private Term[] terms(String field, String...tokens) {
      Term[] terms = new Term[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
        terms[i] = new Term(field, tokens[i]);
      }
      return terms;
    }

  public void testSloppyPhraseQuery() throws Exception {
    assertSloppyPhrase( "a c e b d e f a b", "<B>a c e b</B> d e f <B>a b</B>", 2, "a", "b");
    assertSloppyPhrase( "a b c d a b c d e f", "a b <B>c d a</B> b c d e f", 2, "c", "a");
    assertSloppyPhrase( "Y A X B A", "Y <B>A X B A</B>", 2, "X", "A", "A");

    assertSloppyPhrase( "X A X B A","X <B>A X B A</B>", 2, "X", "A", "A"); // non overlapping minmal!!
    assertSloppyPhrase( "A A A X",null, 2, "X", "A", "A");
    assertSloppyPhrase( "A A X A",  "A <B>A X A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A X A Y B A", "A <B>A X A</B> Y B A", 2, "X", "A", "A");
    assertSloppyPhrase( "A A X", null, 2, "X", "A", "A");
    assertSloppyPhrase( "A X A", null, 1, "X", "A", "A");

    assertSloppyPhrase( "A X B A", "<B>A X B A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A X A X B A X B B A A X B A A", "A <B>A</B> <B>X</B> <B>A</B> <B>X</B> B <B>A</B> <B>X</B> B B <B>A</B> <B>A</B> <B>X</B> B <B>A</B> <B>A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A X A X B A X B B A A X B A A", "A <B>A</B> <B>X</B> <B>A</B> <B>X</B> B <B>A</B> <B>X</B> B B <B>A</B> <B>A</B> <B>X</B> B <B>A</B> <B>A</B>", 2, "X", "A", "A");

    assertSloppyPhrase( "A A X A X B A", "A <B>A</B> <B>X</B> <B>A</B> <B>X</B> B <B>A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A Y A X B A", "A A Y <B>A</B> <B>X</B> B <B>A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A Y A X B A A", "A A Y <B>A</B> <B>X</B> B <B>A</B> <B>A</B>", 2, "X", "A", "A");
    assertSloppyPhrase( "A A X A Y B A", null , 1, "X", "A", "A");
    close();
  }


  private void assertSloppyPhrase(String doc, String expected, int slop, String...query) throws Exception {
    insertDocs(doc);
    PhraseQuery pq = new PhraseQuery();
    for (String string : query) {
      pq.add(new Term(F, string));  
    }
    
    pq.setSlop(slop);
//    System.out.println(doc);
    String[] frags = doSearch(pq, 50);
    if (expected == null) {
      assertNull(frags != null ? frags[0] : "", frags);
    } else {
      assertEquals(expected, frags[0]);
    }
  }
  
}
