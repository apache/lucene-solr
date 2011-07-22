package org.apache.lucene.search.poshighlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.codecs.CoreCodecProvider;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.positions.PositionFilterQuery;
import org.apache.lucene.search.positions.TestBlockPositionsIterator.BlockPositionIteratorFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * TODO: 
 * Phrase and Span Queries
 * positions callback API
 */
public class PosHighlighterTest extends LuceneTestCase {
  
  protected final static String F="f";
  protected Analyzer analyzer;
  protected Directory dir;
  protected IndexSearcher searcher;
  
  private static final String PORRIDGE_VERSE = 
    "Pease porridge hot! Pease porridge cold! Pease porridge in the pot nine days old! Some like it hot, some"
    + " like it cold, Some like it in the pot nine days old! Pease porridge hot! Pease porridge cold!";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random, MockTokenizer.WHITESPACE, false);
    dir = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    if( searcher != null ){
      searcher.close();
      searcher = null;
    }
    dir.close();
    super.tearDown();
  }
  
  // make several docs
  protected void insertDocs (Analyzer analyzer, String... values) throws Exception {
    IndexWriterConfig config = new IndexWriterConfig(
        TEST_VERSION_CURRENT, analyzer).setOpenMode(OpenMode.CREATE);
    config.setCodecProvider(new CoreCodecProvider());
    config.getCodecProvider().setDefaultFieldCodec("Standard");
    IndexWriter writer = new IndexWriter(dir, config);
    if (!writer.getConfig().getCodecProvider().getFieldCodec(F).equals("Standard")) {
      System.out.println ("codec=" + writer.getConfig().getCodecProvider().getFieldCodec(F));
      writer.getConfig().getCodecProvider().setFieldCodec(F, "Standard");
    }
    for( String value: values ) {
      Document doc = new Document();
      Field f = new Field (F, value, Store.YES, Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS);
      doc.add (f);
      writer.addDocument( doc );
    }
    writer.close();
    if (searcher != null) searcher.close();
    searcher = new IndexSearcher( dir, true );
  }
  
  private String[] doSearch(Query q) throws IOException, InvalidTokenOffsetsException {
    return doSearch(q, 100);
  }
  
  private class ConstantScorer implements org.apache.lucene.search.highlight.Scorer {

    @Override
    public TokenStream init(TokenStream tokenStream) throws IOException {
      return tokenStream;
    }

    @Override
    public void startFragment(TextFragment newFragment) {
    }

    @Override
    public float getTokenScore() {
      return 1;
    }

    @Override
    public float getFragmentScore() {
      return 1;
    }    
  }
  
  private String[] doSearch(Query q, int maxFragSize) throws IOException, InvalidTokenOffsetsException {
    return doSearch (q, maxFragSize, 0);
  }
  private String[] doSearch(Query q, int maxFragSize, int docIndex) throws IOException, InvalidTokenOffsetsException {
    // ConstantScorer is a fragment Scorer, not a search result (document) Scorer
    Highlighter highlighter = new Highlighter (new ConstantScorer());
    highlighter.setTextFragmenter(new SimpleFragmenter(maxFragSize));
    PosCollector collector = new PosCollector(10);
    if (q instanceof MultiTermQuery) {
      ((MultiTermQuery)q).setRewriteMethod (MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    }
    searcher.search(q, collector);
    ScorePosDoc doc = collector.docs[docIndex];
    if (doc == null)
      return null;
    String text = searcher.getIndexReader().document(doc.doc).getFieldable(F).stringValue();
    PositionOffsetMapper pom = new PositionOffsetMapper ();
    // FIXME: test error cases: for non-stored fields, and fields w/no term vectors
    searcher.getIndexReader().getTermFreqVector(doc.doc, F, pom);
    
    TextFragment[] fragTexts = highlighter.getBestTextFragments(new PosTokenStream
        (text, new PositionIntervalArrayIterator(doc.sortedPositions(), doc.posCount), pom), 
        text, false, 10);
    String[] frags = new String[fragTexts.length];
    for (int i = 0; i < frags.length; i++)
      frags[i] = fragTexts[i].toString();
    return frags;
  }
  
  public void testTerm () throws Exception {
    insertDocs(analyzer, "This is a test test");
    String frags[] = doSearch (new TermQuery(new Term(F, "test")));
    assertEquals ("This is a <B>test</B> <B>test</B>", frags[0]);
  }
  
  public void testSeveralSnippets () throws Exception {
    String input = "this is some long text.  It has the word long in many places.  In fact, it has long on some different fragments.  " +
    "Let us see what happens to long in this case.";
    String gold = "this is some <B>long</B> text.  It has the word <B>long</B> in many places.  In fact, it has <B>long</B> on some different fragments.  " +
    "Let us see what happens to <B>long</B> in this case.";
    insertDocs(analyzer, input);
    String frags[] = doSearch (new TermQuery(new Term(F, "long")), input.length());
    assertEquals (gold, frags[0]);
  }
  
  public void testBooleanAnd () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.MUST));
    String frags[] = doSearch (bq);
    assertEquals ("<B>This</B> is a <B>test</B>", frags[0]);    
  }
  
  public void testBooleanAndOtherOrder () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.MUST));
    String frags[] = doSearch (bq);
    assertEquals ("<B>This</B> is a <B>test</B>", frags[0]);    
  }

  public void testBooleanOr () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.SHOULD));
    String frags[] = doSearch (bq);
    assertEquals ("<B>This</B> is a <B>test</B>", frags[0]);    
  }
  
  public void testSingleMatchScorer () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "notoccurringterm")), Occur.SHOULD));
    String frags[] = doSearch (bq);
    assertEquals ("This is a <B>test</B>", frags[0]);    
  }
  
  public void testBooleanNrShouldMatch () throws Exception {
    insertDocs(analyzer, "a b c d e f g h i");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "a")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "b")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "no")), Occur.SHOULD));
    
    // This generates a ConjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    String frags[] = doSearch (bq);
    assertEquals ("<B>a</B> <B>b</B> c d e f g h i", frags[0]);
    
    // This generates no scorer
    bq.setMinimumNumberShouldMatch(3);
    frags = doSearch (bq);
    assertNull (frags);
    
    // This generates a DisjunctionSumScorer
    bq.setMinimumNumberShouldMatch(2);
    bq.add(new BooleanClause (new TermQuery(new Term(F, "c")), Occur.SHOULD));
    frags = doSearch (bq);
    assertEquals ("<B>a</B> <B>b</B> <B>c</B> d e f g h i", frags[0]);
  }
  
  public void testPhrase() throws Exception {
    insertDocs(analyzer, "is it that this is a test, is it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "is")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "a")), Occur.MUST));
    PositionFilterQuery pfq = new PositionFilterQuery(bq, new BlockPositionIteratorFilter());
    String frags[] = doSearch (pfq);
    // make sure we highlight the phrase, and not the terms outside the phrase
    assertEquals ("is it that this <B>is</B> <B>a</B> test, is it", frags[0]);
  }
  
  /*
   * Failing ... PhraseQuery scorer needs positions()?
   */
  public void testPhraseOriginal() throws Exception {
    insertDocs(analyzer, "This is a test");
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(F, "a"));
    pq.add(new Term(F, "test"));
    String frags[] = doSearch (pq);
    assertEquals ("This is <B>a</B> <B>test</B>", frags[0]);
  }
  
  public void testNestedBoolean () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.SHOULD));
    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.SHOULD));
    bq2.add(new BooleanClause (new TermQuery(new Term(F, "is")), Occur.SHOULD));
    bq.add(new BooleanClause(bq2, Occur.SHOULD));
    String frags[] = doSearch (bq);
    assertEquals ("<B>This</B> <B>is</B> a <B>test</B>", frags[0]);
  }
  
  public void testWildcard () throws Exception {
    insertDocs(analyzer, "This is a test");
    String frags[] = doSearch (new WildcardQuery(new Term(F, "t*t")));
    assertEquals ("This is a <B>test</B>", frags[0]);
  }
  
  public void testMultipleDocumentsAnd() throws Exception {
    insertDocs(analyzer, 
        "This document has no matches", 
        PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "Pease")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "porridge")), Occur.MUST));
    String frags[] = doSearch (bq, 50, 0);
    assertEquals ("<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>", frags[0]);
    frags = doSearch (bq, 50, 1);
    assertEquals ("This document has some <B>Pease</B> <B>porridge</B> in it", frags[0]);
  }
  
  /*
   * Failing: need positions callback API since DisjunctionSumScorer consumes all of a doc's
   * positions before passing the doc to the collector.
   */
  public void testMultipleDocumentsOr() throws Exception {
    insertDocs(analyzer, 
        "This document has no matches", 
        PORRIDGE_VERSE,
        "This document has some Pease porridge in it");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "Pease")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "porridge")), Occur.SHOULD));
    String frags[] = doSearch (bq, 50, 0);
    assertEquals ("<B>Pease</B> <B>porridge</B> hot! <B>Pease</B> <B>porridge</B> cold! <B>Pease</B>", frags[0]);
    frags = doSearch (bq, 50, 1);
    assertEquals ("This document has some <B>Pease</B> <B>porridge</B> in it", frags[0]);
  }

}
