package org.apache.lucene.search.poshighlight;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
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
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.poshighlight.PosCollector;
import org.apache.lucene.search.poshighlight.PosHighlighter;
import org.apache.lucene.search.positions.OrderedConjunctionPositionIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.WithinPositionIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionIntervalFilter;
import org.apache.lucene.search.spans.MockSpanQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Ignore;
/**
 * Notes: to fully implement, we need:
 * 1) ability to walk the individual terms that matched, possibly in a hierarchical way
 * if we want to implement really clever highlighting?
 * 2) some Collector api like the one I made up, and support in Searcher
 * 3) All (or more) queries implemented
 * 
 * For hl perf testing we could test term queries only using the current impl
 * @author sokolov
 *
 */
public class PosHighlighterTest extends LuceneTestCase {
  
  protected final static String F="f";
  protected Analyzer analyzer;
  protected QueryParser parser;
  protected Directory dir;
  protected IndexSearcher searcher; 
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random, MockTokenizer.WHITESPACE, false);
    parser = new QueryParser(TEST_VERSION_CURRENT,  F, analyzer );
    dir = new SimpleFSDirectory(TEMP_DIR);
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
  
  private String[] doSearch(Query q) throws IOException {
    return doSearch(q, 100);
  }
  
  private String[] doSearch(Query q, int maxFragSize) throws IOException {
    PosHighlighter ph = new PosHighlighter();
    PosCollector collector = new PosCollector (10);
    searcher.search(q, collector);
    return ph.getFirstFragments(collector.docs[0], searcher.getIndexReader(), F, true, 10, maxFragSize);
  }
  
  public void testTerm () throws Exception {
    insertDocs(analyzer, "This is a test");
    String frags[] = doSearch (new TermQuery(new Term(F, "test")));
    assertEquals ("This is a <em>test</em>", frags[0]);
  }
  
  public void testSeveralSnippets () throws Exception {
    String input = "this is some long text.  It has the word long in many places.  In fact, it has long on some different fragments.  " +
    "Let us see what happens to long in this case.";
    String gold = "this is some <em>long</em> text.  It has the word <em>long</em> in many places.  In fact, it has <em>long</em> on some different fragments.  " +
    "Let us see what happens to <em>long</em> in this case.";
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
    assertEquals ("<em>This</em> is a <em>test</em>", frags[0]);    
  }
  
  public void testBooleanAndOtherOrder () throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.MUST));
    String frags[] = doSearch (bq);
    assertEquals ("<em>This</em> is a <em>test</em>", frags[0]);    
  }
  
  public void testBooleanOr () throws Exception {
 // OR queries not implemented yet...
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "test")), Occur.SHOULD));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "This")), Occur.SHOULD));
    String frags[] = doSearch (bq);
    assertEquals ("<em>This</em> is a <em>test</em>", frags[0]);    
  }
  
  @Ignore("not supproted yet")
  public void testPhrase() throws Exception {
    insertDocs(analyzer, "This is a test");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new BooleanClause (new TermQuery(new Term(F, "is")), Occur.MUST));
    bq.add(new BooleanClause (new TermQuery(new Term(F, "a")), Occur.MUST));
    MockSpanQuery msq = new MockSpanQuery(bq, false, F, new Filter(1));
    String frags[] = doSearch (msq);
    assertEquals ("This <em>is a</em> test", frags[0]);
  }
  
  public static class Filter implements PositionIntervalFilter {
    private int slop;
    public Filter(int slop) {
      this.slop = slop;
    }
    @Override
    public PositionIntervalIterator filter(PositionIntervalIterator iter) {
      return new WithinPositionIterator(slop, new OrderedConjunctionPositionIterator(iter));
    }
  }
  @Ignore("not supproted yet")
  public void testPhraseOriginal() throws Exception {
    insertDocs(analyzer, "This is a test");
    PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(F, "a"));
    pq.add(new Term(F, "test"));
    String frags[] = doSearch (pq);
    //searcher.search(new MockSpanQuery(pq, collector.needsPayloads(), F, null), collector);
    assertEquals ("This is a <em>test</em>", frags[0]);
  }
  
  public void testWildcard () throws Exception {
    insertDocs(analyzer, "This is a test");
    WildcardQuery wildcardQuery = new WildcardQuery(new Term(F, "t*t"));
    // TODO enable positions in constant scorer
    wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    String frags[] = doSearch(wildcardQuery);
    assertEquals ("This is a <em>test</em>", frags[0]);
  }
//  
//  @Ignore("file epistolary-novel.xml does not exist")
//  public void testLargerDocument() throws Exception {
//    InputStream in = new FileInputStream ("epistolary-novel.xml");
//    insertDocs(analyzer, IOUtils.toString(in));
//    in.close();
//    BooleanQuery bq = new BooleanQuery();
//    bq.add(new BooleanClause (new TermQuery(new Term(F, "unknown")), Occur.MUST));
//    bq.add(new BooleanClause (new TermQuery(new Term(F, "artist")), Occur.MUST));
//    String frags[] = doSearch (bq, 50);
//    assertEquals ("is a narration by an <em>unknown</em> observer.\n*[[Jean Web", frags[0]);
//    assertEquals ("fin and Sabine]]'' by <em>artist</em> [[Nick Bantock]] is a", frags[1]);
//  }
//  @Ignore("file epistolary-novel.xml does not exist")
//  public void testMultipleDocuments() throws Exception {
//    InputStream in = new FileInputStream ("epistolary-novel.xml");
//    insertDocs(analyzer, 
//        "This document has no matches", 
//        IOUtils.toString(in),
//        "This document has an unknown artist match");
//    BooleanQuery bq = new BooleanQuery();
//    bq.add(new BooleanClause (new TermQuery(new Term(F, "unknown")), Occur.MUST));
//    bq.add(new BooleanClause (new TermQuery(new Term(F, "artist")), Occur.MUST));
//    String frags[] = doSearch (bq, 50);
//    assertEquals ("is a narration by an <em>unknown</em> observer.\n*[[Jean Web", frags[0]);
//    assertEquals ("fin and Sabine]]'' by <em>artist</em> [[Nick Bantock]] is a", frags[1]);
//  }
  

}