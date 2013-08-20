package org.apache.lucene.search.vectorhighlight;
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
import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;


public class FastVectorHighlighterTest extends LuceneTestCase {

  private static final String FIELD = "text";
  
  public void testSimpleHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field field = new Field("field", "This is a test where foo is highlighed and should be highlighted", type);
    
    doc.add(field);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    
    IndexReader reader = DirectoryReader.open(writer, true);
    int docId = 0;
    FieldQuery fieldQuery  = highlighter.getFieldQuery( new TermQuery(new Term("field", "foo")), reader );
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 54, 1);
    // highlighted results are centered 
    assertEquals("This is a test where <b>foo</b> is highlighed and should be highlighted", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 52, 1);
    assertEquals("This is a test where <b>foo</b> is highlighed and should be", bestFragments[0]);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, docId, "field", 30, 1);
    assertEquals("a test where <b>foo</b> is highlighed", bestFragments[0]);
    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testPhraseHighlightLongTextTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field text = new Field("text", 
        "Netscape was the general name for a series of web browsers originally produced by Netscape Communications Corporation, now a subsidiary of AOL The original browser was once the dominant browser in terms of usage share, but as a result of the first browser war it lost virtually all of its share to Internet Explorer Netscape was discontinued and support for all Netscape browsers and client products was terminated on March 1, 2008 Netscape Navigator was the name of Netscape\u0027s web browser from versions 1.0 through 4.8 The first beta release versions of the browser were released in 1994 and known as Mosaic and then Mosaic Netscape until a legal challenge from the National Center for Supercomputing Applications (makers of NCSA Mosaic, which many of Netscape\u0027s founders used to develop), led to the name change to Netscape Navigator The company\u0027s name also changed from Mosaic Communications Corporation to Netscape Communications Corporation The browser was easily the most advanced...", type);
    doc.add(text);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    int docId = 0;
    String field = "text";
    {
      BooleanQuery query = new BooleanQuery();
      query.add(new TermQuery(new Term(field, "internet")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "explorer")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 128, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("first browser war it lost virtually all of its share to <b>Internet</b> <b>Explorer</b> Netscape was discontinued and support for all Netscape browsers", bestFragments[0]);
    }
    
    {
      PhraseQuery query = new PhraseQuery();
      query.add(new Term(field, "internet"));
      query.add(new Term(field, "explorer"));
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 128, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("first browser war it lost virtually all of its share to <b>Internet Explorer</b> Netscape was discontinued and support for all Netscape browsers", bestFragments[0]);
    }
    reader.close();
    writer.close();
    dir.close();
  }
  
  // see LUCENE-4899
  public void testPhraseHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    Field longTermField = new Field("long_term", "This is a test thisisaverylongwordandmakessurethisfails where foo is highlighed and should be highlighted", type);
    Field noLongTermField = new Field("no_long_term", "This is a test where foo is highlighed and should be highlighted", type);

    doc.add(longTermField);
    doc.add(noLongTermField);
    writer.addDocument(doc);
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    int docId = 0;
    String field = "no_long_term";
    {
      BooleanQuery query = new BooleanQuery();
      query.add(new TermQuery(new Term(field, "test")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("<b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
    }
    {
      BooleanQuery query = new BooleanQuery();
      PhraseQuery pq = new PhraseQuery();
      pq.add(new Term(field, "test"));
      pq.add(new Term(field, "foo"));
      pq.add(new Term(field, "highlighed"));
      pq.setSlop(5);
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(pq, Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(0, bestFragments.length);
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
      
    }
    {
      PhraseQuery query = new PhraseQuery();
      query.add(new Term(field, "test"));
      query.add(new Term(field, "foo"));
      query.add(new Term(field, "highlighed"));
      query.setSlop(3);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(0, bestFragments.length);
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
      
    }
    {
      PhraseQuery query = new PhraseQuery();
      query.add(new Term(field, "test"));
      query.add(new Term(field, "foo"));
      query.add(new Term(field, "highlighted"));
      query.setSlop(30);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      assertEquals(0, bestFragments.length);
    }
    {
      BooleanQuery query = new BooleanQuery();
      PhraseQuery pq = new PhraseQuery();
      pq.add(new Term(field, "test"));
      pq.add(new Term(field, "foo"));
      pq.add(new Term(field, "highlighed"));
      pq.setSlop(5);
      BooleanQuery inner = new BooleanQuery();
      inner.add(pq, Occur.MUST);
      inner.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(inner, Occur.MUST);
      query.add(pq, Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      assertEquals(0, bestFragments.length);
      
      bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 30, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and", bestFragments[0]);
    }
    
    field = "long_term";
    {
      BooleanQuery query = new BooleanQuery();
      query.add(new TermQuery(new Term(field,
          "thisisaverylongwordandmakessurethisfails")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "foo")), Occur.MUST);
      query.add(new TermQuery(new Term(field, "highlighed")), Occur.MUST);
      FieldQuery fieldQuery = highlighter.getFieldQuery(query, reader);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader,
          docId, field, 18, 1);
      // highlighted results are centered
      assertEquals(1, bestFragments.length);
      assertEquals("<b>thisisaverylongwordandmakessurethisfails</b>",
          bestFragments[0]);
    }
    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testCommonTermsQueryHighlightTest() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,  new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET)));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    String[] texts = {
        "Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
        "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
        "JFK has been shot", "John Kennedy has been shot",
        "This text has a typo in referring to Keneddy",
        "wordx wordy wordz wordx wordy wordx worda wordb wordy wordc", "y z x y z a b", "lets is a the lets is a the lets is a the lets" };
    for (int i = 0; i < texts.length; i++) {
      Document doc = new Document();
      Field field = new Field("field", texts[i], type);
      doc.add(field);
      writer.addDocument(doc);
    }
    CommonTermsQuery query = new CommonTermsQuery(Occur.MUST, Occur.SHOULD, 2);
    query.add(new Term("field", "text"));
    query.add(new Term("field", "long"));
    query.add(new Term("field", "very"));
   
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(2, hits.totalHits);
    FieldQuery fieldQuery  = highlighter.getFieldQuery(query, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[0].doc, "field", 1000, 1);
    assertEquals("This piece of <b>text</b> refers to Kennedy at the beginning then has a longer piece of <b>text</b> that is <b>very</b> <b>long</b> in the middle and finally ends with another reference to Kennedy", bestFragments[0]);

    fieldQuery  = highlighter.getFieldQuery(query, reader);
    bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[1].doc, "field", 1000, 1);
    assertEquals("Hello this is a piece of <b>text</b> that is <b>very</b> <b>long</b> and contains too much preamble and the meat is really here which says kennedy has been shot", bestFragments[0]);

    reader.close();
    writer.close();
    dir.close();
  }
  
  public void testLotsOfPhrases() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,  new MockAnalyzer(random(), MockTokenizer.SIMPLE, true, MockTokenFilter.ENGLISH_STOPSET)));
    FieldType type = new FieldType(TextField.TYPE_STORED);
    type.setStoreTermVectorOffsets(true);
    type.setStoreTermVectorPositions(true);
    type.setStoreTermVectors(true);
    type.freeze();
    String[] terms = { "org", "apache", "lucene"};
    int iters = 1000; // don't let it go too big, or jenkins will stack overflow: atLeast(1000);
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < iters; i++) {
      builder.append(terms[random().nextInt(terms.length)]).append(" ");
      if (random().nextInt(6) == 3) {
        builder.append("solr").append(" ");
      }
    }
      Document doc = new Document();
      Field field = new Field("field", builder.toString(), type);
      doc.add(field);
      writer.addDocument(doc);
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field", "org"));
    query.add(new Term("field", "apache"));
    query.add(new Term("field", "lucene"));
    
   
    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    IndexReader reader = DirectoryReader.open(writer, true);
    IndexSearcher searcher = newSearcher(reader);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(1, hits.totalHits);
    FieldQuery fieldQuery  = highlighter.getFieldQuery(query, reader);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, reader, hits.scoreDocs[0].doc, "field", 1000, 1);
    for (int i = 0; i < bestFragments.length; i++) {
      String result = bestFragments[i].replaceAll("<b>org apache lucene</b>", "FOOBAR");
      assertFalse(result.contains("org apache lucene"));
    }
    reader.close();
    writer.close();
    dir.close();
  }

  public void testOverlappingPhrases() throws IOException {
    final Analyzer analyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        final Tokenizer source = new MockTokenizer(reader);
        TokenStream sink = source;
        sink = new SynonymFilter(sink);
        return new TokenStreamComponents(source, sink);
      }

    };
    final Directory directory = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, analyzer);
    Document doc = new Document();
    FieldType withVectors = new FieldType(TextField.TYPE_STORED);
    withVectors.setStoreTermVectors(true);
    withVectors.setStoreTermVectorPositions(true);
    withVectors.setStoreTermVectorOffsets(true);
    doc.add(new Field(FIELD, "a b c", withVectors));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();

    // Disjunction of two overlapping phrase queries
    final PhraseQuery pq1 = new PhraseQuery();
    pq1.add(new Term(FIELD, "a"), 0);
    pq1.add(new Term(FIELD, "b"), 1);
    pq1.add(new Term(FIELD, "c"), 2);

    final PhraseQuery pq2 = new PhraseQuery();
    pq2.add(new Term(FIELD, "a"), 0);
    pq2.add(new Term(FIELD, "B"), 1);
    pq2.add(new Term(FIELD, "c"), 2);

    final BooleanQuery bq = new BooleanQuery();
    bq.add(pq1, Occur.SHOULD);
    bq.add(pq2, Occur.SHOULD);

    // Single phrase query with two terms at the same position
    final PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(FIELD, "a"), 0);
    pq.add(new Term(FIELD, "b"), 1);
    pq.add(new Term(FIELD, "B"), 1);
    pq.add(new Term(FIELD, "c"), 2);

    for (Query query : Arrays.asList(pq1, pq2, bq, pq)) {
      assertEquals(1, new IndexSearcher(ir).search(bq, 1).totalHits);

      FastVectorHighlighter highlighter = new FastVectorHighlighter();
      FieldQuery fieldQuery  = highlighter.getFieldQuery(query, ir);
      String[] bestFragments = highlighter.getBestFragments(fieldQuery, ir, 0, FIELD, 1000, 1);
      assertEquals("<b>a b c</b>", bestFragments[0]);
    }

    ir.close();
    iw.close();
    directory.close();
  }

  public void testPhraseWithGap() throws IOException {
    final Directory directory = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
    Document doc = new Document();
    FieldType withVectors = new FieldType(TextField.TYPE_STORED);
    withVectors.setStoreTermVectors(true);
    withVectors.setStoreTermVectorPositions(true);
    withVectors.setStoreTermVectorOffsets(true);
    doc.add(new Field(FIELD, "a b c", withVectors));
    iw.addDocument(doc);
    DirectoryReader ir = iw.getReader();

    final PhraseQuery pq = new PhraseQuery();
    pq.add(new Term(FIELD, "c"), 2);
    pq.add(new Term(FIELD, "a"), 0);

    assertEquals(1, new IndexSearcher(ir).search(pq, 1).totalHits);

    FastVectorHighlighter highlighter = new FastVectorHighlighter();
    FieldQuery fieldQuery  = highlighter.getFieldQuery(pq, ir);
    String[] bestFragments = highlighter.getBestFragments(fieldQuery, ir, 0, FIELD, 1000, 1);
    assertEquals("<b>a</b> b <b>c</b>", bestFragments[0]);

    ir.close();
    iw.close();
    directory.close();
  }

  // Simple token filter that adds 'B' as a synonym of 'b'
  private static class SynonymFilter extends TokenFilter {

    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);

    State pending;

    protected SynonymFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (pending != null) {
        restoreState(pending);
        termAtt.setEmpty().append('B');
        posIncAtt.setPositionIncrement(0);
        pending = null;
        return true;
      }
      if (!input.incrementToken()) {
        return false;
      }
      if (termAtt.toString().equals("b")) {
        pending = captureState();
      }
      return true;
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      pending = null;
    }
  }
}
