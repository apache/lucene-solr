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
package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Map;

public class TestPostingsHighlighter extends LuceneTestCase {
  
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    
    ir.close();
    dir.close();
  }

  public void testFormatWithMatchExceedingContentLength2() throws Exception {
    
    String bodyText = "123 TEST 01234 TEST";

    String[] snippets = formatWithMatchExceedingContentLength(bodyText);
    
    assertEquals(1, snippets.length);
    assertEquals("123 <b>TEST</b> 01234 TE", snippets[0]);
  }

  public void testFormatWithMatchExceedingContentLength3() throws Exception {
    
    String bodyText = "123 5678 01234 TEST TEST";
    
    String[] snippets = formatWithMatchExceedingContentLength(bodyText);
    
    assertEquals(1, snippets.length);
    assertEquals("123 5678 01234 TE", snippets[0]);
  }
  
  public void testFormatWithMatchExceedingContentLength() throws Exception {
    
    String bodyText = "123 5678 01234 TEST";
    
    String[] snippets = formatWithMatchExceedingContentLength(bodyText);
    
    assertEquals(1, snippets.length);
    // LUCENE-5166: no snippet
    assertEquals("123 5678 01234 TE", snippets[0]);
  }

  private String[] formatWithMatchExceedingContentLength(String bodyText) throws IOException {
    
    int maxLength = 17;
    
    final Analyzer analyzer = new MockAnalyzer(random());
    
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    final FieldType fieldType = new FieldType(TextField.TYPE_STORED);
    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    final Field body = new Field("body", bodyText, fieldType);
    
    Document doc = new Document();
    doc.add(body);
    
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    
    Query query = new TermQuery(new Term("body", "test"));
    
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    
    PostingsHighlighter highlighter = new PostingsHighlighter(maxLength);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    
    
    ir.close();
    dir.close();
    return snippets;
  }
  
  // simple test highlighting last word.
  public void testHighlightLastWord() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("This is a <b>test</b>", snippets[0]);
    
    ir.close();
    dir.close();
  }
  
  // simple test with one sentence documents.
  public void testOneSentence() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  // simple test with multiple values that make a result longer than maxLength.
  public void testMaxLengthWithMultivalue() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();
    
    for(int i = 0; i < 3 ; i++) {
      Field body = new Field("body", "", offsetsType);
      body.setStringValue("This is a multivalued field");
      doc.add(body);
    }
    
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter(40);
    Query query = new TermQuery(new Term("body", "field"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(1, snippets.length);
    assertTrue("Snippet should have maximum 40 characters plus the pre and post tags",
        snippets[0].length() == (40 + "<b></b>".length()));
    
    ir.close();
    dir.close();
  }
  
  public void testMultipleFields() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Field title = new Field("title", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    doc.add(title);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    title.setStringValue("I am hoping for the best.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    title.setStringValue("But best may not be good enough.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("title", "best")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    Map<String,String[]> snippets = highlighter.highlightFields(new String [] { "body", "title" }, query.build(), searcher, topDocs);
    assertEquals(2, snippets.size());
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets.get("body")[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets.get("body")[1]);
    assertEquals("I am hoping for the <b>best</b>.", snippets.get("title")[0]);
    assertEquals("But <b>best</b> may not be good enough.", snippets.get("title")[1]);
    ir.close();
    dir.close();
  }
  
  public void testMultipleTerms() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "just")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "first")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("<b>Just</b> a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the <b>first</b> term. ", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testMultiplePassages() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ", snippets[0]);
    assertEquals("This <b>test</b> is another <b>test</b>. ... <b>Test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[1]);
    
    ir.close();
    dir.close();
  }

  public void testUserFailedToIndexOffsets() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType positionsType = new FieldType(TextField.TYPE_STORED);
    positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    Field body = new Field("body", "", positionsType);
    Field title = new StringField("title", "", Field.Store.YES);
    Document doc = new Document();
    doc.add(body);
    doc.add(title);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    title.setStringValue("test");
    iw.addDocument(doc);
    body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
    title.setStringValue("test");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    try {
      highlighter.highlight("body", query, searcher, topDocs, 2);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    
    try {
      highlighter.highlight("title", new TermQuery(new Term("title", "test")), searcher, topDocs, 2);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    ir.close();
    dir.close();
  }
  
  public void testBuddhism() throws Exception {
    String text = "This eight-volume set brings together seminal papers in Buddhist studies from a vast " +
                  "range of academic disciplines published over the last forty years. With a new introduction " + 
                  "by the editor, this collection is a unique and unrivalled research resource for both " + 
                  "student and scholar. Coverage includes: - Buddhist origins; early history of Buddhism in " + 
                  "South and Southeast Asia - early Buddhist Schools and Doctrinal History; Theravada Doctrine " + 
                  "- the Origins and nature of Mahayana Buddhism; some Mahayana religious topics - Abhidharma " + 
                  "and Madhyamaka - Yogacara, the Epistemological tradition, and Tathagatagarbha - Tantric " + 
                  "Buddhism (Including China and Japan); Buddhism in Nepal and Tibet - Buddhism in South and " + 
                  "Southeast Asia, and - Buddhism in China, East Asia, and Japan.";
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
    
    FieldType positionsType = new FieldType(TextField.TYPE_STORED);
    positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", text, positionsType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    PhraseQuery query = new PhraseQuery("body", "buddhist", "origins");
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertTrue(snippets[0].contains("<b>Buddhist</b> <b>origins</b>"));
    ir.close();
    dir.close();
  }
  
  public void testCuriousGeorge() throws Exception {
    String text = "It’s the formula for success for preschoolers—Curious George and fire trucks! " + 
                  "Curious George and the Firefighters is a story based on H. A. and Margret Rey’s " +
                  "popular primate and painted in the original watercolor and charcoal style. " + 
                  "Firefighters are a famously brave lot, but can they withstand a visit from one curious monkey?";
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
    FieldType positionsType = new FieldType(TextField.TYPE_STORED);
    positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", text, positionsType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    PhraseQuery query = new PhraseQuery("body", "curious", "george");
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertFalse(snippets[0].contains("<b>Curious</b>Curious"));
    ir.close();
    dir.close();
  }

  public void testCambridgeMA() throws Exception {
    BufferedReader r = new BufferedReader(new InputStreamReader(
                     this.getClass().getResourceAsStream("CambridgeMA.utf8"), StandardCharsets.UTF_8));
    String text = r.readLine();
    r.close();
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
    FieldType positionsType = new FieldType(TextField.TYPE_STORED);
    positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", text, positionsType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "porter")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "square")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "massachusetts")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10);
    assertEquals(1, topDocs.totalHits);
    PostingsHighlighter highlighter = new PostingsHighlighter(Integer.MAX_VALUE-1);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertTrue(snippets[0].contains("<b>Square</b>"));
    assertTrue(snippets[0].contains("<b>Porter</b>"));
    ir.close();
    dir.close();
  }
  
  public void testPassageRanking() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals("This is a <b>test</b>.  ... Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);
    
    ir.close();
    dir.close();
  }

  public void testBooleanMustNot() throws Exception {
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, analyzer);
    FieldType positionsType = new FieldType(TextField.TYPE_STORED);
    positionsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "This sentence has both terms.  This sentence has only terms.", positionsType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "terms")), BooleanClause.Occur.SHOULD);
    BooleanQuery.Builder query2 = new BooleanQuery.Builder();
    query.add(query2.build(), BooleanClause.Occur.SHOULD);
    query2.add(new TermQuery(new Term("body", "both")), BooleanClause.Occur.MUST_NOT);
    TopDocs topDocs = searcher.search(query.build(), 10);
    assertEquals(1, topDocs.totalHits);
    PostingsHighlighter highlighter = new PostingsHighlighter(Integer.MAX_VALUE-1);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertFalse(snippets[0].contains("<b>both</b>"));
    ir.close();
    dir.close();
  }

  public void testHighlightAllText() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter(10000) {
      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }
    };
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals("This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);
    
    ir.close();
    dir.close();
  }

  public void testSpecificDocIDs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    ScoreDoc[] hits = topDocs.scoreDocs;
    int[] docIDs = new int[2];
    docIDs[0] = hits[0].doc;
    docIDs[1] = hits[1].doc;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 1 }).get("body");
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    
    ir.close();
    dir.close();
  }

  public void testCustomFieldValueSource() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    Document doc = new Document();

    FieldType offsetsType = new FieldType(TextField.TYPE_NOT_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    final String text = "This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.";
    Field body = new Field("body", text, offsetsType);
    doc.add(body);
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);

    PostingsHighlighter highlighter = new PostingsHighlighter(10000) {
        @Override
        protected String[][] loadFieldValues(IndexSearcher searcher, String[] fields, int[] docids, int maxLength) throws IOException {
          assert fields.length == 1;
          assert docids.length == 1;
          String[][] contents = new String[1][1];
          contents[0][0] = text;
          return contents;
        }

        @Override
        protected BreakIterator getBreakIterator(String field) {
          return new WholeBreakIterator();
        }
      };

    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals("This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[0]);
    
    ir.close();
    dir.close();
  }

  /** Make sure highlighter returns first N sentences if
   *  there were no hits. */
  public void testEmptyHighlights() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
    doc.add(body);
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
    assertEquals(1, snippets.length);
    assertEquals("test this is.  another sentence this test has.  ", snippets[0]);

    ir.close();
    dir.close();
  }

  /** Make sure highlighter we can customize how emtpy
   *  highlight is returned. */
  public void testCustomEmptyHighlights() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
    doc.add(body);
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter() {
        @Override
        public Passage[] getEmptyHighlight(String fieldName, BreakIterator bi, int maxPassages) {
          return new Passage[0];
        }
      };
    Query query = new TermQuery(new Term("body", "highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);

    ir.close();
    dir.close();
  }

  /** Make sure highlighter returns whole text when there
   *  are no hits and BreakIterator is null. */
  public void testEmptyHighlightsWhole() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
    doc.add(body);
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter(10000) {
      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }
    };
    Query query = new TermQuery(new Term("body", "highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
    assertEquals(1, snippets.length);
    assertEquals("test this is.  another sentence this test has.  far away is that planet.", snippets[0]);

    ir.close();
    dir.close();
  }

  /** Make sure highlighter is OK with entirely missing
   *  field. */
  public void testFieldIsMissing() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", offsetsType);
    doc.add(body);
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("bogus", "highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"bogus"}, query, searcher, docIDs, new int[] { 2 }).get("bogus");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);

    ir.close();
    dir.close();
  }

  public void testFieldIsJustSpace() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

    Document doc = new Document();
    doc.add(new Field("body", "   ", offsetsType));
    doc.add(new Field("id", "id", offsetsType));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", offsetsType));
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    Query query = new TermQuery(new Term("body", "highlighting"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
    assertEquals(1, snippets.length);
    assertEquals("   ", snippets[0]);

    ir.close();
    dir.close();
  }

  public void testFieldIsEmptyString() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

    Document doc = new Document();
    doc.add(new Field("body", "", offsetsType));
    doc.add(new Field("id", "id", offsetsType));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", offsetsType));
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    Query query = new TermQuery(new Term("body", "highlighting"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, searcher, docIDs, new int[] { 2 }).get("body");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);

    ir.close();
    dir.close();
  }

  public void testMultipleDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

    int numDocs = atLeast(100);
    for(int i=0;i<numDocs;i++) {
      Document doc = new Document();
      String content = "the answer is " + i;
      if ((i & 1) == 0) {
        content += " some more terms";
      }
      doc.add(new Field("body", content, offsetsType));
      doc.add(newStringField("id", ""+i, Field.Store.YES));
      iw.addDocument(doc);

      if (random().nextInt(10) == 2) {
        iw.commit();
      }
    }

    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new TermQuery(new Term("body", "answer"));
    TopDocs hits = searcher.search(query, numDocs);
    assertEquals(numDocs, hits.totalHits);

    String snippets[] = highlighter.highlight("body", query, searcher, hits);
    assertEquals(numDocs, snippets.length);
    for(int hit=0;hit<numDocs;hit++) {
      Document doc = searcher.doc(hits.scoreDocs[hit].doc);
      int id = Integer.parseInt(doc.get("id"));
      String expected = "the <b>answer</b> is " + id;
      if ((id  & 1) == 0) {
        expected += " some more terms";
      }
      assertEquals(expected, snippets[hit]);
    }

    ir.close();
    dir.close();
  }
  
  public void testMultipleSnippetSizes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Field title = new Field("title", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    doc.add(title);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    title.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term("body", "test")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("title", "test")), BooleanClause.Occur.SHOULD);
    Map<String,String[]> snippets = highlighter.highlightFields(new String[] { "title", "body" }, query.build(), searcher, new int[] { 0 }, new int[] { 1, 2 });
    String titleHighlight = snippets.get("title")[0];
    String bodyHighlight = snippets.get("body")[0];
    assertEquals("This is a <b>test</b>. ", titleHighlight);
    assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ", bodyHighlight);
    ir.close();
    dir.close();
  }
  
  public void testEncode() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from <i>postings</i>. Feel free to ignore.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected PassageFormatter getFormatter(String field) {
        return new DefaultPassageFormatter("<b>", "</b>", "... ", true);
      }
    };
    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("Just&#32;a&#32;test&#32;<b>highlighting</b>&#32;from&#32;&lt;i&gt;postings&lt;&#x2F;i&gt;&#46;&#32;", snippets[0]);
    
    ir.close();
    dir.close();
  }
  
  /** customizing the gap separator to force a sentence break */
  public void testGapSeparator() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Document doc = new Document();
    
    Field body1 = new Field("body", "", offsetsType);
    body1.setStringValue("This is a multivalued field");
    doc.add(body1);
    
    Field body2 = new Field("body", "", offsetsType);
    body2.setStringValue("This is something different");
    doc.add(body2);
    
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected char getMultiValuedSeparator(String field) {
        assert field.equals("body");
        return '\u2029';
      }
    };
    Query query = new TermQuery(new Term("body", "field"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("This is a multivalued <b>field</b>\u2029", snippets[0]);
    
    ir.close();
    dir.close();
  }

  // LUCENE-4906
  public void testObjectFormatter() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected PassageFormatter getFormatter(String field) {
        return new PassageFormatter() {
          PassageFormatter defaultFormatter = new DefaultPassageFormatter();

          @Override
          public String[] format(Passage passages[], String content) {
            // Just turns the String snippet into a length 2
            // array of String
            return new String[] {"blah blah", defaultFormatter.format(passages, content).toString()};
          }
        };
      }
    };

    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    int[] docIDs = new int[1];
    docIDs[0] = topDocs.scoreDocs[0].doc;
    Map<String,Object[]> snippets = highlighter.highlightFieldsAsObjects(new String[]{"body"}, query, searcher, docIDs, new int[] {1});
    Object[] bodySnippets = snippets.get("body");
    assertEquals(1, bodySnippets.length);
    assertTrue(Arrays.equals(new String[] {"blah blah", "Just a test <b>highlighting</b> from postings. "}, (String[]) bodySnippets[0]));
    
    ir.close();
    dir.close();
  }

  public void testFieldSometimesMissingFromSegment() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "foo", offsetsType);
    Document doc = new Document();
    doc.add(body);
    iw.addDocument(doc);

    // Make a 2nd segment where body is only stored:
    iw.commit();
    doc = new Document();
    doc.add(new StoredField("body", "foo"));
    iw.addDocument(doc);
    
    IndexReader ir = DirectoryReader.open(iw.w);
    iw.close();
    
    IndexSearcher searcher = new IndexSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    Query query = new MatchAllDocsQuery();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("foo", snippets[0]);
    assertNull(snippets[1]);
    ir.close();
    dir.close();
  }

  public void testCustomScoreQueryHighlight() throws Exception{
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();

    TermQuery termQuery = new TermQuery(new Term("body", "very"));
    PostingsHighlighter highlighter = new PostingsHighlighter();
    CustomScoreQuery query = new CustomScoreQuery(termQuery);

    IndexSearcher searcher = newSearcher(ir);
    TopDocs hits = searcher.search(query, 10);
    assertEquals(1, hits.totalHits);

    String snippets[] = highlighter.highlight("body", query, searcher, hits);
    assertEquals(1, snippets.length);
    assertEquals("This piece of text refers to Kennedy at the beginning then has a longer piece of text that is <b>very</b> long in the middle and finally ends with another reference to Kennedy",
                 snippets[0]);

    ir.close();
    dir.close();
  }
}
