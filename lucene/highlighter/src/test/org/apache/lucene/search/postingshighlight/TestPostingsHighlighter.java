package org.apache.lucene.search.postingshighlight;

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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

@SuppressCodecs({"MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom"})
public class TestPostingsHighlighter extends LuceneTestCase {
  
  public void testBasics() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  // simple test with one sentence documents.
  public void testOneSentence() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
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
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testMultipleFields() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
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
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("title", "best")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    Map<String,String[]> snippets = highlighter.highlightFields(new String [] { "body", "title" }, query, searcher, topDocs);
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
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
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "just")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "first")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("<b>Just</b> a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the <b>first</b> term. ", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testMultiplePassages() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
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
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
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
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
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
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
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
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("body", "buddhist"));
    query.add(new Term("body", "origins"));
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
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("body", "curious"));
    query.add(new Term("body", "george"));
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits);
    PostingsHighlighter highlighter = new PostingsHighlighter();
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs, 2);
    assertEquals(1, snippets.length);
    assertFalse(snippets[0].contains("<b>Curious</b>Curious"));
    ir.close();
    dir.close();
  }
}
