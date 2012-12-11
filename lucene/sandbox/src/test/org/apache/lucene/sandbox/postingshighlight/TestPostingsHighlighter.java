package org.apache.lucene.sandbox.postingshighlight;

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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
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
    PostingsHighlighter highlighter = new PostingsHighlighter("body");
    Query query = new TermQuery(new Term("body", "highlighting"));
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight(query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    
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
    PostingsHighlighter highlighter = new PostingsHighlighter("body");
    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "just")), BooleanClause.Occur.SHOULD);
    query.add(new TermQuery(new Term("body", "first")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight(query, searcher, topDocs);
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
    PostingsHighlighter highlighter = new PostingsHighlighter("body");
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight(query, searcher, topDocs, 2);
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
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
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
    PostingsHighlighter highlighter = new PostingsHighlighter("body");
    Query query = new TermQuery(new Term("body", "test"));
    TopDocs topDocs = searcher.search(query, null, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    try {
      highlighter.highlight(query, searcher, topDocs, 2);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    ir.close();
    dir.close();
  }
}
