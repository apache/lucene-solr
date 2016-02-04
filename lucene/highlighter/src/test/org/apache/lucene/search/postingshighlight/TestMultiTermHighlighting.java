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

import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** 
 * Some tests that override {@link PostingsHighlighter#getIndexAnalyzer} to
 * highlight wilcard, fuzzy, etc queries.
 */
public class TestMultiTermHighlighting extends LuceneTestCase {
  
  public void testWildcards() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = new WildcardQuery(new Term("body", "te*"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // wrong field
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(new WildcardQuery(new Term("bogus", "te*")), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testOnePrefix() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = new PrefixQuery(new Term("body", "te"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // wrong field
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(new PrefixQuery(new Term("bogus", "te")), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testOneRegexp() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = new RegexpQuery(new Term("body", "te.*"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // wrong field
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(new RegexpQuery(new Term("bogus", "te.*")), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testOneFuzzy() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = new FuzzyQuery(new Term("body", "tets"), 1);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // with prefix
    query = new FuzzyQuery(new Term("body", "tets"), 1, 2);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // wrong field
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(new FuzzyQuery(new Term("bogus", "tets"), 1), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testRanges() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = TermRangeQuery.newStringRange("body", "ta", "tf", true, true);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // null start
    query = TermRangeQuery.newStringRange("body", null, "tf", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This <b>is</b> <b>a</b> <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> <b>a</b> <b>one</b> <b>sentence</b> <b>document</b>.", snippets[1]);
    
    // null end
    query = TermRangeQuery.newStringRange("body", "ta", null, true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("<b>This</b> is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // exact start inclusive
    query = TermRangeQuery.newStringRange("body", "test", "tf", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // exact end inclusive
    query = TermRangeQuery.newStringRange("body", "ta", "test", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // exact start exclusive
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(TermRangeQuery.newStringRange("body", "test", "tf", false, true), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    // exact end exclusive
    bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(TermRangeQuery.newStringRange("body", "ta", "test", true, false), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    // wrong field
    bq = new BooleanQuery.Builder();
    bq.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    bq.add(TermRangeQuery.newStringRange("bogus", "ta", "tf", true, true), BooleanClause.Occur.SHOULD);
    topDocs = searcher.search(bq.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testWildcardInBoolean() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new WildcardQuery(new Term("body", "te*")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    // must not
    query = new BooleanQuery.Builder();
    query.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
    query.add(new WildcardQuery(new Term("bogus", "te*")), BooleanClause.Occur.MUST_NOT);
    topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query.build(), searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }

  public void testWildcardInFiltered() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    FilteredQuery query = new FilteredQuery(
        new WildcardQuery(new Term("body", "te*")),
        new QueryWrapperFilter(new TermQuery(new Term("body", "test"))));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
    dir.close();
  }

  public void testWildcardInConstantScore() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    ConstantScoreQuery query = new ConstantScoreQuery(new WildcardQuery(new Term("body", "te*")));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
    dir.close();
  }
  
  public void testWildcardInDisjunctionMax() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    DisjunctionMaxQuery query = new DisjunctionMaxQuery(
        Collections.<Query>singleton(new WildcardQuery(new Term("body", "te*"))), 0);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testSpanWildcard() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    Query query = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testSpanOr() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanOrQuery(new SpanQuery[] { childQuery });
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testSpanNear() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanNearQuery(new SpanQuery[] { childQuery, childQuery }, 0, false);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testSpanNot() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    SpanQuery include = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    SpanQuery exclude = new SpanTermQuery(new Term("body", "bogus"));
    Query query = new SpanNotQuery(include, exclude);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  public void testSpanPositionCheck() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
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
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanFirstQuery(childQuery, 1000000);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, searcher, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    
    ir.close();
    dir.close();
  }
  
  /** Runs a query with two MTQs and confirms the formatter
   *  can tell which query matched which hit. */
  public void testWhichMTQMatched() throws Exception {
    Directory dir = newDirectory();
    // use simpleanalyzer for more natural tokenization (else "test." is a token)
    final Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig iwc = newIndexWriterConfig(analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
    
    FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
    offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    Field body = new Field("body", "", offsetsType);
    Document doc = new Document();
    doc.add(body);
    
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher searcher = newSearcher(ir);
    PostingsHighlighter highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
    };
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new WildcardQuery(new Term("body", "te*")), BooleanClause.Occur.SHOULD);
    query.add(new WildcardQuery(new Term("body", "one")), BooleanClause.Occur.SHOULD);
    query.add(new WildcardQuery(new Term("body", "se*")), BooleanClause.Occur.SHOULD);
    TopDocs topDocs = searcher.search(query.build(), 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query.build(), searcher, topDocs);
    assertEquals(1, snippets.length);
    
    // Default formatter just bolds each hit:
    assertEquals("<b>Test</b> a <b>one</b> <b>sentence</b> document.", snippets[0]);
    
    // Now use our own formatter, that also stuffs the
    // matching term's text into the result:
    highlighter = new PostingsHighlighter() {
      @Override
      protected Analyzer getIndexAnalyzer(String field) {
        return analyzer;
      }
      
      @Override
      protected PassageFormatter getFormatter(String field) {
        return new PassageFormatter() {
          
          @Override
          public Object format(Passage passages[], String content) {
            // Copied from DefaultPassageFormatter, but
            // tweaked to include the matched term:
            StringBuilder sb = new StringBuilder();
            int pos = 0;
            for (Passage passage : passages) {
              // don't add ellipsis if it's the first one, or if it's connected.
              if (passage.startOffset > pos && pos > 0) {
                sb.append("... ");
              }
              pos = passage.startOffset;
              for (int i = 0; i < passage.numMatches; i++) {
                int start = passage.matchStarts[i];
                int end = passage.matchEnds[i];
                // it's possible to have overlapping terms
                if (start > pos) {
                  sb.append(content, pos, start);
                }
                if (end > pos) {
                  sb.append("<b>");
                  sb.append(content, Math.max(pos, start), end);
                  sb.append('(');
                  sb.append(passage.getMatchTerms()[i].utf8ToString());
                  sb.append(')');
                  sb.append("</b>");
                  pos = end;
                }
              }
              // it's possible a "term" from the analyzer could span a sentence boundary.
              sb.append(content, pos, Math.max(pos, passage.endOffset));
              pos = passage.endOffset;
            }
            return sb.toString();
          }
        };
      }
    };
    
    assertEquals(1, topDocs.totalHits);
    snippets = highlighter.highlight("body", query.build(), searcher, topDocs);
    assertEquals(1, snippets.length);
    
    // Default formatter bolds each hit:
    assertEquals("<b>Test(body:te*)</b> a <b>one(body:one)</b> <b>sentence(body:se*)</b> document.", snippets[0]);
    
    ir.close();
    dir.close();
  }
}
