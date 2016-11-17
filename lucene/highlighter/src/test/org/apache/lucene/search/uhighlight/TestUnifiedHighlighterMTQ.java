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
package org.apache.lucene.search.uhighlight;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
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
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.junit.After;
import org.junit.Before;

/**
 * Some tests that highlight wildcard, fuzzy, etc queries.
 */
@SuppressCodecs({"MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene3x"})
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")//Gradle interferes with this Lucene test rule
public class TestUnifiedHighlighterMTQ extends LuceneTestCase {

  final FieldType fieldType;

  BaseDirectoryWrapper dir;
  Analyzer indexAnalyzer;

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return UHTestHelper.parametersFactoryList();
  }

  public TestUnifiedHighlighterMTQ(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  @Before
  public void doBefore() throws IOException {
    dir = newDirectory();
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase
  }

  @After
  public void doAfter() throws IOException {
    dir.close();
  }

  public void testWildcards() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new WildcardQuery(new Term("body", "te*"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // disable MTQ; won't highlight
    highlighter.setHandleMultiTermQuery(false);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);
    highlighter.setHandleMultiTermQuery(true);//reset

    // wrong field
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new WildcardQuery(new Term("bogus", "te*")), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testOnePrefix() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new PrefixQuery(new Term("body", "te"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // wrong field
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new PrefixQuery(new Term("bogus", "te")), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testOneRegexp() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new RegexpQuery(new Term("body", "te.*"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // wrong field
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new RegexpQuery(new Term("bogus", "te.*")), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testOneFuzzy() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new FuzzyQuery(new Term("body", "tets"), 1);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // with prefix
    query = new FuzzyQuery(new Term("body", "tets"), 1, 2);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // wrong field
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new FuzzyQuery(new Term("bogus", "tets"), 1), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testRanges() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = TermRangeQuery.newStringRange("body", "ta", "tf", true, true);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // null start
    query = TermRangeQuery.newStringRange("body", null, "tf", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This <b>is</b> <b>a</b> <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> <b>a</b> <b>one</b> <b>sentence</b> <b>document</b>.", snippets[1]);

    // null end
    query = TermRangeQuery.newStringRange("body", "ta", null, true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("<b>This</b> is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // exact start inclusive
    query = TermRangeQuery.newStringRange("body", "test", "tf", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // exact end inclusive
    query = TermRangeQuery.newStringRange("body", "ta", "test", true, true);
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // exact start exclusive
    BooleanQuery bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(TermRangeQuery.newStringRange("body", "test", "tf", false, true), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    // exact end exclusive
    bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(TermRangeQuery.newStringRange("body", "ta", "test", true, false), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    // wrong field
    bq = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(TermRangeQuery.newStringRange("bogus", "ta", "tf", true, true), BooleanClause.Occur.SHOULD)
        .build();
    topDocs = searcher.search(bq, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", bq, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testWildcardInBoolean() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    BooleanQuery query = new BooleanQuery.Builder()
        .add(new WildcardQuery(new Term("body", "te*")), BooleanClause.Occur.SHOULD)
        .build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    // must not
    query = new BooleanQuery.Builder()
        .add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD)
        .add(new WildcardQuery(new Term("bogus", "te*")), BooleanClause.Occur.MUST_NOT)
        .build();
    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a test.", snippets[0]);
    assertEquals("Test a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testWildcardInFiltered() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    BooleanQuery query = new BooleanQuery.Builder()
        .add(new WildcardQuery(new Term("body", "te*")), BooleanClause.Occur.MUST)
        .add(new TermQuery(new Term("body", "test")), BooleanClause.Occur.FILTER)
        .build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testWildcardInConstantScore() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    ConstantScoreQuery query = new ConstantScoreQuery(new WildcardQuery(new Term("body", "te*")));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testWildcardInDisjunctionMax() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    DisjunctionMaxQuery query = new DisjunctionMaxQuery(
        Collections.singleton(new WildcardQuery(new Term("body", "te*"))), 0);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testSpanWildcard() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testSpanOr() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanOrQuery(new SpanQuery[]{childQuery});
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testSpanNear() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanNearQuery(new SpanQuery[]{childQuery, childQuery}, 0, false);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testSpanNot() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    SpanQuery include = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    SpanQuery exclude = new SpanTermQuery(new Term("body", "bogus"));
    Query query = new SpanNotQuery(include, exclude);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  public void testSpanPositionCheck() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    SpanQuery childQuery = new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term("body", "te*")));
    Query query = new SpanFirstQuery(childQuery, 1000000);
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);

    ir.close();
  }

  /**
   * Runs a query with two MTQs and confirms the formatter
   * can tell which query matched which hit.
   */
  public void testWhichMTQMatched() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    BooleanQuery query = new BooleanQuery.Builder()
        .add(new WildcardQuery(new Term("body", "te*")), BooleanClause.Occur.SHOULD)
        .add(new WildcardQuery(new Term("body", "one")), BooleanClause.Occur.SHOULD)
        .add(new WildcardQuery(new Term("body", "se*")), BooleanClause.Occur.SHOULD)
        .build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(1, snippets.length);

    // Default formatter just bolds each hit:
    assertEquals("<b>Test</b> a <b>one</b> <b>sentence</b> document.", snippets[0]);

    // Now use our own formatter, that also stuffs the
    // matching term's text into the result:
    highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {

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
              // don't add ellipsis if its the first one, or if its connected.
              if (passage.getStartOffset() > pos && pos > 0) {
                sb.append("... ");
              }
              pos = passage.getStartOffset();
              for (int i = 0; i < passage.getNumMatches(); i++) {
                int start = passage.getMatchStarts()[i];
                int end = passage.getMatchEnds()[i];
                // its possible to have overlapping terms
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
              // its possible a "term" from the analyzer could span a sentence boundary.
              sb.append(content, pos, Math.max(pos, passage.getEndOffset()));
              pos = passage.getEndOffset();
            }
            return sb.toString();
          }
        };
      }
    };

    assertEquals(1, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs);
    assertEquals(1, snippets.length);

    // Default formatter bolds each hit:
    assertEquals("<b>Test(body:te*)</b> a <b>one(body:one)</b> <b>sentence(body:se*)</b> document.", snippets[0]);

    ir.close();
  }


  //
  //  All tests below were *not* ported from the PostingsHighlighter; they are new to the U.H.
  //

  public void testWithMaxLen() throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("Alpha Bravo foo foo foo. Foo foo Alpha Bravo");//44 char long, 2 sentences
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(25);//a little past first sentence

    BooleanQuery query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "alpha")), BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term("body", "bra")), BooleanClause.Occur.MUST)
        .build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);//ask for 2 but we'll only get 1
    assertArrayEquals(
        new String[]{"<b>Alpha</b> <b>Bravo</b> foo foo foo. "}, snippets
    );

    ir.close();
  }

  public void testWithMaxLenAndMultipleWildcardMatches() throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    //tests interleaving of multiple wildcard matches with the CompositePostingsEnum
    //In this case the CompositePostingsEnum will have an underlying PostingsEnum that jumps form pos 1 to 9 for bravo
    //and a second with position 2 for Bravado
    body.setStringValue("Alpha Bravo Bravado foo foo foo. Foo foo Alpha Bravo");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(32);//a little past first sentence

    BooleanQuery query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "alpha")), BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term("body", "bra")), BooleanClause.Occur.MUST)
        .build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);//ask for 2 but we'll only get 1
    assertArrayEquals(
        new String[]{"<b>Alpha</b> <b>Bravo</b> <b>Bravado</b> foo foo foo."}, snippets
    );

    ir.close();
  }

  public void testTokenStreamIsClosed() throws Exception {
    // note: test is a derivative of testWithMaxLen()
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("Alpha Bravo foo foo foo. Foo foo Alpha Bravo");
    if (random().nextBoolean()) { // sometimes add a 2nd value (maybe matters?)
      doc.add(new Field("body", "2nd value Alpha Bravo", fieldType));
    }
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    // use this buggy Analyzer at highlight time
    Analyzer buggyAnalyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer buggyTokenizer = new Tokenizer() {
          @Override
          public boolean incrementToken() throws IOException {
            throw new IOException("EXPECTED");
          }
        };
        return new TokenStreamComponents(buggyTokenizer);
      }
    };

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, buggyAnalyzer);
    highlighter.setHandleMultiTermQuery(true);
    if (rarely()) {
      highlighter.setMaxLength(25);//a little past first sentence
    }

    boolean hasClauses = false;
    BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
    if (random().nextBoolean()) {
      hasClauses = true;
      queryBuilder.add(new TermQuery(new Term("body", "alpha")), BooleanClause.Occur.MUST);
    }
    if (!hasClauses || random().nextBoolean()) {
      queryBuilder.add(new PrefixQuery(new Term("body", "bra")), BooleanClause.Occur.MUST);
    }
    BooleanQuery query = queryBuilder.build();
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    try {
      String snippets[] = highlighter.highlight("body", query, topDocs, 2);
      // don't even care what the results are; just want to test exception behavior
      if (fieldType == UHTestHelper.reanalysisType) {
        fail("Expecting EXPECTED IOException");
      }
    } catch (Exception e) {
      if (!e.getMessage().contains("EXPECTED")) {
        throw e;
      }
    }
    ir.close();

    // Now test we can get the tokenStream without it puking due to IllegalStateException for not calling close()

    try (TokenStream ts = buggyAnalyzer.tokenStream("body", "anything")) {
      ts.reset();// hopefully doesn't throw
      // don't call incrementToken; we know it's buggy ;-)
    }
  }

  /**
   * Not empty but nothing analyzes. Ensures we address null term-vectors.
   */
  public void testNothingAnalyzes() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();
    doc.add(new Field("body", " ", fieldType));// just a space! (thus not empty)
    doc.add(newTextField("id", "id", Field.Store.YES));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    Query query = new PrefixQuery(new Term("body", "nonexistent"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[]{"body"}, query, docIDs, new int[]{2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals(" ", snippets[0]);

    ir.close();
  }

  public void testMultiSegment() throws Exception {
    // If we incorrectly got the term vector from mis-matched global/leaf doc ID, this test may fail
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();
    doc.add(new Field("body", "word aberration", fieldType));
    iw.addDocument(doc);

    iw.commit(); // make segment

    doc = new Document();
    doc.add(new Field("body", "word absolve", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new PrefixQuery(new Term("body", "ab"));
    TopDocs topDocs = searcher.search(query, 10);

    String snippets[] = highlighter.highlightFields(new String[]{"body"}, query, topDocs).get("body");
    Arrays.sort(snippets);
    assertEquals("[word <b>aberration</b>, word <b>absolve</b>]", Arrays.toString(snippets));

    ir.close();
  }

  public void testPositionSensitiveWithWildcardDoesNotHighlight() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Document doc = new Document();
    doc.add(new Field("body", "iterate insect ipswitch illinois indirect", fieldType));
    doc.add(newTextField("id", "id", Field.Store.YES));

    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    PhraseQuery pq = new PhraseQuery.Builder()
        .add(new Term("body", "consent"))
        .add(new Term("body", "order"))
        .build();

    BooleanQuery query = new BooleanQuery.Builder()
        .add(new WildcardQuery(new Term("body", "enforc*")), BooleanClause.Occur.MUST)
        .add(pq, BooleanClause.Occur.MUST)
        .build();

    int[] docIds = new int[]{docID};

    String snippets[] = highlighter.highlightFields(new String[]{"body"}, query, docIds, new int[]{2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals("iterate insect ipswitch illinois indirect", snippets[0]);
    ir.close();
  }

  public void testCustomSpanQueryHighlighting() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Document doc = new Document();
    doc.add(new Field("body", "alpha bravo charlie delta echo foxtrot golf hotel india juliet", fieldType));
    doc.add(newTextField("id", "id", Field.Store.YES));

    iw.addDocument(doc);
    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected List<Query> preMultiTermQueryRewrite(Query query) {
        if (query instanceof MyWrapperSpanQuery) {
          return Collections.singletonList(((MyWrapperSpanQuery) query).originalQuery);
        }
        return null;
      }
    };

    int docId = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    WildcardQuery wildcardQuery = new WildcardQuery(new Term("body", "foxtr*"));
    SpanMultiTermQueryWrapper wildcardQueryWrapper = new SpanMultiTermQueryWrapper<>(wildcardQuery);

    SpanQuery wrappedQuery = new MyWrapperSpanQuery(wildcardQueryWrapper);

    BooleanQuery query = new BooleanQuery.Builder()
        .add(wrappedQuery, BooleanClause.Occur.SHOULD)
        .build();

    int[] docIds = new int[]{docId};

    String snippets[] = highlighter.highlightFields(new String[]{"body"}, query, docIds, new int[]{2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals("alpha bravo charlie delta echo <b>foxtrot</b> golf hotel india juliet", snippets[0]);
    ir.close();
  }

  private static class MyWrapperSpanQuery extends SpanQuery {

    private final SpanQuery originalQuery;

    private MyWrapperSpanQuery(SpanQuery originalQuery) {
      this.originalQuery = Objects.requireNonNull(originalQuery);
    }

    @Override
    public String getField() {
      return originalQuery.getField();
    }

    @Override
    public String toString(String field) {
      return "(Wrapper[" + originalQuery.toString(field)+"])";
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      return originalQuery.createWeight(searcher, needsScores, boost);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query newOriginalQuery = originalQuery.rewrite(reader);
      if (newOriginalQuery != originalQuery) {
        return new MyWrapperSpanQuery((SpanQuery)newOriginalQuery);
      }
      return this;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      return originalQuery.equals(((MyWrapperSpanQuery)o).originalQuery);
    }

    @Override
    public int hashCode() {
      return originalQuery.hashCode();
    }
  }

}
