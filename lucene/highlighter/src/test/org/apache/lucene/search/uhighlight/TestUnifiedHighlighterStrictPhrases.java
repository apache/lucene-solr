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
import java.util.Collection;
import java.util.Collections;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QueryBuilder;
import org.junit.After;
import org.junit.Before;

@LuceneTestCase.SuppressCodecs({"MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene3x"})
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")//Gradle interferes with this Lucene test rule
public class TestUnifiedHighlighterStrictPhrases extends LuceneTestCase {

  final FieldType fieldType;

  Directory dir;
  MockAnalyzer indexAnalyzer;
  RandomIndexWriter indexWriter;
  IndexSearcher searcher;
  UnifiedHighlighter highlighter;
  IndexReader indexReader;

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return UHTestHelper.parametersFactoryList();
  }

  public TestUnifiedHighlighterStrictPhrases(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  @Before
  public void doBefore() throws IOException {
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase
    indexAnalyzer.setPositionIncrementGap(3);// more than default
    dir = newDirectory();
    indexWriter = new RandomIndexWriter(random(), dir, indexAnalyzer);
  }

  @After
  public void doAfter() throws IOException {
    IOUtils.close(indexReader, indexWriter, dir);
  }

  private Document newDoc(String... bodyVals) {
    Document doc = new Document();
    for (String bodyVal : bodyVals) {
      doc.add(new Field("body", bodyVal, fieldType));
    }
    return doc;
  }

  private void initReaderSearcherHighlighter() throws IOException {
    indexReader = indexWriter.getReader();
    searcher = newSearcher(indexReader);
    highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setHighlightPhrasesStrictly(true);
  }

  private PhraseQuery newPhraseQuery(String field, String phrase) {
    return (PhraseQuery) new QueryBuilder(indexAnalyzer).createPhraseQuery(field, phrase);
  }

  private PhraseQuery setSlop(PhraseQuery query, int slop) {
    PhraseQuery.Builder builder = new PhraseQuery.Builder();
    Term[] terms = query.getTerms();
    int[] positions = query.getPositions();
    for (int i = 0; i < terms.length; i++) {
      builder.add(terms[i], positions[i]);
    }
    builder.setSlop(slop);
    return builder.build();
  }

  public void testBasics() throws IOException {
    indexWriter.addDocument(newDoc("Yin yang, filter")); // filter out. test getTermToSpanLists reader 1-doc filter
    indexWriter.addDocument(newDoc("yin alone, Yin yang, yin gap yang"));
    initReaderSearcherHighlighter();

    //query:  -filter +"yin yang"
    BooleanQuery query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "filter")), BooleanClause.Occur.MUST_NOT)
        .add(newPhraseQuery("body", "yin yang"), BooleanClause.Occur.MUST)
        .build();


    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"yin alone, <b>Yin</b> <b>yang</b>, yin gap yang"}, snippets);
  }

  public void testWithSameTermQuery() throws IOException {
    indexWriter.addDocument(newDoc("Yin yang, yin gap yang"));
    initReaderSearcherHighlighter();

    BooleanQuery query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "yin")), BooleanClause.Occur.MUST)
        .add(newPhraseQuery("body", "yin yang"), BooleanClause.Occur.MUST)
        // add queries for other fields; we shouldn't highlight these because of that.
        .add(new TermQuery(new Term("title", "yang")), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>Yin</b> <b>yang</b>, <b>yin</b> gap yang"}, snippets);
  }

  public void testPhraseNotInDoc() throws IOException {
    indexWriter.addDocument(newDoc("Whatever yin")); // query matches this; highlight it
    indexWriter.addDocument(newDoc("nextdoc yin"));// query does NOT match this, only the SHOULD clause does
    initReaderSearcherHighlighter();

    BooleanQuery query = new BooleanQuery.Builder()
        //MUST:
        .add(new TermQuery(new Term("body", "whatever")), BooleanClause.Occur.MUST)
        //SHOULD: (yet won't)
        .add(newPhraseQuery("body", "nextdoc yin"), BooleanClause.Occur.SHOULD)
        .add(newPhraseQuery("body", "nonexistent yin"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>Whatever</b> yin"}, snippets);
  }

  public void testSubPhrases() throws IOException {
    indexWriter.addDocument(newDoc("alpha bravo charlie - charlie bravo alpha"));
    initReaderSearcherHighlighter();

    BooleanQuery query = new BooleanQuery.Builder()
        .add(newPhraseQuery("body", "alpha bravo charlie"), BooleanClause.Occur.MUST)
        .add(newPhraseQuery("body", "alpha bravo"), BooleanClause.Occur.MUST)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> - charlie bravo alpha"}, snippets);
  }

  public void testSynonyms() throws IOException {
    indexWriter.addDocument(newDoc("mother father w mom father w dad"));
    initReaderSearcherHighlighter();

    MultiPhraseQuery query = new MultiPhraseQuery.Builder()
        .add(new Term[]{new Term("body", "mom"), new Term("body", "mother")})
        .add(new Term[]{new Term("body", "dad"), new Term("body", "father")})
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>mother</b> <b>father</b> w <b>mom</b> <b>father</b> w dad"}, snippets);
  }

  /**
   * Test it does *not* highlight the same term's not next to the span-near.  "charlie" in this case.
   * This particular example exercises "Rewrite" plus "MTQ" in the same query.
   */
  public void testRewriteAndMtq() throws IOException {
    indexWriter.addDocument(newDoc("alpha bravo charlie - charlie bravo alpha"));
    initReaderSearcherHighlighter();

    SpanNearQuery snq = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term("body", "bravo")),
            new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term("body", "ch")))}, // REWRITES
        0, true);

    BooleanQuery query = new BooleanQuery.Builder()
        .add(snq, BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term("body", "al")), BooleanClause.Occur.MUST) // MTQ
        .add(newPhraseQuery("body", "alpha bravo"), BooleanClause.Occur.MUST)
        // add queries for other fields; we shouldn't highlight these because of that.
        .add(newPhraseQuery("title", "bravo alpha"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> - charlie bravo <b>alpha</b>"},
        snippets);

    // do again, this time with MTQ disabled.  We should only find "alpha bravo".
    highlighter.setHandleMultiTermQuery(false);//disable but leave phrase processing enabled

    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> charlie - charlie bravo alpha"},
        snippets);
  }

  /**
   * Like {@link #testRewriteAndMtq} but no freestanding MTQ
   */
  public void testRewrite() throws IOException {
    indexWriter.addDocument(newDoc("alpha bravo charlie - charlie bravo alpha"));
    initReaderSearcherHighlighter();

    SpanNearQuery snq = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term("body", "bravo")),
            new SpanMultiTermQueryWrapper<>(new PrefixQuery(new Term("body", "ch")))}, // REWRITES
        0, true);
    BooleanQuery query = new BooleanQuery.Builder()
        .add(snq, BooleanClause.Occur.MUST)
//          .add(new PrefixQuery(new Term("body", "al")), BooleanClause.Occur.MUST) // MTQ
        .add(newPhraseQuery("body", "alpha bravo"), BooleanClause.Occur.MUST)
        // add queries for other fields; we shouldn't highlight these because of that.
        .add(newPhraseQuery("title", "bravo alpha"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> - charlie bravo alpha"},
        snippets);

    // do again, this time with MTQ disabled.  We should only find "alpha bravo".
    highlighter.setHandleMultiTermQuery(false);//disable but leave phrase processing enabled

    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> charlie - charlie bravo alpha"},
        snippets);
  }

  /**
   * Like {@link #testRewriteAndMtq} but no rewrite.
   */
  public void testMtq() throws IOException {
    indexWriter.addDocument(newDoc("alpha bravo charlie - charlie bravo alpha"));
    initReaderSearcherHighlighter();

    SpanNearQuery snq = new SpanNearQuery(
        new SpanQuery[]{
            new SpanTermQuery(new Term("body", "bravo")),
            new SpanTermQuery(new Term("body", "charlie"))}, // does NOT rewrite
        0, true);

    BooleanQuery query = new BooleanQuery.Builder()
        .add(snq, BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term("body", "al")), BooleanClause.Occur.MUST) // MTQ
        .add(newPhraseQuery("body", "alpha bravo"), BooleanClause.Occur.MUST)
        // add queries for other fields; we shouldn't highlight these because of that.
        .add(newPhraseQuery("title", "bravo alpha"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> - charlie bravo <b>alpha</b>"},
        snippets);

    // do again, this time with MTQ disabled.
    highlighter.setHandleMultiTermQuery(false);//disable but leave phrase processing enabled

    topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    snippets = highlighter.highlight("body", query, topDocs);

    assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> - charlie bravo alpha"},
        snippets);
  }

  public void testMultiValued() throws IOException {
    indexWriter.addDocument(newDoc("one bravo three", "four bravo six"));
    initReaderSearcherHighlighter();

    BooleanQuery query = new BooleanQuery.Builder()
        .add(newPhraseQuery("body", "one bravo"), BooleanClause.Occur.MUST)
        .add(newPhraseQuery("body", "four bravo"), BooleanClause.Occur.MUST)
        .add(new PrefixQuery(new Term("body", "br")), BooleanClause.Occur.MUST)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs, 2);

    assertArrayEquals(new String[]{"<b>one</b> <b>bravo</b> three... <b>four</b> <b>bravo</b> six"},
        snippets);


    // now test phraseQuery won't span across values
    assert indexAnalyzer.getPositionIncrementGap("body") > 0;

    PhraseQuery phraseQuery = newPhraseQuery("body", "three four");
    // 1 too little; won't span
    phraseQuery = setSlop(phraseQuery, indexAnalyzer.getPositionIncrementGap("body") - 1);

    query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "bravo")), BooleanClause.Occur.MUST)
        .add(phraseQuery, BooleanClause.Occur.SHOULD)
        .build();

    topDocs = searcher.search(query, 10);
    snippets = highlighter.highlight("body", query, topDocs, 2);
    assertEquals("one <b>bravo</b> three... four <b>bravo</b> six", snippets[0]);

    // and add just enough slop to cross the values:
    phraseQuery = newPhraseQuery("body", "three four");
    phraseQuery = setSlop(phraseQuery, indexAnalyzer.getPositionIncrementGap("body")); // just enough to span
    query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "bravo")), BooleanClause.Occur.MUST)
        .add(phraseQuery, BooleanClause.Occur.MUST) // must match and it will
        .build();
    topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits);
    snippets = highlighter.highlight("body", query, topDocs, 2);
    assertEquals("one <b>bravo</b> <b>three</b>... <b>four</b> <b>bravo</b> six", snippets[0]);
  }

  public void testMaxLen() throws IOException {
    indexWriter.addDocument(newDoc("alpha bravo charlie - gap alpha bravo")); // hyphen is at char 21
    initReaderSearcherHighlighter();
    highlighter.setMaxLength(21);

    BooleanQuery query = new BooleanQuery.Builder()
        .add(newPhraseQuery("body", "alpha bravo"), BooleanClause.Occur.MUST)
        .add(newPhraseQuery("body", "gap alpha"), BooleanClause.Occur.MUST)
        .add(newPhraseQuery("body", "charlie gap"), BooleanClause.Occur.SHOULD)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);

    if (fieldType == UHTestHelper.reanalysisType) {
      assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> charlie -"}, snippets);
    } else {
      assertArrayEquals(new String[]{"<b>alpha</b> <b>bravo</b> <b>charlie</b> -"}, snippets);
    }
  }

  public void testFilteredOutSpan() throws IOException {
    indexWriter.addDocument(newDoc("freezing cold stuff like stuff freedom of speech"));
    initReaderSearcherHighlighter();

    WildcardQuery wildcardQuery = new WildcardQuery(new Term("body", "free*"));
    SpanMultiTermQueryWrapper<WildcardQuery> wildcardSpanQuery = new SpanMultiTermQueryWrapper<>(wildcardQuery);
    SpanTermQuery termQuery = new SpanTermQuery(new Term("body", "speech"));
    SpanQuery spanQuery = new SpanNearQuery(new SpanQuery[]{wildcardSpanQuery, termQuery}, 3, false);

    BooleanQuery query = new BooleanQuery.Builder()
        .add(spanQuery, BooleanClause.Occur.MUST)
        .build();

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    String[] snippets = highlighter.highlight("body", query, topDocs);
    assertArrayEquals(new String[]{"freezing cold stuff like stuff <b>freedom</b> of <b>speech</b>"}, snippets);
  }

  public void testMatchNoDocsQuery() throws IOException {
    highlighter = new UnifiedHighlighter(null, indexAnalyzer);
    highlighter.setHighlightPhrasesStrictly(true);
    String content = "whatever";
    Object o = highlighter.highlightWithoutSearcher("body", new MatchNoDocsQuery(), content, 1);
    assertEquals(content, o);
  }

  public void testPreSpanQueryRewrite() throws IOException {
    indexWriter.addDocument(newDoc("There is no accord and satisfaction with this - Consideration of the accord is arbitrary."));
    initReaderSearcherHighlighter();

    highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected Collection<Query> preSpanQueryRewrite(Query query) {
        if (query instanceof MyQuery) {
          return Collections.singletonList(((MyQuery)query).wrapped);
        }
        return null;
      }
    };
    highlighter.setHighlightPhrasesStrictly(true);

    BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
    Query phraseQuery = new BoostQuery(new PhraseQuery("body", "accord", "and", "satisfaction"), 2.0f);
    Query oredTerms = new BooleanQuery.Builder()
        .setMinimumNumberShouldMatch(2)
        .add(new TermQuery(new Term("body", "accord")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("body", "satisfaction")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("body", "consideration")), BooleanClause.Occur.SHOULD)
        .build();
    Query proximityBoostingQuery = new MyQuery(oredTerms);
    Query totalQuery = bqBuilder
        .add(phraseQuery, BooleanClause.Occur.SHOULD)
        .add(proximityBoostingQuery, BooleanClause.Occur.SHOULD)
        .build();
    TopDocs topDocs = searcher.search(totalQuery, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits);
    String[] snippets = highlighter.highlight("body", totalQuery, topDocs);
    assertArrayEquals(new String[]{"There is no <b>accord</b> <b>and</b> <b>satisfaction</b> with this - <b>Consideration</b> of the <b>accord</b> is arbitrary."}, snippets);
  }

  private static class MyQuery extends Query {

    private final Query wrapped;

    MyQuery(Query wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      return wrapped.createWeight(searcher, needsScores, boost);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
      Query newWrapped = wrapped.rewrite(reader);
      if (newWrapped != wrapped) {
        return new MyQuery(newWrapped);
      }
      return this;
    }

    @Override
    public String toString(String field) {
      return "[[["+wrapped.toString(field)+"]]]";
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj.getClass() == getClass() && wrapped.equals(((MyQuery)wrapped).wrapped);
    }

    @Override
    public int hashCode() {
      return wrapped.hashCode();
    }
  }
}
