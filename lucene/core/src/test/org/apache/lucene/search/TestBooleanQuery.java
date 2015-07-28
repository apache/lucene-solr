package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanTopLevelScorers.BoostedScorer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.TestUtil;

public class TestBooleanQuery extends LuceneTestCase {

  public void testEquality() throws Exception {
    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq1.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery nested1 = new BooleanQuery();
    nested1.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested1.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq1.add(nested1, BooleanClause.Occur.SHOULD);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery nested2 = new BooleanQuery();
    nested2.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested2.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq2.add(nested2, BooleanClause.Occur.SHOULD);

    assertEquals(bq1, bq2);
  }

  public void testException() {
    try {
      BooleanQuery.setMaxClauseCount(0);
      fail();
    } catch (IllegalArgumentException e) {
      // okay
    }
  }

  // LUCENE-1630
  public void testNullOrSubScorer() throws Throwable {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c d", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    // this test relies upon coord being the default implementation,
    // otherwise scores are different!
    s.setSimilarity(new DefaultSimilarity());

    BooleanQuery q = new BooleanQuery();
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    // LUCENE-2617: make sure that a term not in the index still contributes to the score via coord factor
    float score = s.search(q, 10).getMaxScore();
    Query subQuery = new TermQuery(new Term("field", "not_in_index"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.SHOULD);
    float score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*.5F, score2, 1e-6);

    // LUCENE-2617: make sure that a clause not in the index still contributes to the score via coord factor
    BooleanQuery qq = q.clone();
    PhraseQuery phrase = new PhraseQuery("field", "not_in_index", "another_not_in_index");
    phrase.setBoost(0);
    qq.add(phrase, BooleanClause.Occur.SHOULD);
    score2 = s.search(qq, 10).getMaxScore();
    assertEquals(score*(1/3F), score2, 1e-6);

    // now test BooleanScorer2
    subQuery = new TermQuery(new Term("field", "b"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.MUST);
    score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*(2/3F), score2, 1e-6);

    // PhraseQuery w/ no terms added returns a null scorer
    PhraseQuery pq = new PhraseQuery("field", new String[0]);
    q.add(pq, BooleanClause.Occur.SHOULD);
    assertEquals(1, s.search(q, 10).totalHits);

    // A required clause which returns null scorer should return null scorer to
    // IndexSearcher.
    q = new BooleanQuery();
    pq = new PhraseQuery("field", new String[0]);
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    q.add(pq, BooleanClause.Occur.MUST);
    assertEquals(0, s.search(q, 10).totalHits);

    DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(1.0f);
    dmq.add(new TermQuery(new Term("field", "a")));
    dmq.add(pq);
    assertEquals(1, s.search(dmq, 10).totalHits);

    r.close();
    w.close();
    dir.close();
  }

  public void testDeMorgan() throws Exception {
    Directory dir1 = newDirectory();
    RandomIndexWriter iw1 = new RandomIndexWriter(random(), dir1);
    Document doc1 = new Document();
    doc1.add(newTextField("field", "foo bar", Field.Store.NO));
    iw1.addDocument(doc1);
    IndexReader reader1 = iw1.getReader();
    iw1.close();

    Directory dir2 = newDirectory();
    RandomIndexWriter iw2 = new RandomIndexWriter(random(), dir2);
    Document doc2 = new Document();
    doc2.add(newTextField("field", "foo baz", Field.Store.NO));
    iw2.addDocument(doc2);
    IndexReader reader2 = iw2.getReader();
    iw2.close();

    BooleanQuery query = new BooleanQuery(); // Query: +foo -ba*
    query.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.MUST);
    WildcardQuery wildcardQuery = new WildcardQuery(new Term("field", "ba*"));
    wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
    query.add(wildcardQuery, BooleanClause.Occur.MUST_NOT);

    MultiReader multireader = new MultiReader(reader1, reader2);
    IndexSearcher searcher = newSearcher(multireader);
    assertEquals(0, searcher.search(query, 10).totalHits);

    final ExecutorService es = Executors.newCachedThreadPool(new NamedThreadFactory("NRT search threads"));
    searcher = new IndexSearcher(multireader, es);
    if (VERBOSE)
      System.out.println("rewritten form: " + searcher.rewrite(query));
    assertEquals(0, searcher.search(query, 10).totalHits);
    es.shutdown();
    es.awaitTermination(1, TimeUnit.SECONDS);

    multireader.close();
    reader1.close();
    reader2.close();
    dir1.close();
    dir2.close();
  }

  public void testBS2DisjunctionNextVsAdvance() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    final int numDocs = atLeast(300);
    for(int docUpto=0;docUpto<numDocs;docUpto++) {
      String contents = "a";
      if (random().nextInt(20) <= 16) {
        contents += " b";
      }
      if (random().nextInt(20) <= 8) {
        contents += " c";
      }
      if (random().nextInt(20) <= 4) {
        contents += " d";
      }
      if (random().nextInt(20) <= 2) {
        contents += " e";
      }
      if (random().nextInt(20) <= 1) {
        contents += " f";
      }
      Document doc = new Document();
      doc.add(new TextField("field", contents, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = w.getReader();
    final IndexSearcher s = newSearcher(r);
    w.close();

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      if (VERBOSE) {
        System.out.println("iter=" + iter);
      }
      final List<String> terms = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f"));
      final int numTerms = TestUtil.nextInt(random(), 1, terms.size());
      while(terms.size() > numTerms) {
        terms.remove(random().nextInt(terms.size()));
      }

      if (VERBOSE) {
        System.out.println("  terms=" + terms);
      }

      final BooleanQuery q = new BooleanQuery();
      for(String term : terms) {
        q.add(new BooleanClause(new TermQuery(new Term("field", term)), BooleanClause.Occur.SHOULD));
      }

      Weight weight = s.createNormalizedWeight(q, true);

      Scorer scorer = weight.scorer(s.leafContexts.get(0));

      // First pass: just use .nextDoc() to gather all hits
      final List<ScoreDoc> hits = new ArrayList<>();
      while(scorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        hits.add(new ScoreDoc(scorer.docID(), scorer.score()));
      }

      if (VERBOSE) {
        System.out.println("  " + hits.size() + " hits");
      }

      // Now, randomly next/advance through the list and
      // verify exact match:
      for(int iter2=0;iter2<10;iter2++) {

        weight = s.createNormalizedWeight(q, true);
        scorer = weight.scorer(s.leafContexts.get(0));

        if (VERBOSE) {
          System.out.println("  iter2=" + iter2);
        }

        int upto = -1;
        while(upto < hits.size()) {
          final int nextUpto;
          final int nextDoc;
          final int left = hits.size() - upto;
          if (left == 1 || random().nextBoolean()) {
            // next
            nextUpto = 1+upto;
            nextDoc = scorer.nextDoc();
          } else {
            // advance
            int inc = TestUtil.nextInt(random(), 1, left - 1);
            nextUpto = inc + upto;
            nextDoc = scorer.advance(hits.get(nextUpto).doc);
          }

          if (nextUpto == hits.size()) {
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, nextDoc);
          } else {
            final ScoreDoc hit = hits.get(nextUpto);
            assertEquals(hit.doc, nextDoc);
            // Test for precise float equality:
            assertTrue("doc " + hit.doc + " has wrong score: expected=" + hit.score + " actual=" + scorer.score(), hit.score == scorer.score());
          }
          upto = nextUpto;
        }
      }
    }

    r.close();
    d.close();
  }

  // LUCENE-4477 / LUCENE-4401:
  public void testBooleanSpanQuery() throws Exception {
    boolean failed = false;
    int hits = 0;
    Directory directory = newDirectory();
    Analyzer indexerAnalyzer = new MockAnalyzer(random());

    IndexWriterConfig config = new IndexWriterConfig(indexerAnalyzer);
    IndexWriter writer = new IndexWriter(directory, config);
    String FIELD = "content";
    Document d = new Document();
    d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
    writer.addDocument(d);
    writer.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(indexReader);

    BooleanQuery query = new BooleanQuery();
    SpanQuery sq1 = new SpanTermQuery(new Term(FIELD, "clockwork"));
    SpanQuery sq2 = new SpanTermQuery(new Term(FIELD, "clckwork"));
    query.add(sq1, BooleanClause.Occur.SHOULD);
    query.add(sq2, BooleanClause.Occur.SHOULD);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000);
    searcher.search(query, collector);
    hits = collector.topDocs().scoreDocs.length;
    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs){
      System.out.println(scoreDoc.doc);
    }
    indexReader.close();
    assertEquals("Bug in boolean query composed of span queries", failed, false);
    assertEquals("Bug in boolean query composed of span queries", hits, 1);
    directory.close();
  }

  public void testOneClauseRewriteOptimization() throws Exception {
    final float BOOST = 3.5F;
    final String FIELD = "content";
    final String VALUE = "foo";

    Directory dir = newDirectory();
    (new RandomIndexWriter(random(), dir)).close();
    IndexReader r = DirectoryReader.open(dir);

    TermQuery expected = new TermQuery(new Term(FIELD, VALUE));
    expected.setBoost(BOOST);

    final int numLayers = atLeast(3);
    boolean needBoost = true;
    Query actual = new TermQuery(new Term(FIELD, VALUE));

    for (int i = 0; i < numLayers; i++) {
      if (needBoost && 0 == TestUtil.nextInt(random(),0,numLayers)) {
        needBoost = false;
        actual.setBoost(BOOST);
      }

      BooleanQuery bq = new BooleanQuery();
      bq.add(actual, random().nextBoolean()
             ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST);
      actual = bq;
    }
    if (needBoost) {
      actual.setBoost(BOOST);
    }

    assertEquals(numLayers + ": " + actual.toString(),
                 expected, actual.rewrite(r));

    r.close();
    dir.close();
  }

  public void testMinShouldMatchLeniency() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(newTextField("field", "a b c d", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w, true);
    IndexSearcher s = newSearcher(r);
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "b")), BooleanClause.Occur.SHOULD);

    // No doc can match: BQ has only 2 clauses and we are asking for minShouldMatch=4
    bq.setMinimumNumberShouldMatch(4);
    assertEquals(0, s.search(bq, 1).totalHits);
    r.close();
    w.close();
    dir.close();
  }

  private static BitSet getMatches(IndexSearcher searcher, Query query) throws IOException {
    final BitSet set = new BitSet();
    searcher.search(query, new SimpleCollector() {
      int docBase = 0;
      @Override
      public boolean needsScores() {
        return random().nextBoolean();
      }
      @Override
      protected void doSetNextReader(LeafReaderContext context)
          throws IOException {
        super.doSetNextReader(context);
        docBase = context.docBase;
      }
      @Override
      public void collect(int doc) throws IOException {
        set.set(docBase + doc);
      }
    });
    return set;
  }

  public void testFILTERClauseBehavesLikeMUST() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c d", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("b d");
    w.addDocument(doc);
    f.setStringValue("d");
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    for (List<String> requiredTerms : Arrays.<List<String>>asList(
        Arrays.asList("a", "d"),
        Arrays.asList("a", "b", "d"),
        Arrays.asList("d"),
        Arrays.asList("e"),
        Arrays.<String>asList())) {
      final BooleanQuery bq1 = new BooleanQuery();
      final BooleanQuery bq2 = new BooleanQuery();
      for (String term : requiredTerms) {
        final Query q = new TermQuery(new Term("field", term));
        bq1.add(q, Occur.MUST);
        bq2.add(q, Occur.FILTER);
      }

      final BitSet matches1 = getMatches(searcher, bq1);
      final BitSet matches2 = getMatches(searcher, bq2);
      assertEquals(matches1, matches2);
    }

    reader.close();
    w.close();
    dir.close();
  }

  private void assertSameScoresWithoutFilters(final IndexSearcher searcher, BooleanQuery bq) throws IOException {
    final BooleanQuery bq2 = new BooleanQuery();
    for (BooleanClause c : bq.getClauses()) {
      if (c.getOccur() != Occur.FILTER) {
        bq2.add(c);
      }
    }
    bq2.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
    bq2.setBoost(bq.getBoost());

    final AtomicBoolean matched = new AtomicBoolean();
    searcher.search(bq, new SimpleCollector() {
      int docBase;
      Scorer scorer;

      @Override
      protected void doSetNextReader(LeafReaderContext context)
          throws IOException {
        super.doSetNextReader(context);
        docBase = context.docBase;
      }

      @Override
      public boolean needsScores() {
        return true;
      }

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        final float actualScore = scorer.score();
        final float expectedScore = searcher.explain(bq2, docBase + doc).getValue();
        assertEquals(expectedScore, actualScore, 10e-5);
        matched.set(true);
      }
    });
    assertTrue(matched.get());
  }

  public void testFilterClauseDoesNotImpactScore() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c d", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    f.setStringValue("b d");
    w.addDocument(doc);
    f.setStringValue("a d");
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery q = new BooleanQuery();
    q.setBoost(random().nextFloat());
    q.add(new TermQuery(new Term("field", "a")), Occur.FILTER);

    // With a single clause, we will rewrite to the underlying
    // query. Make sure that it returns null scores
    assertSameScoresWithoutFilters(searcher, q);

    // Now with two clauses, we will get a conjunction scorer
    // Make sure it returns null scores
    q.add(new TermQuery(new Term("field", "b")), Occur.FILTER);
    assertSameScoresWithoutFilters(searcher, q);

    // Now with a scoring clause, we need to make sure that
    // the boolean scores are the same as those from the term
    // query
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);
    assertSameScoresWithoutFilters(searcher, q);

    // FILTER and empty SHOULD
    q = new BooleanQuery();
    q.setBoost(random().nextFloat());
    q.add(new TermQuery(new Term("field", "a")), Occur.FILTER);
    q.add(new TermQuery(new Term("field", "e")), Occur.SHOULD);
    assertSameScoresWithoutFilters(searcher, q);

    // mix of FILTER and MUST
    q = new BooleanQuery();
    q.setBoost(random().nextFloat());
    q.add(new TermQuery(new Term("field", "a")), Occur.FILTER);
    q.add(new TermQuery(new Term("field", "d")), Occur.MUST);
    assertSameScoresWithoutFilters(searcher, q);

    // FILTER + minShouldMatch
    q = new BooleanQuery();
    q.setBoost(random().nextFloat());
    q.add(new TermQuery(new Term("field", "b")), Occur.FILTER);
    q.add(new TermQuery(new Term("field", "a")), Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "d")), Occur.SHOULD);
    q.setMinimumNumberShouldMatch(1);
    assertSameScoresWithoutFilters(searcher, q);

    reader.close();
    w.close();
    dir.close();
  }

  public void testSingleFilterClause() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);

    BooleanQuery query1 = new BooleanQuery();
    query1.add(new TermQuery(new Term("field", "a")), Occur.FILTER);

    // Single clauses rewrite to a term query
    final Query rewritten1 = query1.rewrite(reader);
    assertTrue(rewritten1 instanceof ConstantScoreQuery);
    assertEquals(0f, rewritten1.getBoost(), 0f);

    // When there are two clauses, we cannot rewrite, but if one of them creates
    // a null scorer we will end up with a single filter scorer and will need to
    // make sure to set score=0
    BooleanQuery query2 = new BooleanQuery();
    query2.add(new TermQuery(new Term("field", "a")), Occur.FILTER);
    query2.add(new TermQuery(new Term("field", "b")), Occur.SHOULD);
    final Weight weight = searcher.createNormalizedWeight(query2, true);
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertEquals(0, scorer.nextDoc());
    assertTrue(scorer.getClass().getName(), scorer instanceof FilterScorer);
    assertEquals(0f, scorer.score(), 0f);

    reader.close();
    w.close();
    dir.close();
  }

  public void testConjunctionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    // not LuceneTestCase.newSearcher to not have the asserting wrappers
    // and do instanceof checks
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery q = new BooleanQuery();
    q.add(pq, Occur.MUST);
    q.add(new TermQuery(new Term("field", "c")), Occur.FILTER);

    final Weight weight = searcher.createNormalizedWeight(q, random().nextBoolean());
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertTrue(scorer instanceof ConjunctionScorer);
    assertNotNull(scorer.asTwoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testDisjunctionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery q = new BooleanQuery();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);

    final Weight weight = searcher.createNormalizedWeight(q, random().nextBoolean());
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof DisjunctionScorer);
    assertNotNull(scorer.asTwoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testBoostedScorerPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    // not LuceneTestCase.newSearcher to not have the asserting wrappers
    // and do instanceof checks
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery q = new BooleanQuery();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "d")), Occur.SHOULD);

    final Weight weight = searcher.createNormalizedWeight(q, random().nextBoolean());
    final Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertTrue(scorer instanceof BoostedScorer || scorer instanceof ExactPhraseScorer);
    assertNotNull(scorer.asTwoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testExclusionPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery q = new BooleanQuery();
    q.add(pq, Occur.SHOULD);
    q.add(new TermQuery(new Term("field", "c")), Occur.MUST_NOT);

    final Weight weight = searcher.createNormalizedWeight(q, random().nextBoolean());
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof ReqExclScorer);
    assertNotNull(scorer.asTwoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }

  public void testReqOptPropagatesApproximations() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    Field f = newTextField("field", "a b c", Field.Store.NO);
    doc.add(f);
    w.addDocument(doc);
    w.commit();

    DirectoryReader reader = w.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null); // to still have approximations

    PhraseQuery pq = new PhraseQuery("field", "a", "b");

    BooleanQuery q = new BooleanQuery();
    q.add(pq, Occur.MUST);
    q.add(new TermQuery(new Term("field", "c")), Occur.SHOULD);

    final Weight weight = searcher.createNormalizedWeight(q, true);
    final Scorer scorer = weight.scorer(reader.leaves().get(0));
    assertTrue(scorer instanceof ReqOptSumScorer);
    assertNotNull(scorer.asTwoPhaseIterator());

    reader.close();
    w.close();
    dir.close();
  }
  
  public void testToString() {
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term("field", "a")), Occur.SHOULD);
    bq.add(new TermQuery(new Term("field", "b")), Occur.MUST);
    bq.add(new TermQuery(new Term("field", "c")), Occur.MUST_NOT);
    bq.add(new TermQuery(new Term("field", "d")), Occur.FILTER);
    assertEquals("a +b -c #d", bq.toString("field"));
  }

  public void testExtractTerms() throws IOException {
    Term a = new Term("f", "a");
    Term b = new Term("f", "b");
    Term c = new Term("f", "c");
    Term d = new Term("f", "d");
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(a), Occur.SHOULD);
    bq.add(new TermQuery(b), Occur.MUST);
    bq.add(new TermQuery(c), Occur.FILTER);
    bq.add(new TermQuery(d), Occur.MUST_NOT);
    IndexSearcher searcher = new IndexSearcher(new MultiReader());

    Set<Term> scoringTerms = new HashSet<>();
    searcher.createNormalizedWeight(bq, true).extractTerms(scoringTerms);
    assertEquals(new HashSet<>(Arrays.asList(a, b)), scoringTerms);

    Set<Term> matchingTerms = new HashSet<>();
    searcher.createNormalizedWeight(bq, false).extractTerms(matchingTerms);
    assertEquals(new HashSet<>(Arrays.asList(a, b, c)), matchingTerms);
  }
}
