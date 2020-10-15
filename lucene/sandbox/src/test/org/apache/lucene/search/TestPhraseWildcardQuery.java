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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.PhraseWildcardQuery.TestCounters;

/**
 * Tests {@link PhraseWildcardQuery}.
 * <p>
 * The main goal of this class is to verify that {@link PhraseWildcardQuery}
 * has the same ranking and same scoring than both {@link MultiPhraseQuery}
 * and {@link SpanNearQuery}.
 * <p>
 * Note that the ranking and scoring are equal if the segment optimization
 * is disabled, otherwise it may change the score, but the ranking is most
 * often the same.
 */
public class TestPhraseWildcardQuery extends LuceneTestCase {

  protected static final int MAX_DOCS = 1000;
  protected static final String[] FIELDS = {"title", "author", "category", "other"};

  protected Directory directory;
  protected IndexReader reader;
  protected IndexSearcher searcher;
  protected boolean differentScoreExpectedForSpanNearQuery;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory,
                                                 newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)); // do not accidentally merge
                                                                                                                 // the two segments we create
                                                                                                                 // here
    iw.setDoRandomForceMerge(false); // Keep the segments separated.
    addSegments(iw);
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
    assertEquals("test test relies on 2 segments", 2, searcher.getIndexReader().leaves().size());
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  public void testOneMultiTerm() throws Exception {
    searchAndCheckResults(field(1), 100, "eric", "br*");
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(1).segmentUse(4).segmentSkip(0);
  }

  public void testTwoMultiTerms() throws Exception {
    searchAndCheckResults(field(1), 100, "e*", "b*");
    assertCounters().singleTermAnalysis(0).multiTermAnalysis(2).segmentUse(4).segmentSkip(0);

    expectDifferentScoreForSpanNearQueryWithMultiTermSubset(() -> {
      searchAndCheckResults(field(2), 100, "tim*", "t*");
      assertCounters().singleTermAnalysis(0).multiTermAnalysis(2).segmentUse(2).segmentSkip(1);
    });
  }

  public void testThreeMultiTerms() throws Exception {
    searchAndCheckResults(field(0), 100, "t*", "ut?pi?", "e*");
    assertCounters().singleTermAnalysis(0).multiTermAnalysis(3).segmentUse(4).segmentSkip(1);

    searchAndCheckResults(field(0), 100, "t?e", "u*", "e*");
    assertCounters().singleTermAnalysis(0).multiTermAnalysis(3).segmentUse(4).segmentSkip(1);

    expectDifferentScoreForSpanNearQueryWithMultiTermSubset(() -> {
      searchAndCheckResults(field(0), 100, "t?e", "b*", "b*");
      assertCounters().singleTermAnalysis(0).multiTermAnalysis(3).segmentUse(4).segmentSkip(1);
    });
  }

  public void testOneSingleTermTwoMultiTerms() throws Exception {
    searchAndCheckResults(field(0), 100, "t*", "utopia", "e*");
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(2).segmentUse(4).segmentSkip(1);

    searchAndCheckResults(field(0), 100, "t?e", "utopia", "e*");
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(2).segmentUse(4).segmentSkip(1);

    searchAndCheckResults(field(0), 100, "t?a", "utopia", "e*");
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(1).segmentUse(3).segmentSkip(2);
  }

  public void testTermDoesNotMatch() throws Exception {
    searchAndCheckResults(field(0), 100, "nomatch", "e*");
    // We expect that createWeight() is not called because the first term does
    // not match so the query is early stopped without multi-term expansion.
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(0).segmentUse(2).segmentSkip(2);

    searchAndCheckResults(field(0), 100, "t*", "nomatch", "e*");
    assertCounters().singleTermAnalysis(1).multiTermAnalysis(0).segmentUse(2).segmentSkip(2);
  }

  public void testNoMultiTerm() throws Exception {
    searchAndCheckResults(field(0), 100, "the", "utopia");
    searchAndCheckResults(field(0), 100, "utopia", "the");
    searchAndCheckResults(field(0), 100, "the", "experiment");
  }

  public void testMaxExpansions() throws Exception {
    // The limit on the number of expansions is different with PhraseWildcardQuery
    // because it applies to each segments individually, and not globally unlike
    // MultiPhraseQuery and SpanMultiTermQueryWrapper.
    // Here we verify the total number of expansions directly from test stats
    // inside PhraseWildcardQuery.

    assertCounters().clear();
    searcher.search(phraseWildcardQuery(field(1), 3, 0, true, "e*", "b*"), MAX_DOCS);
    // We expect 3 expansions even if both multi-terms have potentially more expansions.
    assertCounters().expansion(3);

    assertCounters().clear();
    searcher.search(phraseWildcardQuery(field(0), 4, 0, true, "t?e", "utopia", "e*"), MAX_DOCS);
    // We expect 2 expansions since the "utopia" term matches only in the
    // first segment, so there is no expansion for the second segment.
    assertCounters().expansion(2);
  }

  public void testSegmentOptimizationSingleField() throws Exception {
    searchAndCheckResults(field(0), 100, 0, true, "b*", "e*");
    // Both multi-terms are present in both segments.
    // So expecting 4 segment accesses.
    assertCounters().multiTermAnalysis(2).segmentUse(4).segmentSkip(0).queryEarlyStop(0);

    searchAndCheckResults(field(0), 100, 0, true, "t?e", "b*", "e*");
    // "t?e" matches only in the first segment. This term adds 2 segment accesses and 1 segment skip.
    // The other multi-terms match in the first segment. Each one adds 1 segment access.
    // So expecting 3 segment accesses and 1 segment skips.
    assertCounters().multiTermAnalysis(3).segmentUse(4).segmentSkip(1).queryEarlyStop(0);

    searchAndCheckResults(field(0), 100, 0, true, "t?e", "blind", "e*");
    assertCounters().multiTermAnalysis(1).segmentUse(3).segmentSkip(2).queryEarlyStop(1);

    expectDifferentScoreForSpanNearQueryWithMultiTermSubset(() -> {
      searchAndCheckResults(field(2), 100, 0, true, "tim*", "t*");
      assertCounters().multiTermAnalysis(2).segmentUse(2).segmentSkip(1).queryEarlyStop(0);
    });
  }

  public void testMultiplePhraseWildcards() throws Exception {
    searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
        new String[]{"e*", "b*"},
        new String[]{"t?e", "utopia"}
    });
    searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
        new String[]{"e*", "b*"},
        new String[]{"d*", "b*"}
    });
    searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
        new String[]{"e*", "b*"},
        new String[]{"t?e", "utopia"},
        new String[]{"d*", "b*"}
    });
    expectDifferentScoreForSpanNearQueryWithMultiTermSubset(() ->
        searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
            new String[]{"e*", "b*"},
            new String[]{"b*", "b*"}
        }));
    expectDifferentScoreForSpanNearQueryWithMultiTermSubset(() ->
        searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
            new String[]{"e*", "b*"},
            new String[]{"b*", "b*"},
            new String[]{"t?e", "utopia"}
        }));
    searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
        new String[]{"e*", "b*"},
        new String[]{"e*", "b*"}
    });
    searchAndCheckResultsMultiplePhraseWildcards(new String[]{field(1), field(0), field(3)}, 100, 0, new String[][]{
        new String[]{"e*", "b*"},
        new String[]{"t?e", "utopia"},
        new String[]{"e*", "b*"}
    });
  }

  public void testToString() {
    Query testQuery = phraseWildcardQuery(field(0), 100, 0, true, "t?e", "b*", "e*");
    assertEquals("phraseWildcard(title:\"t?e b* e*\")", testQuery.toString());

    testQuery = phraseWildcardQuery(field(0), 100, 1, true, "t?e", "utopia", "e*");
    assertEquals("phraseWildcard(\"t?e utopia e*\"~1)", testQuery.toString(field(0)));

    testQuery = phraseWildcardQuery(field(0), 100, 1, true, "t?e", "b*", "b*");
    assertEquals("phraseWildcard(\"t?e b* b*\"~1)", testQuery.toString(field(0)));
  }

  public void testExplain() throws IOException {
    Query testQuery = phraseWildcardQuery(field(0), 100, 0, true, "t?e", "b*", "b*");

    // Verify the standard way to get the query explanation.
    for (ScoreDoc scoreDoc : searcher.search(testQuery, MAX_DOCS).scoreDocs) {
      Explanation explanation = searcher.explain(testQuery, scoreDoc.doc);
      assertTrue(explanation.getValue().doubleValue() > 0);
      assertTrue("Unexpected explanation \"" + explanation.getDescription() + "\"",
          explanation.getDescription().startsWith("weight(phraseWildcard(title:\"t?e b* b*\")"));
    }

    // Verify that if we call PhraseWildcardQuery.PhraseWildcardWeight.scorer() twice,
    // the scoring is correct (even if it is not the standard path expected by the scorer() method).
    int resultCount = 0;
    Weight weight = testQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1);
    for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(leafReaderContext);
      if (scorer != null) {
        DocIdSetIterator iterator = scorer.iterator();
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          resultCount++;
        }
      }
    }
    assertEquals(1, resultCount);

    int explanationWithNonNullScoreCount = 0;
    for (LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
      Explanation explanation = weight.explain(leafReaderContext, 1);
      if (explanation.getValue().doubleValue() > 0) {
        explanationWithNonNullScoreCount++;
      }
    }
    assertEquals(1, explanationWithNonNullScoreCount);
  }

  /**
   * With two similar multi-terms which expansions are subsets (e.g. "tim*" and "t*"),
   * we expect {@link PhraseWildcardQuery} and {@link MultiPhraseQuery} to
   * have the same scores, but {@link SpanNearQuery} scores are different.
   */
  protected void expectDifferentScoreForSpanNearQueryWithMultiTermSubset(RunnableWithIOException runnable) throws IOException {
    try {
      differentScoreExpectedForSpanNearQuery = true;
      runnable.run();
    } finally {
      differentScoreExpectedForSpanNearQuery = false;
    }
  }

  /**
   * Compares {@link PhraseWildcardQuery} to both {@link MultiPhraseQuery}
   * and {@link SpanNearQuery}.
   */
  protected void searchAndCheckResults(String field, int maxExpansions, String... terms) throws IOException {
    for (int slop = 0; slop <= 1; slop++) {
      searchAndCheckResults(field, maxExpansions, slop, false, terms);
      searchAndCheckResults(field, maxExpansions, slop, true, terms);
    }
  }

  protected void searchAndCheckResults(String field, int maxExpansions, int slop,
                                       boolean segmentOptimizationEnabled, String... terms) throws IOException {
    searchAndCheckSameResults(
        phraseWildcardQuery(field, maxExpansions, slop, segmentOptimizationEnabled, terms),
        multiPhraseQuery(field, maxExpansions, slop, terms),
        spanNearQuery(field, slop, terms),
        segmentOptimizationEnabled);
  }

  protected void searchAndCheckResultsMultiplePhraseWildcards(String[] fields, int maxExpansions,
                                                              int slop, String[][] multiPhraseTerms) throws IOException {
    searchAndCheckResultsMultiplePhraseWildcards(fields, maxExpansions, slop, false, multiPhraseTerms);
    searchAndCheckResultsMultiplePhraseWildcards(fields, maxExpansions, slop, true, multiPhraseTerms);
  }

  protected void searchAndCheckResultsMultiplePhraseWildcards(String[] fields, int maxExpansions, int slop,
                                                              boolean segmentOptimizationEnabled, String[][] multiPhraseTerms) throws IOException {
    BooleanQuery.Builder phraseWildcardQueryBuilder = new BooleanQuery.Builder();
    BooleanQuery.Builder multiPhraseQueryBuilder = new BooleanQuery.Builder();
    BooleanQuery.Builder spanNearQueryBuilder = new BooleanQuery.Builder();
    for (String[] terms : multiPhraseTerms) {
      BooleanClause.Occur occur = random().nextBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;
      phraseWildcardQueryBuilder.add(disMaxQuery(phraseWildcardQueries(fields, maxExpansions, slop, segmentOptimizationEnabled, terms)), occur);
      multiPhraseQueryBuilder.add(disMaxQuery(multiPhraseQueries(fields, maxExpansions, slop, terms)), occur);
      spanNearQueryBuilder.add(disMaxQuery(spanNearQueries(fields, slop, terms)), occur);
    }
    searchAndCheckSameResults(
        phraseWildcardQueryBuilder.build(),
        multiPhraseQueryBuilder.build(),
        spanNearQueryBuilder.build(),
        segmentOptimizationEnabled
    );
  }

  protected Query disMaxQuery(Query... disjuncts) {
    return new DisjunctionMaxQuery(Arrays.asList(disjuncts), 0.1f);
  }

  protected Query[] phraseWildcardQueries(String[] fields, int maxExpansions, int slop, boolean segmentOptimizationEnabled, String... terms) {
    Query[] queries = new Query[fields.length];
    for (int i = 0; i < fields.length; i++) {
      queries[i] = phraseWildcardQuery(fields[i], maxExpansions, slop, segmentOptimizationEnabled, terms);
    }
    return queries;
  }

  protected Query[] multiPhraseQueries(String[] fields, int maxExpansions, int slop, String... terms) throws IOException {
    Query[] queries = new Query[fields.length];
    for (int i = 0; i < fields.length; i++) {
      queries[i] = multiPhraseQuery(fields[i], maxExpansions, slop, terms);
    }
    return queries;
  }

  protected Query[] spanNearQueries(String[] fields, int slop, String... terms) {
    Query[] queries = new Query[fields.length];
    for (int i = 0; i < fields.length; i++) {
      queries[i] = spanNearQuery(fields[i], slop, terms);
    }
    return queries;
  }

  protected void searchAndCheckSameResults(Query testQuery, Query multiPhraseQuery, Query spanNearQuery, boolean segmentOptimizationEnabled) throws IOException {
    // Search and compare results with MultiPhraseQuery.
    // Do not compare the scores if the segment optimization is enabled because
    // it changes the score (but not the result ranking).
    boolean sameScoreExpected = !segmentOptimizationEnabled;
    searchAndCheckSameResults(testQuery, multiPhraseQuery, sameScoreExpected);

    // Clear the test stats to verify them only with the last test query execution.
    assertCounters().clear();
    // Search and compare results with SpanNearQuery.
    sameScoreExpected = !segmentOptimizationEnabled && !differentScoreExpectedForSpanNearQuery;
    searchAndCheckSameResults(testQuery, spanNearQuery, sameScoreExpected);
  }

  protected void searchAndCheckSameResults(Query testQuery, Query referenceQuery,
                                           boolean compareScores) throws IOException {
    ScoreDoc[] testResults = searcher.search(testQuery, MAX_DOCS).scoreDocs;
    ScoreDoc[] referenceResults = searcher.search(referenceQuery, MAX_DOCS).scoreDocs;
    assertEquals("Number of results differ when comparing to " + referenceQuery.getClass().getSimpleName(),
        referenceResults.length, testResults.length);
    if (compareScores) {
      for (int i = 0; i < testResults.length; i++) {
        ScoreDoc testResult = testResults[i];
        ScoreDoc referenceResult = referenceResults[i];
        assertTrue("Result " + i + " differ when comparing to " + referenceQuery.getClass().getSimpleName()
                + "\ntestResults=" + Arrays.toString(testResults) + "\nreferenceResults=" + Arrays.toString(referenceResults),
            equals(testResult, referenceResult));
      }
    } else {
      Set<Integer> testResultDocIds = Arrays.stream(testResults).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toSet());
      Set<Integer> referenceResultDocIds = Arrays.stream(referenceResults).map(scoreDoc -> scoreDoc.doc).collect(Collectors.toSet());
      assertEquals("Results differ when comparing to " + referenceQuery.getClass().getSimpleName()
              + " ignoring score\ntestResults=" + Arrays.toString(testResults) + "\nreferenceResults=" + Arrays.toString(referenceResults),
          referenceResultDocIds, testResultDocIds);
    }
  }

  protected PhraseWildcardQuery phraseWildcardQuery(String field, int maxExpansions,
                                                    int slop, boolean segmentOptimizationEnabled, String... terms) {
    PhraseWildcardQuery.Builder builder = createPhraseWildcardQueryBuilder(field, maxExpansions, segmentOptimizationEnabled)
        .setSlop(slop);
    for (String term : terms) {
      if (term.contains("*") || term.contains("?")) {
        builder.addMultiTerm(new WildcardQuery(new Term(field, term)));
      } else {
        builder.addTerm(new BytesRef(term));
      }
    }
    return builder.build();
  }

  protected PhraseWildcardQuery.Builder createPhraseWildcardQueryBuilder(
      String field, int maxExpansions, boolean segmentOptimizationEnabled) {
    return new PhraseWildcardQuery.Builder(field, maxExpansions, segmentOptimizationEnabled);
  }

  protected SpanNearQuery spanNearQuery(String field, int slop, String... terms) {
    SpanQuery[] spanQueries = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      spanQueries[i] = term.contains("*") || term.contains("?") ?
          new SpanMultiTermQueryWrapper<>(new WildcardQuery(new Term(field, term)))
          : new SpanTermQuery(new Term(field, term));
    }
    return new SpanNearQuery(spanQueries, slop, true);
  }

  protected MultiPhraseQuery multiPhraseQuery(String field, int maxExpansions, int slop, String... terms) throws IOException {
    MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder()
        .setSlop(slop);
    for (String term : terms) {
      if (term.contains("*") || term.contains("?")) {
        Term[] expansions = expandMultiTerm(field, term, maxExpansions);
        if (expansions.length > 0) {
          builder.add(expansions);
        } else {
          builder.add(new Term(field, "non-matching-term"));
        }
      } else {
        builder.add(new Term(field, term));
      }
    }
    return builder.build();
  }

  protected Term[] expandMultiTerm(String field, String term, int maxExpansions) throws IOException {
    if (maxExpansions == 0) {
      return new Term[0];
    }
    Set<Term> expansions = new HashSet<>();
    WildcardQuery wq = new WildcardQuery(new Term(field, term));
    expansion:
    for (final LeafReaderContext ctx : reader.leaves()) {
      Terms terms = ctx.reader().terms(field);
      if (terms != null) {
        TermsEnum termsEnum = wq.getTermsEnum(terms);
        while (termsEnum.next() != null) {
          expansions.add(new Term(field, termsEnum.term()));
          if (expansions.size() >= maxExpansions) {
            break expansion;
          }
        }
      }
    }
    return expansions.toArray(new Term[0]);
  }

  protected static boolean equals(ScoreDoc result1, ScoreDoc result2) {
    // Due to randomness, the value of the score comparison epsilon varies much.
    // We take 1E-1 epsilon to ensure the test do not flap.
    return result1.doc == result2.doc && (Math.abs(result1.score - result2.score) < 1E-1);
  }

  protected void addSegments(RandomIndexWriter iw) throws IOException {
    // First segment.
    addDocs(iw,
        doc(
            field(field(0), "time conversion"),
            field(field(1), "eric hawk"),
            field(field(2), "time travel")
        ),
        doc(
            field(field(0), "the blinking books"),
            field(field(1), "donald ever"),
            field(field(2), "time travel")
        ),
        doc(
            field(field(0), "the utopia experiment"),
            field(field(1), "dylan brief"),
            field(field(2), "utopia"),
            field(field(3), "travelling to utopiapolis")
        )
    );
    iw.commit();

    // Second segment.
    // No field(2).
    addDocs(iw,
        doc(
            field(field(0), "serene evasion"),
            field(field(1), "eric brown")
        ),
        doc(
            field(field(0), "my blind experiment"),
            field(field(1), "eric bright")
        ),
        doc(
            field(field(3), "two times travel")
        )
    );
    iw.commit();
  }

  protected String field(int index) {
    return FIELDS[index];
  }

  protected static void addDocs(RandomIndexWriter iw, Document... docs) throws IOException {
    iw.addDocuments(Arrays.asList(docs));
  }

  protected static Document doc(Field... fields) {
    Document doc = new Document();
    for (Field field : fields) {
      doc.add(field);
    }
    return doc;
  }

  protected static Field field(String field, String fieldValue) {
    return newTextField(field, fieldValue, Field.Store.NO);
  }

  private interface RunnableWithIOException {

    void run() throws IOException;
  }

  AssertCounters assertCounters() {
    // Expected values for test counters are defined for 2 segments.
    // Only verify test counters if the number of segments is 2 as expected.
    // If the randomization produced a different number of segments,
    // then just ignore test counters.
    return reader.leaves().size() == 2 ? new AssertCounters() : AssertCounters.NO_OP;
  }

  /**
   * Fluent API to assert {@link TestCounters}.
   */
  static class AssertCounters {

    static final AssertCounters NO_OP = new AssertCounters() {
      AssertCounters singleTermAnalysis(int c) {return this;}
      AssertCounters multiTermAnalysis(int c) {return this;}
      AssertCounters segmentUse(int c) {return this;}
      AssertCounters segmentSkip(int c) {return this;}
      AssertCounters queryEarlyStop(int c) {return this;}
      AssertCounters expansion(int c) {return this;}
    };

    AssertCounters singleTermAnalysis(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().singleTermAnalysisCount);
      return this;
    }

    AssertCounters multiTermAnalysis(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().multiTermAnalysisCount);
      return this;
    }

    AssertCounters segmentUse(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().segmentUseCount);
      return this;
    }

    AssertCounters segmentSkip(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().segmentSkipCount);
      return this;
    }

    AssertCounters queryEarlyStop(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().queryEarlyStopCount);
      return this;
    }

    AssertCounters expansion(int expectedCount) {
      assertEquals(expectedCount, TestCounters.get().expansionCount);
      return this;
    }

    AssertCounters clear() {
      TestCounters.get().clear();
      return this;
    }
  }
}
